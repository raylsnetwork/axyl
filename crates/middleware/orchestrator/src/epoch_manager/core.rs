//! Epoch manager.
//! Oversees per-epoch tasks and shared cross-epoch resources.

#[cfg(feature = "cold-storage")]
use super::cold_archive::ColdArchival;
use crate::{
    engine::{ExecutionNode, RaylsBuilder},
    epoch_manager::{
        state::hydrate_prev_epoch_record,
        types::{EpochManager, ENGINE_TASK_MANAGER, EPOCH_TASK_MANAGER, NODE_TASK_MANAGER},
        utils::{catchup_accumulator, recover_executed_anchor},
        vote_triage::{
            backfill_candidate, classify_fetched_record, split_settled_votes, triage_vote,
            ForeignVote, QueuedVote, VoteAction,
        },
    },
    primary::PrimaryNode,
    types::{HealthcheckServer, InitialBatchSeq, RunningOutcome, TransitionCtx},
    worker::worker_task_manager_name,
};
use consensus_metrics::start_prometheus_server;
use eyre::eyre;
use futures::FutureExt;
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus, NodeMode, QueChannel};
use rayls_consensus_state_sync::{
    epoch_committee_valid, epoch_record_valid, spawn_epoch_record_collector,
};
use rayls_consensus_worker::{quorum_waiter::QuorumWaiterTrait, Worker};
use rayls_execution_evm::{reth_env::RethEnv, system_calls::EpochState};
use rayls_infrastructure_config::{KeyConfig, LibP2pConfig, NetworkConfig, RaylsDirs};
use rayls_infrastructure_storage::{
    tables::ConsensusBlocks, EpochStore as _, PENDING_RECORD_LOG_TARGET,
};
use rayls_infrastructure_types::{
    error::HeaderError, gas_accumulator::GasAccumulator, B256Map, BlsAggregateSignature,
    BlsPublicKey, BlsSignature, CameFrom, ConsensusOutput, Database as ReDatabase, Epoch,
    EpochCertificate, EpochRecord, EpochVote, Noticer, Notifier, RaylsReceiver, RaylsSender,
    TaskJoinError, TaskKind, TaskManager, VotesAggregator, B256,
};
use rayls_middleware_processor::{batch::BatchOrdering, reconstruct_batch_digests};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    panic::AssertUnwindSafe,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, error, info, warn};

/// Wall-clock budget for fetching an epoch record from peers after a failed vote collection.
///
/// The active validators certify the record a fraction of a second after this node detects the
/// boundary, so a single round of requests regularly arrives too early. Retrying for half a
/// minute costs nothing on a healthy network and removes the one-epoch wait that follows a
/// failed fetch.
const EPOCH_FETCH_RETRY_BUDGET: Duration = Duration::from_secs(30);

/// Delay between the fetch attempts made inside [`EPOCH_FETCH_RETRY_BUDGET`].
const EPOCH_FETCH_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Initial backoff between full certification attempts (publish vote, collect quorum, fetch from
/// a peer) once one attempt has exhausted [`EPOCH_FETCH_RETRY_BUDGET`] without success.
///
/// Unlike the fetch retry above (which only helps once *some* peer has already certified the
/// record), this covers the case where nobody has - a genuine network-wide stall, not a slow
/// peer (#142). There is no bound on the number of attempts: giving up here means the record is
/// never persisted anywhere, permanently, since the committee members' own votes are the only
/// thing that can ever complete this specific epoch's certification.
const CERTIFICATION_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// Ceiling for the certification backoff. The backoff doubles per attempt up to this, so an
/// epoch whose committee can never reach quorum again (its members left for good) costs a few
/// requests per hour instead of a hundred every two minutes, while the retry itself never stops.
const MAX_CERTIFICATION_RETRY_BACKOFF: Duration = Duration::from_secs(15 * 60);

/// Backoff before certification attempt `attempt + 1`, after `attempt` (1-based) failed:
/// [`CERTIFICATION_RETRY_BACKOFF`] doubled per failed attempt, capped at
/// [`MAX_CERTIFICATION_RETRY_BACKOFF`].
pub(crate) fn certification_retry_backoff(attempt: u32) -> Duration {
    let doubled = CERTIFICATION_RETRY_BACKOFF
        .checked_mul(1u32 << attempt.saturating_sub(1).min(31))
        .unwrap_or(MAX_CERTIFICATION_RETRY_BACKOFF);
    doubled.min(MAX_CERTIFICATION_RETRY_BACKOFF)
}

/// Time allowed for placing one unknown epoch record digest seen in a vote.
const DIGEST_RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

/// How many unknown digests one collection may ask peers about. Beyond this, unknown digests take
/// the alternate-record path (bounded by `MAX_ALT_RECS`), so a peer flooding fresh digests cannot
/// turn a collection into an unbounded series of network requests.
const MAX_DIGEST_RESOLUTIONS: usize = 8;

/// Vote-collection state for one pending (uncertified) epoch record inside the single
/// "Collect Epoch Signatures" task.
///
/// Signatures accumulate for the lifetime of the task: a committee signature over this digest
/// never expires, so votes gathered in one attempt still count in the next. Only the task's
/// end (all done, or the epoch task manager draining it) discards them; the next `run_epoch`
/// starts again from the node's own vote.
struct PendingCertification {
    record: EpochRecord,
    epoch_hash: B256,
    committee: Vec<BlsPublicKey>,
    committee_index: HashMap<BlsPublicKey, usize>,
    quorum: u64,
    my_vote: Option<EpochVote>,
    /// Committee members whose vote has not been counted yet.
    committee_keys: HashSet<BlsPublicKey>,
    sigs: Vec<BlsSignature>,
    signed_authorities: roaring::RoaringBitmap,
    /// Certified and saved (by us or by someone else): nothing left to collect for it.
    done: bool,
}

impl PendingCertification {
    fn new(record: EpochRecord) -> Self {
        let epoch_hash = record.digest();
        let committee = record.committee.clone();
        let committee_index = committee.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        let committee_keys: HashSet<BlsPublicKey> = committee.iter().copied().collect();
        Self {
            epoch_hash,
            quorum: record.super_quorum() as u64,
            record,
            committee,
            committee_index,
            my_vote: None,
            committee_keys,
            sigs: Vec::new(),
            signed_authorities: roaring::RoaringBitmap::new(),
            done: false,
        }
    }

    /// Sign the record as committee member `me` and count that vote.
    fn sign(&mut self, me: &BlsPublicKey, key_config: &KeyConfig) -> EpochVote {
        let vote = self.record.sign_vote(key_config);
        if self.committee_keys.remove(me) {
            self.sigs.push(vote.signature);
            if let Some(idx) = self.committee_index.get(me) {
                self.signed_authorities.insert(*idx as u32);
            }
        }
        self.my_vote = Some(vote);
        vote
    }

    /// Count a committee vote for this record; a repeat from the same signer is ignored.
    fn count(&mut self, vote: &EpochVote) {
        if !self.committee_keys.remove(&vote.public_key) {
            return;
        }
        self.sigs.push(vote.signature);
        if let Some(idx) = self.committee_index.get(&vote.public_key) {
            self.signed_authorities.insert(*idx as u32);
        }
    }

    fn reached_quorum(&self) -> bool {
        self.signed_authorities.len() >= self.quorum
    }

    /// Aggregate the collected signatures into a certificate that verifies against the record.
    fn certificate(&self) -> Option<EpochCertificate> {
        let aggregated = BlsAggregateSignature::aggregate(&self.sigs[..], true).ok()?;
        let cert = EpochCertificate {
            epoch_hash: self.epoch_hash,
            signature: aggregated.to_signature(),
            signed_authorities: self.signed_authorities.clone(),
        };
        self.record.verify_with_cert(&cert).then_some(cert)
    }
}

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Create a new instance of [Self].
    pub(crate) fn new(
        builder: RaylsBuilder,
        rayls_datadir: P,
        passphrase: String,
        consensus_db: DB,
        // Fired once if the consensus DB rejects a hot-tier write or its durability barrier (by
        // the DB's own writer, or cold archival): the node winds down gracefully (keeping the
        // durable state) instead of panicking.
        fatal_db_error: watch::Sender<Option<String>>,
        #[cfg(feature = "cold-storage")] cold_archival: ColdArchival,
    ) -> eyre::Result<Self> {
        // create key config for lifetime of the app
        let key_config = KeyConfig::read_config(&rayls_datadir, passphrase)?;

        // shutdown long-running node components
        let node_shutdown = Notifier::new();
        // seed mode from identity config; identify_node_mode promotes later if in committee
        let initial_mode = if builder.rayls_infrastructure_config.observer {
            NodeMode::Observer
        } else {
            NodeMode::CvvInactive
        };
        let consensus_bus = ConsensusBus::new_with_args(
            initial_mode,
            builder.rayls_infrastructure_config.parameters.gc_depth,
        );
        let worker_event_stream = QueChannel::new();

        // create dbs to survive between sync state transitions
        let reth_db = RethEnv::new_database(&builder.node_config, rayls_datadir.reth_db_path())?;

        // The previous process may have closed an epoch that is not certified yet; its record
        // survives in PendingEpochRecord and is what prev_epoch_record would hold had we not
        // restarted (#142).
        let prev_epoch_record = hydrate_prev_epoch_record(&consensus_db);

        Ok(Self {
            builder,
            rayls_datadir,
            primary_network_handle: None,
            worker_network_handle: None,
            key_config,
            node_shutdown,
            sigterm_trigger: Notifier::new(),
            fatal_db_error,
            reth_db,
            consensus_db,
            consensus_bus,
            worker_event_stream,
            epoch_record: None,
            prev_epoch_record,
            initial_epoch: true,
            #[cfg(feature = "cold-storage")]
            cold_archival,
        })
    }

    /// Run the node, handling epoch transitions.
    pub(crate) async fn run(&mut self) -> eyre::Result<()> {
        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);
        let node_task_spawner = node_task_manager.get_spawner();

        // Bind the healthcheck + readiness probe before any boot-time cold work (crash reconcile +
        // backlog migration). Both are synchronous and can be multi-minute on a large DB; binding
        // the probe first keeps liveness answering throughout (the migration is `spawn_blocking`'d
        // so the runtime stays free), so a short-deadline liveness probe cannot blackout and
        // restart-loop the node during recovery. Readiness (`/readyz`) correctly reports
        // not-ready for the whole boot sequence: `node_mode` starts at its constructor-seeded
        // value (`CvvInactive`/`Observer`) and only becomes ready once consensus promotes it.
        // Propagate a bind failure (e.g. the port is already in use) rather than silently
        // starting the node without the endpoint an operator explicitly asked for.
        if let Some(port) = self.builder.healthcheck {
            HealthcheckServer::spawn(
                node_task_manager.get_spawner(),
                port,
                self.consensus_bus.node_mode().subscribe(),
            )
            .await?;
        }

        // Heal any crash-interrupted archive before serving, while consensus and execution have not
        // started so it cannot race the live path. Cheap: it touches only sealed-but-unreconciled
        // epochs. The bulk first-start backlog migration is deferred until the EL execution anchor
        // is known (below), so it can floor the cutoff by it. Steady-state archival runs in the
        // background seal actor spawned further down.
        // Best-effort on the node path: a failed reconcile is retried at the next boot.
        #[cfg(feature = "cold-storage")]
        if let Err(e) = self.cold_archival.reconcile_at_boot().await {
            warn!(target: "epoch-manager", "cold reconcile failed: {e}");
        }

        info!(target: "epoch-manager", "starting node and launching first epoch");

        // create submanager for engine tasks
        let engine_task_manager = TaskManager::new(ENGINE_TASK_MANAGER);

        // create channels for engine that survive the lifetime of the node
        let (to_engine, for_engine) = mpsc::channel(1000);

        // Create our epoch gas accumulator, we currently have one worker.
        // All nodes have to agree on the worker count, do not change this for an existing chain.
        let rewards_counter = rayls_middleware_rewards::from_db(self.consensus_db.clone());
        let gas_accumulator = GasAccumulator::with_rewards(1, rewards_counter);

        // create the engine
        let engine = self.create_engine(&engine_task_manager, &gas_accumulator).await?;

        // retrieve epoch information from canonical tip on startup
        let EpochState { epoch, .. } = engine.epoch_state_from_canonical_tip().await?;

        // load persisted BatchOrdering or reconstruct from chain history when missing
        let execution_address = self.builder.rayls_infrastructure_config.execution_address();
        let batch_ordering =
            BatchOrdering::from_history(self.consensus_db.clone(), epoch, execution_address);

        // The engine's dedup anchor MUST be the EL execution anchor (the consensus header the
        // highest executed EVM block commits to), NOT the consensus-chain tip. The anchor marks the
        // last output already executed into a block; catch-up replay then re-feeds every committed-
        // but-unexecuted output ABOVE it (e.g. outputs lost to a crash before execution), which the
        // engine has to ADMIT and re-execute. Seeding at the consensus tip would place the anchor
        // at-or-above those replayed outputs, so the engine would drop them (Less/Equal)
        // and the subscriber's replay loop would stall forever waiting for an execution
        // signal that never fires. (Proposer header numbering is seeded separately from the
        // consensus tip in `get_last_executed_consensus`.)
        //
        // Startup runs before the engine starts, so the tip is frozen: the anchor recovery and
        // the dedup-registry reconstruction below read the same chain state. See
        // `recover_executed_anchor` for why the anchor is the max-nonce recent block, not the tip.
        let reth_env = engine.get_reth_env().await;

        let last_execution_block = recover_executed_anchor(&reth_env, &self.consensus_db)?;

        let last_consensus_block =
            self.consensus_db.last_record::<ConsensusBlocks>().map(|(_, header)| header);

        debug!(
            target: "epoch-manager",
            ?last_consensus_block,
            ?last_execution_block,
            "consensus tip vs execution anchor at startup"
        );

        // seed the EVM-execution anchor once at boot; the engine advances it live thereafter.
        self.consensus_bus
            .executed_anchor()
            .send_replace(last_execution_block.clone().unwrap_or_default());

        // reconstruct executed batch digests from recent chain to survive restarts (C-1).
        // Scans from the persisted head (`lookup_head`, the original reconstruction tip); at cold
        // startup this equals the `canonical_tip` used above for the anchor window.
        let executed_batch_registry = reconstruct_batch_digests(
            &reth_env,
            reth_env.lookup_head()?.number,
            &self.consensus_db,
        );

        // First-start backlog migration: archive every epoch safely below the EL execution anchor
        // into cold. Deferred to here (the reconcile above ran earlier) so it reads the seeded
        // executed anchor and never seals an epoch the EL has not executed. Still runs before
        // consensus and execution start, so it does not compete with the live path. Drained in
        // bounded chunks (not one unbounded pass) so a large pre-existing DB never buffers its
        // whole history at once; the probe is already bound so it answers throughout.
        #[cfg(feature = "cold-storage")]
        {
            let el_anchor_epoch =
                self.consensus_bus.executed_anchor().borrow().sub_dag.leader_epoch();
            // Best-effort on the node path: a failed chunk resumes at the next boundary or boot.
            if let Err(e) = self.cold_archival.migrate_backlog(el_anchor_epoch).await {
                warn!(target: "epoch-manager", "cold boot backlog migration failed: {e}");
            }

            // Steady-state archival: the actor fully archives each newly finalized epoch in the
            // background during the live epoch; nothing archival runs on the epoch transition.
            // `node_shutdown` doubles as the actor's chunk-seam cancel flag, so teardown never
            // waits on a seal.
            self.cold_archival.spawn_actor(
                &node_task_manager,
                self.consensus_bus.executed_anchor().subscribe(),
                self.node_shutdown.clone(),
                self.fatal_db_error.clone(),
            );
        }

        // Fires once the engine task has fully drained (its last block executed),
        // so the shutdown flush below runs *after* the final block, not before it.
        let (engine_done_tx, engine_done_rx) = oneshot::channel::<()>();

        engine
            .start_engine(
                for_engine,
                self.node_shutdown.subscribe(),
                gas_accumulator.clone(),
                Some(self.consensus_bus.batch_tracker().clone()),
                batch_ordering,
                Some(self.consensus_bus.executed_anchor().clone()),
                Some(self.consensus_bus.engine_idle().clone()),
                last_execution_block.unwrap_or_default(),
                engine_done_tx,
                executed_batch_registry,
            )
            .await?;
        debug!(target: "epoch-manager", ?epoch, "retrieved epoch state from canonical tip");
        catchup_accumulator(&self.consensus_db, engine.get_reth_env().await, &gas_accumulator)?;

        // read the network config or use the default
        let network_config = NetworkConfig::read_config(&self.rayls_datadir)?;
        self.spawn_node_networks(
            node_task_spawner,
            &network_config,
            self.consensus_bus.network_metrics(),
        )?;
        let primary_network_handle =
            self.primary_network_handle.as_ref().expect("primary network").clone();
        let epoch_vote_topic = LibP2pConfig::epoch_vote_topic();
        let consensus_output_topic = LibP2pConfig::consensus_output_topic();
        info!(target: "epoch-manager::gossipsub", ?epoch_vote_topic, ?consensus_output_topic, "subscribing to node-level gossipsub topics");

        primary_network_handle.inner_handle().subscribe(epoch_vote_topic.clone()).await?;
        primary_network_handle.inner_handle().subscribe(consensus_output_topic.clone()).await?;

        // log mesh state after subscribing to node-level topics
        let connected = primary_network_handle.connected_peers_count().await.unwrap_or(0);
        let mesh_consensus = primary_network_handle
            .inner_handle()
            .mesh_peers(consensus_output_topic.clone())
            .await
            .map(|p| p.len())
            .unwrap_or(0);
        let mesh_epoch_vote = primary_network_handle
            .inner_handle()
            .mesh_peers(epoch_vote_topic)
            .await
            .map(|p| p.len())
            .unwrap_or(0);
        info!(
            target: "epoch-manager::gossipsub",
            connected,
            mesh_consensus,
            mesh_epoch_vote,
            "node-level gossipsub subscriptions complete"
        );

        spawn_epoch_record_collector(
            self.consensus_db.clone(),
            primary_network_handle,
            self.consensus_bus.clone(),
            node_task_manager.get_spawner(),
            self.node_shutdown.subscribe(),
        )
        .await?;
        // start consensus metrics for the epoch
        let metrics_shutdown = Notifier::new();
        if let Some(metrics_socket) = self.builder.metrics {
            start_prometheus_server(
                metrics_socket,
                &node_task_manager,
                metrics_shutdown.subscribe(),
            );
        }

        // node-scoped (not epoch-scoped): engine drains queued outputs past epoch shutdown.
        // epoch-scoped death would leave recently_executed_blocks stale and re-replay executed
        // outputs.
        self.spawn_engine_update_task(
            self.node_shutdown.subscribe(),
            engine.canonical_block_stream().await,
            engine.clone(),
            &node_task_manager,
        );

        // add engine task manager
        node_task_manager.add_task_manager(engine_task_manager);
        node_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS\n");

        // Catch the termination signal ourselves so we can drive a graceful, ORDERED
        // shutdown - rather than letting a task-manager join catch it (which would also fire
        // its notifier and tear node tasks down). On SIGTERM/ctrl-c this listener fires ONLY
        // `sigterm_trigger`, never `node_shutdown`, so the tasks that subscribe to
        // `node_shutdown` directly (engine, network, vote collector) are NOT woken
        // concurrently with the ordered epoch teardown. The engine stays alive THROUGH the
        // epoch teardown (the subscriber flushes to a live engine); `node_shutdown` is fired
        // only afterward (below), for the engine drain barrier + node-level teardown.
        node_task_manager.get_spawner().spawn_task("shutdown-signal", {
            let sigterm_trigger = self.sigterm_trigger.clone();
            async move {
                TaskManager::exit().await;
                info!(target: "epoch-manager", "termination signal received; winding down");
                sigterm_trigger.notify();
            }
        });

        // wrap the select in catch_unwind so the explicit flush below runs even if run_epochs
        // panics; the panic is re-raised after the flush.
        //
        // We do NOT cancel `run_epochs` on shutdown: it observes `sigterm_trigger` and winds
        // the current epoch down through the ordered `controlled_shutdown` (producers reaped
        // before consumers). The only thing raced against it here is a node-task CRASH.
        let node_shutdown = self.node_shutdown.clone();
        let sigterm_trigger = self.sigterm_trigger.clone();
        // Keep one `to_engine` sender alive past `run_epochs` so the engine's input does NOT close
        // until we say so. `run_epochs` owns the other senders, so its return would close the input
        // BEFORE `node_shutdown.notify()` below sets the engine's `shutdown_requested` - and the
        // engine faults (`ConsensusOutputStreamClosed`) on an input close while that flag is false
        // (a TOCTOU on its shutdown check). We drop this only AFTER `notify()`, making notify()
        // strictly happen-before the close: the engine polls rx_shutdown before its input, so it
        // sees shutdown_requested=true and exits Ok. Cheap mpsc clone, held for run()'s lifetime.
        let engine_input_keepalive = to_engine.clone();
        // A consensus-DB write error is fatal WITHOUT a panic: the durable state is the last
        // known valid data, and the mem cache plus any uncommitted mdbx transaction are safe
        // to lose. The select arm below requests the same ordered wind-down the crash path
        // uses, then surfaces the error (non-zero exit) so a supervisor can restart and
        // rebuild the cache.
        let fatal_db_error = self.fatal_db_error.subscribe();
        let outcome = AssertUnwindSafe(async {
            let epochs = self.run_epochs(&engine, network_config, to_engine, gas_accumulator);
            tokio::pin!(epochs);
            // `join` (do_exit=false) so this does NOT catch the termination signal - only a
            // node-task crash (or `node_shutdown`) completes it. SIGTERM is handled by the
            // listener above; on SIGTERM this stays pending and is dropped via the `epochs`
            // branch, leaving node tasks (the engine) running until the post-teardown
            // `node_shutdown` below.
            let node_join = node_task_manager.join(node_shutdown.clone());
            tokio::pin!(node_join);

            let fatal_db_error = fatal_db_error;
            tokio::pin!(fatal_db_error);

            tokio::select! {
                // run_epochs returned on its own: a graceful loop break after sigterm_trigger
                // (SIGTERM/ctrl-c), or an epoch error. Nothing left to wind down.
                epoch_result = &mut epochs => epoch_result,

                // A node task crashed first. Request the graceful epoch wind-down, AWAIT
                // run_epochs so the current epoch tears down ordered, then surface the error.
                node_res = &mut node_join => {
                    sigterm_trigger.notify();
                    let epoch_result = epochs.await;
                    match node_res {
                        Ok(()) => epoch_result,
                        Err(e) => epoch_result.and(Err(eyre!("Node task shutdown: {e}"))),
                    }
                }

                // A hot-tier write/durability failure (cold archival). Same ordered wind-down,
                // surfaced as an error instead of a panic. `fatal_db_error` preserves the first
                // (root-cause) signal via `send_if_modified` in both `db_run::trip` and
                // `cold_archive::seal_due_epochs`, so `borrow()` is the original writer failure,
                // not a secondary archival error.
                _ = fatal_db_error.changed() => {
                    let msg = fatal_db_error.borrow().clone().unwrap_or_default();
                    error!(
                        target: "epoch-manager",
                        "consensus DB write error: {msg}"
                    );
                    sigterm_trigger.notify();
                    let epoch_result = epochs.await;
                    epoch_result.and(Err(eyre!("consensus DB write error: {msg}")))
                }
            }
        })
        .catch_unwind()
        .await;

        // Drain barrier: wait for the engine to finish executing its last block
        // before flushing. The select above can complete via `run_epochs` (which
        // returns fast on shutdown) while the engine task is still draining queued
        // outputs; flushing then persists a prefix and lets a later block land
        // post-flush (the serialize-replay fork). Signal shutdown (idempotent) so
        // the engine begins its drain, then wait - UNBOUNDED - for it to report done.
        // A finite timeout that fired mid-drain (e.g. a large queued backlog, each output
        // up to seconds) would flush a prefix and let a later block land post-flush - the
        // exact serialize-replay fork. The engine is Drainable and exits cleanly on
        // shutdown, so engine_done fires in every case except a genuine execution deadlock;
        // that rare hang is bounded externally by the supervisor's SIGKILL and is fork-safe
        // (the unflushed tail replays deterministically on restart).
        self.node_shutdown.notify();
        // Now that `shutdown_requested` is being set, release the engine's input. The close is
        // observed by the engine only after `notify()` (program order: this drop is after the
        // notify), so it exits Ok via its shutdown path rather than faulting.
        drop(engine_input_keepalive);

        match engine_done_rx.await {
            Ok(()) => info!(target: "engine", "engine drained before shutdown flush"),
            // Sender dropped without signalling: the engine task was torn down before its
            // drain completed (so no in-flight block was finalized) - safe to flush.
            Err(_) => {
                warn!(target: "engine", "engine task ended without drain signal; flushing")
            }
        }

        // Flush both layers, each under catch_unwind so a panic in one still runs the other.
        // Consensus goes first so a crash between them leaves consensus >= execution (replayable).
        // A flush failure here is logged, not faulted: the process is exiting, so the layered
        // cache's failed rows die with it (no unbounded pinning), and consensus >= execution
        // replays the unflushed tail on restart.
        let consensus_flush = AssertUnwindSafe(self.consensus_db.persist()).catch_unwind().await;
        match consensus_flush {
            Ok(Ok(())) => info!(target: "engine", "shutdown consensus DB flush complete"),
            Ok(Err(e)) => error!(target: "engine", ?e, "shutdown consensus DB flush failed"),
            Err(_) => error!(target: "engine", "shutdown consensus DB flush panicked"),
        }
        let engine_flush = AssertUnwindSafe(engine.flush_persistence()).catch_unwind().await;
        match engine_flush {
            Ok(Ok(())) => info!(target: "engine", "shutdown engine flush complete"),
            Ok(Err(e)) => error!(target: "engine", ?e, "shutdown engine flush failed"),
            Err(_) => error!(target: "engine", "shutdown engine flush panicked"),
        }

        metrics_shutdown.notify();

        // Reap the node-level tasks. On SIGTERM the `node_join` above was dropped (epochs
        // branch), so node tasks (engine submanager, network) weren't awaited there; with
        // `node_shutdown` now fired they wind down, and this join awaits their drop instead
        // of leaving it to `Drop`. `node_shutdown` is already notified, so this runs in drain
        // mode (ordered). On the crash path `node_join` already reaped them, so this is a
        // near-empty no-op.
        let _ = node_task_manager.join(self.node_shutdown.clone()).await;

        match outcome {
            Ok(result) => result,
            Err(panic_payload) => std::panic::resume_unwind(panic_payload),
        }
    }

    /// Execute a loop to start new epochs until shutdown.
    async fn run_epochs(
        &mut self,
        engine: &ExecutionNode,
        network_config: NetworkConfig,
        to_engine: mpsc::Sender<(CameFrom, ConsensusOutput)>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<()> {
        // initial_epoch lives on self; cleared at the end of each run_epoch

        let node_ended_sub = self.sigterm_trigger.subscribe();
        let mut mode_transition_rx = self.consensus_bus.mode_transition().subscribe();
        // loop through epochs
        loop {
            let epoch_result = self
                .run_epoch(
                    engine,
                    &network_config,
                    &to_engine,
                    gas_accumulator.clone(),
                    &mut mode_transition_rx,
                )
                .await;

            // ensure no errors
            epoch_result.inspect_err(|e| {
                error!(target: "epoch-manager", ?e, "epoch returned error");
            })?;

            info!(target: "epoch-manager", "looping run epoch");
            self.consensus_bus.reset_for_epoch();
            // Make sure we don't start a new epoch when we are shutting down.
            if node_ended_sub.noticed() {
                break Ok(());
            }
        }
    }

    /// Run a single epoch.
    ///
    /// If it returns Ok(true) this indicates a mode change occurred and a restart
    /// is required.
    async fn run_epoch(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
        to_engine: &mpsc::Sender<(CameFrom, ConsensusOutput)>,
        gas_accumulator: GasAccumulator,
        mode_transition_rx: &mut watch::Receiver<Option<NodeMode>>,
    ) -> eyre::Result<()> {
        info!(target: "epoch-manager", "Starting epoch");
        let node_ended = self.sigterm_trigger.subscribe();

        // The task manager that resets every epoch and manages
        // short-running tasks for the lifetime of the epoch.
        let mut epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);
        // Rayls: allow time for tasks to release resources
        epoch_task_manager.set_join_wait(1000);

        // subscribe to output early to prevent missed messages
        let consensus_output = self.consensus_bus.consensus_output().subscribe();

        // create primary and worker nodes
        let (primary, worker_node, consensus_config) = self
            .create_consensus(engine, &epoch_task_manager, network_config, gas_accumulator.clone())
            .await?;
        // Epoch boundary for this epoch, fixed at config creation. Snapshotted here and handed to
        // detect_epoch_boundary by value (no shared/atomic state on the bus).
        let epoch_boundary = consensus_config.epoch_boundary();
        // consensus config for shutdown subscribers
        let consensus_shutdown = primary.shutdown_signal().await;
        let epoch_shutdown_rx = consensus_shutdown.subscribe();
        // This needs to be created early so required machinery for other tasks exists when needed.
        let mut worker = worker_node.new_worker().await?;
        worker.set_batch_tracker(self.consensus_bus.batch_tracker().clone());
        let current_committee = primary.current_committee().await;
        let current_epoch = current_committee.epoch();

        let consensus_metrics = self.consensus_bus.consensus_metrics();
        consensus_metrics.current_epoch.set(current_epoch as i64);
        consensus_metrics.committee_size.set(current_committee.size() as i64);

        // Produce a "dummy" epoch 0 EpochRecord if missing.
        // This will let us use simple code to find any epoch including 0 at startup.
        if self.consensus_db.get_committee_keys(0).is_none() {
            if current_epoch != 0 {
                return Err(eyre::eyre!(
                    "We have epoch 0 in our database if we are past epoch 0, on {current_epoch}"
                ));
            }
            // No keys for epoch 0, fix that.
            // We are on epoch 0 so load up that committee in Db as well.
            let committee: Vec<BlsPublicKey> = primary.current_committee().await.bls_keys();
            let next_committee = committee.clone();
            let epoch_rec =
                EpochRecord { epoch: 0, committee, next_committee, ..Default::default() };
            // Save the "dummy" record, should be overwritten once epoch 0 closes.
            // This will NOT be signed.
            if let Err(e) = self.consensus_db.save_epoch_record(&epoch_rec) {
                error!(
                    target: "epoch-manager",
                    "failed to save epoch 0 record: {e}",
                );
            }
        }
        gas_accumulator.rewards_counter().set_committee(primary.current_committee().await);

        // The txpool is intentionally not persisted across restarts (standard mempool semantics):
        // a client resubmits any transaction that was accepted but not yet mined.

        // Check for incomplete epoch transition from a previous crash.
        self.recover_partial_transition(&primary, engine).await?;

        // wait for the replay - then go on with the spawning below.
        let (execution_replay_completed_tx, mut execution_replay_completed_rx) =
            tokio::sync::watch::channel(());

        // start primary (spawns consensus + subscriber, waits for execution catch-up,
        // then spawns the proposer)
        primary
            .start(&epoch_task_manager, to_engine.clone(), execution_replay_completed_tx)
            .await?;

        // Be sure get_missing_consensus has finished - i.e the execution replay - so the
        // subscriber's catch-up runs to completion BEFORE detect_epoch_boundary's live relay
        // starts feeding the engine (serialized, no dual delivery).
        let _ = execution_replay_completed_rx.changed().await;

        // Start nothing while a transition is pending - the outer select is about to tear this
        // epoch down. Otherwise an active CVV builds batches and an observer forwards.
        let mode = *self.consensus_bus.node_mode().borrow();
        let transition_pending = self.consensus_bus.mode_transition().borrow().is_some();
        if !transition_pending {
            if mode.is_batch_producing() {
                match self.resolve_initial_batch_seq(&worker, &primary, current_epoch).await {
                    InitialBatchSeq::Use(seq) => {
                        // Spawn the worker-side consumer before the engine-side producer so the
                        // batch channel has a receiver for the first sealed batch.
                        let worker_task_manager_name =
                            worker_task_manager_name(worker_node.id().await);
                        worker.spawn_batch_builder(&worker_task_manager_name, &epoch_task_manager);
                        engine
                            .start_batch_builder(
                                worker.id(),
                                worker.batches_tx(),
                                &epoch_task_manager.get_spawner(),
                                gas_accumulator.base_fee(worker.id()),
                                current_epoch,
                                seq,
                                epoch_boundary,
                            )
                            .await?;
                    }
                    InitialBatchSeq::Defer => {
                        info!(target: "epoch-manager",
                        "execution replay incomplete; deferring batch builder to next epoch");
                    }
                    InitialBatchSeq::Shutdown => return Ok(()),
                }
            } else if mode.is_observer() {
                // An observer cannot seal, so it forwards its RPC-accepted transactions to the
                // committee instead of the batch builder.
                engine
                    .start_txn_forwarder(
                        worker.id(),
                        worker.network_handle(),
                        self.consensus_bus.executed_anchor().subscribe(),
                        // peer-derived latest header: the catch-up gate compares it against the
                        // executed anchor so a lagging node never re-sends
                        self.consensus_bus.last_consensus_header().subscribe(),
                        // slot-ordered (authorities sorted by id), matching receiver-side dispatch
                        primary
                            .current_committee()
                            .await
                            .authorities()
                            .iter()
                            .map(|authority| *authority.protocol_key())
                            .collect(),
                        &epoch_task_manager.get_spawner(),
                        network_config.libp2p_config().max_gossip_message_size,
                    )
                    .await?;
            }
        }

        // update tasks
        epoch_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?epoch_task_manager, "EPOCH TASKS\n");

        // await the epoch boundary or the epoch task manager exiting
        // this can also happen due to committee nodes re-syncing and errors
        let consensus_shutdown_clone = consensus_shutdown.clone();

        // New Epoch, should be able to collect the certs from the last epoch.
        if let Some(epoch_rec) = self.epoch_record.take() {
            // epoch_rec is the record for the epoch that just closed. The next
            // epoch's transition needs its digest for parent_hash, but it isn't
            // written to EpochRecords until its cert is collected (on vote quorum).
            // Keep an in-memory copy so parent_hash still works in that gap.
            self.prev_epoch_record = Some(epoch_rec);
        }
        // Certify the epoch that just closed together with any earlier close still awaiting
        // its cert. write_epoch_record already saved the new record to PendingEpochRecord, so
        // the table is the complete work set. One task for all of them: the vote queue is
        // single-consumer (#142).
        self.resume_pending_certification(&primary, &epoch_task_manager).await;

        // biased: node shutdown > consensus shutdown > boundary > mode_transition > task crash
        // snapshot before select: join() fires shutdown as side-effect
        let was_externally_shutdown = epoch_shutdown_rx.noticed();

        let outcome = tokio::select! {
            biased;

            _ = node_ended => RunningOutcome::NodeShutdown,

            // An external consensus shutdown arriving during the select resolves here rather than
            // through the join arm below, where a critical task exiting Ok in response to it would
            // be misclassified as a crash and kill the node. `was_externally_shutdown` only
            // samples the state before the select, so it cannot catch a notify that lands during.
            _ = epoch_shutdown_rx => RunningOutcome::NodeShutdown,

            res = self.detect_epoch_boundary(epoch_boundary, to_engine, consensus_output) => {
                match res {
                    Ok((target_hash, boundary_output)) => {
                        RunningOutcome::EpochBoundary(target_hash, Box::new(boundary_output))
                    }
                    Err(e) => RunningOutcome::TaskCrash(e),
                }
            },

            Ok(_) = mode_transition_rx.changed() => {
                // clear the latch after consumption so identify_node_mode on a
                // subsequent respawn does not re-apply the stale request
                let mut taken = None;
                self.consensus_bus.mode_transition().send_if_modified(|v| {
                    if v.is_some() {
                        taken = v.take();
                        true
                    } else {
                        false
                    }
                });
                let _ = mode_transition_rx.borrow_and_update();
                match taken {
                    Some(target_mode) => RunningOutcome::ModeTransition(target_mode),
                    None => RunningOutcome::NodeShutdown,
                }
            },

            res = epoch_task_manager.join(consensus_shutdown_clone) => {
                match res {
                    Ok(()) => {
                        info!(target: "epoch-manager", "epoch task manager exited - likely syncing with committee");
                        RunningOutcome::NodeShutdown
                    }
                    Err(TaskJoinError::CriticalExitOk(task)) => {
                        if was_externally_shutdown {
                            info!(target: "epoch-manager", ?task, "epoch task manager exited - syncing with committee");
                            RunningOutcome::NodeShutdown
                        } else {
                            error!(target: "epoch-manager", ?task, "critical task exited Ok without external shutdown - treating as crash");
                            RunningOutcome::TaskCrash(TaskJoinError::CriticalExitOk(task).into())
                        }
                    }
                    Err(e) => {
                        error!(target: "epoch-manager", ?e, "failed to reach epoch boundary");
                        RunningOutcome::TaskCrash(e.into())
                    }
                }
            },
        };

        // Handle the outcome sequentially, outside the select.
        match outcome {
            RunningOutcome::EpochBoundary(target_hash, boundary_output) => {
                let ctx = TransitionCtx {
                    engine,
                    to_engine,
                    primary: &primary,
                    consensus_shutdown,
                    epoch_task_manager: &mut epoch_task_manager,
                    gas_accumulator,
                };
                let result = self.run_epoch_transition(target_hash, *boundary_output, ctx).await;
                result?;
            }
            RunningOutcome::NodeShutdown => {
                // Ordered teardown - same producer→consumer sequencing as an epoch/mode
                // transition, instead of `abort_all_tasks()` (which hard-aborts Drainable
                // consumers unordered, alongside producers, and never awaits their drop).
                // `drain_round = None`: node shutdown doesn't run the subscriber drain
                // handshake (the node-level engine drain barrier in `run()` handles execution
                // fork-safety); we just want the kind-ordered, awaited join so producers are
                // reaped before consumers.
                self.controlled_shutdown(
                    consensus_shutdown,
                    &mut epoch_task_manager,
                    None,
                    Duration::from_secs(60),
                )
                .await;
            }
            RunningOutcome::ModeTransition(target_mode) => {
                info!(
                    target: "epoch-manager",
                    ?target_mode,
                    "mode transition requested; running controlled transition",
                );
                let ctx = TransitionCtx {
                    engine,
                    to_engine,
                    primary: &primary,
                    consensus_shutdown,
                    epoch_task_manager: &mut epoch_task_manager,
                    gas_accumulator,
                };
                let result = self.run_mode_transition(target_mode, ctx).await;
                result?;
            }
            RunningOutcome::TaskCrash(e) => {
                error!(target: "epoch-manager", ?e, "epoch ended due to task crash");
                return Err(e);
            }
        }

        self.initial_epoch = false;

        Ok(())
    }

    /// Resolve the starting batch seq for a node already eligible to produce batches.
    ///
    /// Eligibility is settled by the caller ([`NodeMode::is_batch_producing`], no pending
    /// transition). `Defer` means replay is unfinished (retry next epoch); `Shutdown` aborts the
    /// epoch.
    async fn resolve_initial_batch_seq<QW>(
        &self,
        worker: &Worker<DB, QW>,
        primary: &PrimaryNode<DB>,
        current_epoch: Epoch,
    ) -> InitialBatchSeq
    where
        QW: QuorumWaiterTrait,
    {
        // Observers disburse txns rather than sequence them; no real seq is read.
        if self.consensus_bus.node_mode().borrow().is_observer() {
            return InitialBatchSeq::Use(0);
        }

        // Active CVV: a persisted counter is authoritative and needs no replay.
        if let Some(seq) = worker.get_persisted_batch_seq() {
            return InitialBatchSeq::Use(seq);
        }

        // Dev (single-node): no peers to replay from - the execution_replay_complete
        // gate below waits on a signal that can be missed on the initial epoch, leaving
        // the batch builder unstarted and txs never mined. Resolve directly.
        #[cfg(feature = "dev-single-node-setup")]
        if primary.current_committee().await.size() == 1 {
            let authority_id = primary.authority_id().await;
            return InitialBatchSeq::Use(
                worker.recover_batch_seq_from_history(authority_id, current_epoch),
            );
        }

        // No counter (first epoch after a fresh sync): wait for replay, then recover the seq from
        // committed history.
        match await_execution_replay(
            // Fresh subs so our observe doesn't consume the outer receiver's signal.
            self.consensus_bus.execution_replay_complete().subscribe(),
            self.consensus_bus.mode_transition().subscribe(),
            // Abort the replay wait on graceful wind-down - the SAME signal run_epoch's outer
            // select observes. Must be `sigterm_trigger`, not `node_shutdown`: `node_shutdown`
            // is deferred until after run_epoch returns, so a replay wait keyed to it would
            // deadlock a SIGTERM arriving during setup. Fresh Noticer so we don't consume the
            // outer subscription.
            self.sigterm_trigger.subscribe(),
        )
        .await
        {
            ReplayWaitOutcome::Ready => {
                let authority_id = primary.authority_id().await;
                InitialBatchSeq::Use(
                    worker.recover_batch_seq_from_history(authority_id, current_epoch),
                )
            }
            ReplayWaitOutcome::Defer => InitialBatchSeq::Defer,
            ReplayWaitOutcome::Shutdown => InitialBatchSeq::Shutdown,
        }
    }

    /// Try to fetch an epoch certificate directly from a peer (catch-up fast-path).
    /// Return `true` if the cert was fetched and saved.
    async fn try_fetch_epoch_cert(
        &self,
        primary: &PrimaryNode<DB>,
        epoch_rec: &EpochRecord,
        epoch_hash: B256,
        committee: &[BlsPublicKey],
    ) -> bool {
        let network = primary.network_handle().await;
        // single budget: request_epoch_cert owns the retry + per-request timeout.
        // wrapping it in an outer timeout was cancelling mid-retry and defeating the loop
        let Ok((peer_rec, cert)) = network.request_epoch_cert(Some(epoch_rec.epoch), None).await
        else {
            return false;
        };
        if peer_rec.digest() != epoch_hash
            || !peer_rec.verify_with_cert(&cert)
            || !epoch_committee_valid(&peer_rec, committee)
        {
            return false;
        }
        info!(target: "epoch-manager", epoch = epoch_rec.epoch, "fast-path: fetched epoch cert from peer");
        // Save the peer's record (matching the verified cert), not our local copy.
        if let Err(e) = self.consensus_db.save_epoch_record_with_cert(&peer_rec, &cert) {
            error!(target: "epoch-manager", "failed to save fast-path epoch record and cert: {e}");
        }
        true
    }

    /// Ack and drop queued epoch votes whose record is already certified locally.
    ///
    /// The vote queue is only read while a "Collect Epoch Signatures" task runs. On the catch-up
    /// fast path no such task runs, so that close's votes would stay queued and be read by a
    /// later collection as votes for a different record. Returns the number of votes dropped.
    async fn drain_settled_epoch_votes(&self) -> usize {
        // Only drain when the receiver is free: a running collection owns the queue and drains
        // it itself.
        let Some(mut rx) = self.consensus_bus.try_subscribe_epoch_votes() else {
            return 0;
        };
        let (dropped, kept) = split_settled_votes(&mut rx, &self.consensus_db);
        // Anything we could not settle goes back on the now-empty queue, in order, for the next
        // collection to read.
        Self::requeue_epoch_votes(&self.consensus_bus, kept);
        if dropped > 0 {
            debug!(target: "epoch-manager", dropped, "dropped settled epoch votes from the queue");
        }
        dropped
    }

    /// Put votes taken off the queue but not processed back on it, in order, for the next
    /// collection. Dropping them would close their senders' channels, which the gossip handler
    /// logs as an error per vote. A vote that does not fit (queue full) is acked so its sender is
    /// not penalised for our backlog, and logged, since it is lost to the next collection.
    fn requeue_epoch_votes(consensus_bus: &ConsensusBus, votes: VecDeque<QueuedVote>) {
        for item in votes {
            if let Err(err) = consensus_bus.new_epoch_votes().try_send(item) {
                let (vote, vote_tx) = match err {
                    rayls_infrastructure_types::TrySendError::Full(item)
                    | rayls_infrastructure_types::TrySendError::Closed(item)
                    | rayls_infrastructure_types::TrySendError::Broadcast(item) => item,
                };
                warn!(
                    target: "epoch-manager",
                    digest = %vote.epoch_hash,
                    signer = ?vote.public_key,
                    "epoch vote queue full; an unsettled vote could not be requeued and is lost to the next collection",
                );
                let _ = vote_tx.send(Ok(()));
            }
        }
    }

    /// Aggregate and save the certificate for a record that reached quorum. `false` means the
    /// signatures did not aggregate or verify, or the write failed; the record stays pending and
    /// is retried.
    fn persist_certificate(consensus_db: &DB, state: &PendingCertification) -> bool {
        let epoch_hash = state.epoch_hash;
        let Some(cert) = state.certificate() else {
            error!(
                target: "epoch-manager",
                "failed to aggregate or verify the epoch cert for {epoch_hash}",
            );
            return false;
        };
        match consensus_db.save_epoch_record_with_cert(&state.record, &cert) {
            Ok(()) => {
                info!(
                    target: "epoch-manager",
                    epoch = state.record.epoch,
                    "reached quorum on epoch close for {epoch_hash}",
                );
                true
            }
            Err(err) => {
                error!(
                    target: "epoch-manager",
                    ?err,
                    "Failed to insert epoch record and cert for {epoch_hash}",
                );
                false
            }
        }
    }

    /// Log a bad epoch vote and answer the gossip handler, so it can penalise the sender.
    fn reject_epoch_vote(
        vote: &EpochVote,
        vote_tx: oneshot::Sender<Result<(), HeaderError>>,
        err: HeaderError,
    ) {
        error!(
            target: "epoch-manager",
            ?err,
            "Received an invalid epoch cert from {} for {}.",
            vote.public_key,
            vote.epoch_hash,
        );
        if let Err(err) = vote_tx.send(Err(err)) {
            error!(
                target: "epoch-manager",
                ?err,
                "Failed to send error for invalid epoch cert from {} for {}.",
                vote.public_key,
                vote.epoch_hash,
            );
        }
    }

    /// Resolve a foreign epoch record digest seen in a vote by asking a peer for that record.
    ///
    /// A record for an epoch *older* than the one being collected is stale gossip, and if we do
    /// not hold that record it is also a hole in our record chain: save it while we have it.
    /// A record for the epoch being collected is a genuine competing record that the network
    /// already certified; it is saved too, which ends our collection for that epoch (the cert
    /// check sees it) instead of rejecting every honest vote for it until an alternate quorum.
    /// A digest that cannot be resolved stays `Unknown`.
    ///
    /// So this function has a side effect: any certified record it fetches and validates is
    /// written to `EpochRecords`.
    async fn resolve_foreign_digest(
        primary_network: &PrimaryNetworkHandle,
        consensus_db: &DB,
        digest: B256,
        current_epoch: Epoch,
    ) -> ForeignVote {
        let Ok((record, cert)) = primary_network.request_epoch_cert(None, Some(digest)).await
        else {
            return ForeignVote::Unknown;
        };
        if record.digest() != digest {
            return ForeignVote::Unknown;
        }
        // Same validation the epoch record collector applies before it writes a record: anchored
        // to the parent when we hold it, certificate-only (with a real committee) when we do not.
        if !epoch_record_valid(consensus_db, record.epoch, &record, &cert) {
            return ForeignVote::Unknown;
        }
        let class = classify_fetched_record(record.epoch, current_epoch);
        let already_certified =
            consensus_db.get_epoch_by_number(record.epoch).is_some_and(|(_, cert)| cert.is_some());
        match class {
            ForeignVote::Stale { .. } => {
                if backfill_candidate(record.epoch, current_epoch, already_certified) {
                    match consensus_db.save_epoch_record_with_cert(&record, &cert) {
                        Ok(()) => info!(
                            target: "epoch-manager",
                            epoch = record.epoch,
                            "backfilled a missing epoch record found through a stale vote",
                        ),
                        Err(err) => error!(
                            target: "epoch-manager",
                            ?err,
                            epoch = record.epoch,
                            "failed to save a backfilled epoch record",
                        ),
                    }
                }
            }
            ForeignVote::Competing if !already_certified => {
                // The network came to quorum on a record for this epoch that is not the one we
                // built. Its cert verified above, so adopt it: this also deletes our pending row
                // and ends our collection for the epoch.
                match consensus_db.save_epoch_record_with_cert(&record, &cert) {
                    Ok(()) => {
                        warn!(
                            target: "epoch-manager",
                            epoch = record.epoch,
                            %digest,
                            "network certified a different record for the epoch being collected; adopted it",
                        );
                        info!(
                            target: PENDING_RECORD_LOG_TARGET,
                            epoch = record.epoch,
                            adopted = %digest,
                            "pending record replaced: the network's certified record was adopted and our pending row removed"
                        );
                    }
                    Err(err) => error!(
                        target: "epoch-manager",
                        ?err,
                        epoch = record.epoch,
                        "failed to save the network's certified record for the epoch being collected",
                    ),
                }
            }
            ForeignVote::Competing | ForeignVote::Unknown => {}
        }
        class
    }

    /// Certify every closed epoch whose record was built but has no certificate yet.
    ///
    /// The work set is the `PendingEpochRecord` table: `write_epoch_record` adds the epoch that
    /// just closed before this runs, and a row for an older epoch means certification did not
    /// finish in a previous `run_epoch` (drained at a transition, or lost to a restart). The
    /// chain can advance past an uncertified epoch (#142), so there may be several.
    ///
    /// All of them are collected by ONE task. The epoch-vote queue is single-consumer
    /// (`QueChannel::subscribe` panics on a second subscriber) and every pending epoch's votes
    /// arrive on it interleaved, so a task per epoch would crash on subscribe and, even alone,
    /// would read its siblings' honest votes as competing records and punish their senders.
    ///
    /// Cheap and a no-op when nothing is pending.
    pub(super) async fn resume_pending_certification(
        &self,
        primary: &PrimaryNode<DB>,
        epoch_task_manager: &TaskManager,
    ) {
        let certified = |epoch: Epoch| {
            self.consensus_db.get_epoch_by_number(epoch).is_some_and(|(_, c)| c.is_some())
        };
        let catching_up = !self.consensus_bus.node_mode().borrow().is_active_cvv();
        let me = self.builder.rayls_infrastructure_config.primary_bls_key();
        let primary_network = primary.network_handle().await;

        let pending = self.consensus_db.pending_epoch_records();
        let newest_pending = pending.last().map(|record| record.epoch);
        if !pending.is_empty() {
            info!(
                target: PENDING_RECORD_LOG_TARGET,
                epochs = ?pending.iter().map(|record| record.epoch).collect::<Vec<_>>(),
                catching_up,
                "resuming certification of every pending epoch in one collector task"
            );
        }
        let mut states: Vec<PendingCertification> = Vec::new();
        let mut fast_pathed = false;
        for record in pending {
            let epoch = record.epoch;
            if certified(epoch) {
                // Certified since the row was written (e.g. by a peer's backfill); tidy up.
                info!(
                    target: PENDING_RECORD_LOG_TARGET,
                    epoch,
                    "pending row is stale: the epoch is already certified on disk, clearing it"
                );
                if let Err(e) = self.consensus_db.clear_pending_epoch_record(epoch) {
                    error!(target: "epoch-manager", ?e, epoch, "failed to clear stale pending epoch record");
                }
                continue;
            }
            let epoch_hash = record.digest();

            // Peers have very likely certified an epoch older than the newest pending one while
            // this node was away, whatever mode it is in now. Ask first, vote only if that fails.
            let older_than_newest = Some(epoch) != newest_pending;
            if catching_up || older_than_newest {
                // trigger epoch record collector as background fallback
                self.consensus_bus.requested_missing_epoch().send_if_modified(|current| {
                    if epoch > *current {
                        *current = epoch;
                        true
                    } else {
                        false
                    }
                });
                if self.try_fetch_epoch_cert(primary, &record, epoch_hash, &record.committee).await
                {
                    info!(
                        target: PENDING_RECORD_LOG_TARGET,
                        epoch,
                        %epoch_hash,
                        "pending epoch certified from a peer's certificate; no vote collection needed"
                    );
                    fast_pathed = true;
                    continue;
                }
            }

            let mut state = PendingCertification::new(record);
            info!(
                target: PENDING_RECORD_LOG_TARGET,
                epoch,
                %epoch_hash,
                in_committee = state.committee_keys.contains(me),
                "pending epoch enters vote collection"
            );
            // We are in the committee so sign and gossip the epoch record.
            if state.committee_keys.contains(me) {
                let vote = state.sign(me, &self.key_config);
                info!(target: "epoch-manager", epoch, "publishing epoch record {epoch_hash}");

                // Dev (single-node): self-certify - no peers to gossip to or collect votes
                // from. The sole vote already meets super_quorum(1)==1. The `== 1` guard is
                // kept (not redundant): it keeps the production gossip/vote-collection path
                // below reachable in dev builds and acts as a cheap invariant guard.
                #[cfg(feature = "dev-single-node-setup")]
                if state.committee.len() == 1 {
                    match state.certificate() {
                        Some(cert) => {
                            match self
                                .consensus_db
                                .save_epoch_record_with_cert(&state.record, &cert)
                            {
                                Ok(_) => {
                                    info!(target: "epoch-manager", epoch, %epoch_hash, "self-certified epoch (single-node)")
                                }
                                Err(err) => {
                                    error!(target: "epoch-manager", ?err, epoch, %epoch_hash, "failed to save epoch cert")
                                }
                            }
                        }
                        None => {
                            error!(target: "epoch-manager", epoch, %epoch_hash, "failed to build a self-signed epoch cert")
                        }
                    }
                    continue;
                }

                let _ = primary_network.publish_epoch_vote(vote).await;
            }
            states.push(state);
        }

        if states.is_empty() {
            if fast_pathed {
                // The records are certified locally now and no collection task will run, so
                // nothing would ever read the votes the committee gossiped for them. Drop them
                // (acked) before they can be read as votes for an alternate record.
                self.drain_settled_epoch_votes().await;
            }
            return;
        }
        if states.len() > 1 {
            let epochs: Vec<Epoch> = states.iter().map(|s| s.record.epoch).collect();
            warn!(
                target: "epoch-manager",
                ?epochs,
                "several closed epochs have no cert on disk; certifying them together",
            );
        }

        let consensus_db = self.consensus_db.clone();
        let consensus_bus = self.consensus_bus.clone();
        // This is a Drainable consumer, so it drains on the task manager's `local_shutdown`  -
        // fired by `join_internal`'s consumer phase AFTER producers are reaped (or by `Drop`).
        // That makes wind-down graceful AND ordered for every teardown (epoch/mode transition
        // and SIGTERM). NOT `node_shutdown` (now deferred → this would be force-aborted) and
        // NOT `sigterm_trigger` (fires at the start → would exit concurrently with producers).
        let vote_shutdown = epoch_task_manager.shutdown_subscriber();
        epoch_task_manager.spawn_classified_task(
            "Collect Epoch Signatures",
            async move {
                Self::certify_pending_epochs(
                    states,
                    consensus_db,
                    consensus_bus,
                    primary_network,
                    vote_shutdown,
                )
                .await
            },
            TaskKind::Drainable,
        );
    }

    /// Body of the single "Collect Epoch Signatures" task: drive every pending record in
    /// `states` to a certificate.
    ///
    /// Retries the whole publish-vote / collect-quorum / fetch-from-peer sequence indefinitely
    /// (bounded only by shutdown or success) instead of giving up after one pass. Closes #142:
    /// if certification never completes within the fetch retry budget - a genuine network-wide
    /// stall, not just a slow peer - the records exist only in `PendingEpochRecord`, and this
    /// task (or an identical one resumed by the next `run_epoch`) is the only thing that can
    /// ever finish certifying them.
    async fn certify_pending_epochs(
        mut states: Vec<PendingCertification>,
        consensus_db: DB,
        consensus_bus: ConsensusBus,
        primary_network: PrimaryNetworkHandle,
        vote_shutdown: Noticer,
    ) {
        let certified = |epoch: Epoch| {
            consensus_db.get_epoch_by_number(epoch).is_some_and(|(_, c)| c.is_some())
        };
        // `attempt` is for logging only.
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            // Certified by someone else while we were backing off (a peer's backfill/fetch, or a
            // previous attempt's peer fetch landing late): nothing left to do for those.
            states.retain(|s| !certified(s.record.epoch));
            if states.is_empty() {
                return;
            }
            if attempt > 1 {
                for state in &states {
                    info!(
                        target: "epoch-manager",
                        epoch = state.record.epoch, attempt,
                        "retrying epoch certification; no quorum reached on the previous attempt",
                    );
                    if let Some(vote) = state.my_vote {
                        let _ = primary_network.publish_epoch_vote(vote).await;
                    }
                }
            }
            // The newest pending epoch is the reference for placing a digest that matches none
            // of the records being collected: a record of an older epoch is stale gossip, one
            // for the newest epoch is a competing record.
            let reference = &states[states.len() - 1];
            let reference_hash = reference.epoch_hash;
            let reference_epoch = reference.record.epoch;
            let alt_quorum = reference.quorum;
            let by_digest: B256Map<usize> =
                states.iter().enumerate().map(|(i, s)| (s.epoch_hash, i)).collect();
            // Committee membership for a vote that matches no pending record: any of theirs.
            let union_committee: Vec<BlsPublicKey> = states
                .iter()
                .flat_map(|s| s.committee.iter().copied())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();

            let mut rx = consensus_bus.new_epoch_votes().subscribe();
            // Votes left over from closes this node did not collect for are settled: their
            // record is already certified here, so they can only confuse this collection. Ack
            // and drop them, and hand the rest to the loop to process before it waits on the
            // queue.
            let (settled_votes, mut queued): (usize, VecDeque<QueuedVote>) =
                split_settled_votes(&mut rx, &consensus_db);
            if settled_votes > 0 {
                debug!(
                    target: "epoch-manager",
                    settled_votes,
                    "dropped settled epoch votes before collecting",
                );
            }
            let timeout = Duration::from_secs(5);
            let mut timeouts = 0;
            let mut alt_recs: B256Map<VotesAggregator<EpochVote>> = B256Map::default();
            // Unplaceable digests already warned about this attempt (once per digest).
            let mut warned_unresolvable: HashSet<B256> = HashSet::new();
            // Digests resolved over the network, so one unknown digest costs one request, and a
            // hard cap on how many such requests one collection may make.
            let mut resolved_digests: B256Map<ForeignVote> = B256Map::default();
            let mut resolutions = 0usize;
            loop {
                // Break promptly when the consumer phase is signalled, rather than only
                // noticing after the recv timeout. `biased` so shutdown wins over a vote arriving.
                let result = if let Some(queued_vote) = queued.pop_front() {
                    // Votes in hand must not outrank the shutdown the transition waits on.
                    if vote_shutdown.noticed() {
                        let _ = queued_vote.1.send(Ok(()));
                        break;
                    }
                    Ok(Some(queued_vote))
                } else {
                    tokio::select! {
                        biased;
                        _ = &vote_shutdown => break,
                        res = tokio::time::timeout(timeout, rx.recv()) => res,
                    }
                };
                match result {
                    Ok(Some((vote, vote_tx))) => {
                        // A vote for one of the records being collected: count it there.
                        if let Some(&i) = by_digest.get(&vote.epoch_hash) {
                            let state = &mut states[i];
                            if triage_vote(
                                &vote,
                                state.epoch_hash,
                                state.record.epoch,
                                &state.committee,
                                None,
                                None,
                            ) == VoteAction::Count
                            {
                                let _ = vote_tx.send(Ok(())); // If we lost this channel somehow then no big deal.
                                if state.done {
                                    // A straggler for a record we already certified.
                                    continue;
                                }
                                state.count(&vote);
                                // Persist the moment quorum is reached: a 2f+1 cert is a valid
                                // cert, and waiting for stragglers would hold this record's cert
                                // hostage to the slowest of the others.
                                if state.reached_quorum()
                                    && Self::persist_certificate(&consensus_db, state)
                                {
                                    state.done = true;
                                }
                                if states.iter().all(|s| s.done) {
                                    break;
                                }
                            } else {
                                // Send an error back to punish the peer that sent a bad epoch
                                // vote.
                                let err = if state.committee.contains(&vote.public_key) {
                                    HeaderError::UnknownAuthority(format!(
                                        "{} not in the committee for epoch {}",
                                        vote.public_key, state.epoch_hash
                                    ))
                                } else {
                                    HeaderError::PeerNotAuthor
                                };
                                Self::reject_epoch_vote(&vote, vote_tx, err);
                            }
                            continue;
                        }

                        // Place a foreign digest before it can feed any aggregator: stale gossip
                        // from an earlier close must not look like a competing record.
                        let local_record = |digest| {
                            consensus_db
                                .get_epoch_by_hash(digest)
                                .map(|(rec, cert)| (rec.epoch, cert.is_some()))
                        };
                        let mut action = triage_vote(
                            &vote,
                            reference_hash,
                            reference_epoch,
                            &union_committee,
                            local_record(vote.epoch_hash),
                            resolved_digests.get(&vote.epoch_hash).copied(),
                        );
                        if action == VoteAction::NeedsResolve {
                            // Bounded on every axis: one request per digest, a few per
                            // collection, a short timeout each, and a shutdown always wins.
                            let resolved = if resolutions >= MAX_DIGEST_RESOLUTIONS {
                                ForeignVote::Unknown
                            } else {
                                resolutions += 1;
                                tokio::select! {
                                    biased;
                                    _ = &vote_shutdown => {
                                        let _ = vote_tx.send(Ok(()));
                                        break;
                                    }
                                    resolved = tokio::time::timeout(
                                        DIGEST_RESOLVE_TIMEOUT,
                                        Self::resolve_foreign_digest(
                                            &primary_network,
                                            &consensus_db,
                                            vote.epoch_hash,
                                            reference_epoch,
                                        ),
                                    ) => resolved.unwrap_or(ForeignVote::Unknown),
                                }
                            };
                            resolved_digests.insert(vote.epoch_hash, resolved);
                            action = triage_vote(
                                &vote,
                                reference_hash,
                                reference_epoch,
                                &union_committee,
                                local_record(vote.epoch_hash),
                                Some(resolved),
                            );
                        }

                        match action {
                            // `Count` cannot happen here (the digest matched no pending record),
                            // but treat it like honest gossip rather than punishing anyone.
                            VoteAction::Count => {
                                let _ = vote_tx.send(Ok(()));
                            }
                            VoteAction::IgnoreStale { epoch: stale_epoch } => {
                                debug!(
                                    target: "epoch-manager",
                                    stale_epoch,
                                    digest = %vote.epoch_hash,
                                    "ignoring a stale epoch vote from an earlier close",
                                );
                                // Honest gossip: ack it so the peer is not punished.
                                let _ = vote_tx.send(Ok(()));
                            }
                            VoteAction::Alternate
                            | VoteAction::Unresolvable
                            | VoteAction::NeedsResolve => {
                                // `NeedsResolve` cannot survive the resolution above; if it
                                // did, nothing was proven, so treat it as unresolvable.
                                let competing = action == VoteAction::Alternate;
                                // track votes on alternate epoch records per-validator;
                                // break on quorum. per-validator tracking prevents inflation.
                                const MAX_ALT_RECS: usize = 100;
                                let reached_alt_quorum = if alt_recs.len() < MAX_ALT_RECS
                                    || alt_recs.contains_key(&vote.epoch_hash)
                                {
                                    let agg = alt_recs
                                        .entry(vote.epoch_hash)
                                        .or_insert_with(|| VotesAggregator::new(alt_quorum));
                                    agg.append(vote, 1).unwrap_or(false)
                                } else {
                                    false
                                };
                                if reached_alt_quorum {
                                    if competing {
                                        error!(
                                            target: "epoch-manager",
                                            "Reached quorum on epoch record {} instead of {}.",
                                            vote.epoch_hash,
                                            reference_hash,
                                        );
                                        if let Err(err) =
                                            vote_tx.send(Err(HeaderError::InvalidHeaderDigest))
                                        {
                                            error!(
                                                target: "epoch-manager",
                                                ?err,
                                                "Failed to send error for invalid epoch record {} from {}.",
                                                vote.epoch_hash,
                                                vote.public_key,
                                            );
                                        }
                                        break;
                                    }
                                    // Not a fork signal we can act on: a quorum of committee
                                    // members signing a digest nobody can place is what their
                                    // re-votes for an uncertified earlier epoch look like to a
                                    // node that never built that record. Keep collecting; the
                                    // fetch fallback covers a real competing record anyway.
                                    if warned_unresolvable.insert(vote.epoch_hash) {
                                        warn!(
                                            target: "epoch-manager",
                                            digest = %vote.epoch_hash,
                                            "a quorum of committee members signed an epoch record digest this node cannot place",
                                        );
                                    }
                                }
                                if competing {
                                    Self::reject_epoch_vote(
                                        &vote,
                                        vote_tx,
                                        HeaderError::InvalidHeaderDigest,
                                    );
                                } else {
                                    // A valid committee signature over a digest nobody can
                                    // explain is not evidence of misbehaviour: ack it so the
                                    // sender is not punished (#142).
                                    let _ = vote_tx.send(Ok(()));
                                }
                            }
                            VoteAction::Reject => {
                                Self::reject_epoch_vote(
                                    &vote,
                                    vote_tx,
                                    HeaderError::InvalidHeaderDigest,
                                );
                            }
                        }
                    }
                    Ok(None) => break, // channel issues...
                    Err(_) => {
                        // Failed after a minute: break and try to request the certs instead.
                        // (Shutdown is handled by the select arm above, not polled here.)
                        if timeouts > 12 {
                            break;
                        }
                        timeouts += 1;

                        // The epoch record collector, or a peer's record adopted while resolving
                        // a digest, may have certified some of these in the background. Those
                        // are done; the others keep collecting undisturbed.
                        for state in states.iter_mut().filter(|s| !s.done) {
                            if certified(state.record.epoch) {
                                info!(
                                    target: "epoch-manager",
                                    epoch = state.record.epoch,
                                    "epoch cert appeared in the DB during vote collection",
                                );
                                state.done = true;
                            }
                        }
                        if states.iter().all(|s| s.done) {
                            break;
                        }
                        // Timed out, maybe we are not the only ones having issues so republish.
                        for state in states.iter().filter(|s| !s.done) {
                            if let Some(vote) = state.my_vote {
                                if let Err(err) = primary_network.publish_epoch_vote(vote).await {
                                    error!(
                                        target: "epoch-manager",
                                        ?err,
                                        "Failed to republish epoch vote for {}.",
                                        vote.epoch_hash,
                                    );
                                }
                            }
                        }
                    }
                }
            }
            // Votes taken off the queue but not processed (shutdown, abort, or everything done)
            // go back for the next collection; dropping them would close their senders' channels.
            Self::requeue_epoch_votes(&consensus_bus, queued);
            // Release the single-consumer queue while fetching and backing off.
            drop(rx);

            // Certs were persisted as each record reached quorum. Retry the write for any that
            // reached quorum but could not be saved, and keep everything still uncertified.
            let mut remaining = Vec::with_capacity(states.len());
            for state in states.drain(..) {
                if state.done || certified(state.record.epoch) {
                    continue;
                }
                if state.reached_quorum() {
                    if Self::persist_certificate(&consensus_db, &state) {
                        continue;
                    }
                    // Keep it: the signatures stay counted and the write is retried next round.
                    remaining.push(state);
                    continue;
                }
                let epoch_hash = state.epoch_hash;
                error!(
                    target: "epoch-manager",
                    "failed to reach quorum on epoch close for {epoch_hash} {:?}", state.record,
                );
                // Wake the epoch record collector as a background fallback in case every attempt
                // below fails.
                let epoch = state.record.epoch;
                consensus_bus.requested_missing_epoch().send_if_modified(|current| {
                    if epoch > *current {
                        *current = epoch;
                        true
                    } else {
                        false
                    }
                });
                remaining.push(state);
            }
            states = remaining;
            if states.is_empty() {
                return;
            }

            // Peers certify a record a fraction of a second after we detect the boundary, so
            // one round of requests often arrives too early. Retry on a bounded budget, checking
            // the DB between rounds in case the record collector landed it first.
            let deadline = tokio::time::Instant::now() + EPOCH_FETCH_RETRY_BUDGET;
            let mut attempts = 0;
            'retry: loop {
                attempts += 1;
                states.retain(|s| {
                    let done = certified(s.record.epoch);
                    if done {
                        info!(
                            target: "epoch-manager",
                            epoch = s.record.epoch,
                            "epoch record appeared in the DB while retrying the fetch",
                        );
                    }
                    !done
                });
                if states.is_empty() {
                    return;
                }
                let mut i = 0;
                while i < states.len() {
                    if Self::fetch_pending_cert_from_peers(
                        &primary_network,
                        &consensus_db,
                        &states[i],
                    )
                    .await
                    {
                        states.remove(i);
                    } else {
                        i += 1;
                    }
                }
                if states.is_empty() {
                    return;
                }
                if tokio::time::Instant::now() >= deadline {
                    break 'retry;
                }
                tokio::select! {
                    biased;
                    _ = &vote_shutdown => return,
                    _ = tokio::time::sleep(EPOCH_FETCH_RETRY_INTERVAL) => {}
                }
            }

            // if we didn't return before, means we didn't find them
            let epochs: Vec<Epoch> = states.iter().map(|s| s.record.epoch).collect();
            error!(
                target: "epoch-manager",
                attempts,
                ?epochs,
                "Failed to retrieve epoch records from peers",
            );

            // Neither quorum nor a peer's cert. Back off (doubling per attempt, capped) and try
            // the whole sequence again - the records are only in PendingEpochRecord now, and only
            // this loop (or an identical one resumed by the next run_epoch, see
            // resume_pending_certification) can ever finish certifying them.
            tokio::select! {
                biased;
                _ = &vote_shutdown => return,
                _ = tokio::time::sleep(certification_retry_backoff(attempt)) => {}
            }
        }
    }

    /// One round of asking peers (up to three) for a certified record of `state`'s epoch.
    /// Returns `true` once a verified record+cert for that epoch is saved - the one we built, or
    /// the one the network actually came to quorum on if our digest was wrong.
    async fn fetch_pending_cert_from_peers(
        primary_network: &PrimaryNetworkHandle,
        consensus_db: &DB,
        state: &PendingCertification,
    ) -> bool {
        let epoch = state.record.epoch;
        let epoch_hash = state.epoch_hash;
        // ask up to peer count in the case we get a different hash
        let connected_peers_count = primary_network.connected_peers_count().await.unwrap_or(0);
        for _ in 0..connected_peers_count.min(3) {
            // Request by epoch number in case we had a bad hash...
            let Ok((new_epoch_rec, cert)) =
                primary_network.request_epoch_cert(Some(epoch), None).await
            else {
                error!(
                    target: "epoch-manager",
                    ?epoch_hash,
                    "failed to retrieve epoch from a peer",
                );
                continue;
            };

            // invalid epoch record or cert, skip
            if !new_epoch_rec.verify_with_cert(&cert)
                || !epoch_committee_valid(&new_epoch_rec, &state.committee)
                || new_epoch_rec.parent_hash != state.record.parent_hash
            {
                continue;
            }

            let new_epoch_hash = new_epoch_rec.digest();
            if new_epoch_hash == epoch_hash {
                info!(
                    target: "epoch-manager",
                    "retrieved cert for epoch {new_epoch_hash} from a peer",
                );
            } else {
                // Humm, we got another epoch record than the one we expected...
                // The network came to quorum on this one so lets go with it...
                warn!(
                    target: "epoch-manager",
                    "Received wrong epoch record: {new_epoch_hash}, expected {epoch_hash}",
                );
            }
            if let Err(err) = consensus_db.save_epoch_record_with_cert(&new_epoch_rec, &cert) {
                error!(
                    target: "epoch-manager",
                    ?err,
                    "Failed to insert epoch record and cert for {new_epoch_hash}",
                );
            }
            return true;
        }
        false
    }

    /// Detect the epoch boundary by monitoring consensus output.
    ///
    /// Forwards all non-boundary consensus output to the engine for execution.
    /// When the boundary subdag is found, returns the target hash along with the
    /// boundary output (with `close_epoch = true` already set). The boundary output
    /// is NOT sent to the engine here -- that happens later in the sequential
    /// transition phases.
    async fn detect_epoch_boundary(
        &self,
        epoch_boundary: u64,
        to_engine: &mpsc::Sender<(CameFrom, ConsensusOutput)>,
        mut consensus_output: impl RaylsReceiver<ConsensusOutput>,
    ) -> eyre::Result<(B256, ConsensusOutput)> {
        while let Some(mut output) = consensus_output.recv().await {
            if output.reaches_epoch_boundary(epoch_boundary) {
                info!(
                    target: "epoch-manager",
                    epoch=?output.leader().epoch(),
                    commit=?output.committed_at(),
                    epoch_boundary=?epoch_boundary,
                    "epoch boundary detected",
                );

                // Mark the output for epoch closing and extract the target hash.
                output.close_epoch = true;
                let target_hash = output.consensus_header_hash();

                // Return WITHOUT sending to engine -- the caller will send it
                // in the EXECUTION_COMPLETE phase after drain and shutdown.
                return Ok((target_hash, output));
            } else {
                to_engine.send((CameFrom::DetectEpochBoundary, output)).await?;
            }
        }
        Err(eyre::eyre!("consensus output channel closed before epoch boundary"))
    }

    /// Wait for the engine to execute the epoch-closing boundary output.
    ///
    /// Sends the boundary output to the engine, then subscribes to the `executed_anchor` watch and
    /// waits until `anchor.number >= boundary_output.number` - a monotonic, drop-free completion
    /// signal, immune to which block ends up the canonical tip (e.g. a drained parked batch, whose
    /// block anchors to a previous output). `target_hash` is used only for diagnostics, not
    /// matching. Consensus shutdown must already be complete before calling this.
    pub(super) async fn await_epoch_execution(
        &self,
        engine: &ExecutionNode,
        to_engine: &mpsc::Sender<(CameFrom, ConsensusOutput)>,
        boundary_output: ConsensusOutput,
        gas_accumulator: &GasAccumulator,
        target_hash: B256,
    ) -> eyre::Result<()> {
        // Anchor on execution PROGRESS (the boundary output's number), NOT on a block's
        // `parent_beacon_block_root`. The engine advances `executed_anchor` to each output's own
        // consensus header as it finishes executing it, so `anchor.number >= boundary.number` is an
        // unambiguous, monotonic completion signal. Matching a block's beacon instead fails when
        // the boundary output drains a previously-parked batch: that block lands as the
        // canonical tip but anchors to its ORIGIN output, so the tip's beacon never equals
        // `target_hash` and the old loop timed out (network-wide) even though the output
        // HAD executed. `executed_anchor` is a `watch`, so it also can't silently drop like
        // the canonical broadcast stream.
        let boundary_number = boundary_output.number;

        // Subscribe BEFORE sending so we cannot miss the anchor update for the boundary output.
        let mut anchor_rx = self.consensus_bus.executed_anchor().subscribe();

        // send the boundary output to the engine for execution
        to_engine.send((CameFrom::AwaitEpochExecution, boundary_output)).await?;

        loop {
            let anchor_number = anchor_rx.borrow_and_update().number;
            info!(
                target: "epoch-manager",
                anchor_number,
                boundary_number,
                ?target_hash,
                reached = anchor_number >= boundary_number,
                "await_epoch_execution: executed-anchor check"
            );
            // Boundary output (and everything before it) has executed.
            if anchor_number >= boundary_number {
                // adjust base fees against the resulting EVM tip, then clear the accumulator.
                let tip_number = engine.get_reth_env().await.canonical_tip().number;
                self.adjust_base_fees(gas_accumulator, tip_number);
                gas_accumulator.clear();
                return Ok(());
            }
            // Wait for the next anchor advance. An error means the sender was dropped - the engine
            // task is gone, so execution can never complete.
            if anchor_rx.changed().await.is_err() {
                error!(
                    target: "epoch-manager",
                    "executed_anchor sender dropped while awaiting engine execution for closing epoch",
                );
                return Err(eyre!("engine failed to report execution for closing epoch"));
            }
        }
    }
}

/// Outcome of waiting for execution replay before recovering the batch seq.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReplayWaitOutcome {
    /// Replay is complete; safe to walk committed history for the seq.
    Ready,
    /// A mode transition was requested while waiting - defer to the next epoch.
    Defer,
    /// Node shutdown signaled - abort the epoch.
    Shutdown,
}

/// Wait for execution replay before walking history for the batch seq.
///
/// Entered only by an active CVV with no persisted counter. Races node shutdown and a pending
/// mode transition (a node leaving the committee must not produce batches). A free function so
/// the channel logic is unit-testable without a full `EpochManager`.
pub(crate) async fn await_execution_replay(
    mut replay_rx: watch::Receiver<bool>,
    mut transition_rx: watch::Receiver<Option<NodeMode>>,
    shutdown: Noticer,
) -> ReplayWaitOutcome {
    if *replay_rx.borrow() {
        return ReplayWaitOutcome::Ready;
    }

    info!(target: "epoch-manager",
        "waiting for execution replay to complete before reading batch sequence");
    tokio::select! {
        biased;
        _ = &shutdown => ReplayWaitOutcome::Shutdown,
        _ = transition_rx.wait_for(|t| t.is_some()) => {
            info!(target: "epoch-manager",
                "mode transition pending during replay wait; deferring");
            ReplayWaitOutcome::Defer
        }
        _ = replay_rx.wait_for(|v| *v) => ReplayWaitOutcome::Ready,
    }
}

#[cfg(test)]
mod pending_certification_tests {
    use super::PendingCertification;
    use rand::{rngs::StdRng, SeedableRng as _};
    use rayls_infrastructure_types::{
        BlsKeypair, BlsPublicKey, BlsSignature, BlsSigner, EpochRecord, Signer as _, B256,
    };
    use std::sync::Arc;

    /// Minimal [`BlsSigner`] so the test can produce real, verifiable votes.
    #[derive(Clone)]
    struct TestSigner(Arc<BlsKeypair>);

    impl BlsSigner for TestSigner {
        fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
            self.0.sign(msg)
        }

        fn public_key(&self) -> BlsPublicKey {
            *self.0.public()
        }
    }

    /// Signatures accumulate across attempts (nothing is reset), repeats and outsiders are
    /// ignored, and a quorum aggregates into a certificate that verifies against the record.
    #[test]
    fn votes_accumulate_into_a_verifying_quorum_cert() {
        let mut rng = StdRng::seed_from_u64(21);
        let signers: Vec<TestSigner> =
            (0..4).map(|_| TestSigner(Arc::new(BlsKeypair::generate(&mut rng)))).collect();
        let mut committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        committee.sort_unstable();
        let record = EpochRecord {
            epoch: 303,
            committee: committee.clone(),
            next_committee: committee,
            parent_hash: B256::ZERO,
            ..Default::default()
        };

        let mut state = PendingCertification::new(record.clone());
        assert_eq!(state.quorum, 3);
        assert!(!state.reached_quorum());
        assert!(state.certificate().is_none(), "nothing to aggregate yet");

        // Two votes in a first attempt.
        state.count(&record.sign_vote(&signers[0]));
        state.count(&record.sign_vote(&signers[1]));
        assert!(!state.reached_quorum());
        // A repeat from the same signer does not count twice.
        state.count(&record.sign_vote(&signers[1]));
        assert_eq!(state.signed_authorities.len(), 2);

        // The third arrives in a later attempt. Nothing was reset, so it completes the quorum.
        state.count(&record.sign_vote(&signers[2]));
        assert!(state.reached_quorum());
        let cert = state.certificate().expect("a quorum aggregates into a verifying cert");
        assert!(record.verify_with_cert(&cert));
        assert_eq!(cert.signed_authorities.len(), 3);

        // An outsider's vote is ignored.
        let outsider = TestSigner(Arc::new(BlsKeypair::generate(&mut rng)));
        state.count(&record.sign_vote(&outsider));
        assert_eq!(state.signed_authorities.len(), 3);
    }
}
