//! Epoch manager.
//! Oversees per-epoch tasks and shared cross-epoch resources.

use crate::{
    engine::{ExecutionNode, RaylsBuilder},
    epoch_manager::{
        types::{EpochManager, ENGINE_TASK_MANAGER, EPOCH_TASK_MANAGER, NODE_TASK_MANAGER},
        utils::catchup_accumulator,
    },
    primary::PrimaryNode,
    types::{HealthcheckServer, InitialBatchSeq, RunningOutcome, TransitionCtx},
    worker::worker_task_manager_name,
};
use consensus_metrics::start_prometheus_server;
use eyre::eyre;
use futures::FutureExt;
use rayls_consensus_primary::{ConsensusBus, NodeMode, QueChannel};
use rayls_consensus_state_sync::{
    epoch_committee_valid, highest_executed_anchor, last_executed_consensus_from_anchor,
    spawn_epoch_record_collector,
};
use rayls_consensus_worker::{quorum_waiter::QuorumWaiterTrait, Worker};
use rayls_execution_evm::{reth_env::RethEnv, system_calls::EpochState};
use rayls_infrastructure_config::{KeyConfig, LibP2pConfig, NetworkConfig, RaylsDirs};
use rayls_infrastructure_storage::{tables::ConsensusBlocks, EpochStore as _};
use rayls_infrastructure_types::{
    error::HeaderError, gas_accumulator::GasAccumulator, BlsAggregateSignature, BlsPublicKey,
    BlsSignature, CameFrom, ConsensusOutput, Database as ReDatabase, Epoch, EpochCertificate,
    EpochRecord, EpochVote, Noticer, Notifier, RaylsReceiver, RaylsSender, TaskJoinError, TaskKind,
    TaskManager, VotesAggregator, B256,
};
use rayls_middleware_processor::{batch::BatchOrdering, reconstruct_batch_digests};
use std::{
    collections::{HashMap, HashSet},
    panic::AssertUnwindSafe,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, error, info, warn};

/// Number of recent EVM blocks scanned to recover the restart execution anchor.
///
/// A drained parked (out-of-order seq) batch's block carries its ORIGIN output's lower nonce and
/// that output's digest as `parent_beacon_block_root`, yet lands after the in-order filler and
/// becomes the canonical tip, so the tip can anchor to a PREVIOUS output. Scanning this window and
/// taking the max-nonce block recovers the true highest executed output's consensus-header digest.
/// Sized well above any plausible reorder/park run.
const LAST_EXECUTED_SCAN_DEPTH: u64 = 200;

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

        Ok(Self {
            builder,
            rayls_datadir,
            primary_network_handle: None,
            worker_network_handle: None,
            key_config,
            node_shutdown,
            sigterm_trigger: Notifier::new(),
            reth_db,
            consensus_db,
            consensus_bus,
            worker_event_stream,
            epoch_record: None,
            prev_epoch_record: None,
            initial_epoch: true,
        })
    }

    /// Run the node, handling epoch transitions.
    pub(crate) async fn run(&mut self) -> eyre::Result<()> {
        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);
        let node_task_spawner = node_task_manager.get_spawner();

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
        let batch_ordering = BatchOrdering::from_history(self.consensus_db.clone(), epoch);

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
        // The anchor must be the HIGHEST-nonce recent EVM block's consensus header, not the literal
        // canonical tip: a drained parked (out-of-order seq) batch's block carries its ORIGIN
        // output's lower nonce/anchor yet lands after the in-order filler and becomes the tip, so
        // the tip can anchor to a PREVIOUS output and regress the watermark. Scan the recent window
        // and pick the max-nonce block's anchor; fall back to the tip when the window is empty.
        // Startup runs before the engine starts, so the tip is frozen: acquire one `reth_env`
        // handle and one tip, shared by both the execution-anchor recovery and the dedup-registry
        // reconstruction below (they must agree on the same tip).
        let reth_env = engine.get_reth_env().await;
        let tip = reth_env.canonical_tip();

        let last_execution_block = {
            let start = tip.number.saturating_sub(LAST_EXECUTED_SCAN_DEPTH);
            let window = reth_env.blocks_for_range(start..=tip.number)?;
            let anchor = highest_executed_anchor(&window).or(tip.header().parent_beacon_block_root);
            last_executed_consensus_from_anchor(anchor, &self.consensus_db)
        };

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
            &node_task_manager,
        );

        // add engine task manager
        node_task_manager.add_task_manager(engine_task_manager);
        node_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS\n");

        // spawn node healthcheck service if enabled
        if let Some(port) = self.builder.healthcheck {
            let _ = HealthcheckServer::spawn(node_task_manager.get_spawner(), port).await;
        }

        // Catch the termination signal ourselves so we can drive a graceful, ORDERED
        // shutdown — rather than letting a task-manager join catch it (which would also fire
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
        // BEFORE `node_shutdown.notify()` below sets the engine's `shutdown_requested` — and the
        // engine faults (`ConsensusOutputStreamClosed`) on an input close while that flag is false
        // (a TOCTOU on its shutdown check). We drop this only AFTER `notify()`, making notify()
        // strictly happen-before the close: the engine polls rx_shutdown before its input, so it
        // sees shutdown_requested=true and exits Ok. Cheap mpsc clone, held for run()'s lifetime.
        let engine_input_keepalive = to_engine.clone();
        let outcome = AssertUnwindSafe(async {
            let epochs = self.run_epochs(&engine, network_config, to_engine, gas_accumulator);
            tokio::pin!(epochs);
            // `join` (do_exit=false) so this does NOT catch the termination signal — only a
            // node-task crash (or `node_shutdown`) completes it. SIGTERM is handled by the
            // listener above; on SIGTERM this stays pending and is dropped via the `epochs`
            // branch, leaving node tasks (the engine) running until the post-teardown
            // `node_shutdown` below.
            let node_join = node_task_manager.join(node_shutdown.clone());
            tokio::pin!(node_join);

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
            }
        })
        .catch_unwind()
        .await;

        // Drain barrier: wait for the engine to finish executing its last block
        // before flushing. The select above can complete via `run_epochs` (which
        // returns fast on shutdown) while the engine task is still draining queued
        // outputs; flushing then persists a prefix and lets a later block land
        // post-flush (the serialize-replay fork). Signal shutdown (idempotent) so
        // the engine begins its drain, then wait — UNBOUNDED — for it to report done.
        // A finite timeout that fired mid-drain (e.g. a large queued backlog, each output
        // up to seconds) would flush a prefix and let a later block land post-flush — the
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
            // drain completed (so no in-flight block was finalized) — safe to flush.
            Err(_) => {
                warn!(target: "engine", "engine task ended without drain signal; flushing")
            }
        }

        // Flush both layers, each under catch_unwind so a panic in one still runs the other.
        // Consensus goes first so a crash between them leaves consensus >= execution (replayable).
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
        let current_epoch = primary.current_committee().await.epoch();

        self.consensus_bus.consensus_metrics().current_epoch.set(current_epoch as i64);

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

        self.orphan_batches(engine.clone(), worker.clone()).await?;

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

        // Only eligible nodes build batches, and not while a transition is pending - the outer
        // select is about to tear this epoch down.
        let mode = *self.consensus_bus.node_mode().borrow();
        let transition_pending = self.consensus_bus.mode_transition().borrow().is_some();
        if mode.is_batch_producing() && !transition_pending {
            match self.resolve_initial_batch_seq(&worker, &primary, current_epoch).await {
                InitialBatchSeq::Use(seq) => {
                    // Spawn the worker-side consumer before the engine-side producer so the
                    // batch channel has a receiver for the first sealed batch.
                    let worker_task_manager_name = worker_task_manager_name(worker_node.id().await);
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
            // written to disk until its cert is collected (on vote quorum, later in
            // collect_epoch_votes). Keep an in-memory copy so parent_hash still works
            // in that gap.
            self.prev_epoch_record = Some(epoch_rec.clone());
            self.collect_epoch_votes(&primary, epoch_rec, &epoch_task_manager).await;
        }

        // biased: shutdown > boundary > mode_transition > task crash
        // snapshot before select: join() fires shutdown as side-effect
        let was_externally_shutdown = epoch_shutdown_rx.noticed();

        let outcome = tokio::select! {
            biased;

            _ = node_ended => RunningOutcome::NodeShutdown,

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
                // Ordered teardown — same producer→consumer sequencing as an epoch/mode
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

        // Dev (single-node): no peers to replay from — the execution_replay_complete
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
            // Abort the replay wait on graceful wind-down — the SAME signal run_epoch's outer
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

    /// Start a task to collect the epoch record votes previous epochs record.
    /// This should run quickly at epoch start and make epoch records/certs available to syncing
    /// nodes.
    async fn collect_epoch_votes(
        &self,
        primary: &PrimaryNode<DB>,
        epoch_rec: EpochRecord,
        epoch_task_manager: &TaskManager,
    ) {
        if let Some((_, Some(_))) = self.consensus_db.get_epoch_by_number(epoch_rec.epoch) {
            // We already have this record and cert...
            return;
        }

        let committee = epoch_rec.committee.clone();
        let epoch_hash = epoch_rec.digest();

        let catching_up = !self.consensus_bus.node_mode().borrow().is_active_cvv();
        if catching_up {
            // trigger epoch record collector as background fallback
            self.consensus_bus.requested_missing_epoch().send_if_modified(|current| {
                if epoch_rec.epoch > *current {
                    *current = epoch_rec.epoch;
                    true
                } else {
                    false
                }
            });
            if self.try_fetch_epoch_cert(primary, &epoch_rec, epoch_hash, &committee).await {
                return;
            }
        }
        let mut committee_keys: HashSet<BlsPublicKey> = committee.iter().copied().collect();
        let committee_index: HashMap<BlsPublicKey, usize> =
            committee.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        let consensus_db = self.consensus_db.clone();

        let me = self.builder.rayls_infrastructure_config.primary_bls_key();
        let committee_size = committee_keys.len() as u64;
        let quorum = epoch_rec.super_quorum();
        let mut sigs = Vec::new();
        let mut signed_authorities = roaring::RoaringBitmap::new();
        let primary_network = primary.network_handle().await;
        let mut my_vote = None;
        // We are in the committee so sign and gossip the epoch record.
        if committee_keys.contains(me) {
            committee_keys.remove(me);
            let epoch_vote = epoch_rec.sign_vote(&self.key_config);
            sigs.push(epoch_vote.signature);
            if let Some(idx) = committee_index.get(&self.key_config.primary_public_key()) {
                signed_authorities.insert(*idx as u32);
            }
            info!(
                target: "epoch-manager",
                "publishing epoch record {epoch_hash}",
            );

            // Dev (single-node): self-certify and return — no peers to gossip to or
            // collect votes from. The sole vote already meets super_quorum(1)==1.
            // The `== 1` guard is kept (not redundant): it keeps the production
            // gossip/vote-collection path below reachable in dev builds and acts as a
            // cheap invariant guard.
            #[cfg(feature = "dev-single-node-setup")]
            if committee_size == 1 {
                let Ok(agg) = BlsAggregateSignature::aggregate(&sigs[..], true) else {
                    error!(target: "epoch-manager", epoch = epoch_rec.epoch, %epoch_hash, "failed to aggregate signatures");
                    return;
                };

                let cert = EpochCertificate {
                    epoch_hash,
                    signature: agg.to_signature(),
                    signed_authorities,
                };

                if !epoch_rec.verify_with_cert(&cert) {
                    error!(target: "epoch-manager", epoch = epoch_rec.epoch, %epoch_hash, "epoch cert verification failed");
                    return;
                }

                match self.consensus_db.save_epoch_record_with_cert(&epoch_rec, &cert) {
                    Ok(_) => {
                        info!(target: "epoch-manager", epoch = epoch_rec.epoch, %epoch_hash, "self-certified epoch (single-node)")
                    }
                    Err(err) => {
                        error!(target: "epoch-manager", ?err, epoch = epoch_rec.epoch, %epoch_hash, "failed to save epoch cert")
                    }
                }
                return;
            }

            let _ = primary_network.publish_epoch_vote(epoch_vote).await;
            my_vote = Some(epoch_vote);
        }

        let mut rx = self.consensus_bus.new_epoch_votes().subscribe();
        // This is a Drainable consumer, so it drains on the task manager's `local_shutdown` —
        // fired by `join_internal`'s consumer phase AFTER producers are reaped (or by `Drop`).
        // That makes wind-down graceful AND ordered for every teardown (epoch/mode transition
        // and SIGTERM). NOT `node_shutdown` (now deferred → this would be force-aborted) and
        // NOT `sigterm_trigger` (fires at the start → would exit concurrently with producers).
        let vote_shutdown = epoch_task_manager.shutdown_subscriber();
        epoch_task_manager.spawn_classified_task("Collect Epoch Signatures", async move {
            let mut reached_quorum = false;
            let mut timeout = Duration::from_secs(5);
            let mut timeouts = 0;
            let mut alt_recs: HashMap<B256, VotesAggregator<EpochVote>> = HashMap::default();
            loop {
                // Break promptly when the consumer phase is signalled, rather than only
                // noticing after the recv timeout. `biased` so shutdown wins over a vote arriving.
                let result = tokio::select! {
                    biased;
                    _ = &vote_shutdown => break,
                    res = tokio::time::timeout(timeout, rx.recv()) => res,
                };
                match result {
                    Ok(Some((vote, vote_tx))) => {
                        if let Some(source) =
                            Self::signed_by_committee(&committee, &vote, epoch_hash)
                        {
                            let _ = vote_tx.send(Ok(())); // If we lost this channel somehow then no big deal.
                            if committee_keys.remove(&source) {
                                sigs.push(vote.signature);
                                if let Some(idx) = committee_index.get(&source) {
                                    signed_authorities.insert(*idx as u32);
                                }
                                if signed_authorities.len() >= quorum as u64 {
                                    reached_quorum = true;
                                    // We have quorum so just wait a sec longer for new certs then
                                    // move on.
                                    timeout = Duration::from_secs(1);
                                }
                                if signed_authorities.len() >= committee_size {
                                    break;
                                }
                            }
                        } else {
                            // Send an error back to punish the peer that sent a bad epoch vote.
                            let err = if vote.epoch_hash != epoch_hash {
                                if committee.contains(&vote.public_key)
                                    && vote.check_signature()
                                {
                                    // track votes on alternate epoch records per-validator;
                                    // break on quorum. per-validator tracking prevents inflation.
                                    const MAX_ALT_RECS: usize = 100;
                                    let reached_alt_quorum = if alt_recs.len() < MAX_ALT_RECS
                                        || alt_recs.contains_key(&vote.epoch_hash)
                                    {
                                        let agg = alt_recs.entry(vote.epoch_hash).or_insert_with(
                                            || VotesAggregator::new(quorum as u64),
                                        );
                                        agg.append(vote, 1).unwrap_or(false)
                                    } else {
                                        false
                                    };
                                    if reached_alt_quorum {
                                        error!(
                                            target: "epoch-manager",
                                            "Reached quorum on epoch record {} instead of {}.",
                                            vote.epoch_hash,
                                            epoch_hash,
                                        );
                                        if let Err(err) = vote_tx.send(Err(HeaderError::InvalidHeaderDigest)) {
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
                                }
                                HeaderError::InvalidHeaderDigest
                            } else if committee.contains(&vote.public_key) {
                                HeaderError::UnknownAuthority(format!(
                                    "{} not in the committee for epoch {epoch_hash}",
                                    vote.public_key
                                ))
                            } else {
                                HeaderError::PeerNotAuthor
                            };
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
                    }
                    Ok(None) => break, // channel issues...
                    Err(_) => {
                        // timed out with quorum reached, or failed after a minute;
                        // break and try to request the cert instead. (Shutdown is handled by
                        // the select arm above, not polled here.)
                        if reached_quorum || timeouts > 12 {
                            break;
                        }
                        timeouts += 1;

                        // epoch record collector may have fetched the cert in the background
                        if consensus_db.get_epoch_by_number(epoch_rec.epoch).is_some_and(|(_, c)| c.is_some()) {
                            info!(
                                target: "epoch-manager",
                                "epoch cert for {epoch_hash} appeared in DB during vote collection, exiting early",
                            );
                            return;
                        }
                        // Timed out, maybe we are not the only ones having issues so republish.
                        if let Some(vote) = my_vote {
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

            if reached_quorum {
                info!(
                    target: "epoch-manager",
                    "reached quorum on epoch close for {epoch_hash}",
                );

                let Ok(aggregated_signature) = BlsAggregateSignature::aggregate(&sigs[..], true) else {
                    error!(
                        target: "epoch-manager",
                        "failed to aggregate epoch record signatures for {epoch_hash}",
                    );
                    return;
                };

                let signature: BlsSignature = aggregated_signature.to_signature();
                let cert = EpochCertificate { epoch_hash, signature, signed_authorities };
                // Sanity check that we have generated a valid cert before saving.
                if !epoch_rec.verify_with_cert(&cert) {
                    error!(
                        target: "epoch-manager",
                        "failed to verify epoch record and cert for {epoch_hash}",
                    );
                    return;
                }

                if let Err(err) = consensus_db.save_epoch_record_with_cert(&epoch_rec, &cert) {
                    error!(
                        target: "epoch-manager",
                        ?err,
                        "Failed to insert epoch record and cert for {epoch_hash}",
                    );
                }
            } else {
                error!(
                    target: "epoch-manager",
                    "failed to reach quorum on epoch close for {epoch_hash} {epoch_rec:?}",
                );
                let epoch = epoch_rec.epoch;

                // ask up to peer count in the case we get a different hash
                let connected_peers_count = primary_network.connected_peers_count().await.unwrap_or(0);
                for _ in 0..connected_peers_count.min(3) {
                    // Request by epoch number in case we had a bad hash...
                    let Ok((new_epoch_rec, cert)) =
                        primary_network.request_epoch_cert(Some(epoch), None).await else {
                        error!(
                            target: "epoch-manager",
                            ?epoch_hash,
                            "failed to retrieve epoch from a peer",
                        );

                        continue;
                    };

                    // invalid epoch record or cert, skip
                    if !new_epoch_rec.verify_with_cert(&cert)
                        || !epoch_committee_valid(&new_epoch_rec, &committee)
                        || new_epoch_rec.parent_hash != epoch_rec.parent_hash {
                        continue;
                    }

                    let new_epoch_hash = new_epoch_rec.digest();

                    // if we found the correct hash, save it and RETURN here
                    if new_epoch_hash == epoch_hash {
                        info!(
                            target: "epoch-manager",
                            "retrieved cert for epoch {new_epoch_hash} from a peer",
                        );
                        if let Err(err) = consensus_db.save_epoch_record_with_cert(&new_epoch_rec, &cert) {
                            error!(
                                target: "epoch-manager",
                                ?err,
                                "Failed to insert epoch record and cert for {new_epoch_hash}",
                            );
                        }
                        return;
                    }

                    // Humm, we got another epoch record than the one we expected...
                    // The network came to quorum on this one so lets go with it...
                    warn!(
                        target: "epoch-manager",
                        "Received wrong epoch record: {new_epoch_hash}, expected {epoch_hash}",
                    );

                    // if most of the peers return this, probably we got the wrong hash
                    // save it and return here
                    if let Err(e) = consensus_db.save_epoch_record_with_cert(&new_epoch_rec, &cert) {
                        error!(
                            target: "epoch-manager",
                            "failed to save epoch record with cert: {e}",
                        );
                    }

                    return;
                }

                // if we didn't return before, means we didn't find it
                error!(
                    target: "epoch-manager",
                    "Failed to retrieve an epoch record for epoch {}", epoch_rec.epoch,
                );
            }
        }, TaskKind::Drainable);
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
    /// waits until `anchor.number >= boundary_output.number` — a monotonic, drop-free completion
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
            // Wait for the next anchor advance. An error means the sender was dropped — the engine
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
