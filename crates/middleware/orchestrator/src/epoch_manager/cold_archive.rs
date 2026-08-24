//! Background cold archival: a node-scoped actor owns the whole lifecycle.
//!
//! Each due epoch is archived during the live epoch on a de-prioritized OS thread, so nothing
//! archival runs on the boundary. A crashed pass heals at the next boot reconcile; a failed one
//! is logged and retried past its cooldown.

use std::{
    path::Path,
    sync::{Arc, OnceLock},
};

use prometheus::{
    default_registry, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, IntCounter, Registry,
};
use rayls_infrastructure_storage::{
    cold::ColdError, cold_archiver_for, ColdArchiver, ColdArchiverType, DatabaseType, SealOutcome,
};
use rayls_infrastructure_types::{
    ConsensusHeader, Database, Epoch, Notifier, TaskKind, TaskManager,
};
use reth_db::lockfile::StorageLock;
use tokio::sync::{oneshot, watch};
use tracing::{error, info, warn};

/// Acquires the process-exclusivity lock on the consensus-DB directory: the cold tier assumes a
/// single archiver process (a concurrent writer could truncate a torn jar under a reconcile).
/// Stale locks (dead PID) are evicted; a live foreign holder fails closed.
pub(crate) fn acquire_consensus_db_lock(consensus_db_dir: &Path) -> eyre::Result<StorageLock> {
    StorageLock::try_acquire(consensus_db_dir).map_err(|e| {
        eyre::eyre!("consensus DB at {consensus_db_dir:?} is locked by another process: {e}")
    })
}

/// The node's cold-archival aggregate: one entry point over the backend's [`ColdArchiver`],
/// owning boot healing, the first-start backlog migration, and the background seal actor.
///
/// Wraps the cfg-selected [`ColdArchiverType`]; a stack without a cold tier (redb) yields an
/// inert aggregate whose operations no-op, so callers carry no per-backend branching.
#[derive(Clone, Debug)]
pub(crate) struct ColdArchival {
    archiver: ColdArchiverType,
}

impl ColdArchival {
    /// Builds the aggregate over `db`'s cold tier; inert when the backend has none.
    pub(crate) fn new(db: &DatabaseType) -> Self {
        Self { archiver: cold_archiver_for(db) }
    }

    /// Returns whether the stack has a cold tier to archive into.
    pub(crate) fn is_active(&self) -> bool {
        self.archiver.is_some()
    }

    /// Heals an archive interrupted by a crash. Run once at boot, before serving.
    ///
    /// The node's boot path treats a failure as best-effort (logged, healed at the next boot),
    /// while `cold-migrate` propagates it to the exit code.
    pub(crate) async fn reconcile_at_boot(&self) -> eyre::Result<()> {
        let Some(archiver) = &self.archiver else { return Ok(()) };
        let started = std::time::Instant::now();
        let archiver = Arc::clone(archiver);
        match tokio::task::spawn_blocking(move || archiver.reconcile()).await {
            Ok(result) => {
                if result.is_ok() {
                    info!(target: "epoch-manager", elapsed = ?started.elapsed(), "cold boot reconcile complete");
                }
                result
            }
            Err(e) => Err(eyre::eyre!("cold reconcile task panicked: {e}")),
        }
    }

    /// Drains the first-start backlog into cold in bounded chunks, before the node serves.
    ///
    /// Each chunk archives at most [`BOOT_BACKLOG_CHUNK`] epochs and resumes past the persisted
    /// high-water mark, so a large DB never materializes its whole history at once.
    ///
    /// # Errors
    ///
    /// Returns the first failed chunk's error. The drain is resumable, so a caller may retry.
    pub(crate) async fn migrate_backlog(&self, el_anchor_epoch: Epoch) -> eyre::Result<()> {
        let Some(archiver) = &self.archiver else { return Ok(()) };
        loop {
            let archiver = Arc::clone(archiver);
            let started = std::time::Instant::now();
            let pass = tokio::task::spawn_blocking(move || {
                archiver.archive_due(el_anchor_epoch, Some(BOOT_BACKLOG_CHUNK))
            })
            .await;
            match pass {
                Ok(Ok(stats)) => {
                    if stats.epochs_sealed > 0 {
                        info!(
                            target: "epoch-manager",
                            epochs = stats.epochs_sealed,
                            blocks = stats.blocks_archived,
                            batches = stats.batches_archived,
                            elapsed = ?started.elapsed(),
                            "cold boot backlog chunk",
                        );
                    }
                    // A short (or empty) chunk means the backlog below the cutoff is drained.
                    if (stats.epochs_sealed as usize) < BOOT_BACKLOG_CHUNK {
                        return Ok(());
                    }
                }
                Ok(Err(e)) => return Err(e.wrap_err("cold boot backlog chunk failed")),
                Err(e) => return Err(eyre::eyre!("cold boot backlog task panicked: {e}")),
            }
        }
    }

    /// Spawns the sole owner of cold archival, woken by the `executed_anchor` watch and gated on
    /// its leader epoch changing, so steady state is one pass per epoch.
    ///
    /// `shutdown` doubles as the cancel flag at chunk seams. The actor is [`TaskKind::Drainable`]
    /// so teardown joins its blocking pass rather than aborting mid-pass and detaching a thread
    /// that still holds a database handle.
    ///
    /// `fatal_db_error` is a `watch` that is overwritten on `send`; both the writer thread
    /// (`layered_db::trip`) and this actor may signal it, but `send_if_modified` preserves the
    /// first (root-cause) error so `core.rs` logs the original failure.
    pub(crate) fn spawn_actor(
        &self,
        task_manager: &TaskManager,
        mut anchor_rx: watch::Receiver<ConsensusHeader>,
        shutdown: Notifier,
        fatal_db_error: watch::Sender<Option<String>>,
    ) {
        let Some(archiver) = self.archiver.clone() else { return };
        let noticer = shutdown.subscribe();
        task_manager.spawn_classified_task(
            "Cold Seal Actor",
            async move {
                let mut sealed_for: Option<Epoch> = None;
                let mut retry_at = tokio::time::Instant::now();
                loop {
                    // The cutoff advances only with the anchor's leader epoch, so per-output
                    // anchor ticks do not each cost a blocking pass.
                    let anchor_epoch = anchor_rx.borrow_and_update().sub_dag.leader_epoch();
                    if sealed_for != Some(anchor_epoch) && tokio::time::Instant::now() >= retry_at {
                        // Only a drained pass closes the epoch out; anything else is retried, but
                        // no sooner than the cooldown (see [`FAILED_PASS_COOLDOWN`]). A pass the
                        // shutdown flag stopped is not a failure, so it must not arm the cooldown:
                        // a later non-teardown cancel source would otherwise stall archival.
                        if seal_due_epochs(&archiver, anchor_epoch, &shutdown, &fatal_db_error)
                            .await
                        {
                            sealed_for = Some(anchor_epoch);
                        } else if !shutdown.was_notified() {
                            retry_at = tokio::time::Instant::now() + FAILED_PASS_COOLDOWN;
                        }
                    }
                    // A write error was signaled: the node is winding down gracefully (run()'
                    // handles the ordered teardown), so this actor must NOT arm the retry
                    // cooldown and keep sealing into a failing tier.
                    if fatal_db_error.borrow().is_some() {
                        return;
                    }
                    tokio::select! {
                        biased;
                        // Priority is semantic: shutdown must win, so teardown never waits on
                        // another seal pass being started.
                        _ = &noticer => return,
                        changed = anchor_rx.changed() => {
                            // A closed watch means the bus is tearing down, not a fault.
                            if changed.is_err() {
                                return;
                            }
                        }
                    }
                }
            },
            TaskKind::Drainable,
        );
    }
}

/// Epochs migrated per chunk during the first-start backlog drain: caps peak memory and keeps a
/// multi-million-block backlog draining in resumable steps.
const BOOT_BACKLOG_CHUNK: usize = 64;

/// Quiet period before a failed pass is retried.
///
/// Well above one epoch's seal, so an epoch that can never seal costs only a small duty cycle
/// instead of re-walking itself back to back; far below an epoch, so a transient fault heals
/// long before the anchor moves on.
const FAILED_PASS_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(300);

/// Prometheus metrics for the background cold archival, on the node's scrape registry.
struct ColdArchiveMetrics {
    /// Wall-clock to fully archive one epoch (seal + index + high-water mark + yielding prune).
    archive_duration_seconds: Histogram,
    /// Epochs fully archived (sealed and pruned) by the background actor.
    cold_epochs_archived: IntCounter,
    /// Failed or panicked archive passes (best-effort: retried on later wakes).
    cold_archive_failures: IntCounter,
}

impl ColdArchiveMetrics {
    /// Registers the metrics against `registry`, erroring on a double registration.
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        // Buckets span fast NVMe (tens of ms) to IO-starved cloud volumes (minutes).
        let buckets = vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0];
        Ok(Self {
            archive_duration_seconds: register_histogram_with_registry!(
                "cold_archive_duration_seconds",
                "Wall-clock to fully archive one epoch (seal plus finalize plus prune)",
                buckets,
                registry,
            )?,
            cold_epochs_archived: register_int_counter_with_registry!(
                "cold_epochs_archived",
                "Epochs fully archived (sealed and pruned) by the background actor",
                registry,
            )?,
            cold_archive_failures: register_int_counter_with_registry!(
                "cold_archive_failures",
                "Failed or panicked background archive passes",
                registry,
            )?,
        })
    }

    /// Returns the process-wide instance, registered on first use.
    ///
    /// Registers on the shared [`default_registry`] the node's endpoint scrapes; a failure there
    /// is a double registration (a second node built in one process, as tests do), so it falls
    /// back to a private registry so construction still succeeds, just unscraped.
    fn get() -> &'static Self {
        static METRICS: OnceLock<ColdArchiveMetrics> = OnceLock::new();
        METRICS.get_or_init(|| {
            Self::try_new(default_registry()).unwrap_or_else(|_| {
                warn!(
                    target: "epoch-manager",
                    "cold archive metrics already registered; falling back to a private registry \
                     that no endpoint scrapes"
                );
                Self::try_new(&Registry::new()).expect("cold archive metrics on a fresh registry")
            })
        })
    }
}

/// Fully archives every newly due epoch (seal + finalize + yielding prune), one blocking pass each.
///
/// Returns whether the backlog below the cutoff was drained; a spawn failure, a failed pass, a
/// panicked pass and a cancelled pass all return `false`, leaving the epoch due for a retry.
/// A hot-tier write failure instead signals `fatal_db_error` and returns `false`: the node winds
/// down gracefully (keeping the durable tier), and the actor exits without retrying.
async fn seal_due_epochs<DB: Database>(
    archiver: &Arc<ColdArchiver<DB>>,
    el_anchor_epoch: Epoch,
    shutdown: &Notifier,
    fatal_db_error: &watch::Sender<Option<String>>,
) -> bool {
    loop {
        // Checked before each pass, not only at a chunk seam: a shutdown landing on a completed
        // seal would otherwise spawn one more thread whose only work is to cancel itself.
        if shutdown.was_notified() {
            return false;
        }
        let archiver = Arc::clone(archiver);
        let cancel = shutdown.clone();
        let started = std::time::Instant::now();
        let metrics = ColdArchiveMetrics::get();
        // A dedicated de-prioritized OS thread, not the shared tokio blocking pool: a pool
        // thread's priority must not be lowered (it is reused for unrelated work).
        let (tx, rx) = oneshot::channel();
        let spawned = std::thread::Builder::new().name("cold-seal".to_string()).spawn(move || {
            lower_current_thread_priority();
            let outcome = archiver.seal_due(el_anchor_epoch, move || cancel.was_notified());
            // A dropped receiver (the actor was force-aborted past its drain phase) just
            // discards the outcome.
            let _ = tx.send(outcome);
        });
        let pass = match spawned {
            Ok(pass) => pass,
            Err(e) => {
                metrics.cold_archive_failures.inc();
                warn!(target: "epoch-manager", "cold seal thread spawn failed: {e}");
                return false;
            }
        };
        // A recv error means the thread panicked and dropped the sender before replying.
        let outcome = rx.await;
        // Reap the thread, not just its reply: the pass holds its own database handle, and only
        // the join proves that handle is released before teardown drops the last one. Off the
        // runtime because a panicking pass drops its sender during unwind, so the reply can arrive
        // while the thread is still running its panic hook.
        let _ = tokio::task::spawn_blocking(move || pass.join()).await;
        match outcome {
            Ok(Ok(SealOutcome::Sealed(epoch))) => {
                metrics.archive_duration_seconds.observe(started.elapsed().as_secs_f64());
                metrics.cold_epochs_archived.inc();
                info!(
                    target: "epoch-manager",
                    epoch,
                    elapsed = ?started.elapsed(),
                    "cold epoch archived",
                );
            }
            Ok(Ok(SealOutcome::Drained)) => return true,
            // A cancel is a teardown, not a failure: the epoch stays due, and a later pass or
            // the boot reconcile completes it.
            Ok(Ok(SealOutcome::Cancelled)) => return false,
            Ok(Err(e)) => {
                metrics.cold_archive_failures.inc();
                // A hot-tier durability failure is fatal, but the node shuts down GRACEFULLY
                // rather than panicking: the durable tier is the last known valid data, and
                // losing the mem cache and any uncommitted mdbx transaction is acceptable.
                // Signal the epoch manager (run() requests the ordered wind-down and exits
                // non-zero) and stop retrying — retrying the same epoch only pins more rows in
                // the failing cache, and the process exit discards them anyway. Any other pass
                // failure (corruption, contiguity) stays retriable after the cooldown.
                if e.downcast_ref::<ColdError>()
                    .is_some_and(|err| matches!(err, ColdError::WriteFailed(_)))
                {
                    error!(
                        target: "epoch-manager",
                        ?e,
                        "cold epoch archive: hot-tier write not durable, requesting graceful node shutdown"
                    );
                    // Preserve the first fatal signal (root cause). The writer thread's `trip()`
                    // may have already sent the original `write_txn`/`commit` failure; `watch`
                    // overwrites on `send`, so only set if still `None`.
                    let _ = fatal_db_error.send_if_modified(|cur| {
                        if cur.is_none() {
                            *cur = Some(format!("{e:?}"));
                            true
                        } else {
                            false
                        }
                    });
                    return false;
                }
                warn!(target: "epoch-manager", "cold epoch archive failed: {e}");
                return false;
            }
            Err(_) => {
                metrics.cold_archive_failures.inc();
                warn!(target: "epoch-manager", "cold epoch archive panicked");
                return false;
            }
        }
    }
}

/// Best-effort lowers the calling OS thread's scheduling priority so a seal pass yields to live
/// consensus: macOS background QoS, Linux nice 19. Errors are ignored; non-Unix is a no-op.
#[cfg(unix)]
fn lower_current_thread_priority() {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: sets only this thread's QoS class, passes no pointers, and reports failure via
        // the return value, which is intentionally ignored (best-effort).
        unsafe {
            libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_BACKGROUND, 0);
        }
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: setpriority(PRIO_PROCESS, 0, ..) targets the calling thread on Linux (nice is
        // per-thread there); it passes no pointers and its error is intentionally ignored.
        unsafe {
            libc::setpriority(libc::PRIO_PROCESS as _, 0, 19);
        }
    }
}

/// Non-Unix platforms have no cheap per-thread priority knob; the pass runs at normal priority.
#[cfg(not(unix))]
fn lower_current_thread_priority() {}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::{
        cold_archiver_for,
        mdbx::MdbxConfig,
        open_db, open_db_with_consensus_config,
        tables::{Batches, ColdBatchLocations, ConsensusBlocks},
        DatabaseType,
    };
    use rayls_infrastructure_types::{
        Batch, BlockHash, Certificate, CommittedSubDag, ConsensusHeader, DbTx, DbTxMut, Epoch,
        Header, ReputationScores,
    };
    use tempfile::TempDir;

    /// Node-order teardown around the seal actor: the cancel flag stops an in-flight blocking
    /// pass at its next chunk seam (uncommitted, so the epoch re-seals whole later), and the
    /// task-manager join reaps the actor only once that pass has been joined, so no thread
    /// outlives teardown still holding a database handle. Only then do the final handle drops
    /// join the layered DB writer thread.
    #[tokio::test]
    async fn seal_actor_joins_cancelled_pass_before_db_teardown() {
        let tmp = TempDir::new().unwrap();
        let db = open_db(tmp.path());
        // One large epoch (chunks close at 64 fresh batch digests, so ~100 chunk seams) plus two
        // marker epochs that make epoch 0 due. The seam count is what lets the cancel flag stop
        // the pass mid-seal; a seamless epoch would only test the actor-future abort.
        const BLOCKS: u64 = 6_400;
        db.with_write_txn(|txn| {
            for number in 0..BLOCKS {
                let digest = digest_for(number);
                txn.insert::<Batches>(&digest, &batch_for(number))?;
                txn.insert::<ConsensusBlocks>(&number, &header_with_batch(number, 0, digest))?;
            }
            txn.insert::<ConsensusBlocks>(&BLOCKS, &header_for(BLOCKS, 1))?;
            txn.insert::<ConsensusBlocks>(&(BLOCKS + 1), &header_for(BLOCKS + 1, 2))?;
            Ok(())
        })
        .expect("seed");
        db.sync_persist().expect("persist");

        let archiver = cold_archiver_for(&db).expect("mdbx stack has a cold tier");
        let shutdown = Notifier::new();
        let mut task_manager = TaskManager::new("seal-actor-teardown-test");
        task_manager.set_join_wait(500);
        let (anchor_tx, anchor_rx) = watch::channel(ConsensusHeader::default());
        // A dropped temporary: the aggregate must not hold a lasting archiver clone, or the
        // strong-count probes below (pass in flight, pass exited) would be off by one.
        ColdArchival { archiver: Some(Arc::clone(&archiver)) }.spawn_actor(
            &task_manager,
            anchor_rx,
            shutdown.clone(),
            watch::channel(None).0,
        );

        // Wake the actor and wait until a blocking pass is provably in flight: the pass's
        // closure holds its own archiver clone (count: ours + the actor's + the pass's).
        anchor_tx.send_replace(header_for(BLOCKS + 2, 1_000));
        while Arc::strong_count(&archiver) < 3 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        // Teardown in node order: cancel flag first, then the drain-mode join reaps the actor.
        shutdown.notify();
        task_manager.join(shutdown.clone()).await.expect("drain-mode join");

        // The join is the barrier: a pass still running here would hold a database handle past
        // the node's final flush, so the writer thread is neither told to stop nor joined.
        assert_eq!(
            Arc::strong_count(&archiver),
            1,
            "the seal pass must be joined before the actor is reaped"
        );
        // A finalize now would index any sealed jar; a cancelled pass left nothing to index.
        archiver.reconcile().expect("finalize probe");
        assert!(!is_archived(&db), "a cancelled mid-seal pass must not commit its epoch");

        // Only now is the writer thread joined: the final drops complete instead of hanging
        // (LayeredDatabase joins its db_run thread on the last handle drop).
        drop(archiver);
        drop(db);
    }

    /// Builds a unique batch digest for `number`.
    fn digest_for(number: u64) -> BlockHash {
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&number.to_be_bytes());
        seed[31] = 0xA5;
        BlockHash::from(seed)
    }

    /// Builds a minimal batch stored under a header's payload digest.
    fn batch_for(number: u64) -> Batch {
        Batch { transactions: vec![vec![number as u8]], seq: number, ..Default::default() }
    }

    /// Builds a consensus header whose certificate payload references `digest`.
    fn header_with_batch(number: u64, epoch: Epoch, digest: BlockHash) -> ConsensusHeader {
        let payload = std::iter::once((digest, 0u16)).collect();
        let mut leader = Certificate::default();
        leader.header = Header { epoch, ..Default::default() };
        let mut cert = Certificate::default();
        cert.header = Header { epoch, payload, ..Default::default() };
        let sub_dag =
            CommittedSubDag::new(vec![cert], leader, 0, ReputationScores::default(), None);
        ConsensusHeader { sub_dag, number, ..Default::default() }
    }

    /// Builds a consensus header with an empty payload whose sub-dag leader fixes `epoch`.
    fn header_for(number: u64, epoch: Epoch) -> ConsensusHeader {
        let mut leader = Certificate::default();
        leader.header = Header { epoch, ..Default::default() };
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusHeader { sub_dag, number, ..Default::default() }
    }

    /// A failed pass must not close its epoch out: once the cause clears the next wake past the
    /// cooldown archives it, rather than stalling until the anchor's epoch changes.
    ///
    /// The clock is paused, so polling yields rather than sleeps and cannot auto-advance it.
    #[tokio::test(start_paused = true)]
    async fn failed_pass_is_retried_after_its_cooldown() {
        let tmp = TempDir::new().unwrap();
        let db = open_db(tmp.path());
        // Epoch 0 is gapped (blocks 0 and 2), which aborts its seal on the contiguity check;
        // epoch 2's block makes epoch 0 due once the anchor reaches epoch 2.
        db.with_write_txn(|txn| {
            for number in [0, 2] {
                let digest = digest_for(number);
                txn.insert::<Batches>(&digest, &batch_for(number))?;
                txn.insert::<ConsensusBlocks>(&number, &header_with_batch(number, 0, digest))?;
            }
            txn.insert::<ConsensusBlocks>(&3, &header_for(3, 2))?;
            Ok(())
        })
        .expect("seed");
        db.sync_persist().expect("persist");

        let shutdown = Notifier::new();
        let mut task_manager = TaskManager::new("seal-actor-retry-test");
        task_manager.set_join_wait(500);
        let (anchor_tx, anchor_rx) = watch::channel(ConsensusHeader::default());
        ColdArchival::new(&db).spawn_actor(
            &task_manager,
            anchor_rx,
            shutdown.clone(),
            watch::channel(None).0,
        );

        let failures = || ColdArchiveMetrics::get().cold_archive_failures.get();
        let before = failures();
        anchor_tx.send_replace(header_for(4, 2));
        while failures() == before {
            tokio::task::yield_now().await;
        }

        // The cause clears, but the cooldown still holds the retry back.
        db.with_write_txn(|txn| {
            let digest = digest_for(1);
            txn.insert::<Batches>(&digest, &batch_for(1))?;
            txn.insert::<ConsensusBlocks>(&1, &header_with_batch(1, 0, digest))
        })
        .expect("repair the gap");
        db.sync_persist().expect("persist");
        anchor_tx.send_replace(header_for(5, 2));
        for _ in 0..1_000 {
            tokio::task::yield_now().await;
        }
        assert!(
            !is_archived(&db),
            "a retry inside the cooldown would re-walk the epoch on every tick"
        );

        tokio::time::advance(FAILED_PASS_COOLDOWN).await;
        anchor_tx.send_replace(header_for(6, 2));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !is_archived(&db) {
            assert!(
                std::time::Instant::now() < deadline,
                "a failed pass must retry once its cooldown expires"
            );
            tokio::task::yield_now().await;
        }

        shutdown.notify();
        task_manager.join(shutdown.clone()).await.expect("drain-mode join");
    }

    /// Returns whether epoch 0's archive committed, probed through the hot-only auxiliary index.
    fn is_archived(db: &DatabaseType) -> bool {
        db.with_read_txn(|tx| tx.contains_key::<ColdBatchLocations>(&digest_for(0)))
            .expect("probe aux index")
    }

    /// A hot-tier durability failure must signal the fatal channel (the epoch manager then winds
    /// the node down gracefully, keeping the durable tier) instead of panicking: losing the mem
    /// cache and any uncommitted mdbx transaction is acceptable.
    #[tokio::test]
    async fn write_failure_signals_fatal_shutdown_not_a_panic() {
        let tmp = TempDir::new().unwrap();
        let cfg = MdbxConfig::default().with_max_db_size(1024 * 1024).with_growth_step(256 * 1024);
        let db = open_db_with_consensus_config(tmp.path(), &cfg);

        // Seed one small due epoch (blocks 0-2 in epoch 0) plus marker epochs making it due.
        db.with_write_txn(|txn| {
            for number in 0..3u64 {
                let digest = digest_for(number);
                txn.insert::<Batches>(&digest, &batch_for(number))?;
                txn.insert::<ConsensusBlocks>(&number, &header_with_batch(number, 0, digest))?;
            }
            txn.insert::<ConsensusBlocks>(&3, &header_for(3, 1))?;
            txn.insert::<ConsensusBlocks>(&4, &header_for(4, 2))?;
            Ok(())
        })
        .expect("seed");
        db.sync_persist().expect("persist");

        // Overflow the map: the writer's commit of this oversized txn fails with MAP_FULL and
        // poisons the DB. Queueing may itself fail mid-batch — once the writer trips, the
        // producer's in-flight inserts fail fast — but the DB is poisoned either way. The junk
        // never commits, so the seal's reads stay clean; the archival pass then fails on the
        // poisoned DB and surfaces the failure as a `WriteFailed`.
        let big = Batch { transactions: vec![vec![0u8; 4096]], ..Default::default() };
        match db.with_write_txn(|txn| {
            for i in 0..4_000u64 {
                txn.insert::<Batches>(&digest_for(10_000 + i), &big)?;
            }
            Ok(())
        }) {
            Ok(()) => {}
            Err(e) => assert!(
                e.to_string().contains("poisoned"),
                "a mid-batch queueing failure must be the poison, got: {e}"
            ),
        }

        let archiver = cold_archiver_for(&db).expect("mdbx stack has a cold tier");
        let shutdown = Notifier::new();
        let (fatal_tx, fatal_rx) = watch::channel(None);
        let drained = seal_due_epochs(&archiver, 3, &shutdown, &fatal_tx).await;
        assert!(!drained, "a failed pass must not report its epoch drained");
        assert!(
            fatal_rx.borrow().is_some(),
            "a hot-tier durability failure must signal the graceful node shutdown, not panic"
        );
    }

    /// A failed archive chunk must surface as an error (the CLI's exit code), never be swallowed
    /// into a success: a gapped epoch fails the seal's contiguity check mid-drain.
    #[tokio::test]
    async fn migrate_backlog_propagates_chunk_failure() {
        let tmp = TempDir::new().unwrap();
        let db = open_db(tmp.path());
        // Epoch 0 has a numbering gap (blocks 0 and 2); epoch 2's block makes it due for sealing.
        db.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&0, &header_for(0, 0))?;
            txn.insert::<ConsensusBlocks>(&2, &header_for(2, 0))?;
            txn.insert::<ConsensusBlocks>(&3, &header_for(3, 2))?;
            Ok(())
        })
        .expect("seed");
        db.sync_persist().expect("persist");

        let archival = ColdArchival::new(&db);
        let result = archival.migrate_backlog(2).await;
        assert!(result.is_err(), "failed chunk must propagate, got {result:?}");
    }

    /// The consensus-DB lock fails closed against a live foreign holder and evicts a stale one.
    #[test]
    fn consensus_db_lock_refuses_live_foreign_holder() {
        let tmp = TempDir::new().unwrap();

        // Fabricate a lock held by a live foreign process (any but ours): the lockfile stores
        // exactly `pid\nstart_time`, and liveness matches both.
        let own = std::process::id();
        let system = sysinfo::System::new_all();
        // Minimal container environments can hide every other process from sysinfo; the test's
        // premise is gone, so skip rather than abort.
        let Some((pid, start_time)) = system
            .processes()
            .iter()
            .find(|(pid, _)| pid.as_u32() != own)
            .map(|(pid, process)| (pid.as_u32(), process.start_time()))
        else {
            eprintln!("skipping: no other live process visible");
            return;
        };
        std::fs::write(tmp.path().join("lock"), format!("{pid}\n{start_time}")).unwrap();
        assert!(
            acquire_consensus_db_lock(tmp.path()).is_err(),
            "a live foreign holder must fail closed"
        );

        // A stale holder (dead PID) is evicted and acquisition succeeds.
        std::fs::write(tmp.path().join("lock"), format!("{}\n{}", u32::MAX, 0)).unwrap();
        let _lock = acquire_consensus_db_lock(tmp.path()).expect("stale lock must be evicted");
    }
}
