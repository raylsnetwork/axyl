use crate::{
    engine::ExecutionNode,
    epoch_manager::types::EpochManager,
    primary::PrimaryNode,
    types::{EpochTransitionCheckpoint, EpochTransitionPhase, ShutdownOutcome, TransitionCtx},
};
use eyre::eyre;
use rayls_consensus_primary::NodeMode;
use rayls_infrastructure_config::RaylsDirs;
use rayls_infrastructure_storage::{
    tables::{EpochTransitionCheckpoints, LastProposed, LastProposedByAuthority},
    CheckpointStore,
};
use rayls_infrastructure_types::{
    BlockHash, ConsensusHeader, ConsensusOutput, Database as ReDatabase, Epoch, Notifier,
    TaskManager, B256,
};
use std::time::Duration;
use tracing::{error, info, warn};

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Shared shutdown helper used by both epoch and mode transitions.
    ///
    /// Sends the drain signal (if applicable), shuts down consensus tasks,
    /// joins the task manager with a timeout, and waits for the subscriber
    /// drain acknowledgement.
    pub(super) async fn controlled_shutdown(
        &mut self,
        consensus_shutdown: Notifier,
        epoch_task_manager: &mut TaskManager,
        drain_round: Option<u32>,
        join_timeout: Duration,
    ) -> ShutdownOutcome {
        let needs_drain =
            drain_round.is_some() && self.consensus_bus.node_mode().borrow().is_active_cvv();

        // Step 1: Send drain signal if requested and node is active CVV.
        if let Some(round) = drain_round {
            if needs_drain {
                if let Err(e) = self.consensus_bus.drain_signal().send(Some(round)) {
                    warn!(target: "epoch-manager", ?e, "failed to send drain signal - subscriber may have already exited");
                }
            }
        }

        // Step 2: Shut down consensus tasks.
        consensus_shutdown.notify();
        epoch_task_manager.abort_doomed_tasks();

        // Step 3: Join with timeout, abort remaining on timeout.
        match tokio::time::timeout(
            join_timeout,
            epoch_task_manager.join(consensus_shutdown.clone()),
        )
        .await
        {
            Ok(Ok(())) => info!(target: "epoch-manager", "tasks joined cleanly"),
            Ok(Err(e)) => {
                warn!(target: "epoch-manager", ?e, "task join returned error")
            }
            Err(_) => {
                warn!(
                    target: "epoch-manager",
                    "task manager join timed out after {join_timeout:?}, aborting remaining tasks"
                );
                epoch_task_manager.abort_all_tasks();
            }
        }

        // Step 4: Wait for drain ack if drain was sent.
        let mut drain_confirmed = false;
        if needs_drain {
            if let Some(drain_ack_rx) = self.consensus_bus.take_drain_ack_rx() {
                match tokio::time::timeout(Duration::from_secs(30), drain_ack_rx).await {
                    Ok(Ok(())) => {
                        info!(target: "epoch-manager", "subscriber drain confirmed");
                        drain_confirmed = true;
                    }
                    Ok(Err(_)) => {
                        warn!(target: "epoch-manager", "subscriber drain ack channel dropped, subscriber likely exited");
                    }
                    Err(_) => {
                        warn!(target: "epoch-manager", "drain ack timeout");
                    }
                }
            }
        } else {
            // No drain needed - treat as confirmed (nothing to drain).
            drain_confirmed = true;
        }

        ShutdownOutcome { drain_confirmed }
    }

    /// Wait for the engine to finish executing its admitted backlog before a mode transition
    /// completes.
    ///
    /// The engine and its `to_engine` channel are node-lifetime and shared across epochs, but only
    /// the *epoch* transition waits for execution (`await_epoch_execution`). Without a matching
    /// wait here, outputs the engine already admitted keep executing into the NEXT `run_epoch`,
    /// where they collide with that epoch's `get_missing_consensus` anchor snapshot: an output
    /// the engine finishes concurrently is dropped as stale (the demote→rejoin flap race).
    ///
    /// We wait on the engine's own `engine_idle` signal — `pending_task.is_none() &&
    /// queued.is_empty()`, i.e. it has executed everything it admitted — NOT on the recorded
    /// consensus tip. The producers are stopped before this runs (and the forwarder was cancelled
    /// by the run_epoch select even earlier), so nothing new can be admitted: the engine just
    /// finishes its fixed admitted queue and reports idle. Outputs recorded but never admitted
    /// (stranded in `consensus_output` when the forwarder was cancelled) are not in the engine,
    /// so they must NOT be waited on — they replay cleanly via the next epoch's
    /// `get_missing_consensus` (unadmitted ⇒ not stale-dropped).
    ///
    /// Normally this returns the instant the engine reports idle (immediately if it already is).
    /// The engine also publishes idle when its task exits, so a dying engine (shutdown /
    /// ConsensusFork / stream-close) unblocks us promptly. `backstop` only guards a genuinely
    /// *hung* engine that never publishes — it bounds the wait (matching the rest of the
    /// transition pipeline) and warns rather than stalling the transition forever.
    async fn drain_engine_backlog(&self, backstop: Duration) {
        let mut idle_rx = self.consensus_bus.engine_idle().subscribe();
        let wait = async {
            // The engine publishes idle=true when its queue empties (poll Pending path) and also
            // when its task exits (node_inner publishes on exit), so a dying engine unblocks us
            // promptly. `changed()` itself won't error on engine-task exit — the bus retains
            // `tx_engine_idle` for the node's lifetime, so the channel never closes — hence the
            // timeout below is the real backstop for a genuinely hung engine that never publishes.
            while !*idle_rx.borrow_and_update() {
                if idle_rx.changed().await.is_err() {
                    return; // all senders dropped (full node teardown)
                }
            }
        };

        if tokio::time::timeout(backstop, wait).await.is_err() {
            warn!(
                target: "epoch-manager",
                ?backstop,
                "engine-idle drain backstop fired before mode transition; engine did not report \
                 idle in time — proceeding (next epoch's get_missing_consensus replay covers any \
                 unexecuted remainder)"
            );
        }
    }

    /// Execute the epoch boundary transition pipeline.
    ///
    /// Linear phases: checkpoint → shutdown → execution → flush → finalize.
    pub(super) async fn run_epoch_transition(
        &mut self,
        target_hash: B256,
        boundary_output: ConsensusOutput,
        ctx: TransitionCtx<'_, DB>,
    ) -> eyre::Result<()> {
        let TransitionCtx {
            engine,
            to_engine,
            primary,
            consensus_shutdown,
            epoch_task_manager,
            gas_accumulator,
        } = ctx;

        let epoch = boundary_output.leader().epoch();
        let boundary_round = boundary_output.leader_round();

        // Phase 1: CHECKPOINT - record the transition intent.
        self.save_transition_checkpoint(
            epoch,
            EpochTransitionPhase::BoundaryDetected,
            target_hash,
        )?;
        info!(target: "epoch-manager", "phase 1/6: CHECKPOINT");

        // Phase 2: SHUTDOWN - drain + stop consensus tasks.
        let outcome = self
            .controlled_shutdown(
                consensus_shutdown,
                epoch_task_manager,
                Some(boundary_round),
                Duration::from_secs(60),
            )
            .await;

        if !outcome.drain_confirmed {
            self.clear_transition_checkpoint(epoch)?;
            return Err(eyre!(
                "subscriber drain timeout - epoch transition aborted to prevent data loss"
            ));
        }

        self.save_transition_checkpoint(
            epoch,
            EpochTransitionPhase::ConsensusShutdown,
            target_hash,
        )?;
        self.save_transition_checkpoint(epoch, EpochTransitionPhase::Draining, target_hash)?;
        // Flush the consensus DB now that consensus has stopped: the writer queue is bounded, so
        // this makes the transition checkpoints durable without stalling the cut (see phase 1).
        self.consensus_db.persist().await?;
        engine.flush_persistence().await?;
        info!(target: "epoch-manager", "phase 2/6: SHUTDOWN (drain confirmed)");

        // Phase 3: EXECUTION - send boundary output to engine, wait for execution.
        let execution_timeout = Duration::from_secs(180);
        match tokio::time::timeout(
            execution_timeout,
            self.await_epoch_execution(
                engine,
                to_engine,
                boundary_output,
                &gas_accumulator,
                target_hash,
            ),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                error!(
                    target: "epoch-manager",
                    ?epoch,
                    ?target_hash,
                    "CRITICAL: await_epoch_execution timed out after {execution_timeout:?}"
                );
                return Err(eyre!("await_epoch_execution timed out after {execution_timeout:?}"));
            }
        };

        self.save_transition_checkpoint(
            epoch,
            EpochTransitionPhase::ExecutionComplete,
            target_hash,
        )?;
        info!(target: "epoch-manager", "phase 3/6: EXECUTION");

        // Phase 4: FLUSH - ensure all deferred MDBX writes are on disk.
        engine.flush_persistence().await?;
        self.write_epoch_record(primary, engine, epoch, target_hash).await?;

        info!(target: "epoch-manager", "phase 4/6: PERSISTENCE_FLUSH");

        // Phase 5: FINALIZE - clear consensus DB and checkpoint.
        self.clear_consensus_db_for_next_epoch()?;
        self.clear_transition_checkpoint(epoch)?;
        self.consensus_db.persist().await?;

        // Phase 6: reset peer-derived signals.
        // Stale network head would make try_rejoin_consensus compare against the old epoch
        self.consensus_bus.last_consensus_header().send_replace(ConsensusHeader::default());
        self.consensus_bus
            .last_published_consensus_num_hash()
            .send_replace((0, BlockHash::default()));
        info!(target: "epoch-manager", "phase 5-6/6: FINALIZE");

        Ok(())
    }

    /// Execute a mode transition: shutdown -> flush -> apply.
    pub(super) async fn run_mode_transition(
        &mut self,
        target_mode: NodeMode,
        ctx: TransitionCtx<'_, DB>,
    ) -> eyre::Result<()> {
        let TransitionCtx { engine, consensus_shutdown, epoch_task_manager, .. } = ctx;

        let prior_mode = *self.consensus_bus.node_mode().borrow();
        info!(
            target: "epoch-manager",
            ?prior_mode,
            ?target_mode,
            "mode-change requested via mode_transition channel"
        );

        // Phase 1: SHUTDOWN (cert_manager drains internally)
        let _outcome = self
            .controlled_shutdown(
                consensus_shutdown,
                epoch_task_manager,
                None,
                Duration::from_secs(15),
            )
            .await;
        info!(target: "epoch-manager", ?target_mode, "mode-change phase 1/3: SHUTDOWN");

        // Let the engine finish executing what it already admitted before flipping mode, so the
        // next run_epoch starts with a settled anchor and get_missing_consensus has nothing
        // in-flight to race (the demote→rejoin flap stale-drop). Producers are stopped
        // above; we wait on the engine's idle signal (executed == admitted), NOT the
        // recorded tip — stranded-but-unadmitted outputs replay cleanly next epoch. Bounded by a
        // backstop so a hung engine warns and proceeds instead of stalling the transition forever.
        self.drain_engine_backlog(Duration::from_secs(15)).await;

        // Phase 2: PERSISTENCE_FLUSH + clear LastProposed; a stale header would be
        // reproposed at the same (round, epoch) with outdated parents, causing a fork
        // if transition from Inactive to Active then it is safe to keep the LastPropose as there is
        // so stale headers
        if prior_mode != NodeMode::CvvInactive || target_mode != NodeMode::CvvActive {
            self.consensus_db.clear_table::<LastProposed>()?;
            self.consensus_db.clear_table::<LastProposedByAuthority>()?;
        }
        engine.flush_persistence().await?;
        info!(target: "epoch-manager", ?target_mode, "mode-change phase 2/3: PERSISTENCE_FLUSH");

        // Phase 3: APPLY - switch node mode, clear transition request, clear guard.
        // skip the mode write if caller tried to demote Observer
        if prior_mode == NodeMode::Observer && target_mode != NodeMode::Observer {
            warn!(target: "epoch-manager", ?target_mode, "mode-change phase 3/3: APPLY (mode write skipped - Observer is sticky)");
        } else {
            self.consensus_bus.node_mode().send_replace(target_mode);
            info!(target: "epoch-manager", ?target_mode, "mode-change phase 3/3: APPLY");
        }
        // send_if_modified (not send_replace) - unconditional notify would
        // re-fire the select arm next run_epoch iteration
        self.consensus_bus.mode_transition().send_if_modified(|v| {
            let changed = v.is_some();
            *v = None;
            changed
        });

        Ok(())
    }

    /// Check for and recover from an incomplete epoch transition after a crash.
    ///
    /// Reads the checkpoint from the DB and completes any remaining phases.
    /// This is called at the start of each `run_epoch()`.
    pub(super) async fn recover_partial_transition(
        &mut self,
        primary: &PrimaryNode<DB>,
        engine: &ExecutionNode,
    ) -> eyre::Result<()> {
        // The checkpoint is keyed by the *closing* epoch. It must NOT be looked up by the
        // canonical-tip epoch: executing the boundary block runs `closeEpoch`, which advances
        // the on-chain ConsensusRegistry epoch to closing+1, so the tip epoch stops matching the
        // checkpoint key the instant the boundary block is canonical. Keying recovery off the tip
        // epoch silently misses the checkpoint in the post-execution crash window. Select by
        // scanning the table and take the closing epoch from the checkpoint itself.
        let checkpoint = match select_recovery_checkpoint(&self.consensus_db) {
            Some(cp) => cp,
            None => return Ok(()), // No recovery needed
        };
        let epoch = checkpoint.epoch;

        info!(
            target: "epoch-manager",
            ?epoch,
            phase = ?checkpoint.completed_phase,
            target_hash = ?checkpoint.target_hash,
            "recovering partial epoch transition"
        );

        match checkpoint.completed_phase {
            EpochTransitionPhase::BoundaryDetected
            | EpochTransitionPhase::Draining
            | EpochTransitionPhase::ConsensusShutdown => {
                // For early phases the engine may or may not have executed the boundary output.
                // Decide from the durable execution state: the boundary output runs concludeEpoch,
                // so if the closing epoch executed, the canonical tip's epoch state has advanced
                // past it. (The former recently_executed_blocks/parent_beacon fast-path was dead
                // here — recently_executed_blocks is empty this early in startup,
                // before any replay — and fragile: a drained parked batch makes the
                // tip's beacon differ from target_hash.)
                let tip_state = engine.epoch_state_from_canonical_tip().await?;
                let execution_done = tip_state.epoch > epoch;

                if execution_done {
                    info!(target: "epoch-manager", "recovery: execution already complete for target hash");
                    // Does not persist to disk: only repopulates self.epoch_record.
                    // The record is later written atomically with its cert by
                    // collect_epoch_votes once vote quorum is reached.
                    // The record epoch comes from the checkpoint (the closing epoch):
                    // the committee and registry already report closing+1 here.
                    self.write_epoch_record(primary, engine, epoch, checkpoint.target_hash).await?;
                    self.clear_consensus_db_for_next_epoch()?;
                    self.consensus_db.persist().await?;
                    self.clear_transition_checkpoint(epoch)?;
                    info!(target: "epoch-manager", "recovery: cleared tables after partial transition");
                } else {
                    // Execution not complete. The boundary output is lost (it was
                    // in-memory). Clear the checkpoint and let the new epoch
                    // proceed normally -- the consensus layer will re-reach this
                    // epoch boundary on restart.
                    warn!(
                        target: "epoch-manager",
                        "recovery: execution not complete for target hash, clearing stale checkpoint. \
                         The epoch will be re-run from the beginning."
                    );
                    self.clear_transition_checkpoint(epoch)?;
                }
            }
            EpochTransitionPhase::ExecutionComplete => {
                // Execution finished but tables were not cleared yet.
                // Repopulates self.epoch_record so the next run_epoch can persist
                // (record, cert) atomically via collect_epoch_votes.
                info!(target: "epoch-manager", "recovery: completing table clear from ExecutionComplete phase");
                // The record epoch comes from the checkpoint (the closing epoch):
                // the committee and registry already report closing+1 here.
                self.write_epoch_record(primary, engine, epoch, checkpoint.target_hash).await?;
                self.clear_consensus_db_for_next_epoch()?;
                self.consensus_db.persist().await?;
                self.clear_transition_checkpoint(epoch)?;
                info!(target: "epoch-manager", "recovery: cleared tables after partial transition");
            }
            EpochTransitionPhase::Cleared => {
                // Everything was done, just clean up the checkpoint.
                self.clear_transition_checkpoint(epoch)?;
                info!(target: "epoch-manager", "recovery: cleaned up stale Cleared checkpoint");
            }
        }

        Ok(())
    }

    /// Persist a transition checkpoint to the consensus DB.
    fn save_transition_checkpoint(
        &self,
        epoch: Epoch,
        phase: EpochTransitionPhase,
        target_hash: B256,
    ) -> eyre::Result<()> {
        let checkpoint = EpochTransitionCheckpoint {
            epoch,
            completed_phase: phase,
            target_hash,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        self.consensus_db.save_checkpoint(&checkpoint)?;
        Ok(())
    }

    /// Remove the transition checkpoint after a successful transition.
    fn clear_transition_checkpoint(&self, epoch: Epoch) -> eyre::Result<()> {
        self.consensus_db.clear_checkpoint(epoch)?;
        Ok(())
    }
}

/// Select the in-progress epoch-transition checkpoint to recover, if any.
///
/// Returns the checkpoint with the highest closing epoch. The checkpoint is keyed by the
/// closing epoch, so selection must not depend on the canonical-tip epoch, which advances to
/// closing+1 the instant the boundary block executes. Comparing the decoded `epoch` field keeps
/// the result correct regardless of the table's key byte-encoding.
pub(crate) fn select_recovery_checkpoint<DB: ReDatabase>(
    consensus_db: &DB,
) -> Option<EpochTransitionCheckpoint> {
    consensus_db
        .iter::<EpochTransitionCheckpoints>()
        .max_by_key(|(epoch, _)| *epoch)
        .map(|(_, checkpoint)| checkpoint)
}
