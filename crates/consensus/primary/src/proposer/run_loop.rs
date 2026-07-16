use crate::{
    error::ProposerResult,
    proposer::{
        Proposer, BACKPRESSURE_DELAY, EXECUTION_BACKPRESSURE_DELAY, EXECUTION_LAG_THRESHOLD,
        PENDING_BACKPRESSURE_THRESHOLD,
    },
};
use rayls_infrastructure_storage::ProposerStore;
use rayls_infrastructure_types::{Database, RaylsReceiver, RaylsSender};
use tokio::{sync::oneshot, time::sleep};
use tracing::{debug, info, warn};

impl<DB: Database> Proposer<DB> {
    /// Returns `Some(exec_round)` (the execution-anchor leader round) when execution lags consensus
    /// beyond [`EXECUTION_LAG_THRESHOLD`], else `None`. The proposer throttles while this is
    /// `Some`.
    ///
    /// Reads the monotonic execution anchor - the leader round of the highest executed output - NOT
    /// `recently_executed_blocks().latest_block()`, whose tip regresses below the true frontier
    /// after a drained parked (out-of-order seq) batch and would wedge the proposer
    /// permanently.
    pub(crate) fn execution_lag(&self) -> Option<u64> {
        let exec_round =
            self.consensus_bus.executed_anchor().borrow().sub_dag.leader_round() as u64;
        ((self.round as u64) > exec_round + EXECUTION_LAG_THRESHOLD).then_some(exec_round)
    }

    /// Run the proposer task.
    /// Returns Ok on shutdown or an error to indicate a fatal condition.
    pub(super) async fn run(&mut self) -> ProposerResult<()> {
        // Wait for execution replay to complete before proposing headers.
        // On restart, recently_executed_blocks may be stale; replaying ensures we don't embed
        // outdated exec_digest values that cause validator divergence.
        // Subscribe before borrowing to avoid TOCTOU deadlock  where the signal is marked "seen"
        // before we check it.

        let mut replay_rx = self.consensus_bus.execution_replay_complete().subscribe();
        if !*replay_rx.borrow() {
            info!(target: "primary::proposer", "waiting for execution replay to complete before proposing");
            loop {
                tokio::select! {
                    biased;
                    _ = &self.rx_shutdown => return Ok(()),
                    // watch::Receiver::changed() is cancellation-safe — it only updates
                    // the "seen" mark, the value persists in the channel.
                    res = replay_rx.changed() => {
                        if res.is_err() || *replay_rx.borrow() {
                            break;
                        }
                    }
                }
            }
            info!(target: "primary::proposer", "execution replay complete, starting proposer loop");
        }

        let mut rx_our_digests = self.consensus_bus.our_digests().subscribe();
        let mut rx_parents = self.consensus_bus.parents().subscribe();
        let mut rx_committed_own_headers = self.consensus_bus.committed_own_headers().subscribe();

        let mut pending_header = None;
        let mut max_delay_timed_out = false;
        let mut min_delay_timed_out = false;
        loop {
            tokio::select! {
                _ = &self.rx_shutdown => {
                    return Ok(())
                }
                // check for new digests from workers and send ack back to worker
                //
                // ack to worker implies that the block is recorded on the primary
                // and will be tracked until the block is included
                // ie) primary will attempt to propose this digest until it is
                // committed/sequenced in the DAG or the epoch concludes
                //
                // NOTE: this will not persist primary restarts
                Some(msg) = rx_our_digests.recv() =>
                {
                    debug!(target: "primary::proposer", authority=?self.authority_id, round=self.round, "received digest");

                    // parse message into parts
                    let (ack, digest) = msg.process();
                    let _ = ack.send(());
                    self.push_digest(digest);
                }
                // check for new parent certificates
                // synchronizer sends collection of certificates when there is quorum (2f+1)
                Some((certs, round)) = rx_parents.recv() => {
                    debug!(target: "primary::proposer", authority=?self.authority_id, this_round=self.round, parent_round=round, num_parents=certs.len(), "received parents");
                    self.process_parents(certs, round)?;
                }
                Some((commit_round, committed_headers)) = rx_committed_own_headers.recv() => {
                    debug!(target: "primary::proposer", authority=?self.authority_id, round=self.round, "received committed update for own header");
                    self.process_committed_headers(commit_round, committed_headers);
                }
                res = Self::pending_header(&mut pending_header) => {
                    pending_header = None;
                    debug!(target: "primary::proposer", authority=?self.authority_id, "pending header task complete!");
                    if let Err(e) = self.handle_proposal_result(res) {
                        // If we've been signalled to shut down, the epoch is tearing down and the
                        // certifier (and other peer tasks) are being aborted -- controlled_shutdown
                        // notifies our shutdown signal before aborting them -- so any in-flight
                        // proposal failure (e.g. the certifier send) is expected. Exit cleanly rather
                        // than panicking this critical task. Outside shutdown it is a real fault.
                        if self.rx_shutdown.noticed() {
                            info!(target: "primary::proposer", authority=?self.authority_id, ?e, "shutdown signalled; proposer exiting (proposal send failed)");
                            return Ok(());
                        }
                        return Err(e);
                    }
                }
                // tick intervals to ensure they advance
                _ = self.max_delay_interval.tick() => {
                    max_delay_timed_out = true;
                }
                _ = self.min_delay_interval.tick() => {
                    min_delay_timed_out = true;
                }
            }

            // Check if pending queue is high before proposing - backpressure mechanism
            let pending_count = self
                .consensus_bus
                .primary_metrics()
                .node_metrics
                .certificates_currently_suspended
                .get()
                .max(0) as usize;

            if pending_count > PENDING_BACKPRESSURE_THRESHOLD {
                warn!(
                    target: "primary::proposer",
                    pending_count,
                    threshold = PENDING_BACKPRESSURE_THRESHOLD,
                    "Pending queue high, delaying proposal"
                );
                sleep(BACKPRESSURE_DELAY).await;
                continue; // Skip this proposal cycle
            }

            // Check if execution is lagging behind consensus - throttle to let it catch up
            if let Some(exec_round) = self.execution_lag() {
                warn!(
                    target: "primary::proposer",
                    consensus_round = self.round as u64,
                    execution_round = exec_round,
                    lag = self.round as u64 - exec_round,
                    threshold = EXECUTION_LAG_THRESHOLD,
                    "Execution lagging behind consensus, delaying proposal"
                );
                sleep(EXECUTION_BACKPRESSURE_DELAY).await;
                continue; // Skip this proposal cycle
            }

            if pending_header.is_some() {
                // continue the loop, don't try to propose a header since we are already working
                // on one.
                continue;
            }

            // proposer doesn't have a pending header
            // Check if conditions are met for proposing a new header
            //
            // New headers are proposed when:
            //
            // 1) a quorum of parents (certificates) received for the current round
            // 2) the execution layer successfully executed the previous round (parent
            //    `BlockNumHash`)
            // 3) One of the following:
            // - the interval expired:
            //      - this primary timed out on the leader
            //      - or quit trying to gather enough votes for the leader
            // - the worker created enough blocks (header_num_of_batches_threshold)
            //      - this is happy path
            //      - vote for leader or leader already has enough votes to trigger commit
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.digests.len() >= self.header_num_of_batches_threshold;

            // evaluate conditions for bool value
            let should_create_header = enough_parents
                && (max_delay_timed_out
                    || (self.advance_round && (enough_digests || min_delay_timed_out)));

            // If we have not proposed a header in more than a max_header_delay time then repropose.
            // We may be in a race condition on a network restart...
            //
            // No epoch/mode-transition guard: repropose only re-sends the existing `last_proposed`
            // header. A stale header can't fork — the certifier rejects any header whose epoch
            // != committee.epoch(), and `run_mode_transition` clears `LastProposed` (so repropose
            // is a no-op there). The old `is_transitioning()` check was racy (TOCTOU) and thus
            // never a correctness guarantee anyway.
            let should_repropose_header = !should_create_header && max_delay_timed_out;

            debug!(
                target: "primary::proposer",
                authority=?self.authority_id,
                round=self.round,
                enough_parents,
                enough_digests,
                self.advance_round,
                min_delay_timed_out,
                max_delay_timed_out,
                should_create_header,
                "polled...",
            );

            // if all conditions are met, create the next header
            if should_create_header {
                if max_delay_timed_out {
                    // expect this interval to expire occassionally
                    //
                    // if it expires too often, it either means some validators are Byzantine or
                    // that the network is experiencing periods of asynchrony
                    //
                    // periods of asynchrony possibly caused by misconfigured `max_header_delay`
                    warn!(target: "primary::proposer", interval=?self.max_delay_interval.period(), "max delay interval expired for round {}", self.round);
                }

                // obtain reason for metrics
                let reason = if max_delay_timed_out {
                    "max_timeout"
                } else if enough_digests {
                    "threshold_size_reached"
                } else {
                    "min_timeout"
                };

                debug!(target: "primary::proposer", authority=?self.authority_id, ?reason, "proposing next header!");

                // propose header
                pending_header = Some(self.propose_next_header(reason.to_string())?);
                max_delay_timed_out = false;
                min_delay_timed_out = false;
            } else if should_repropose_header {
                if let Ok(Some(last_proposed)) = self.proposer_store.get_last_proposed() {
                    warn!(target: "primary::proposer", interval=?self.max_delay_interval.period(), "re-proposing last header after max delay interval expired for round {}", self.round);
                    let (tx, rx) = oneshot::channel();
                    let consensus_bus = self.consensus_bus.clone();
                    let proposer_store = self.proposer_store.clone();
                    self.task_spawner.spawn_task("re-propose header after delay", async move {
                        // use this instead of store_and_send to because rx always expects a Header
                        let res = Proposer::repropose_header(
                            last_proposed,
                            proposer_store,
                            &consensus_bus,
                            "repropose header after delay".to_string(),
                        )
                        .await;
                        let _ = tx.send(res);
                    });
                    max_delay_timed_out = false;
                    min_delay_timed_out = false;
                    pending_header = Some(rx);
                }
            }
        }
    }
}
