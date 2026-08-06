use crate::{
    error::ProposerResult,
    proposer::{
        Proposer, BACKPRESSURE_DELAY, EXECUTION_BACKPRESSURE_DELAY, EXECUTION_LAG_THRESHOLD,
        PENDING_BACKPRESSURE_THRESHOLD,
    },
};
use rayls_infrastructure_storage::ProposerStore;
use rayls_infrastructure_types::{Database, Epoch, RaylsReceiver, RaylsSender, Round};
use tokio::{sync::oneshot, time::sleep};
use tracing::{debug, info, warn};

/// Whether a stored `last_proposed` header is stale relative to the proposer's current position,
/// and therefore must NOT be re-proposed on the max-delay retransmit path.
///
/// Stale means either:
/// - a different epoch — the check is `!=`, so a header from any epoch other than the current one
///   is stale (a future epoch can't occur in normal operation, but the predicate rejects it too).
///   Rounds reset per epoch, so the epoch is checked *before* the round (a round-3 header of epoch
///   N+1 is newer than a round-4 header of epoch N), or
/// - the same epoch but a round below `self_round`.
///
/// `self_round` can advance past `last_proposed` via the `process_parents` jump-ahead (see
/// `round.rs`), which bumps the round WITHOUT writing `last_proposed`. Re-proposing a stale header
/// then draws a "too old"/equivocation rejection from peers and — because the certifier aborts any
/// in-flight proposal when a new header arrives — cancels the in-flight current-round
/// certification every cycle, a livelock. Skipping it keeps `last_proposed` monotonic-in-round
/// within an epoch and lets a fresh current-round header get certified instead.
pub(super) fn last_proposed_is_stale(
    self_round: Round,
    self_epoch: Epoch,
    header_round: Round,
    header_epoch: Epoch,
) -> bool {
    header_epoch != self_epoch || header_round < self_round
}

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
            // No *outer* mode-transition guard here: this condition only decides *whether* to enter
            // the re-propose path. Epoch/round staleness of `last_proposed` IS guarded, inside the
            // re-propose block below (see `last_proposed_is_stale`). A stale header can't fork
            // regardless — the certifier rejects any header whose epoch != committee.epoch(), and
            // `run_mode_transition` clears `LastProposed` (so repropose is a no-op there). The old
            // `is_transitioning()` check was racy (TOCTOU) and thus never a correctness guarantee.
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
                    // Only re-propose a header that is still current. `self.round` can advance
                    // past our last stored header via the `process_parents` jump-ahead (see
                    // `round.rs`), which bumps the round WITHOUT writing `last_proposed`. So
                    // `last_proposed` legitimately lags `self.round`. Re-proposing that stale
                    // header is guaranteed a "too old" rejection by peers, and — because the
                    // certifier aborts any in-flight proposal when a new header arrives — it also
                    // cancels the in-flight current-round certification every cycle, a livelock.
                    // Skip it; wait to build a fresh header at `self.round`. (A header from an
                    // older epoch is likewise stale — rounds reset per epoch, so compare epochs
                    // before rounds.)
                    if last_proposed_is_stale(
                        self.round,
                        self.committee.epoch(),
                        last_proposed.round(),
                        last_proposed.epoch(),
                    ) {
                        warn!(
                            target: "primary::proposer",
                            authority=?self.authority_id,
                            self_round = self.round,
                            self_epoch = self.committee.epoch(),
                            last_proposed_round = last_proposed.round(),
                            last_proposed_epoch = last_proposed.epoch(),
                            last_proposed_digest = ?last_proposed.digest(),
                            "skipping re-propose of stale header (older round/epoch than current)",
                        );
                        // Reset timers so we don't busy-spin the skip; the next max-delay tick
                        // re-evaluates. This only avoids *harm* (re-proposing a stale header that
                        // would be rejected and would cancel the in-flight current-round
                        // certification) -- it does not itself make progress. A fresh current-round
                        // header is built only once the aggregator delivers a quorum of parents on
                        // `parents()` (aggregators/certificates.rs), which requires the committee
                        // to reach quorum for the round; if it can't (e.g.
                        // a counted member isn't producing), this skips
                        // quietly and the proposer waits.
                        max_delay_timed_out = false;
                        min_delay_timed_out = false;
                    } else {
                        warn!(target: "primary::proposer", interval=?self.max_delay_interval.period(), self_round = self.round, self_epoch = self.committee.epoch(), last_proposed_round = last_proposed.round(), last_proposed_epoch = last_proposed.epoch(), last_proposed_digest = ?last_proposed.digest(), "re-proposing last header after max delay interval expired");
                        let (tx, rx) = oneshot::channel();
                        let consensus_bus = self.consensus_bus.clone();
                        let proposer_store = self.proposer_store.clone();
                        self.task_spawner.spawn_task("re-propose header after delay", async move {
                            // use this instead of store_and_send to because rx always expects a
                            // Header
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
}

#[cfg(test)]
mod tests {
    use super::last_proposed_is_stale;

    // The re-propose guard must skip a stored header that is behind the proposer's current
    // position — this is what stops the round-N→round-(N-1) `last_proposed` regression that
    // caused the equivocation wedge (val1 rebuilding a conflicting round-4 header on restart).
    #[test]
    fn stale_when_header_round_behind_current_round_same_epoch() {
        // self at round 4, stored header at round 3, same epoch → stale (must skip).
        assert!(last_proposed_is_stale(4, 579, 3, 579));
    }

    #[test]
    fn not_stale_when_header_matches_current_round() {
        // stored header IS the current round → re-propose is legitimate (restart recovery).
        assert!(!last_proposed_is_stale(4, 579, 4, 579));
    }

    #[test]
    fn not_stale_when_header_ahead_of_current_round() {
        // `header_round > self_round` can't occur in normal operation: `last_proposed` is only
        // written on a successful proposal, and proposals only happen at `self.round`, so a stored
        // header never leads the current round. The predicate treats it as not-stale (only
        // `header_round < self_round` is stale); this test pins that boundary.
        assert!(!last_proposed_is_stale(4, 579, 5, 579));
    }

    #[test]
    fn stale_when_older_epoch_even_if_round_looks_higher() {
        // rounds reset per epoch: a round-4 header of epoch 579 is stale once we're in epoch 580
        // at round 2 — epoch must be checked before round, or we'd wrongly re-propose it.
        assert!(last_proposed_is_stale(2, 580, 4, 579));
    }

    #[test]
    fn stale_when_newer_epoch_even_if_same_round() {
        // the check is `!=`, not `<`: a header from a *future* epoch is stale too. This can't
        // happen in normal operation, but the predicate pins it — guards against a later change
        // to `<` that would wrongly re-propose it.
        assert!(last_proposed_is_stale(2, 579, 2, 580));
    }

    #[test]
    fn not_stale_across_epoch_boundary_for_current_epoch_low_round() {
        // fresh epoch 580, self at round 2, stored header is this epoch's round 2 → not stale.
        assert!(!last_proposed_is_stale(2, 580, 2, 580));
    }

    #[test]
    fn not_stale_at_genesis() {
        // round 0 / epoch 0 everywhere → not stale.
        assert!(!last_proposed_is_stale(0, 0, 0, 0));
    }
}
