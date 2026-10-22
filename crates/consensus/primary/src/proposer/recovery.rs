use crate::proposer::{types::ProposerDigest, Proposer, DIGEST_QUEUE_WARN_THRESHOLD};
use rayls_infrastructure_types::{batch_tracker::RequeueReason, Database, Header, Round};
use std::collections::VecDeque;
use tracing::{debug, warn};

/// Rounds below a foreign commit round before an uncommitted own header is re-proposed.
///
/// Ordinary commit lag (certificates still collecting votes) stays inside the grace window, so
/// only a header that has clearly missed its commit is retransmitted.
pub(super) const FALLBACK_REQUEUE_GRACE_ROUNDS: Round = 4;

impl<DB: Database> Proposer<DB> {
    /// Rayls: Push a batch digest; never drops.
    ///
    /// Dropping a quorum'd digest gaps the per-authority seq stream and parks every later batch
    /// from this authority. Growth is bounded by single-flight batch building and the per-epoch
    /// reset.
    pub(super) fn push_digest(&mut self, digest: ProposerDigest) {
        if self.digests.len() >= DIGEST_QUEUE_WARN_THRESHOLD {
            warn!(
                target: "primary::proposer",
                queue_size = self.digests.len(),
                "digest queue unusually large; header certification may be stalled"
            );
        }
        self.consensus_bus.batch_tracker().digest_queued_in_proposer(digest.digest);
        self.digests.push_back(digest);
    }

    /// Re-queues the payload digests of `headers` ahead of the pending queue and reports them to
    /// the batch tracker under `reason`; returns how many digests were re-queued.
    fn requeue_front<'a>(
        &mut self,
        headers: impl IntoIterator<Item = &'a Header>,
        reason: RequeueReason,
    ) -> usize {
        let mut requeued: VecDeque<ProposerDigest> = headers
            .into_iter()
            .flat_map(|header| header.payload().into_iter())
            .map(|(digest, worker_id)| ProposerDigest { digest: *digest, worker_id: *worker_id })
            .collect();
        let count = requeued.len();
        self.consensus_bus
            .batch_tracker()
            .digests_requeued_in_proposer(requeued.iter().map(|d| d.digest), reason);
        requeued.append(&mut self.digests);
        self.digests = requeued;
        count
    }

    /// Rayls: Drop proposed headers more than `gc_depth` below the committed round, re-queueing
    /// their digests (upstream never evicts proposed headers; the requeue keeps its never-drop
    /// rule on this fork-added path).
    ///
    /// The horizon keys on the committed round, not `self.round`: commits trail the proposal
    /// frontier, so a header in the lag window is still committable. Commit rounds are monotone,
    /// so every future leader round L >= C and an evicted round R <= C - gc_depth is already
    /// excluded by `order_dag`; re-proposing its digests cannot double-commit the old header.
    /// Re-queueing is mandatory: the digests are quorum'd and seq-consumed, so dropping them gaps
    /// the original seq permanently on peers. Queue growth is bounded by the seal-ahead gate.
    pub(super) fn evict_old_proposed_headers(&mut self) {
        let committed = *self.consensus_bus.committed_round_updates().borrow();
        let Some(gc_round) = committed.checked_sub(self.gc_depth) else {
            return;
        };
        if gc_round == 0 {
            return;
        }

        // rounds <= gc_round stay behind in `self.proposed_headers`, the rest are retained
        let retained = self.proposed_headers.split_off(&gc_round.saturating_add(1));
        let evicted_headers = std::mem::replace(&mut self.proposed_headers, retained);
        if evicted_headers.is_empty() {
            return;
        }

        let requeued_digests = self.requeue_front(evicted_headers.values(), RequeueReason::GcEvict);

        debug!(
            target: "primary::proposer",
            evicted = evicted_headers.len(),
            requeued_digests,
            gc_round,
            current_round = self.round,
            remaining = self.proposed_headers.len(),
            "Evicted old proposed headers, requeued their digests"
        );
    }

    /// Processes a commit notification for the proposer's own headers.
    ///
    /// Committed rounds leave `self.proposed_headers`; headers old enough that they can no longer
    /// commit are removed too, with their payload digests re-queued at the front so the batches
    /// land in the next proposal.
    pub(super) fn process_committed_headers(
        &mut self,
        commit_round: Round,
        committed_headers: Vec<Round>,
    ) {
        // drain every committed round (not just the lowest) so the retransmit split below cannot
        // re-queue a round that already committed
        for round in committed_headers.iter().copied() {
            self.proposed_headers.remove(&round);
        }

        // Fall back to the commit round when none of our own headers committed: otherwise a
        // validator whose proposals keep getting rejected strands its quorum'd digests (consumed
        // seqs) in proposed_headers until the GC-horizon requeue, parking its batches on peers for
        // gc_depth rounds. Unlike the own-commit key this is NOT airtight: a requeued header below
        // a foreign commit round may still commit later (this authority's last_committed did not
        // advance), so the original AND the re-proposal can both land.
        // `ExecutedBatchRegistry::try_register` absorbs that duplicate at execution admission, so
        // the registry dedup is load-bearing here: do not weaken one without the other. The
        // horizon sits `FALLBACK_REQUEUE_GRACE_ROUNDS` below the commit round so ordinary commit
        // lag never triggers it.
        let highest_committed = committed_headers
            .iter()
            .copied()
            .max()
            .unwrap_or_else(|| commit_round.saturating_sub(FALLBACK_REQUEUE_GRACE_ROUNDS));
        // Split at the horizon: rounds below it are retransmitted; rounds at or above are fresh
        // (certificates still collecting votes) and the normal commit path handles them, so
        // re-queueing those would only create duplicates.
        let fresh = self.proposed_headers.split_off(&highest_committed);
        let retransmitted = std::mem::replace(&mut self.proposed_headers, fresh);
        if retransmitted.is_empty() {
            return;
        }

        // Re-queued, never dropped: these digests are quorum'd and seq-consumed, so dropping one
        // gaps that seq permanently on peers. Pinned by
        // `digests_survive_gc_advance_and_later_round_commit`.
        let retransmit_rounds: Vec<Round> = retransmitted.keys().copied().collect();
        let num_digests_to_resend =
            self.requeue_front(retransmitted.values(), RequeueReason::CommitLag);

        warn!(
            target: "primary::proposer",
            "Repropose {num_digests_to_resend} batches in undelivered headers {retransmit_rounds:?} at commit round {commit_round:?}, remaining headers {}",
            self.proposed_headers.len()
        );

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .proposer_resend_headers
            .inc_by(retransmit_rounds.len() as u64);
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .proposer_resend_batches
            .inc_by(num_digests_to_resend as u64);
    }
}
