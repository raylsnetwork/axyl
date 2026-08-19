use crate::proposer::{types::ProposerDigest, Proposer, DIGEST_QUEUE_WARN_THRESHOLD};
use rayls_infrastructure_types::{Database, DbTxMut, Round};
use std::collections::VecDeque;
use tracing::{debug, warn};

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

    /// Rayls: Evict proposed headers beyond gc_depth from current round.
    pub(super) fn evict_old_proposed_headers(&mut self) {
        if self.round <= self.gc_depth {
            return;
        }
        let gc_round = self.round - self.gc_depth;
        let old_count = self.proposed_headers.len();

        // Remove all headers from rounds at or before gc_round
        self.proposed_headers.retain(|&round, _| round > gc_round);

        let evicted = old_count - self.proposed_headers.len();
        if evicted > 0 {
            debug!(
                target: "primary::proposer",
                evicted,
                gc_round,
                current_round = self.round,
                remaining = self.proposed_headers.len(),
                "Evicted old proposed headers"
            );
        }
    }

    /// Process notifications that Proposer's own headers have been committed in the DAG for a
    /// particular round.
    ///
    /// Committed headers are removed from the collection of `self.proposed_headers`. Headers
    /// that are skipped with no hope of being committed (proposed in a previous round) are also
    /// removed after adding the expired header's proposed block digests and system messages to
    /// the beginning of the queue.
    ///
    /// This method ensures batches that were previously proposed but weren't committed are
    /// added back to the queue so their transactions are included in the next proposal.
    pub(super) fn process_committed_headers(
        &mut self,
        commit_round: Round,
        committed_headers: Vec<Round>,
    ) {
        // drain every committed round (not just the lowest-matching one) so later
        // rounds cannot be re-queued by the retransmit loop below
        for round in committed_headers.iter().copied() {
            self.proposed_headers.remove(&round);
        }

        // Fall back to the commit round when none of our own headers committed: otherwise a
        // validator whose proposals keep getting rejected strands its quorum'd digests (consumed
        // seqs) in proposed_headers until GC, leaving a permanent per-authority seq gap on peers.
        let highest_committed = committed_headers.iter().copied().max().unwrap_or(commit_round);
        let Some(&lowest_uncommitted) = self.proposed_headers.keys().next() else { return };
        if lowest_uncommitted >= highest_committed {
            return;
        }

        // re-insert batches for any proposed header from a round below the current commit
        //
        // ensure batches are FIFO to re-send them
        //
        // payloads: oldest -> newest
        let mut digests_to_resend = VecDeque::new();
        // Oldest to newest rounds.
        let mut retransmit_rounds = Vec::new();

        // loop through proposed headers in order by round
        for (header_round, header) in &mut self.proposed_headers {
            let mut digests = header
                .payload()
                .into_iter()
                .map(|(k, v)| ProposerDigest { digest: *k, worker_id: *v })
                .collect();

            // add payloads and system messages from oldest to newest
            digests_to_resend.append(&mut digests);
            retransmit_rounds.push(*header_round);
        }

        // process rounds that need to be retransmitted
        if retransmit_rounds.is_empty() {
            return;
        }

        let num_digests_to_resend = digests_to_resend.len();

        // prepend missing batches from previous round and update `self`
        digests_to_resend.append(&mut self.digests);
        self.digests = digests_to_resend;

        // remove the old headers that failed
        // the proposed blocks are included in the next header
        for round in &retransmit_rounds {
            self.proposed_headers.remove(round);
        }

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
