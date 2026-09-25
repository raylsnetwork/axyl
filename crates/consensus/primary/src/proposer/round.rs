use crate::{error::ProposerResult, proposer::Proposer};
use rayls_infrastructure_types::{Certificate, Database, Hash as _, Round, VotingPower};
use std::cmp::Ordering;
use tokio::time::Duration;
use tracing::{debug, error, info, trace};

impl<DB: Database> Proposer<DB> {
    /// Calculate the max delay to use when resetting the max_delay_interval.
    ///
    /// The max delay is reduced when this authority expects to become the leader of the next round.
    /// Reducing the max delay increases its chance of being included in the DAG. Leaders are only
    /// elected on even rounds, so the normal max delay interval is used for odd rounds.
    pub(super) fn calc_max_delay(&self) -> Duration {
        // check next round
        let next_round = self.round + 1;

        if next_round.is_multiple_of(2)
            && self.leader_schedule.leader(self.round + 1).id() == self.authority_id
        {
            self.max_header_delay / 2
        } else {
            self.max_header_delay
        }
    }

    /// Calculate the min delay to use when resetting the min_delay_interval.
    ///
    /// The min delay is reduced when this authority expects to become the leader of the next round.
    /// Reducing the min delay increases the chances of successfully committing a leader.
    ///
    /// NOTE: If the next round is even, the leader schedule is used to identify the next leader. If
    /// the next round is odd, the whole committee is used in order to keep the proposal rate as
    /// high as possible (which leads to a higher round rates). Using the entire committee here also
    /// helps boost scores for weaker nodes that may be trying to resync.
    pub(super) fn calc_min_delay(&self) -> Duration {
        // Single-node (dev): this authority is always the sole leader, so the
        // leader-election fast path below would always return ZERO and the proposer
        // would spin as fast as the CPU allows, ignoring the configured header
        // delays (flooding the chain with empty blocks). Honor `min_header_delay`
        // to pace block production instead.
        #[cfg(feature = "dev-single-node-setup")]
        if self.committee.size() == 1 {
            return self.min_header_delay;
        }

        // check next round
        let next_round = self.round + 1;

        // compare:
        // - leader schedule for even rounds
        // - entire committee for odd rounds
        //
        // NOTE: committee size is asserted >1 during Committee::load()
        if !(next_round.is_multiple_of(2)
            && self.leader_schedule.leader(next_round).id() != self.authority_id)
        {
            Duration::ZERO
        } else {
            self.min_header_delay
        }
    }

    /// Update the last leader certificate.
    ///
    /// This is called after processing parent certificates during even rounds.
    /// The returned boolean indicates if `Self::last_leader` was updated.
    fn update_leader(&mut self) -> bool {
        let leader = self.leader_schedule.leader(self.round);
        self.last_leader =
            self.last_parents.iter().find(|cert| cert.origin() == &leader.id()).cloned();

        debug!(target: "primary::proposer", leader=?self.last_leader, round=self.round, "Last leader for round?");

        self.last_leader.is_some()
    }

    /// Check if proposer has received enough votes to elect a new leader for the round.
    ///
    /// This method returns true for any of the following:
    /// - if this primary is the leader for the next round
    /// - f+1 votes for a new leader
    /// - 2f+1 nodes didn't vote for a new leader
    /// - there is no leader to vote for
    ///
    /// This is called after processing parent certificates during odd rounds.
    fn enough_votes(&self) -> bool {
        if self.leader_schedule.leader(self.round + 1).id() == self.authority_id {
            debug!(target: "primary::proposer", "enough_votes eval to true - this node anticipated leader for next round");
            return true;
        }

        let leader = match &self.last_leader {
            Some(x) => x.digest(),
            None => return true,
        };

        let mut votes_for_leader = 0;
        let mut no_votes = 0;
        for certificate in &self.last_parents {
            let stake = self.committee.voting_power_by_id(certificate.origin());
            if certificate.header().parents().contains(&leader) {
                votes_for_leader += stake;
            } else {
                no_votes += stake;
            }
        }

        // return true if either:
        // - enough votes for availability (f+1)
        // - a quorum of no_votes (2f+1)
        votes_for_leader >= self.committee.validity_threshold()
            || no_votes >= self.committee.quorum_threshold()
    }

    /// Check if conditions support advancing the round for the DAG.
    ///
    /// Odd rounds check if there are enough votes for a new leader.
    /// Even rounds check if there is the new leader certificate is in `Self::last_parents`.
    ///
    /// This method is called from `Self::process_parents`.
    /// NOTE: this value is ignored if max_delay_interval expires.
    fn ready(&mut self) -> bool {
        match self.round % 2 {
            0 => self.update_leader(),
            _ => self.enough_votes(),
        }
    }

    /// Process certificates received for this round.
    ///
    /// If the certificates are valid, include them as parents for the next header.
    pub(super) fn process_parents(
        &mut self,
        parents: Vec<Certificate>,
        round: Round,
    ) -> ProposerResult<()> {
        // Sanity check: verify provided certs are of the correct round & epoch.
        for parent in parents.iter() {
            if parent.round() != round {
                error!(target: "primary::proposer", "received certificate {parent:?} that failed to match expected round {round}. This should not be possible.");
            }
        }

        // Compare the parents' round number with our current round.
        match round.cmp(&self.round) {
            Ordering::Greater => {
                trace!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    round=?self.round,
                    parent_round=?round,
                    "processing parents from future round - advacing to catch up...",
                );

                let total_stake: VotingPower = parents
                    .iter()
                    .map(|cert| self.committee.voting_power_by_id(cert.origin()))
                    .sum();

                if total_stake >= self.committee.quorum_threshold() {
                    // proposer accepts a future round then jumps ahead in case it was
                    // late (or just joined the network).
                    self.round = round;
                    // broadcast new round
                    self.consensus_bus.primary_round_updates().send_replace(self.round);
                    self.last_parents = parents;
                    // Reset advance flag.
                    self.advance_round = false;
                    // NOTE: min_delay_interval is marked as `ready()` but max_delay_interval is
                    // reset to wait the appropriate amount of time for the
                    // previous round's leader.
                    //
                    // Disabling min_delay_interval will expidite the next proposal attempt. It's
                    // important to propose next header ASAP so this node doesn't fall
                    // behind again. If proposer waits another min_header_delay after
                    // receiving parents from a future round, it's likely that more
                    // parents from another future round will arrive while this node
                    // tries to catch up.
                    //
                    // Disabling min_delay_interval should help node sync with quorum.
                    // This is also important if this node expects to become the leader for the next
                    // round.
                    self.max_delay_interval.reset_after(self.calc_max_delay());
                    self.min_delay_interval.reset_immediately();
                } else {
                    info!(
                        target: "primary::proposer",
                        authority=?self.authority_id,
                        round=?self.round,
                        parent_round=?round,
                        total_stake,
                        "received parents from future round but did not meet quorum threshold - ignoring",
                    );
                }
            }
            Ordering::Less => {
                trace!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    round=?self.round,
                    parent_round=?round,
                    "ignoring older parents",
                );
                // Ignore parents from older rounds.
            }
            Ordering::Equal => {
                trace!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    round=?self.round,
                    parent_round=?round,
                    "adding parents for current round",
                );
                // certs arrive from the state synchronizer once quorum is reached
                // so these are extra parents
                self.last_parents.extend(parents);
                // the schedule can change after an odd round proposal
                //
                // need to ensure the interval is reset correctly for the round leader
                // no harm doing this here as well
                if self.calc_min_delay().is_zero() {
                    // min_delay_interval is ready
                    self.min_delay_interval.reset_immediately();
                }
            }
        }

        // check conditions for advancing the round
        //
        // if max_delay_interval expires, this check is ignored and the round is advanced regardless
        trace!(target: "primary::proposer", authority=?self.authority_id, advance_round=self.advance_round, round=self.round, "checking if self.ready()...");
        self.advance_round = self.ready();
        debug!(target: "primary::proposer", authority=?self.authority_id, advance_round=self.advance_round, round=self.round, "parents");

        // update metrics
        // Use only round_type label to avoid cardinality growth
        // ready_to_advance status is tracked separately via gauge
        let round_type = if self.round.is_multiple_of(2) { "even" } else { "odd" };
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .proposer_ready_to_advance
            .with_label_values(&[round_type])
            .inc();
        // Track ready status via gauge instead of counter label
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .proposer_ready_status
            .set(self.advance_round as i64);
        Ok(())
    }
}
