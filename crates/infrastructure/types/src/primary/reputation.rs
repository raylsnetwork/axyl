//! Reputation scores for validation nodes participating in consensus.

use crate::{
    bcs_layout::{BcsCursor, BcsLayout, BcsLayoutError},
    AuthorityIdentifier, Certificate, CommittedSubDag, Committee, Hash as _,
};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap};
use tracing::warn;

/// The reputation scores for authorities participating in consensus.
#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ReputationScores {
    /// Holds the score for every authority. If an authority is not amongst
    /// the records of the map then we assume that its score is zero.
    pub scores_per_authority: HashMap<AuthorityIdentifier, u64>,
    /// When true it notifies us that those scores will be the last updated scores of the
    /// current schedule before they get reset for the next schedule and start
    /// scoring from the beginning. In practice we can leverage this information to
    /// use the scores during the next schedule until the next final ones are calculated.
    pub final_of_schedule: bool,
}

impl ReputationScores {
    /// Creating a new ReputationScores instance pre-populating the authorities entries with
    /// zero score value.
    pub fn new(committee: &Committee) -> Self {
        let scores_per_authority =
            committee.authorities().iter().map(|a| (a.id(), 0_u64)).collect();

        Self { scores_per_authority, final_of_schedule: false }
    }
    /// Adds the provided `score` to the existing score for the provided `authority`
    pub fn add_score(&mut self, authority: &AuthorityIdentifier, score: u64) {
        if let Some(val) = self.scores_per_authority.get_mut(authority) {
            *val += score;
        } else {
            self.scores_per_authority.insert(authority.clone(), score);
        }
    }

    /// The total number of authorities.
    pub fn total_authorities(&self) -> u64 {
        self.scores_per_authority.len() as u64
    }

    /// The scores to store alongside a committed sub dag, computed from data the consensus block
    /// digest covers: the sub dag's certificates, its leader's nonce, and the previous sub dag of
    /// the same epoch. Bullshark uses it when committing, and a catching-up node uses it to
    /// replace the scores a peer sent, which no signature or digest covers.
    ///
    /// The scores reset at the first sub dag of every schedule window (and when there is no
    /// previous sub dag), otherwise the previous scores carry over. Then every authority whose
    /// certificate in the round after the previous leader references that leader gains one point.
    /// The last sub dag of a window is marked `final_of_schedule`.
    pub fn resolve(
        committee: &Committee,
        previous: Option<&CommittedSubDag>,
        committed_sequence: &[Certificate],
        sub_dag_index: u64,
        num_sub_dags_per_schedule: u32,
    ) -> Self {
        // sub_dag_index is based on epoch and round, so / 2 counts commit rounds (every other one).
        let mut reputation_score =
            if (sub_dag_index / 2).is_multiple_of(num_sub_dags_per_schedule as u64) {
                Self::new(committee)
            } else if let Some(last) = previous {
                // The previous sub dag may have been read back from storage, where an older node
                // may have kept scores sent by a peer. Carry them over only when they have the
                // shape every locally computed set has, otherwise restart the window from zero.
                if last.reputation_score.matches_committee(committee) {
                    last.reputation_score.clone()
                } else {
                    warn!(
                        target: "consensus::reputation",
                        sub_dag_index,
                        scored = last.reputation_score.total_authorities(),
                        committee_size = committee.size(),
                        "previous reputation scores do not match the committee, resetting"
                    );
                    Self::new(committee)
                }
            } else {
                Self::new(committee)
            };

        // Score the previous leader. Without a previous sub dag this is the first commit, so
        // there is nothing to score.
        if let Some(last) = previous {
            let leader_digest = last.leader.digest();
            let leader_round = last.leader.round();
            for certificate in committed_sequence.iter().filter(|c| c.round() == leader_round + 1) {
                if certificate.header().parents().contains(&leader_digest) {
                    reputation_score.add_score(certificate.origin(), 1);
                }
            }
        }

        reputation_score.final_of_schedule =
            ((sub_dag_index / 2) + 1).is_multiple_of(num_sub_dags_per_schedule as u64);

        reputation_score
    }

    /// True when the scores hold exactly one entry per member of `committee` and nothing else.
    ///
    /// Every set computed locally has this shape: it starts from [`Self::new`] and only adds
    /// scores for certificate origins. A set read back from storage may have been received from a
    /// peer and is not covered by the block digest, so callers check this before trusting it.
    pub fn matches_committee(&self, committee: &Committee) -> bool {
        self.scores_per_authority.len() == committee.size()
            && self.scores_per_authority.keys().all(|id| committee.authority(id).is_some())
    }

    /// Boolean if any authority reputations are above 0.
    pub fn all_zero(&self) -> bool {
        !self.scores_per_authority.values().any(|e| *e > 0)
    }

    /// Returns the authorities by score in descending order.
    pub fn authorities_by_score_desc(&self) -> Vec<(AuthorityIdentifier, u64)> {
        let mut authorities: Vec<_> = self
            .scores_per_authority
            .iter()
            .map(|(authority, score)| (authority.clone(), *score))
            .collect();

        authorities.sort_by(|a1, a2| {
            match a2.1.cmp(&a1.1) {
                Ordering::Equal => {
                    // we resolve the score equality deterministically by ordering in authority
                    // identifier order descending.
                    a2.0.cmp(&a1.0)
                }
                result => result,
            }
        });

        authorities
    }
}

/// BCS layout: `scores_per_authority: HashMap<AuthorityIdentifier, u64>` then
/// `final_of_schedule: bool`. Keep in lockstep with the struct.
impl BcsLayout for ReputationScores {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<HashMap<AuthorityIdentifier, u64>>()?.skip::<bool>()?;
        Ok(())
    }
}
