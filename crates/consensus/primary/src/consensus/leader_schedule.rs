//! Leader schedule for identifying the next leader for the round.

use super::Dag;
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IndexedRandom as _, SeedableRng};
use rayls_infrastructure_storage::ConsensusStore;
use rayls_infrastructure_types::{
    Authority, AuthorityIdentifier, Certificate, Committee, ReputationScores, Round, VotingPower,
};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Arc,
};
use tracing::{debug, trace, warn};

#[cfg(test)]
#[path = "tests/leader_schedule_tests.rs"]
mod leader_schedule_tests;

#[derive(Default, Clone)]
pub struct LeaderSwapTable {
    /// The round on which the leader swap table get into effect.
    round: Round,
    /// The list of `f` authorities with best scores as those defined by the provided
    /// `ReputationScores`. Those authorities will be used in the position of the `bad_nodes`
    /// on the final leader schedule.
    good_nodes: Vec<Authority>,
    /// The set of `f` authorities with the worst scores as those defined by the
    /// provided `ReputationScores`. Every time where such authority is elected as leader on
    /// the schedule, it will swapped by one of the authorities of the `good_nodes`.
    bad_nodes: HashMap<AuthorityIdentifier, Authority>,
}

impl Debug for LeaderSwapTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "LeaderSwapTable round:{}, good_nodes:{:?} with voting power:{}, bad_nodes:{:?} with voting power:{}",
            self.round,
            self.good_nodes.iter().map(|a| a.id()).collect::<Vec<AuthorityIdentifier>>(),
            self.good_nodes.iter().map(|a| a.voting_power()).sum::<VotingPower>(),
            self.bad_nodes.iter().map(|a| a.0.clone()).collect::<Vec<AuthorityIdentifier>>(),
            self.bad_nodes.iter().map(|a| a.1.voting_power()).sum::<VotingPower>(),
        ))
    }
}

impl LeaderSwapTable {
    /// Constructs a new table based on the provided reputation scores. The
    /// `bad_nodes_percent_threshold` designates the total percent nodes that can be
    /// considered as "bad" based on their scores and will be replaced by good nodes.
    ///
    /// The `bad_nodes_percent_threshold` should be in the range of [0 - 33].
    ///
    /// Nodes should not be on the bad list unless they are underperfoming.
    /// No more than `bad_nodes_percent_threshold` of our nodes should be
    /// "bad" (less is fine).
    ///
    /// If we have bad nodes then we MUST have good nodes to swap with (an empty good list will lead
    /// to panics later).  We want at least `bad_nodes_percent_threshold` of nodes on the good list
    /// if we have bad nodes (more is fine).
    pub fn new(
        committee: &Committee,
        round: Round,
        reputation_scores: &ReputationScores,
        bad_nodes_percent_threshold: u64,
    ) -> Self {
        assert!((0..=33).contains(&bad_nodes_percent_threshold), "The bad_nodes_percent_threshold should be in range [0 - 33], out of bounds parameter detected");
        assert!(reputation_scores.final_of_schedule, "Only reputation scores that have been calculated on the end of a schedule are accepted");

        // Scores are not covered by the block digest, so a set read back from storage may come
        // from a peer. Refuse any set that is not exactly one entry per committee member and fall
        // back to the plain round-robin schedule, instead of dividing by zero on an empty set.
        if !reputation_scores.matches_committee(committee) {
            warn!(
                target: "consensus::leader_schedule",
                round,
                scored = reputation_scores.total_authorities(),
                committee_size = committee.size(),
                "refusing reputation scores that do not match the committee, no leader swaps"
            );
            return Self { round, good_nodes: vec![], bad_nodes: HashMap::default() };
        }

        let auths_by_score = reputation_scores.authorities_by_score_desc();
        let list_threshold =
            ((auths_by_score.len() * bad_nodes_percent_threshold as usize) / 100).max(1);
        // Most validators should have a rep near this highest value if they are honest and are
        // partcipating.
        let highest_rep = auths_by_score.first().map(|(_, rep)| *rep).unwrap_or(0);
        // Do we have any validators that are not participating?  If so this will be a lot lower
        // than highest_rep.
        let lowest_rep = auths_by_score.last().map(|(_, rep)| *rep).unwrap_or(0);
        let mean_rep = if auths_by_score.is_empty() {
            0
        } else {
            let mean_rep: u64 = auths_by_score.iter().map(|(_, r)| r).sum();
            mean_rep / auths_by_score.len() as u64
        };
        let mut standard_dev: u64 = auths_by_score
            .iter()
            .map(|(_, r)| {
                let v = *r as i64 - mean_rep as i64;
                (v * v) as u64
            })
            .sum();
        standard_dev /= auths_by_score.len() as u64;
        standard_dev = (standard_dev as f64).sqrt() as u64;
        // Generate the "good" and "bad" nodes.  We need to have a bad_nodes_percent_threshold > 0
        // AND we want a reasonable delta between the highest and lowest reputation
        // otherwise we don't tag any node as "bad". Rayls network is running a closed
        // validator set with high hardware and network requirements so this is really just
        // to filter out down validators or nodes with a broken net connection etc so this
        // should be OK.
        let (good_nodes, bad_nodes) =
            if standard_dev == 0 || lowest_rep > highest_rep.saturating_sub(2 * standard_dev) {
                (vec![], HashMap::default())
            } else {
                // calc bad nodes
                let mut bad_ceil = highest_rep.saturating_sub(2 * standard_dev);
                let mut old_bad_ceil = bad_ceil;
                let bad_nodes = loop {
                    let bad_nodes: Vec<Authority> =
                        auths_by_score
                            .iter()
                            .rev()
                            .filter_map(|(id, rep)| {
                                if *rep <= bad_ceil {
                                    committee.authority(id)
                                } else {
                                    None
                                }
                            })
                            .collect();
                    if bad_nodes.len() <= list_threshold {
                        break bad_nodes;
                    }
                    bad_ceil = bad_ceil.saturating_sub(standard_dev);
                    if old_bad_ceil == bad_ceil {
                        // If we have bottomed out the bad ceiling and still have more than 1/3
                        // nodes. Something is wrong...
                        break bad_nodes;
                    }
                    old_bad_ceil = bad_ceil;
                };
                // calculating the good nodes
                // This good floor should guarentee that at least one node will always be on the
                // good list. It is important to have a good list if we have a bad
                // list.
                let mut good_floor = highest_rep.saturating_sub(standard_dev).max(bad_ceil + 1);
                let mut old_good_floor = good_floor;
                let good_nodes = loop {
                    let good_nodes: Vec<Authority> =
                        auths_by_score
                            .iter()
                            .filter_map(|(id, rep)| {
                                if *rep >= good_floor {
                                    committee.authority(id)
                                } else {
                                    None
                                }
                            })
                            .collect();
                    if good_nodes.len() >= list_threshold {
                        break good_nodes;
                    }
                    good_floor = good_floor.saturating_sub(standard_dev).max(bad_ceil + 1);
                    if old_good_floor == good_floor {
                        break good_nodes;
                    }
                    old_good_floor = good_floor;
                };

                if !bad_nodes.is_empty() {
                    // Make sure we have good nodes if we have bad nodes.
                    // It should not be possible to get in this condition.
                    assert!(!good_nodes.is_empty());
                }

                let bad_nodes = bad_nodes
                    .into_iter()
                    .map(|authority| (authority.id(), authority))
                    .collect::<HashMap<AuthorityIdentifier, Authority>>();
                (good_nodes, bad_nodes)
            };

        good_nodes.iter().for_each(|good_node| {
            debug!(
                "Good node on round {}: {} -> {}",
                round,
                good_node.id(),
                reputation_scores
                    .scores_per_authority
                    .get(&good_node.id())
                    .expect("good node in scores per authority")
            );
        });

        bad_nodes.iter().for_each(|(_id, bad_node)| {
            debug!(
                "Bad node on round {}: {} -> {}",
                round,
                bad_node.id(),
                reputation_scores
                    .scores_per_authority
                    .get(&bad_node.id())
                    .expect("bad node in scores by authority")
            );
        });

        debug!("Reputation scores on round {round}: {reputation_scores:?}");

        Self { round, good_nodes, bad_nodes }
    }

    /// Checks whether the provided leader is a bad performer and needs to be swapped in the
    /// schedule with a good performer. If not, then the method returns None. Otherwise the
    /// leader to swap with is returned instead. The `leader_round` represents the DAG round on
    /// which the provided AuthorityIdentifier is a leader on and is used as a seed to random
    /// function in order to calculate the good node that will swap in that round with the bad
    /// node. We are intentionally not doing weighted randomness as we want to give to all the
    /// good nodes equal opportunity to get swapped with bad nodes.
    pub fn swap(&self, leader: &AuthorityIdentifier, leader_round: Round) -> Option<Authority> {
        if self.bad_nodes.contains_key(leader) {
            let mut seed_bytes = [0u8; 32];
            seed_bytes[32 - 8..].copy_from_slice(&(leader_round as u64).to_le_bytes());
            let mut rng = StdRng::from_seed(seed_bytes);

            let good_node = self
                .good_nodes
                .choose(&mut rng)
                .expect("There should be at least one good node available");

            trace!(
                "Swapping bad leader {} -> {} for round {}",
                leader,
                good_node.id(),
                leader_round
            );

            return Some(good_node.to_owned());
        }
        None
    }
}

/// The LeaderSchedule is responsible for producing the leader schedule across an epoch.
///
/// It provides methods to derive the leader of a round based on the provided leader swap table.
/// This struct can be cloned and shared freely as the internal parts are atomically updated.
#[derive(Clone, Debug)]
pub struct LeaderSchedule {
    /// The committee for this schedule.
    pub committee: Committee,
    /// The leader swap table.
    pub leader_swap_table: Arc<RwLock<LeaderSwapTable>>,
}

impl LeaderSchedule {
    pub fn new(committee: Committee, table: LeaderSwapTable) -> Self {
        Self { committee, leader_swap_table: Arc::new(RwLock::new(table)) }
    }

    /// Restores the LeaderSchedule by using the storage. It will attempt to retrieve the last
    /// committed "final" ReputationScores and use them to create build a LeaderSwapTable to use
    /// for the LeaderSchedule.
    pub fn from_store<DB: ConsensusStore>(
        committee: Committee,
        store: DB,
        bad_nodes_percent_threshold: u64,
    ) -> Self {
        let table = store
            .read_latest_commit_with_final_reputation_scores(committee.epoch())
            .map_or(LeaderSwapTable::default(), |subdag| {
                LeaderSwapTable::new(
                    &committee,
                    subdag.leader_round(),
                    &subdag.reputation_score,
                    bad_nodes_percent_threshold,
                )
            });
        // create the schedule
        Self::new(committee, table)
    }

    /// Atomically updates the leader swap table with the new provided one. Any leader queried from
    /// now on will get calculated according to this swap table until a new one is provided again.
    pub fn update_leader_swap_table(&self, table: LeaderSwapTable) {
        trace!("Updating swap table {:?}", table);

        let mut write = self.leader_swap_table.write();
        *write = table;
    }

    /// Returns the leader for the provided round. Keep in mind that this method will return a
    /// leader according to the provided LeaderSwapTable. Providing a different table can
    /// potentially produce a different leader for the same round.
    pub fn leader(&self, round: Round) -> Authority {
        assert_eq!(round % 2, 0, "We should never attempt to do a leader election for odd rounds");

        // We apply round robin in leader election. Since we expect round to be an even number,
        // 2, 4, 6, 8... it can't work well for leader election as we'll omit leaders. Thus
        // we can always divide by 2 to get a monotonically incremented sequence,
        // 2/2 = 1, 4/2 = 2, 6/2 = 3, 8/2 = 4  etc, and then do minus 1 so we can always
        // start with base zero 0.
        let next_leader = (round as usize / 2).saturating_sub(1) % self.committee.size();

        let leader: Authority = self
            .committee
            .authorities()
            .get(next_leader)
            .expect("authority out of bounds!")
            .clone();
        let table = self.leader_swap_table.read();

        table.swap(&leader.id(), round).unwrap_or(leader)
    }

    /// Returns the certificate originated by the leader of the specified round (if any). The
    /// Authority leader of the round is always returned and that's irrespective of whether the
    /// certificate exists as that's deterministically determined. The provided
    /// `leader_swap_table` is being used to determine any overrides that need to be performed
    /// to the original schedule.
    pub fn leader_certificate<'a>(
        &self,
        round: Round,
        dag: &'a Dag,
    ) -> (Authority, Option<&'a Certificate>) {
        // Note: this function is often called with even rounds only. While we do not aim at random
        // selection yet (see issue https://github.com/MystenLabs/sui/issues/5182), repeated calls to this function
        // should still pick from the whole roster of leaders.
        let leader = self.leader(round);

        // Return its certificate and the certificate's digest.
        match dag.get(&round).and_then(|x| x.get(&leader.id())) {
            None => (leader, None),
            Some((_, certificate)) => (leader, Some(certificate)),
        }
    }

    pub fn num_of_bad_nodes(&self) -> usize {
        let read = self.leader_swap_table.read();
        read.bad_nodes.len()
    }
}
