// SPDX-License-Identifier: BUSL-1.1
//! Consensus-DB-backed [`RewardsBackend`] implementation.
//!
//! Walks the consensus `ConsensusBlocks` table on demand at epoch boundary
//! to produce the per-address leader-execution tally for withdrawals.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use parking_lot::{Mutex, RwLock};
use rayls_infrastructure_storage::tables::ConsensusBlocks;
use rayls_infrastructure_types::{
    rewards::{RewardsBackend, RewardsError},
    Address, AuthorityIdentifier, Committee, ConsensusHeaderMeta, Database, DbTx, Epoch,
    WALK_PROGRESS_LOG_EVERY,
};
use std::{collections::BTreeMap, sync::Arc};
use tracing::{debug, info, warn};

pub use rayls_infrastructure_types::rewards::{NoopRewardsBackend, RewardsCounter};

/// `RewardsBackend` backed by the consensus DB.
pub struct ConsensusRewardsCounter<DB: Database> {
    inner: Arc<Inner<DB>>,
}

struct Inner<DB: Database> {
    consensus_db: DB,
    leader_counts: Mutex<BTreeMap<AuthorityIdentifier, u32>>,
    committee: RwLock<Option<Committee>>,
}

impl<DB: Database> std::fmt::Debug for ConsensusRewardsCounter<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusRewardsCounter").finish_non_exhaustive()
    }
}

impl<DB: Database> Clone for ConsensusRewardsCounter<DB> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<DB: Database> ConsensusRewardsCounter<DB> {
    /// Create a counter over `consensus_db`. Committee starts uninstalled.
    pub fn new(consensus_db: DB) -> Self {
        Self {
            inner: Arc::new(Inner {
                consensus_db,
                committee: RwLock::new(None),
                leader_counts: Mutex::new(BTreeMap::new()),
            }),
        }
    }
}

impl<DB: Database> RewardsBackend for ConsensusRewardsCounter<DB> {
    fn tally(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<BTreeMap<Address, u32>, RewardsError> {
        let committee = self
            .inner
            .committee
            .read()
            .as_ref()
            .cloned()
            .ok_or(RewardsError::MissingCommittee { epoch })?;

        let counts: BTreeMap<Address, u32> = self
            .inner
            .consensus_db
            .with_read_txn(|txn| {
                // The walk reverse-scans every ConsensusBlocks entry for the
                // epoch; on a node with a cold page cache (e.g. just resumed
                // after a backup) this can exceed mdbx's default 30s read-txn
                // safety window and get force-aborted mid-walk. Opt this txn
                // out — it's bounded by the epoch length and reads only
                // immutable historical headers, so a long snapshot pin is
                // acceptable.
                txn.disable_long_read_safety();
                info!(target: "rewards", ?epoch, "tally: read txn opened");

                let mut acc: BTreeMap<Address, u32> = BTreeMap::new();
                let mut walked: u64 = 0;
                for (_key_bytes, value_bytes) in txn.reverse_raw_iter::<ConsensusBlocks>() {
                    walked += 1;
                    let meta = ConsensusHeaderMeta::from_bytes(&value_bytes)?;
                    let header_epoch = meta.leader_epoch;
                    let header_round = meta.leader_round;

                    // walked=1 isolates the cold-cache cost of positioning
                    // the cursor at the rightmost leaf; subsequent ticks at
                    // every WALK_PROGRESS_LOG_EVERY reads show per-chunk
                    // wall-clock via log timestamp deltas.
                    if walked == 1 || walked.is_multiple_of(WALK_PROGRESS_LOG_EVERY) {
                        info!(
                            target: "rewards",
                            ?epoch,
                            walked,
                            cur_epoch = header_epoch,
                            cur_round = header_round,
                            "tally: walk progress",
                        );
                    }

                    // subscriber raced past the boundary - future epoch, not ours.
                    if header_epoch > epoch {
                        continue;
                    }
                    // crossed below current epoch - reverse-iter is done.
                    if header_epoch < epoch {
                        break;
                    }
                    // round not yet executed by the engine.
                    if header_round > last_executed_round {
                        continue;
                    }
                    // genesis has no leader.
                    if header_round == 0 {
                        continue;
                    }

                    let id = meta.leader_author;
                    if let Some(authority) = committee.authority(&id) {
                        *acc.entry(authority.execution_address()).or_insert(0) += 1;
                    }
                }
                info!(target: "rewards", ?epoch, walked, "tally: walk done");
                Ok(acc)
            })
            .map_err(RewardsError::Database)?;

        debug!(
            target: "rewards",
            ?epoch,
            ?last_executed_round,
            entries = counts.len(),
            "tally completed",
        );

        let in_memory_counts = self.get_address_counts();
        if in_memory_counts != counts {
            warn!(
                target: "rewards",
                ?epoch,
                ?last_executed_round,
                ?in_memory_counts,
                ?counts,
                "in-memory and in-accumulator leader counts differ",
            );
        }

        Ok(counts)
    }

    fn get_authority_address(&self, id: &AuthorityIdentifier) -> Option<Address> {
        self.inner
            .committee
            .read()
            .as_ref()
            .and_then(|c| c.authority(id).map(|a| a.execution_address()))
    }

    fn set_committee(&self, committee: Committee) {
        *self.inner.committee.write() = Some(committee);
    }

    fn get_address_counts(&self) -> BTreeMap<Address, u32> {
        let committee = self.inner.committee.read().as_ref().cloned();
        let counts = self.inner.leader_counts.lock();
        let mut result = BTreeMap::default();
        if let Some(committee) = committee {
            for (authority, count) in counts.iter() {
                if let Some(auth) = committee.authority(authority) {
                    let address = auth.execution_address();
                    // duplicate execution addresses across validators should not happen
                    // but merge the counts defensively if they do.
                    if let Some(c) = result.get_mut(&address) {
                        *c += count;
                    } else {
                        result.insert(address, *count);
                    }
                }
            }
        }
        result
    }

    fn set_leader_counts(&self, leader_counts: BTreeMap<AuthorityIdentifier, u32>) {
        let mut guard = self.inner.leader_counts.lock();
        *guard = leader_counts;
    }

    fn inc_leader_count(&self, leader: &AuthorityIdentifier) {
        let mut guard = self.inner.leader_counts.lock();
        *guard.entry(leader.clone()).or_insert(0) += 1;
    }

    fn clear(&self) {
        let mut guard = self.inner.leader_counts.lock();
        guard.clear();
    }
}

/// Build a production handle backed by any consensus database.
pub fn from_db<DB: Database>(db: DB) -> RewardsCounter {
    RewardsCounter::from_impl(ConsensusRewardsCounter::new(db))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use rayls_infrastructure_storage::{cold_archiver_for, open_db, SealOutcome};
    use rayls_infrastructure_types::{
        BlsKeypair, Certificate, CommittedSubDag, CommitteeBuilder, ConsensusHeader, DbTxMut,
        Header, ReputationScores, Round,
    };
    use tempfile::TempDir;

    /// Builds a committee of `n` authorities with distinct execution addresses.
    fn committee_with(n: usize) -> (Committee, Vec<AuthorityIdentifier>) {
        let mut rng = StdRng::seed_from_u64(7);
        let mut builder = CommitteeBuilder::new(0);
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let keypair = BlsKeypair::generate(&mut rng);
            let key = *keypair.public();
            builder.add_authority(key, 1, Address::repeat_byte(i as u8 + 1));
            ids.push(AuthorityIdentifier::from(key));
        }
        (builder.build(), ids)
    }

    /// Builds a stored consensus block whose sub-dag leader fixes `(author, round, epoch)`.
    fn block(
        number: u64,
        epoch: Epoch,
        round: Round,
        author: AuthorityIdentifier,
    ) -> ConsensusHeader {
        let mut leader = Certificate::default();
        leader.header = Header { author, round, epoch, ..Default::default() };
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusHeader { sub_dag, number, ..Default::default() }
    }

    /// The closing epoch's tally must survive cold archival advanced to the EL-anchor floor.
    ///
    /// The tally walks `ConsensusBlocks` with a hot-only reverse iterator, so the closing epoch
    /// `C` is readable at tally time only because the archival cutoff is floored by the EL anchor,
    /// which is still inside `C` while `C`'s boundary block executes. A subscriber racing ahead
    /// can commit epoch `C + 1` rows before the tally runs, so the consensus tip alone would
    /// allow sealing `C`: the anchor floor is the single protection. This fails if the floor is
    /// dropped or off-by-oned so epoch `C` itself seals (the walk sees nothing and the tally
    /// empties); the cutoff-policy red/green tests live in the storage crate's cold tests.
    #[test]
    fn tally_survives_cold_archival_at_anchor_floor() {
        let (committee, ids) = committee_with(3);
        let tmp = TempDir::new().unwrap();
        let db = open_db(tmp.path().join("consensus"));

        // Epoch 0 (sealable), the closing epoch 1, and a first epoch-2 row modeling a subscriber
        // that raced past the boundary before the epoch-1 tally ran.
        let blocks = [
            block(0, 0, 0, ids[0].clone()), // genesis round 0: never counted
            block(1, 0, 1, ids[1].clone()),
            block(2, 0, 2, ids[2].clone()),
            block(3, 1, 3, ids[0].clone()),
            block(4, 1, 4, ids[1].clone()),
            block(5, 1, 5, ids[2].clone()),
            block(6, 1, 6, ids[0].clone()),
            block(7, 2, 7, ids[1].clone()),
        ];
        db.with_write_txn(|txn| {
            for b in &blocks {
                txn.insert::<ConsensusBlocks>(&b.number, b)?;
            }
            Ok(())
        })
        .expect("seed");
        db.sync_persist();

        let counter = ConsensusRewardsCounter::new(db.clone());
        counter.set_committee(committee);
        let before = counter.tally(1, 6).expect("pre-archival tally");
        assert!(!before.is_empty(), "fixture must produce a non-empty tally");

        // Advance archival to the floor: the EL anchor is still inside the closing epoch 1, so
        // exactly epoch 0 seals though consensus has committed into epoch 2.
        let archiver = cold_archiver_for(&db).expect("mdbx stack has a cold tier");
        assert_eq!(archiver.seal_due(1, || false).expect("seal"), SealOutcome::Sealed(0));
        assert_eq!(archiver.seal_due(1, || false).expect("drained"), SealOutcome::Drained);
        db.sync_persist();

        let after = counter.tally(1, 6).expect("post-archival tally");
        assert_eq!(after, before, "the closing epoch's tally must survive archival at the floor");
    }
}
