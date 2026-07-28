// SPDX-License-Identifier: BUSL-1.1
//! Consensus-DB-backed [`RewardsBackend`] implementation.
//!
//! Walks the consensus `ConsensusBlocks` table on demand at epoch boundary
//! to produce the per-address leader-execution tally for withdrawals.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use parking_lot::{Mutex, RwLock};
use rayls_infrastructure_storage::tables::ConsensusBlocks;
use rayls_infrastructure_types::{
    rewards::{HybridEpochTally, RewardsBackend, RewardsError, ValidatorRoundTally},
    Address, AuthorityIdentifier, Committee, ConsensusHeaderMeta, ConsensusHeaderParticipation,
    Database, DbTx, Epoch, WALK_PROGRESS_LOG_EVERY,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
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

    fn tally_hybrid(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<HybridEpochTally, RewardsError> {
        let committee = self
            .inner
            .committee
            .read()
            .as_ref()
            .cloned()
            .ok_or(RewardsError::MissingCommittee { epoch })?;

        self.inner
            .consensus_db
            .with_read_txn(|txn| {
                // See the identical comment in `tally()`: this walk is bounded by the
                // epoch length and reads only immutable historical headers, so opting
                // out of the 30s read-txn safety window is acceptable here too.
                txn.disable_long_read_safety();
                info!(target: "rewards", ?epoch, "tally_hybrid: read txn opened");

                let mut per_address: BTreeMap<Address, ValidatorRoundTally> = BTreeMap::new();
                let mut total_rounds: u32 = 0;
                let mut walked: u64 = 0;
                for (_key_bytes, value_bytes) in txn.reverse_raw_iter::<ConsensusBlocks>() {
                    walked += 1;
                    let meta = ConsensusHeaderParticipation::from_bytes(&value_bytes)?;
                    let header_epoch = meta.leader_epoch;
                    let header_round = meta.leader_round;

                    if walked == 1 || walked.is_multiple_of(WALK_PROGRESS_LOG_EVERY) {
                        info!(
                            target: "rewards",
                            ?epoch,
                            walked,
                            cur_epoch = header_epoch,
                            cur_round = header_round,
                            "tally_hybrid: walk progress",
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
                    // genesis has no leader / participants.
                    if header_round == 0 {
                        continue;
                    }

                    total_rounds += 1;

                    if let Some(authority) = committee.authority(&meta.leader_author) {
                        per_address
                            .entry(authority.execution_address())
                            .or_default()
                            .leader_rounds += 1;
                    }

                    // Dedup on the resolved execution address, not the pre-resolution
                    // authority id: a round credits each address at most once, so two
                    // distinct authority ids that resolve to the same execution address
                    // (invariant-precluded, but defended for symmetry with the legacy
                    // `get_address_counts` merge) can't double-count within one sub-dag.
                    let mut seen = BTreeSet::new();
                    for author in &meta.participants {
                        if let Some(authority) = committee.authority(author) {
                            let address = authority.execution_address();
                            if seen.insert(address) {
                                per_address.entry(address).or_default().participation_rounds += 1;
                            }
                        }
                    }
                }
                info!(target: "rewards", ?epoch, walked, total_rounds, "tally_hybrid: walk done");
                Ok(HybridEpochTally { per_address, total_rounds })
            })
            .map_err(RewardsError::Database)
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
    use alloy::primitives::BlockHash;
    use indexmap::IndexMap;
    use rand::{rngs::StdRng, SeedableRng};
    use rayls_infrastructure_storage::{
        cold_archiver_for, mem_db::MemDatabase, open_db, SealOutcome,
    };
    use rayls_infrastructure_types::{
        BlsKeypair, Certificate, CertificateDigest, CommittedSubDag, CommitteeBuilder,
        ConsensusHeader, DbTxMut, Header, HeaderBuilder, ReputationScores, Round, WorkerId,
    };
    use tempfile::TempDir;

    /// Build an `n`-authority committee with real BLS-derived identifiers
    /// (`Committee::authority` looks up by `Authority::id()`, a hash of the
    /// protocol key - `dummy_for_test` ids never resolve). Returns the
    /// committee plus each authority's id and execution address, in the order
    /// added.
    fn test_committee(n: u8) -> (Committee, Vec<(AuthorityIdentifier, Address)>) {
        let mut rng = StdRng::seed_from_u64(0x633);
        let mut builder = CommitteeBuilder::new(0);
        let mut ids = Vec::with_capacity(n as usize);
        for i in 0..n {
            let keypair = BlsKeypair::generate(&mut rng);
            let address = Address::with_last_byte(i + 1);
            builder.add_authority(*keypair.public(), 1, address);
            ids.push((AuthorityIdentifier::from(*keypair.public()), address));
        }
        (builder.build(), ids)
    }

    /// Builds a stored consensus block whose sub-dag leader fixes `(author, round, epoch)`.
    fn block(
        number: u64,
        epoch: Epoch,
        round: Round,
        author: &AuthorityIdentifier,
    ) -> ConsensusHeader {
        let mut leader = Certificate::default();
        leader.header = Header { author: author.clone(), round, epoch, ..Default::default() };
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
        let (committee, ids) = test_committee(3);
        let tmp = TempDir::new().unwrap();
        let db = open_db(tmp.path().join("consensus"));

        // Epoch 0 (sealable), the closing epoch 1, and a first epoch-2 row modeling a subscriber
        // that raced past the boundary before the epoch-1 tally ran.
        let blocks = [
            block(0, 0, 0, &ids[0].0), // genesis round 0: never counted
            block(1, 0, 1, &ids[1].0),
            block(2, 0, 2, &ids[2].0),
            block(3, 1, 3, &ids[0].0),
            block(4, 1, 4, &ids[1].0),
            block(5, 1, 5, &ids[2].0),
            block(6, 1, 6, &ids[0].0),
            block(7, 2, 7, &ids[1].0),
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

    fn make_cert(author: &AuthorityIdentifier, round: u32, epoch: u32) -> Certificate {
        let header = HeaderBuilder::default()
            .author(author.clone())
            .round(round)
            .epoch(epoch)
            .created_at(0)
            .payload(IndexMap::<BlockHash, WorkerId>::new())
            .parents(std::collections::BTreeSet::<CertificateDigest>::new())
            .build();
        let mut cert = Certificate::default();
        cert.update_header_for_test(header);
        cert
    }

    /// Encode a `ConsensusHeader` for one committed round: `leader_author`
    /// leads at `leader_round`, and every address in `participant_authors`
    /// (which may include the leader) has a certificate in the sub-dag.
    fn make_bytes(
        leader_author: &AuthorityIdentifier,
        leader_round: u32,
        epoch: u32,
        participant_authors: &[&AuthorityIdentifier],
    ) -> Vec<u8> {
        let leader = make_cert(leader_author, leader_round, epoch);
        let certs = participant_authors
            .iter()
            .map(|a| make_cert(a, leader_round.saturating_sub(1), epoch))
            .collect();
        let sub_dag = CommittedSubDag::new(certs, leader, 0, ReputationScores::default(), None);
        let header = ConsensusHeader {
            parent_hash: BlockHash::repeat_byte(0xAB),
            sub_dag,
            number: leader_round as u64,
            extra: BlockHash::ZERO,
        };
        rayls_infrastructure_types::encode(&header)
    }

    /// The scenario from axyl-private#633: a US validator (`ids[0]`) leads
    /// every round and is the only one credited under the leader-only tally
    /// (`tally()`). A latency-distant validator (`ids[1]`,
    /// "Tokyo") never leads but its certificate is included in every
    /// committed sub-dag. `tally_hybrid` should credit all three roughly
    /// equally on participation, while crediting only `ids[0]` on
    /// leader/anchor rounds - in one single walk, not two.
    #[test]
    fn tally_hybrid_credits_participation_and_leader_rounds_in_one_walk() {
        let (committee, ids) = test_committee(3);
        let (leader_id, leader_addr) = &ids[0];
        let (_, tokyo_addr) = &ids[1];
        let (_, third_addr) = &ids[2];
        let all_ids: Vec<&AuthorityIdentifier> = ids.iter().map(|(id, _)| id).collect();

        let db = MemDatabase::default();
        for (i, round) in [2u32, 4, 6, 8].into_iter().enumerate() {
            let bytes = make_bytes(leader_id, round, 0, &all_ids);
            let header: ConsensusHeader = rayls_infrastructure_types::decode(&bytes);
            db.insert::<ConsensusBlocks>(&(i as u64), &header).unwrap();
        }

        let counter = ConsensusRewardsCounter::new(db);
        counter.set_committee(committee);
        let tally = counter.tally_hybrid(0, u32::MAX).unwrap();

        assert_eq!(tally.total_rounds, 4);
        // Leader-only tally() would show leader_addr=4, tokyo_addr=0, third_addr=0.
        // Both other addresses still get a `per_address` entry (created by the
        // participation side of the walk), just with leader_rounds at its
        // `ValidatorRoundTally::default()` of 0 - not absent from the map.
        assert_eq!(tally.per_address[leader_addr].leader_rounds, 4);
        assert_eq!(tally.per_address[tokyo_addr].leader_rounds, 0);
        assert_eq!(tally.per_address[third_addr].leader_rounds, 0);
        // tally_hybrid credits all three on participation, unlike tally().
        for (_, address) in &ids {
            assert_eq!(tally.per_address[address].participation_rounds, 4);
        }
    }

    #[test]
    fn tally_hybrid_dedupes_repeated_author_within_one_subdag() {
        // order_dag can flatten multiple underlying DAG rounds into one
        // committed sub-dag, so the same author's cert can appear twice.
        let (committee, ids) = test_committee(2);
        let (id_a, _) = &ids[0];
        let (id_b, addr_b) = &ids[1];
        let leader = make_cert(id_a, 4, 0);
        let certs = vec![make_cert(id_b, 2, 0), make_cert(id_b, 3, 0)];
        let sub_dag = CommittedSubDag::new(certs, leader, 0, ReputationScores::default(), None);
        let header = ConsensusHeader {
            parent_hash: BlockHash::repeat_byte(0xAB),
            sub_dag,
            number: 4,
            extra: BlockHash::ZERO,
        };

        let db = MemDatabase::default();
        db.insert::<ConsensusBlocks>(&0u64, &header).unwrap();

        let counter = ConsensusRewardsCounter::new(db);
        counter.set_committee(committee);
        let tally = counter.tally_hybrid(0, u32::MAX).unwrap();

        assert_eq!(tally.per_address[addr_b].participation_rounds, 1);
    }

    #[test]
    fn tally_hybrid_excludes_genesis_and_out_of_range_rounds() {
        // CommitteeBuilder requires size > 1; only id_a's rounds are exercised.
        let (committee, ids) = test_committee(2);
        let (id_a, _) = &ids[0];

        let db = MemDatabase::default();
        // genesis: leader_round == 0
        let genesis_bytes = make_bytes(id_a, 0, 0, &[id_a]);
        let genesis_header: ConsensusHeader = rayls_infrastructure_types::decode(&genesis_bytes);
        db.insert::<ConsensusBlocks>(&0u64, &genesis_header).unwrap();
        // not yet executed (round 100 > last_executed_round 10)
        let future_bytes = make_bytes(id_a, 100, 0, &[id_a]);
        let future_header: ConsensusHeader = rayls_infrastructure_types::decode(&future_bytes);
        db.insert::<ConsensusBlocks>(&1u64, &future_header).unwrap();

        let counter = ConsensusRewardsCounter::new(db);
        counter.set_committee(committee);
        let tally = counter.tally_hybrid(0, 10).unwrap();

        assert_eq!(tally.total_rounds, 0);
        assert!(tally.per_address.is_empty());
    }
}
