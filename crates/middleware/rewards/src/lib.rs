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

        // The bitmap index space belongs to `epoch`'s membership, not to whatever committee
        // happens to be installed. `bls_keys()` iterates a `BTreeMap<BlsPublicKey, _>`, so an
        // added or removed authority shifts the rank of every key sorted after it and the same
        // bit then resolves to a different address — silently, because every lookup still
        // succeeds. The live path installs the committee at epoch start (`epoch_manager`), but
        // re-execution of a close block during catch-up runs this walk with whatever epoch the
        // node has since moved to, so the two can diverge. Refuse rather than guess.
        //
        // `tally()` is deliberately left unguarded: it resolves by identity, so a mismatch there
        // drops unknown authorities instead of misattributing, and it is the live path on every
        // real network (`HybridRewards` is `ForkCondition::Never` on devnet/testnet/mainnet).
        // Making that path fail closed changes production behaviour and belongs on its own.
        if committee.epoch() != epoch {
            return Err(RewardsError::CommitteeEpochMismatch {
                requested: epoch,
                installed: committee.epoch(),
            });
        }

        // Signer bitmaps index authorities sorted by BLS public key, which is exactly
        // `Committee::bls_keys()` order and NOT `Committee::authorities()` order (sorted by
        // identifier) — see `Certificate::skip_with_header_and_signers_span`. Resolved once per
        // tally rather than per round. `None` entries are impossible (every key comes from the
        // committee's own map) but are skipped rather than defaulted, so a future refactor
        // cannot silently credit the zero address.
        let signer_addresses: Vec<Option<Address>> = committee
            .bls_keys()
            .iter()
            .map(|key| committee.authority_by_key(key).map(|a| a.execution_address()))
            .collect();

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
                // Reused across rounds (cleared each round) to avoid a per-round heap
                // allocation in this once-per-epoch-close walk.
                let mut seen: BTreeSet<Address> = BTreeSet::new();
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

                    total_rounds = total_rounds.saturating_add(1);

                    if let Some(authority) = committee.authority(&meta.leader_author) {
                        let tally = per_address.entry(authority.execution_address()).or_default();
                        tally.leader_rounds = tally.leader_rounds.saturating_add(1);
                    }

                    // Participation credits SIGNING, not authorship: an authority counts for
                    // this round when its vote reached a certificate that was committed,
                    // whether or not its own certificate was committed. Authorship penalises
                    // latency-distant validators, whose certificates routinely miss the
                    // commit while their votes still land (axyl#246).
                    //
                    // Dedup on the resolved execution address, not the committee index: a
                    // round credits each address at most once, so two distinct authorities
                    // resolving to the same execution address (invariant-precluded, but
                    // defended for symmetry with the legacy `get_address_counts` merge)
                    // can't double-count within one sub-dag.
                    seen.clear();
                    for index in &meta.signers {
                        if let Some(address) =
                            signer_addresses.get(*index as usize).copied().flatten()
                        {
                            if seen.insert(address) {
                                let tally = per_address.entry(address).or_default();
                                tally.participation_rounds =
                                    tally.participation_rounds.saturating_add(1);
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
        BlsKeypair, BlsPublicKey, BlsSignature, Certificate, CertificateDigest, CommittedSubDag,
        CommitteeBuilder, ConsensusHeader, DbTxMut, Header, HeaderBuilder, ReputationScores, Round,
        WorkerId,
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
        db.sync_persist().expect("persist");

        let counter = ConsensusRewardsCounter::new(db.clone());
        counter.set_committee(committee);
        let before = counter.tally(1, 6).expect("pre-archival tally");
        assert!(!before.is_empty(), "fixture must produce a non-empty tally");

        // Advance archival to the floor: the EL anchor is still inside the closing epoch 1, so
        // exactly epoch 0 seals though consensus has committed into epoch 2.
        let archiver = cold_archiver_for(&db).expect("mdbx stack has a cold tier");
        assert_eq!(archiver.seal_due(1, || false).expect("seal"), SealOutcome::Sealed(0));
        assert_eq!(archiver.seal_due(1, || false).expect("drained"), SealOutcome::Drained);
        db.sync_persist().expect("persist");

        let after = counter.tally(1, 6).expect("post-archival tally");
        assert_eq!(after, before, "the closing epoch's tally must survive archival at the floor");
    }

    /// Build a certificate authored by `author` and signed by `signers`.
    ///
    /// Goes through the production constructor so the `signed_authorities` bitmap is built by
    /// the same code the tally decodes. A hand-set bitmap would let the test agree with a wrong
    /// index space; this way the committee ordering is exercised rather than assumed.
    fn make_cert(
        committee: &Committee,
        author: &AuthorityIdentifier,
        round: u32,
        epoch: u32,
        signers: &[&AuthorityIdentifier],
    ) -> Certificate {
        let header = HeaderBuilder::default()
            .author(author.clone())
            .round(round)
            .epoch(epoch)
            .created_at(0)
            .payload(IndexMap::<BlockHash, WorkerId>::new())
            .parents(std::collections::BTreeSet::<CertificateDigest>::new())
            .build();
        let votes: Vec<(AuthorityIdentifier, BlsSignature)> =
            signers.iter().map(|s| ((*s).clone(), BlsSignature::default())).collect();
        Certificate::new_unsigned_for_test(committee, header, votes)
            .expect("test certificate builds")
    }

    /// Encode one committed sub-dag from explicit `(author, signers)` pairs.
    fn make_bytes_per_cert(
        committee: &Committee,
        leader_author: &AuthorityIdentifier,
        leader_round: u32,
        epoch: u32,
        certs: &[(&AuthorityIdentifier, &[&AuthorityIdentifier])],
    ) -> Vec<u8> {
        let leader = make_cert(committee, leader_author, leader_round, epoch, &[]);
        let certs = certs
            .iter()
            .map(|(author, signers)| {
                make_cert(committee, author, leader_round.saturating_sub(1), epoch, signers)
            })
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

    /// Encode a `ConsensusHeader` for one committed round: `leader_author`
    /// leads at `leader_round`, and every address in `participant_authors`
    /// (which may include the leader) has a certificate in the sub-dag.
    fn make_bytes(
        committee: &Committee,
        leader_author: &AuthorityIdentifier,
        leader_round: u32,
        epoch: u32,
        participant_authors: &[&AuthorityIdentifier],
        signers: &[&AuthorityIdentifier],
    ) -> Vec<u8> {
        let certs: Vec<(&AuthorityIdentifier, &[&AuthorityIdentifier])> =
            participant_authors.iter().map(|a| (*a, signers)).collect();
        make_bytes_per_cert(committee, leader_author, leader_round, epoch, &certs)
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
            let bytes = make_bytes(&committee, leader_id, round, 0, &all_ids, &all_ids);
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
    fn tally_hybrid_dedupes_repeated_signer_within_one_subdag() {
        // order_dag can flatten multiple underlying DAG rounds into one committed sub-dag, so
        // the same authority can sign several certificates inside one round. It must be
        // credited once for the round, not once per certificate.
        let (committee, ids) = test_committee(2);
        let (id_a, _) = &ids[0];
        let (id_b, addr_b) = &ids[1];

        let bytes = make_bytes_per_cert(
            &committee,
            id_a,
            4,
            0,
            &[(id_a, &[id_b][..]), (id_a, &[id_b][..])],
        );
        let header: ConsensusHeader = rayls_infrastructure_types::decode(&bytes);

        let db = MemDatabase::default();
        db.insert::<ConsensusBlocks>(&0u64, &header).unwrap();

        let counter = ConsensusRewardsCounter::new(db);
        counter.set_committee(committee);
        let tally = counter.tally_hybrid(0, u32::MAX).unwrap();

        assert_eq!(tally.per_address[addr_b].participation_rounds, 1);
    }

    /// The axyl#246 case: a latency-distant validator whose own certificates never get
    /// committed, but whose votes do land. Authorship scores it zero; signing scores it full.
    #[test]
    fn tally_hybrid_credits_a_validator_that_signs_but_never_authors() {
        // Four authorities, only three of them signing: the signer set must stay a *proper*
        // subset of the committee. With every member signing, any permutation of the index space
        // maps the full set onto itself, and the test would pass even if the bitmap were resolved
        // against the wrong committee ordering.
        let (committee, ids) = test_committee(4);
        let (near_id, near_addr) = &ids[0];
        let (distant_id, distant_addr) = &ids[1];
        let (third_id, _) = &ids[2];
        let (_, silent_addr) = &ids[3];
        let signers = [near_id, distant_id, third_id];

        let db = MemDatabase::default();
        for (i, round) in [2u32, 4, 6, 8].into_iter().enumerate() {
            // every committed certificate is authored by the near validators only
            let bytes = make_bytes(&committee, near_id, round, 0, &[near_id, third_id], &signers);
            let header: ConsensusHeader = rayls_infrastructure_types::decode(&bytes);
            db.insert::<ConsensusBlocks>(&(i as u64), &header).unwrap();
        }

        let counter = ConsensusRewardsCounter::new(db);
        counter.set_committee(committee);
        let tally = counter.tally_hybrid(0, u32::MAX).unwrap();

        assert_eq!(tally.total_rounds, 4);
        // never authored a committed certificate, yet credited for every round
        assert_eq!(tally.per_address[distant_addr].participation_rounds, 4);
        assert_eq!(tally.per_address[near_addr].participation_rounds, 4);
        // and it still earns nothing from the anchor component
        assert_eq!(tally.per_address[distant_addr].leader_rounds, 0);
        // the non-signer earns nothing: this is what pins the index space, since crediting it
        // would mean the bitmap was resolved against the wrong committee ordering
        assert_eq!(
            tally.per_address.get(silent_addr).map(|t| t.participation_rounds).unwrap_or(0),
            0,
        );
    }

    /// The seeded authority set, ordered the way `Committee::bls_keys` reports it (the
    /// iteration order of a `BTreeMap<BlsPublicKey, _>`), so a slice of it is a committee
    /// with a known index space.
    fn seeded_members(n: u8) -> Vec<(BlsPublicKey, Address)> {
        let mut rng = StdRng::seed_from_u64(0x633);
        let mut members: Vec<(BlsPublicKey, Address)> = (0..n)
            .map(|i| (*BlsKeypair::generate(&mut rng).public(), Address::with_last_byte(i + 1)))
            .collect();
        members.sort_by_key(|(key, _)| *key);
        members
    }

    /// Build a committee at `epoch` from `members`.
    fn committee_of(epoch: Epoch, members: &[(BlsPublicKey, Address)]) -> Committee {
        let mut builder = CommitteeBuilder::new(epoch);
        for (key, address) in members {
            builder.add_authority(*key, 1, *address);
        }
        builder.build()
    }

    /// The bitmap index space belongs to the epoch being tallied, not to whatever committee
    /// happens to be installed. A mismatch is the silent-misattribution path, so the walk must
    /// refuse rather than hand back a tally that looks entirely plausible.
    #[test]
    fn tally_hybrid_refuses_a_committee_from_another_epoch() {
        // Epoch 1's certificates, signed by a proper subset so the bitmap actually carries
        // position information; epoch 0's committee is the same set minus the authority that
        // sorts first, so every surviving rank has slid down by one.
        let members = seeded_members(4);
        let signing_committee = committee_of(1, &members);
        let stale_committee = committee_of(0, &members[1..]);

        let ids: Vec<AuthorityIdentifier> =
            members.iter().map(|(key, _)| AuthorityIdentifier::from(*key)).collect();
        let signers: Vec<&AuthorityIdentifier> = ids[..3].iter().collect();

        let db = MemDatabase::default();
        let bytes = make_bytes(&signing_committee, &ids[0], 2, 1, &[&ids[0]], &signers);
        let header: ConsensusHeader = rayls_infrastructure_types::decode(&bytes);
        db.insert::<ConsensusBlocks>(&0u64, &header).unwrap();

        let counter = ConsensusRewardsCounter::new(db);

        // The epoch-1 rows are present and would be walked, so without the guard this returns a
        // populated tally resolved against the wrong index space — credit landing on addresses
        // that signed nothing. Refusing is the only safe answer.
        counter.set_committee(stale_committee);
        let err = counter
            .tally_hybrid(1, u32::MAX)
            .expect_err("a committee from another epoch must not resolve this bitmap");
        assert!(
            matches!(
                err,
                RewardsError::CommitteeEpochMismatch { requested: 1, installed: 0 }
            ),
            "unexpected error: {err:?}"
        );

        // The guard rejects the mismatch specifically, not every call: the right committee
        // tallies, and credits exactly the three signers.
        counter.set_committee(signing_committee);
        let tally = counter.tally_hybrid(1, u32::MAX).expect("matching epoch still tallies");
        assert_eq!(tally.total_rounds, 1);
        for (_, address) in &members[..3] {
            assert_eq!(tally.per_address[address].participation_rounds, 1);
        }
        assert_eq!(
            tally.per_address.get(&members[3].1).map(|t| t.participation_rounds).unwrap_or(0),
            0,
            "the non-signer must not be credited"
        );
    }

    /// Why the guard above is load-bearing. A bitmap index is a rank in
    /// `Committee::bls_keys()`, which iterates a `BTreeMap<BlsPublicKey, _>`. Dropping one
    /// authority renumbers every key sorted after it, so the *same* bit resolves to a different
    /// execution address — and every lookup still succeeds, so nothing signals the error.
    #[test]
    fn bls_key_index_space_renumbers_when_membership_changes() {
        let members = seeded_members(4);
        let before = committee_of(0, &members);
        // The authority sorting first leaves; everyone after it moves down one slot.
        let after = committee_of(1, &members[1..]);

        let addr_at = |committee: &Committee, i: usize| {
            committee
                .authority_by_key(&committee.bls_keys()[i])
                .map(|a| a.execution_address())
                .expect("key comes from the committee's own map")
        };

        assert_ne!(
            addr_at(&before, 0),
            addr_at(&after, 0),
            "bit 0 must resolve to a different validator once membership changes"
        );
        assert_eq!(
            addr_at(&before, 1),
            addr_at(&after, 0),
            "rank 1 slides into rank 0: that shift is exactly the misattribution"
        );
    }

    #[test]
    fn tally_hybrid_credits_a_signer_present_on_only_one_certificate_in_the_round() {
        let (committee, ids) = test_committee(3);
        let (id_a, _) = &ids[0];
        let (id_b, addr_b) = &ids[1];
        let (_, addr_c) = &ids[2];

        // id_b signs only the first certificate; id_c signs neither
        let bytes = make_bytes_per_cert(
            &committee,
            id_a,
            4,
            0,
            &[(id_a, &[id_a, id_b][..]), (id_a, &[id_a][..])],
        );
        let header: ConsensusHeader = rayls_infrastructure_types::decode(&bytes);

        let db = MemDatabase::default();
        db.insert::<ConsensusBlocks>(&0u64, &header).unwrap();

        let counter = ConsensusRewardsCounter::new(db);
        counter.set_committee(committee);
        let tally = counter.tally_hybrid(0, u32::MAX).unwrap();

        assert_eq!(tally.per_address[addr_b].participation_rounds, 1);
        assert_eq!(
            tally.per_address.get(addr_c).map(|t| t.participation_rounds).unwrap_or(0),
            0,
            "an authority that signed nothing in the round must not be credited"
        );
    }

    #[test]
    fn tally_hybrid_excludes_genesis_and_out_of_range_rounds() {
        // CommitteeBuilder requires size > 1; only id_a's rounds are exercised.
        let (committee, ids) = test_committee(2);
        let (id_a, _) = &ids[0];

        let db = MemDatabase::default();
        // genesis: leader_round == 0
        let genesis_bytes = make_bytes(&committee, id_a, 0, 0, &[id_a], &[id_a]);
        let genesis_header: ConsensusHeader = rayls_infrastructure_types::decode(&genesis_bytes);
        db.insert::<ConsensusBlocks>(&0u64, &genesis_header).unwrap();
        // not yet executed (round 100 > last_executed_round 10)
        let future_bytes = make_bytes(&committee, id_a, 100, 0, &[id_a], &[id_a]);
        let future_header: ConsensusHeader = rayls_infrastructure_types::decode(&future_bytes);
        db.insert::<ConsensusBlocks>(&1u64, &future_header).unwrap();

        let counter = ConsensusRewardsCounter::new(db);
        counter.set_committee(committee);
        let tally = counter.tally_hybrid(0, 10).unwrap();

        assert_eq!(tally.total_rounds, 0);
        assert!(tally.per_address.is_empty());
    }
}
