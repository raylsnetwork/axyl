//! Snapshot-backed [`RewardsBackend`] for archive replay.
//!
//! Serves the close-epoch leader tally read from the snapshot block's
//! `withdrawals`, delegating committee and address resolution to
//! [`NoopRewardsBackend`]. Hybrid-reward epochs (post `HybridRewards` fork)
//! need per-validator participation rounds, which a block does not preserve;
//! those are recomputed over the snapshot's `ConsensusBlocks` by
//! [`BoundedHybridWalker`] and cross-checked against the withdrawals. Injected
//! only by `rayls-replay`.

use parking_lot::{Mutex, RwLock};
use rayls_infrastructure_storage::tables::ConsensusBlocks;
use rayls_infrastructure_types::{
    rewards::{
        HybridEpochTally, NoopRewardsBackend, RewardsBackend, RewardsCounter, RewardsError,
        ValidatorRoundTally,
    },
    Address, AuthorityIdentifier, Committee, ConsensusHeaderParticipation, Database, DbTx, Epoch,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, OnceLock},
};
use tracing::{info, warn};

/// Shared epoch -> committed-tally store.
///
/// The replay consumer fills it from each close block's snapshot withdrawals
/// before that block executes; [`SnapshotRewardsBackend::tally`] reads it.
#[derive(Clone, Debug, Default)]
pub struct SnapshotTallyStore(Arc<Mutex<BTreeMap<Epoch, BTreeMap<Address, u32>>>>);

impl SnapshotTallyStore {
    /// Record the committed tally for a closing `epoch`.
    pub fn insert(&self, epoch: Epoch, tally: BTreeMap<Address, u32>) {
        self.0.lock().insert(epoch, tally);
    }

    /// Committed tally for `epoch`, empty if none was recorded.
    fn get(&self, epoch: Epoch) -> BTreeMap<Address, u32> {
        self.0.lock().get(&epoch).cloned().unwrap_or_default()
    }
}

/// Late-bound consensus-DB walker serving hybrid-reward tallies.
///
/// The archive env (and its [`RewardsCounter`]) is built before the consensus DB
/// is opened, so the walker is attached afterwards through this shared slot.
/// Attach before the first committee install: `set_committee` only forwards to
/// a walker that is already present.
#[derive(Clone, Debug, Default)]
pub struct HybridTallySource(Arc<OnceLock<RewardsCounter>>);

impl HybridTallySource {
    /// Install the consensus-DB walker. Returns `false` if one was already attached.
    pub fn attach(&self, walker: RewardsCounter) -> bool {
        self.0.set(walker).is_ok()
    }

    fn get(&self) -> Option<&RewardsCounter> {
        self.0.get()
    }
}

/// [`RewardsBackend`] that serves the snapshot's committed close-epoch tally.
#[derive(Debug, Default)]
pub struct SnapshotRewardsBackend {
    committee: NoopRewardsBackend,
    tallies: SnapshotTallyStore,
    hybrid: HybridTallySource,
}

impl SnapshotRewardsBackend {
    /// Build a backend reading committed tallies from `tallies` and hybrid
    /// tallies from the walker later attached to `hybrid`.
    pub fn new(tallies: SnapshotTallyStore, hybrid: HybridTallySource) -> Self {
        Self { committee: NoopRewardsBackend::default(), tallies, hybrid }
    }

    /// Wrap into the type-erased [`RewardsCounter`] handle for `RethEnv`.
    pub fn into_counter(self) -> RewardsCounter {
        RewardsCounter::from_impl(self)
    }
}

impl RewardsBackend for SnapshotRewardsBackend {
    fn tally(
        &self,
        epoch: Epoch,
        _last_executed_round: u32,
    ) -> Result<BTreeMap<Address, u32>, RewardsError> {
        Ok(self.tallies.get(epoch))
    }

    fn tally_hybrid(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<HybridEpochTally, RewardsError> {
        // A `Withdrawal` carries one `u32` per validator (its leader rounds), so the
        // snapshot block alone cannot recover `participation_rounds`. Walk the
        // snapshot's consensus DB exactly as the live node did, then hold the
        // walk to the block's committed leader counts so the snapshot stays the
        // oracle: a disagreement means its consensus and execution DBs diverged.
        let walker = self.hybrid.get().ok_or_else(|| {
            RewardsError::Unsupported(format!(
                "hybrid-reward replay of epoch {epoch} needs the snapshot consensus DB, \
                 but no walker is attached"
            ))
        })?;
        let tally = walker.tally_hybrid(epoch, last_executed_round)?;

        // Exact map equality is intended, zero entries included. The live close block
        // writes one withdrawal per `per_address` entry with `amount = leader_rounds`
        // and no filtering (`CloseEpochTally::withdrawal_counts` -> `build_withdrawals`
        // in `crates/execution/evm/src/evm/block.rs`), and `snapshot_close_epoch_tally`
        // reads them back unfiltered. A validator that participated but never led is
        // therefore present on both sides with 0. Do NOT drop zeros here: that would
        // hide a walk crediting a leader the block never recorded.
        let committed = self.tallies.get(epoch);
        let walked: BTreeMap<Address, u32> =
            tally.per_address.iter().map(|(addr, t)| (*addr, t.leader_rounds)).collect();
        if walked != committed {
            return Err(RewardsError::Unsupported(format!(
                "hybrid tally for epoch {epoch} disagrees with the snapshot's committed \
                 withdrawals: consensus-DB leader rounds {walked:?} != withdrawals {committed:?}"
            )));
        }
        Ok(tally)
    }

    fn get_authority_address(&self, id: &AuthorityIdentifier) -> Option<Address> {
        self.committee.get_authority_address(id)
    }

    fn set_committee(&self, committee: Committee) {
        if let Some(walker) = self.hybrid.get() {
            walker.set_committee(committee.clone());
        }
        self.committee.set_committee(committee);
    }

    fn get_address_counts(&self) -> BTreeMap<Address, u32> {
        self.committee.get_address_counts()
    }

    fn set_leader_counts(&self, leader_counts: BTreeMap<AuthorityIdentifier, u32>) {
        self.committee.set_leader_counts(leader_counts);
    }

    fn inc_leader_count(&self, leader: &AuthorityIdentifier) {
        self.committee.inc_leader_count(leader);
    }

    fn clear(&self) {
        self.committee.clear();
    }
}

/// Rows of a later epoch tolerated past an epoch boundary before the walk stops.
/// Leader epochs are non-decreasing in consensus block number, so a well-formed
/// boundary interleaves by at most a handful of rows; 16 was chosen well above
/// any observed interleave depth. The cap should therefore almost never fire —
/// it is a safety net against interleaved or corrupt rows, not a functional
/// bound, so the exact value is not load-bearing. A firing cap is logged.
const BOUNDARY_LOOKAHEAD: u32 = 16;

/// Forward, cursor-based hybrid tally over the snapshot's `ConsensusBlocks`.
///
/// The live walker (`rayls_middleware_rewards::ConsensusRewardsCounter::tally_hybrid`)
/// iterates the table in reverse from its newest row until it drops below the closing
/// epoch. On a live node that is one epoch of rows. Against a snapshot whose consensus
/// DB runs millions of rows past the epoch being replayed it is a full tail scan per
/// epoch close (about 50s per epoch on a 5.7M-block devnet dump), which made
/// post-`HybridRewards` replay two orders of magnitude slower than the blocks before
/// the fork. Replay closes epochs in ascending order, so this walker keeps a cursor at
/// the first row of the next epoch and reads each epoch's rows exactly once with keyed
/// point lookups (`ConsensusBlocks` is keyed by the dense consensus block number). The
/// first call positions the cursor by binary search on the leader epoch.
///
/// Crediting is identical to the live walker: rounds `1..=last_executed_round` of the
/// epoch, one leader credit per row, one participation credit per distinct execution
/// address per row. [`SnapshotRewardsBackend::tally_hybrid`] cross-checks the leader
/// rounds against the block's committed withdrawals, so any deviation aborts the
/// replay instead of diverging silently.
pub struct BoundedHybridWalker<DB: Database> {
    db: DB,
    committee: RwLock<Option<Committee>>,
    /// Consensus block number at which the next epoch's rows start; `None` until the
    /// first tally positions it.
    cursor: Mutex<Option<u64>>,
}

impl<DB: Database> std::fmt::Debug for BoundedHybridWalker<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedHybridWalker")
            .field("cursor", &*self.cursor.lock())
            .finish_non_exhaustive()
    }
}

impl<DB: Database> BoundedHybridWalker<DB> {
    /// Walk `db`'s `ConsensusBlocks`. Committee starts uninstalled.
    pub fn new(db: DB) -> Self {
        Self { db, committee: RwLock::new(None), cursor: Mutex::new(None) }
    }

    /// Leader epoch of the row at `key`, or of the first existing row after it up to
    /// `last_key` (keys are dense, so a miss is a gap or the end of the table).
    fn epoch_at(txn: &impl DbTx, key: u64, last_key: u64) -> eyre::Result<Option<Epoch>> {
        let mut k = key;
        while k <= last_key {
            if let Some(bytes) = txn.raw_get::<ConsensusBlocks>(&k)? {
                return Ok(Some(ConsensusHeaderParticipation::from_bytes(&bytes)?.leader_epoch));
            }
            k += 1;
        }
        Ok(None)
    }

    /// Smallest key in `first_key..=last_key + 1` (saturating at `u64::MAX`) whose
    /// leader epoch is `>= epoch` (binary search; leader epochs are non-decreasing
    /// in consensus block number).
    fn position(txn: &impl DbTx, epoch: Epoch, first_key: u64, last_key: u64) -> eyre::Result<u64> {
        let (mut lo, mut hi) = (first_key, last_key.saturating_add(1));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match Self::epoch_at(txn, mid, last_key)? {
                Some(e) if e >= epoch => hi = mid,
                Some(_) => lo = mid + 1,
                None => hi = mid,
            }
        }
        Ok(lo)
    }
}

impl<DB: Database> RewardsBackend for BoundedHybridWalker<DB> {
    fn tally(
        &self,
        _epoch: Epoch,
        _last_executed_round: u32,
    ) -> Result<BTreeMap<Address, u32>, RewardsError> {
        Err(RewardsError::Unsupported(
            "legacy (leader-only) tally is withdrawal-backed in replay; the bounded walker \
             only serves hybrid tallies"
                .into(),
        ))
    }

    fn tally_hybrid(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<HybridEpochTally, RewardsError> {
        let committee = self
            .committee
            .read()
            .as_ref()
            .cloned()
            .ok_or(RewardsError::MissingCommittee { epoch })?;
        let mut cursor = self.cursor.lock();

        self.db
            .with_read_txn(|txn| {
                // Bounded to one epoch of keyed reads over immutable rows, like the live
                // walk, so opting out of the read-txn safety window is acceptable here too.
                txn.disable_long_read_safety();
                let Some((last_key, _)) = txn.last_record::<ConsensusBlocks>() else {
                    return Ok(HybridEpochTally::default());
                };
                let start = match *cursor {
                    Some(k) => k,
                    None => {
                        let first_key =
                            txn.iter::<ConsensusBlocks>().next().map(|(k, _)| k).unwrap_or(0);
                        let k = Self::position(txn, epoch, first_key, last_key)?;
                        info!(
                            target: "rayls_replay::rewards",
                            epoch, first_key, last_key, start = k,
                            "hybrid walk positioned by binary search"
                        );
                        k
                    }
                };

                let mut per_address: BTreeMap<Address, ValidatorRoundTally> = BTreeMap::new();
                let mut total_rounds: u32 = 0;
                let mut rows_read: u64 = 0;
                let mut seen: BTreeSet<Address> = BTreeSet::new();
                let mut next_epoch_start: Option<u64> = None;
                let mut lookahead_rows: u32 = 0;
                let mut k = start;
                while k <= last_key {
                    let Some(bytes) = txn.raw_get::<ConsensusBlocks>(&k)? else {
                        k += 1;
                        continue;
                    };
                    rows_read += 1;
                    let meta = ConsensusHeaderParticipation::from_bytes(&bytes)?;
                    k += 1;

                    if meta.leader_epoch > epoch {
                        // first row of a later epoch is where the next close starts; keep
                        // reading a few rows in case the boundary interleaves, then stop.
                        next_epoch_start.get_or_insert(k - 1);
                        lookahead_rows += 1;
                        if lookahead_rows > BOUNDARY_LOOKAHEAD {
                            warn!(
                                target: "rayls_replay::rewards",
                                epoch, key = k - 1, lookahead_rows,
                                "epoch boundary lookahead cap exceeded; stopping the walk \
                                 (rows of this epoch past the cap were not credited)"
                            );
                            break;
                        }
                        continue;
                    }
                    if meta.leader_epoch < epoch {
                        continue;
                    }
                    // round not yet executed by the engine at the close block, or genesis.
                    if meta.leader_round > last_executed_round || meta.leader_round == 0 {
                        continue;
                    }

                    total_rounds = total_rounds.saturating_add(1);

                    if let Some(authority) = committee.authority(&meta.leader_author) {
                        let tally = per_address.entry(authority.execution_address()).or_default();
                        tally.leader_rounds = tally.leader_rounds.saturating_add(1);
                    }

                    // Dedup on the resolved execution address (one participation credit per
                    // address per committed round), exactly as the live walker does.
                    seen.clear();
                    for author in &meta.participants {
                        if let Some(authority) = committee.authority(author) {
                            let address = authority.execution_address();
                            if seen.insert(address) {
                                let tally = per_address.entry(address).or_default();
                                tally.participation_rounds =
                                    tally.participation_rounds.saturating_add(1);
                            }
                        }
                    }
                }

                let next = next_epoch_start.unwrap_or(k);
                *cursor = Some(next);
                info!(
                    target: "rayls_replay::rewards",
                    epoch, start, next_start = next, rows_read, lookahead_rows, rows_walked = total_rounds,
                    "hybrid walk done"
                );
                Ok(HybridEpochTally { per_address, total_rounds })
            })
            .map_err(RewardsError::Database)
    }

    fn get_authority_address(&self, id: &AuthorityIdentifier) -> Option<Address> {
        self.committee.read().as_ref().and_then(|c| c.authority(id).map(|a| a.execution_address()))
    }

    fn set_committee(&self, committee: Committee) {
        *self.committee.write() = Some(committee);
    }

    fn get_address_counts(&self) -> BTreeMap<Address, u32> {
        BTreeMap::new()
    }

    fn set_leader_counts(&self, _leader_counts: BTreeMap<AuthorityIdentifier, u32>) {}

    fn inc_leader_count(&self, _leader: &AuthorityIdentifier) {}

    fn clear(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use rayls_infrastructure_storage::mem_db::MemDatabase;
    use rayls_infrastructure_types::{
        BlsKeypair, Certificate, CommittedSubDag, CommitteeBuilder, ConsensusHeader, Header,
        ReputationScores,
    };
    use rayls_middleware_rewards::ConsensusRewardsCounter;

    fn addr(n: u8) -> Address {
        Address::with_last_byte(n)
    }

    /// Build an `n`-authority committee with real BLS-derived identifiers
    /// (`Committee::authority` resolves by id hash, so dummy ids never match).
    /// Returns the committee plus each authority's id and execution address,
    /// in the order added (address = `addr(i + 1)`).
    fn test_committee(n: u8) -> (Committee, Vec<(AuthorityIdentifier, Address)>) {
        let mut rng = StdRng::seed_from_u64(0x633);
        let mut builder = CommitteeBuilder::new(0);
        let mut ids = Vec::with_capacity(n as usize);
        for i in 0..n {
            let keypair = BlsKeypair::generate(&mut rng);
            let address = addr(i + 1);
            builder.add_authority(*keypair.public(), 1, address);
            ids.push((AuthorityIdentifier::from(*keypair.public()), address));
        }
        (builder.build(), ids)
    }

    /// One `ConsensusBlocks` row: `leader_author` leads `leader_round` of
    /// `epoch`, and every id in `participants` holds a certificate in the
    /// committed sub-dag (participants never include the leader implicitly —
    /// pass it explicitly to credit it with participation).
    fn insert(
        db: &MemDatabase,
        number: u64,
        leader_author: &AuthorityIdentifier,
        leader_round: u32,
        epoch: u32,
        participants: &[&AuthorityIdentifier],
    ) {
        let cert = |author: &AuthorityIdentifier, round: u32| {
            let mut c = Certificate::default();
            c.header = Header { author: author.clone(), round, epoch, ..Default::default() };
            c
        };
        let leader = cert(leader_author, leader_round);
        let certificates = participants
            .iter()
            .map(|author| cert(author, leader_round.saturating_sub(1)))
            .collect();
        let sub_dag =
            CommittedSubDag::new(certificates, leader, 0, ReputationScores::default(), None);
        let row = ConsensusHeader { sub_dag, number, ..Default::default() };
        db.insert::<ConsensusBlocks>(&number, &row).expect("seed row");
    }

    /// Stand-in for the consensus-DB walk: serves a fixed hybrid tally.
    #[derive(Debug)]
    struct FixedWalker(HybridEpochTally);

    impl RewardsBackend for FixedWalker {
        fn tally(&self, _: Epoch, _: u32) -> Result<BTreeMap<Address, u32>, RewardsError> {
            unreachable!("legacy tally is withdrawal-backed")
        }
        fn tally_hybrid(&self, _: Epoch, _: u32) -> Result<HybridEpochTally, RewardsError> {
            Ok(self.0.clone())
        }
        fn get_authority_address(&self, _: &AuthorityIdentifier) -> Option<Address> {
            None
        }
        fn set_committee(&self, _: Committee) {}
        fn get_address_counts(&self) -> BTreeMap<Address, u32> {
            BTreeMap::new()
        }
        fn set_leader_counts(&self, _: BTreeMap<AuthorityIdentifier, u32>) {}
        fn inc_leader_count(&self, _: &AuthorityIdentifier) {}
        fn clear(&self) {}
    }

    fn hybrid(rows: &[(u8, u32, u32)]) -> HybridEpochTally {
        HybridEpochTally {
            per_address: rows
                .iter()
                .map(|(a, participation_rounds, leader_rounds)| {
                    (
                        addr(*a),
                        ValidatorRoundTally {
                            participation_rounds: *participation_rounds,
                            leader_rounds: *leader_rounds,
                        },
                    )
                })
                .collect(),
            total_rounds: rows.iter().map(|(_, _, l)| l).sum(),
        }
    }

    fn backend_with(
        walker: Option<HybridEpochTally>,
    ) -> (SnapshotTallyStore, SnapshotRewardsBackend) {
        let store = SnapshotTallyStore::default();
        let source = HybridTallySource::default();
        if let Some(tally) = walker {
            assert!(source.attach(RewardsCounter::from_impl(FixedWalker(tally))));
        }
        (store.clone(), SnapshotRewardsBackend::new(store, source))
    }

    #[test]
    fn tally_serves_stored_epoch() {
        let (store, backend) = backend_with(None);
        let expected: BTreeMap<Address, u32> = [(addr(1), 3), (addr(2), 1)].into_iter().collect();
        store.insert(7, expected.clone());
        assert_eq!(backend.tally(7, 0).unwrap(), expected);
    }

    #[test]
    fn tally_hybrid_without_walker_errors_loudly() {
        let (_, backend) = backend_with(None);
        let err = backend.tally_hybrid(7, 0).expect_err("must not silently succeed");
        assert!(!err.is_transient(), "a missing walker is not a retryable DB error");
    }

    #[test]
    fn tally_hybrid_serves_walk_matching_withdrawals() {
        // addr(3) participated but never led. The walk creates its `per_address` entry
        // with `leader_rounds == 0`, and the live block writes a zero-amount withdrawal
        // for it (no filtering on either side), so the maps must compare equal.
        let walked = hybrid(&[(1, 5, 3), (2, 4, 2), (3, 5, 0)]);
        let (store, backend) = backend_with(Some(walked.clone()));
        store.insert(7, [(addr(1), 3), (addr(2), 2), (addr(3), 0)].into_iter().collect());
        assert_eq!(backend.tally_hybrid(7, 0).unwrap(), walked);
    }

    #[test]
    fn tally_hybrid_rejects_walk_disagreeing_with_withdrawals() {
        let (store, backend) = backend_with(Some(hybrid(&[(1, 5, 3), (2, 4, 2)])));
        store.insert(7, [(addr(1), 3), (addr(2), 1)].into_iter().collect());
        let err = backend.tally_hybrid(7, 0).expect_err("leader rounds differ from withdrawals");
        assert!(!err.is_transient());
        assert!(err.to_string().contains("disagrees"), "{err}");
    }

    #[test]
    fn attach_is_once() {
        let source = HybridTallySource::default();
        assert!(source.attach(RewardsCounter::default()));
        assert!(!source.attach(RewardsCounter::default()));
    }

    #[test]
    fn tally_empty_for_unknown_epoch() {
        let (_, backend) = backend_with(None);
        assert!(backend.tally(99, 0).unwrap().is_empty());
    }

    #[test]
    fn store_insert_overwrites() {
        let (store, backend) = backend_with(None);
        store.insert(1, [(addr(1), 1)].into_iter().collect());
        store.insert(1, [(addr(1), 5)].into_iter().collect());
        assert_eq!(backend.tally(1, 0).unwrap().get(&addr(1)), Some(&5));
    }

    /// The bounded walker must credit exactly like the live node's walker
    /// (`ConsensusRewardsCounter::tally_hybrid`) on identical rows: that is the
    /// design contract behind replacing the reverse full-tail scan with the
    /// forward cursor-bounded walk.
    #[test]
    fn bounded_hybrid_matches_live_walker_per_epoch() {
        let (committee, ids) = test_committee(3);
        let (id_a, _) = &ids[0];
        let (id_b, addr_b) = &ids[1]; // participates in every round, never leads
        let (id_c, _) = &ids[2];

        let db = MemDatabase::default();
        insert(&db, 0, id_a, 0, 0, &[]); // genesis round 0: never credited
                                         // epoch 1: rounds 1..=4
        insert(&db, 1, id_a, 1, 1, &[id_a, id_b, id_c]);
        insert(&db, 2, id_c, 2, 1, &[id_b]);
        insert(&db, 3, id_a, 3, 1, &[id_a, id_b, id_c]);
        insert(&db, 4, id_c, 4, 1, &[id_b]);
        // epoch 2: rounds 1..=3
        insert(&db, 5, id_c, 1, 2, &[id_a, id_b]);
        insert(&db, 6, id_a, 2, 2, &[id_b]);
        insert(&db, 7, id_c, 3, 2, &[id_a, id_b, id_c]);

        let live = ConsensusRewardsCounter::new(db.clone());
        let bounded = BoundedHybridWalker::new(db.clone());
        live.set_committee(committee.clone());
        bounded.set_committee(committee);

        // Replay closes epochs ascending; each close must equal the live walk.
        let e1 = bounded.tally_hybrid(1, u32::MAX).unwrap();
        assert_eq!(
            e1,
            live.tally_hybrid(1, u32::MAX).unwrap(),
            "epoch 1 must match the live walker"
        );
        let e2 = bounded.tally_hybrid(2, u32::MAX).unwrap();
        assert_eq!(
            e2,
            live.tally_hybrid(2, u32::MAX).unwrap(),
            "epoch 2 must match the live walker"
        );

        // Ground truth (guards against both walkers sharing a bug).
        assert_eq!(e1.total_rounds, 4);
        assert_eq!(
            e1.per_address[&addr(1)],
            ValidatorRoundTally { participation_rounds: 2, leader_rounds: 2 }
        );
        assert_eq!(
            e1.per_address[addr_b],
            ValidatorRoundTally { participation_rounds: 4, leader_rounds: 0 }
        );
        assert_eq!(
            e1.per_address[&addr(3)],
            ValidatorRoundTally { participation_rounds: 2, leader_rounds: 2 }
        );
        assert_eq!(e2.total_rounds, 3);
        assert_eq!(
            e2.per_address[&addr(1)],
            ValidatorRoundTally { participation_rounds: 2, leader_rounds: 1 }
        );
        assert_eq!(
            e2.per_address[addr_b],
            ValidatorRoundTally { participation_rounds: 3, leader_rounds: 0 }
        );
        // id_c leads rounds 1 and 3 but its certificate is only included in
        // round 3's sub-dag (round 1's participants are a and b).
        assert_eq!(
            e2.per_address[&addr(3)],
            ValidatorRoundTally { participation_rounds: 1, leader_rounds: 2 }
        );
    }

    #[test]
    fn bounded_hybrid_excludes_genesis_and_unexecuted_rounds() {
        let (committee, ids) = test_committee(2);
        let (id_a, addr_a) = &ids[0];
        let (id_b, addr_b) = &ids[1];

        let db = MemDatabase::default();
        insert(&db, 0, id_a, 0, 0, &[]); // genesis: leader_round == 0
        insert(&db, 1, id_b, 1, 0, &[id_a, id_b]);
        insert(&db, 2, id_a, 100, 0, &[id_a, id_b]); // 100 > last_executed_round 10

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);
        let tally = walker.tally_hybrid(0, 10).unwrap();

        assert_eq!(tally.total_rounds, 1);
        assert_eq!(
            tally.per_address[addr_a],
            ValidatorRoundTally { participation_rounds: 1, leader_rounds: 0 }
        );
        assert_eq!(
            tally.per_address[addr_b],
            ValidatorRoundTally { participation_rounds: 1, leader_rounds: 1 }
        );
        assert_eq!(tally.per_address.len(), 2);
    }

    /// Two distinct protocol keys resolving to one execution address must earn
    /// at most one participation credit per round: the walk dedupes on the
    /// resolved address, exactly like the live walker.
    #[test]
    fn bounded_hybrid_dedupes_participation_by_execution_address() {
        let mut rng = StdRng::seed_from_u64(0x634);
        let (key1, key2, key3) = (
            BlsKeypair::generate(&mut rng),
            BlsKeypair::generate(&mut rng),
            BlsKeypair::generate(&mut rng),
        );
        let id_1 = AuthorityIdentifier::from(*key1.public());
        let id_2 = AuthorityIdentifier::from(*key2.public());
        let id_3 = AuthorityIdentifier::from(*key3.public());
        let shared = addr(1);
        let other = addr(2);

        let mut builder = CommitteeBuilder::new(0);
        builder.add_authority(*key1.public(), 1, shared);
        builder.add_authority(*key2.public(), 1, shared);
        builder.add_authority(*key3.public(), 1, other);
        let committee = builder.build();

        let db = MemDatabase::default();
        insert(&db, 0, &id_3, 1, 0, &[&id_1, &id_2]); // both shared-address ids in one sub-dag

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);
        let tally = walker.tally_hybrid(0, u32::MAX).unwrap();

        assert_eq!(tally.total_rounds, 1);
        assert_eq!(
            tally.per_address[&shared],
            ValidatorRoundTally { participation_rounds: 1, leader_rounds: 0 }
        );
        assert_eq!(
            tally.per_address[&other],
            ValidatorRoundTally { participation_rounds: 0, leader_rounds: 1 }
        );
    }

    /// The table has epochs 0 and 2; closing absent epoch 1 must position at the
    /// first epoch-2 row and tally empty — and the parked cursor must be exactly
    /// where the epoch-2 close resumes.
    #[test]
    fn positioning_epoch_absent_from_table_yields_empty_tally() {
        let (committee, ids) = test_committee(2);
        let (id_a, addr_a) = &ids[0];
        let (id_b, addr_b) = &ids[1];

        let db = MemDatabase::default();
        insert(&db, 0, id_a, 1, 0, &[id_a]);
        insert(&db, 1, id_b, 1, 2, &[id_b]);
        insert(&db, 2, id_a, 2, 2, &[id_a]);

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);

        let missing = walker.tally_hybrid(1, u32::MAX).unwrap();
        assert_eq!(missing, HybridEpochTally::default());
        assert_eq!(*walker.cursor.lock(), Some(1), "cursor parks at the first epoch-2 row");

        let next = walker.tally_hybrid(2, u32::MAX).unwrap();
        assert_eq!(next.total_rounds, 2);
        assert_eq!(
            next.per_address[addr_a],
            ValidatorRoundTally { participation_rounds: 1, leader_rounds: 1 }
        );
        assert_eq!(
            next.per_address[addr_b],
            ValidatorRoundTally { participation_rounds: 1, leader_rounds: 1 }
        );
    }

    /// Every row older than the target epoch: positioning saturates past the
    /// last key, the walk body never runs, and the tally is empty.
    #[test]
    fn positioning_all_rows_before_epoch_yields_empty_tally() {
        let (committee, ids) = test_committee(2);
        let (id_a, _) = &ids[0];

        let db = MemDatabase::default();
        for round in 1..=3u32 {
            insert(&db, (round - 1) as u64, id_a, round, 0, &[id_a]);
        }

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);
        let tally = walker.tally_hybrid(5, u32::MAX).unwrap();
        assert_eq!(tally, HybridEpochTally::default());
        assert_eq!(*walker.cursor.lock(), Some(3), "cursor parks past the last key");
    }

    /// The consensus table is dense in production, but both `epoch_at` and the
    /// walk advance over missing keys; a gap mid-epoch must still credit every
    /// present row exactly once.
    #[test]
    fn positioning_walks_past_key_gaps() {
        let (committee, ids) = test_committee(2);
        let (id_a, addr_a) = &ids[0];
        let (id_b, addr_b) = &ids[1];

        let db = MemDatabase::default();
        insert(&db, 0, id_a, 1, 0, &[id_a]); // keys 1,2 absent
        insert(&db, 3, id_b, 2, 0, &[id_b]); // key 4 absent
        insert(&db, 5, id_a, 3, 0, &[id_a, id_b]);

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);
        let tally = walker.tally_hybrid(0, u32::MAX).unwrap();

        assert_eq!(tally.total_rounds, 3);
        assert_eq!(tally.per_address[addr_a].leader_rounds, 2);
        assert_eq!(tally.per_address[addr_b].leader_rounds, 1);
        assert_eq!(tally.per_address[addr_a].participation_rounds, 2);
        assert_eq!(tally.per_address[addr_b].participation_rounds, 2);
    }

    /// A snapshot table need not start at key 0; positioning must use the
    /// table's actual first key, not assume genesis is present.
    #[test]
    fn positioning_first_key_not_zero() {
        let (committee, ids) = test_committee(2);
        let (id_a, addr_a) = &ids[0];

        let db = MemDatabase::default();
        insert(&db, 10, id_a, 1, 1, &[id_a]);
        insert(&db, 11, id_a, 2, 1, &[id_a]);

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);
        let tally = walker.tally_hybrid(1, u32::MAX).unwrap();

        assert_eq!(tally.total_rounds, 2);
        assert_eq!(tally.per_address[addr_a].leader_rounds, 2);
    }

    /// Ascending closes: the second tally resumes at the parked cursor and reads
    /// only the next epoch's rows. Re-tallying an already-consumed epoch in the
    /// same process reads nothing (each replay run builds a fresh walker, so this
    /// is not a replay path — it pins the cursor semantics).
    #[test]
    fn cursor_advances_ascending_and_repeat_tally_is_empty() {
        let (committee, ids) = test_committee(2);
        let (id_a, _) = &ids[0];
        let (id_b, _) = &ids[1];

        let db = MemDatabase::default();
        insert(&db, 0, id_a, 1, 0, &[id_a]);
        insert(&db, 1, id_a, 1, 1, &[id_a]);
        insert(&db, 2, id_b, 2, 1, &[id_b]);
        insert(&db, 3, id_a, 1, 2, &[id_a]);
        insert(&db, 4, id_b, 2, 2, &[id_b]);

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);

        let e1 = walker.tally_hybrid(1, u32::MAX).unwrap();
        assert_eq!(e1.total_rounds, 2);
        assert_eq!(*walker.cursor.lock(), Some(3), "cursor parks at the first epoch-2 row");

        let repeat = walker.tally_hybrid(1, u32::MAX).unwrap();
        assert_eq!(repeat, HybridEpochTally::default());

        let e2 = walker.tally_hybrid(2, u32::MAX).unwrap();
        assert_eq!(e2.total_rounds, 2);
        assert_eq!(*walker.cursor.lock(), Some(5));
    }

    /// Leader epochs are non-decreasing in production, so a same-epoch row after
    /// `BOUNDARY_LOOKAHEAD` next-epoch rows is corrupt data: the walk must stop
    /// at the cap (dropping the trailing row) rather than loop, and log a warn.
    #[test]
    fn lookahead_cap_stops_past_a_corrupted_boundary() {
        let (committee, ids) = test_committee(2);
        let (id_a, addr_a) = &ids[0];
        let (id_b, _) = &ids[1];

        let db = MemDatabase::default();
        insert(&db, 0, id_a, 1, 1, &[id_a]);
        insert(&db, 1, id_a, 2, 1, &[id_a]);
        for i in 0..=BOUNDARY_LOOKAHEAD {
            insert(&db, 2 + i as u64, id_b, 1 + i, 2, &[id_b]);
        }
        insert(&db, 2 + BOUNDARY_LOOKAHEAD as u64 + 1, id_a, 3, 1, &[id_a]);

        let walker = BoundedHybridWalker::new(db);
        walker.set_committee(committee);
        let tally = walker.tally_hybrid(1, u32::MAX).unwrap();

        assert_eq!(tally.total_rounds, 2, "rows past the lookahead cap are not credited");
        assert_eq!(tally.per_address[addr_a].leader_rounds, 2);
    }

    #[test]
    fn bounded_hybrid_requires_committee() {
        let walker = BoundedHybridWalker::new(MemDatabase::default());
        let err = walker.tally_hybrid(0, u32::MAX).expect_err("no committee installed");
        assert!(!err.is_transient());
        assert!(matches!(err, RewardsError::MissingCommittee { .. }), "{err}");
    }

    #[test]
    fn bounded_hybrid_empty_db_yields_default_tally() {
        let (committee, _) = test_committee(2);
        let walker = BoundedHybridWalker::new(MemDatabase::default());
        walker.set_committee(committee);
        assert_eq!(walker.tally_hybrid(0, u32::MAX).unwrap(), HybridEpochTally::default());
    }

    #[test]
    fn bounded_hybrid_legacy_tally_is_unsupported() {
        let (committee, _) = test_committee(2);
        let walker = BoundedHybridWalker::new(MemDatabase::default());
        walker.set_committee(committee);
        let err = walker.tally(0, u32::MAX).expect_err("legacy tally is withdrawal-backed");
        assert!(!err.is_transient());
        assert!(err.to_string().contains("withdrawal-backed"), "{err}");
    }

    /// End-to-end wiring as in `main`: the walker attaches to the
    /// `HybridTallySource` before any committee install, then the committee
    /// reaches it through `SnapshotRewardsBackend::set_committee`. The walk
    /// result is returned only when it matches the committed withdrawals.
    #[test]
    fn backend_serves_hybrid_tally_from_bounded_walker() {
        let (committee, ids) = test_committee(2);
        let (id_a, addr_a) = &ids[0];
        let (id_b, addr_b) = &ids[1];

        let db = MemDatabase::default();
        insert(&db, 0, id_a, 0, 0, &[]);
        insert(&db, 1, id_b, 1, 1, &[id_a, id_b]);
        insert(&db, 2, id_a, 2, 1, &[id_a, id_b]);

        let store = SnapshotTallyStore::default();
        let source = HybridTallySource::default();
        let walker = BoundedHybridWalker::new(db);
        assert!(source.attach(RewardsCounter::from_impl(walker)));
        let backend = SnapshotRewardsBackend::new(store.clone(), source);
        backend.set_committee(committee);

        store.insert(1, [(*addr_a, 1), (*addr_b, 1)].into_iter().collect());
        let tally = backend.tally_hybrid(1, u32::MAX).unwrap();
        assert_eq!(tally.total_rounds, 2);
        assert_eq!(
            tally.per_address[addr_a],
            ValidatorRoundTally { participation_rounds: 2, leader_rounds: 1 }
        );
        assert_eq!(
            tally.per_address[addr_b],
            ValidatorRoundTally { participation_rounds: 2, leader_rounds: 1 }
        );
    }

    /// Same wiring, but the committed withdrawals credit a round the walk does
    /// not: the backend must abort (non-transient) instead of diverging.
    #[test]
    fn backend_aborts_when_bounded_walker_disagrees_with_withdrawals() {
        let (committee, ids) = test_committee(2);
        let (id_a, _) = &ids[0];
        let (id_b, addr_b) = &ids[1];

        let db = MemDatabase::default();
        insert(&db, 1, id_b, 1, 1, &[id_a, id_b]);
        insert(&db, 2, id_b, 2, 1, &[id_a, id_b]); // b leads both rounds

        let store = SnapshotTallyStore::default();
        let source = HybridTallySource::default();
        let walker = BoundedHybridWalker::new(db);
        assert!(source.attach(RewardsCounter::from_impl(walker)));
        let backend = SnapshotRewardsBackend::new(store.clone(), source);
        backend.set_committee(committee);

        store.insert(1, [(addr(1), 1), (*addr_b, 2)].into_iter().collect());
        let err = backend.tally_hybrid(1, u32::MAX).expect_err("walk must not match");
        assert!(!err.is_transient());
        assert!(err.to_string().contains("disagrees"), "{err}");
    }
}
