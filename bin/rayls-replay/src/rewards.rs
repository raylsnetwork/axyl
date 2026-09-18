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
use tracing::info;

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
/// Leader epochs are non-decreasing in consensus block number, so this only guards
/// against a boundary that interleaves by a handful of rows.
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

    /// Smallest key in `first_key..=last_key + 1` whose leader epoch is `>= epoch`
    /// (binary search; leader epochs are non-decreasing in consensus block number).
    fn position(txn: &impl DbTx, epoch: Epoch, first_key: u64, last_key: u64) -> eyre::Result<u64> {
        let (mut lo, mut hi) = (first_key, last_key + 1);
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
                let mut walked: u64 = 0;
                let mut seen: BTreeSet<Address> = BTreeSet::new();
                let mut next_epoch_start: Option<u64> = None;
                let mut lookahead: u32 = 0;
                let mut k = start;
                while k <= last_key {
                    let Some(bytes) = txn.raw_get::<ConsensusBlocks>(&k)? else {
                        k += 1;
                        continue;
                    };
                    walked += 1;
                    let meta = ConsensusHeaderParticipation::from_bytes(&bytes)?;
                    k += 1;

                    if meta.leader_epoch > epoch {
                        // first row of a later epoch is where the next close starts; keep
                        // reading a few rows in case the boundary interleaves, then stop.
                        next_epoch_start.get_or_insert(k - 1);
                        lookahead += 1;
                        if lookahead > BOUNDARY_LOOKAHEAD {
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
                    epoch, start, next_start = next, walked, total_rounds,
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

    fn addr(n: u8) -> Address {
        Address::with_last_byte(n)
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
}
