//! Snapshot-backed [`RewardsBackend`] for archive replay.
//!
//! Serves the close-epoch leader tally read from the snapshot block's
//! `withdrawals`, delegating committee and address resolution to
//! [`NoopRewardsBackend`]. Injected only by `rayls-replay`.

use parking_lot::Mutex;
use rayls_infrastructure_types::{
    rewards::{HybridEpochTally, NoopRewardsBackend, RewardsBackend, RewardsCounter, RewardsError},
    Address, AuthorityIdentifier, Committee, Epoch,
};
use std::{collections::BTreeMap, sync::Arc};

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

/// [`RewardsBackend`] that serves the snapshot's committed close-epoch tally.
#[derive(Debug, Default)]
pub struct SnapshotRewardsBackend {
    committee: NoopRewardsBackend,
    tallies: SnapshotTallyStore,
}

impl SnapshotRewardsBackend {
    /// Build a backend reading committed tallies from `tallies`.
    pub fn new(tallies: SnapshotTallyStore) -> Self {
        Self { committee: NoopRewardsBackend::default(), tallies }
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
        _last_executed_round: u32,
    ) -> Result<HybridEpochTally, RewardsError> {
        // A `Withdrawal` (what a snapshot preserves) carries one `u32` per
        // validator; a snapshot alone cannot recover per-validator
        // `participation_rounds`. Fail loudly rather than silently
        // reconstructing a wrong tally; archive-replay support for
        // hybrid-reward epochs is a deferred follow-up (as is the hybrid
        // activation itself). `Unsupported`, not `Database` - this is a
        // permanent capability gap, not a retryable read failure.
        Err(RewardsError::Unsupported(format!(
            "hybrid-reward archive replay not implemented: snapshot epoch {epoch} cannot \
             recover per-validator participation_rounds from Withdrawal.amount alone"
        )))
    }

    fn get_authority_address(&self, id: &AuthorityIdentifier) -> Option<Address> {
        self.committee.get_authority_address(id)
    }

    fn set_committee(&self, committee: Committee) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> Address {
        Address::with_last_byte(n)
    }

    #[test]
    fn tally_serves_stored_epoch() {
        let store = SnapshotTallyStore::default();
        let expected: BTreeMap<Address, u32> = [(addr(1), 3), (addr(2), 1)].into_iter().collect();
        store.insert(7, expected.clone());

        let backend = SnapshotRewardsBackend::new(store);
        assert_eq!(backend.tally(7, 0).unwrap(), expected);
    }

    #[test]
    fn tally_hybrid_errors_loudly_instead_of_returning_wrong_data() {
        let backend = SnapshotRewardsBackend::new(SnapshotTallyStore::default());
        let err = backend.tally_hybrid(7, 0).expect_err("must not silently succeed");
        assert!(
            !err.is_transient(),
            "this is a permanent capability gap, not a retryable DB error"
        );
    }

    #[test]
    fn tally_empty_for_unknown_epoch() {
        let backend = SnapshotRewardsBackend::new(SnapshotTallyStore::default());
        assert!(backend.tally(99, 0).unwrap().is_empty());
    }

    #[test]
    fn store_insert_overwrites() {
        let store = SnapshotTallyStore::default();
        store.insert(1, [(addr(1), 1)].into_iter().collect());
        store.insert(1, [(addr(1), 5)].into_iter().collect());

        let backend = SnapshotRewardsBackend::new(store);
        assert_eq!(backend.tally(1, 0).unwrap().get(&addr(1)), Some(&5));
    }
}
