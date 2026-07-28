//! Per-epoch validator block-reward tally computed from consensus history.

use crate::{Address, AuthorityIdentifier, Committee, Epoch};
use alloy::rpc::types::{Withdrawal, Withdrawals};
use parking_lot::{Mutex, RwLock};
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

/// Per-validator round counts feeding the hybrid (participation + anchor) reward tally.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValidatorRoundTally {
    /// Distinct committed rounds whose sub-dag included this validator's certificate.
    pub participation_rounds: u32,
    /// Committed rounds this validator led ("Anchor Commit Share" component;
    /// identical signal to what [`RewardsBackend::tally`] counts today).
    pub leader_rounds: u32,
}

/// Per-epoch hybrid reward tally: every validator's round counts plus the
/// epoch-wide committed-round count (the anchor-share/floor denominator).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HybridEpochTally {
    /// Per-validator counts, keyed by execution address.
    pub per_address: BTreeMap<Address, ValidatorRoundTally>,
    /// Total committed rounds walked for the epoch (same denominator for
    /// every validator; exactly one leader per round, so `leader_rounds`
    /// summed across `per_address` equals this).
    pub total_rounds: u32,
}

/// Compute per-address leader-execution counts for closing-epoch withdrawals.
pub trait RewardsBackend: Debug + Send + Sync + 'static {
    /// Tally leader counts for `epoch` over rounds `1..=last_executed_round`,
    /// resolving each `AuthorityIdentifier` against the held committee.
    ///
    /// Returns a canonically address-ordered `BTreeMap`. Authorities not
    /// present in the current committee are silently skipped.
    fn tally(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<BTreeMap<Address, u32>, RewardsError>;

    /// Tally participation + leader rounds for `epoch` over rounds
    /// `1..=last_executed_round`, in one walk. This is the input for the hybrid
    /// reward blend; `tally` above remains the leader-only tally and is untouched
    /// by this method's existence. (Wiring this into block execution — deciding
    /// when a block uses the hybrid vs. leader-only path — is a separate change.)
    fn tally_hybrid(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<HybridEpochTally, RewardsError>;

    /// Resolve an authority identifier to its execution address using the
    /// current committee. Used for empty-block beneficiary assignment.
    fn get_authority_address(&self, id: &AuthorityIdentifier) -> Option<Address>;

    /// Install the committee for the active epoch. Replaces atomically.
    fn set_committee(&self, committee: Committee);

    fn get_address_counts(&self) -> BTreeMap<Address, u32>;

    fn set_leader_counts(&self, leader_counts: BTreeMap<AuthorityIdentifier, u32>);
    fn inc_leader_count(&self, leader: &AuthorityIdentifier);

    fn clear(&self);
}

/// Errors surfaced by [`RewardsBackend`] operations.
#[derive(Debug, thiserror::Error)]
pub enum RewardsError {
    /// Underlying consensus DB read failed.
    #[error("consensus DB read failed")]
    Database(#[source] eyre::Report),

    /// Tally was requested before a committee was installed for the epoch.
    #[error("committee not initialized for epoch {epoch}")]
    MissingCommittee {
        /// Epoch the caller requested.
        epoch: Epoch,
    },

    /// The requested tally is a known, permanent capability gap (not a
    /// transient failure) - e.g. `SnapshotRewardsBackend::tally_hybrid` can't
    /// recover per-validator participation rounds from a plain withdrawals
    /// snapshot. Retrying will never succeed; distinct from [`Self::Database`]
    /// specifically so callers don't loop retrying something that can't work.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl RewardsError {
    /// Classify whether the error is recoverable by retrying the same call.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Database(_))
    }
}

/// Build the `Withdrawals` payload from an already-computed address tally.
///
/// Free function so callers that already hold a tally don't double-walk the
/// consensus DB. Mirrors the deterministic ordering produced by `BTreeMap`.
pub fn build_withdrawals(counts: &BTreeMap<Address, u32>) -> Withdrawals {
    Withdrawals::new(
        counts
            .iter()
            .map(|(address, amount)| Withdrawal {
                index: 0,
                validator_index: 0,
                address: *address,
                amount: *amount as u64,
            })
            .collect(),
    )
}

/// `RewardsBackend` that always tallies empty.
///
/// Used by non-execution paths (tx-pool validation, pipeline init, genesis
/// bootstrap) that never reach the close-epoch read.
#[derive(Debug, Default)]
pub struct NoopRewardsBackend {
    leader_counts: Arc<Mutex<BTreeMap<AuthorityIdentifier, u32>>>,
    committee: Arc<RwLock<Option<Committee>>>,
}

impl RewardsBackend for NoopRewardsBackend {
    fn tally(
        &self,
        _epoch: Epoch,
        _last_executed_round: u32,
    ) -> Result<BTreeMap<Address, u32>, RewardsError> {
        Ok(BTreeMap::new())
    }

    fn tally_hybrid(
        &self,
        _epoch: Epoch,
        _last_executed_round: u32,
    ) -> Result<HybridEpochTally, RewardsError> {
        Ok(HybridEpochTally::default())
    }

    fn get_authority_address(&self, id: &AuthorityIdentifier) -> Option<Address> {
        self.committee.read().as_ref().and_then(|c| c.authority(id).map(|a| a.execution_address()))
    }

    fn set_committee(&self, committee: Committee) {
        *self.committee.write() = Some(committee);
    }

    fn get_address_counts(&self) -> BTreeMap<Address, u32> {
        let counts = self.leader_counts.lock();
        let mut result = BTreeMap::default();
        if let Some(committee) = self.committee.read().as_ref() {
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
        let mut guard = self.leader_counts.lock();
        *guard = leader_counts;
    }
    fn inc_leader_count(&self, leader: &AuthorityIdentifier) {
        let mut guard = self.leader_counts.lock();
        *guard.entry(leader.clone()).or_insert(0) += 1;
    }

    fn clear(&self) {
        let mut guard = self.leader_counts.lock();
        guard.clear();
    }
}

/// Type-erased calculator handle threaded through the EVM ctx and config.
///
/// `Default` returns a Noop-wrapped handle so non-execution call sites can
/// construct via `Default::default()` without holding a consensus DB handle.
#[derive(Clone, Debug)]
pub struct RewardsCounter(Arc<dyn RewardsBackend>);

impl Default for RewardsCounter {
    fn default() -> Self {
        Self(Arc::new(NoopRewardsBackend::default()))
    }
}

impl RewardsCounter {
    /// Wrap an existing `RewardsBackend` impl into the type-erased handle.
    pub fn from_impl<C: RewardsBackend>(calc: C) -> Self {
        Self(Arc::new(calc))
    }

    /// Compute the close-epoch leader tally.
    pub fn tally(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<BTreeMap<Address, u32>, RewardsError> {
        self.0.tally(epoch, last_executed_round)
    }

    /// Compute the close-epoch hybrid (participation + anchor) tally.
    pub fn tally_hybrid(
        &self,
        epoch: Epoch,
        last_executed_round: u32,
    ) -> Result<HybridEpochTally, RewardsError> {
        self.0.tally_hybrid(epoch, last_executed_round)
    }

    /// Resolve an authority identifier to its execution address.
    pub fn get_authority_address(&self, id: &AuthorityIdentifier) -> Option<Address> {
        self.0.get_authority_address(id)
    }

    /// Install the committee for the active epoch.
    pub fn set_committee(&self, committee: Committee) {
        self.0.set_committee(committee)
    }

    /// Compute address counts via [`tally`] using the supplied epoch range.
    ///
    /// Convenience shim retained for tests that previously read the
    /// in-memory leader map directly.
    pub fn get_address_counts(&self) -> BTreeMap<Address, u32> {
        self.0.get_address_counts()
    }

    /// Increment the in-memory leader mirror. Production uses this as a
    /// self-check against the authoritative consensus-DB walk in `tally()`;
    /// divergences surface as a warn at epoch close.
    pub fn inc_leader_count(&self, leader: &AuthorityIdentifier) {
        self.0.inc_leader_count(leader);
    }

    /// Build withdrawals from the current address-tally snapshot.
    /// Production callers prefer [`tally`] + [`build_withdrawals`] for
    /// deterministic on-chain effects; this in-memory variant exists for
    /// tests and empty-block paths.
    pub fn generate_withdrawals(&self) -> Withdrawals {
        build_withdrawals(&self.get_address_counts())
    }

    pub fn set_leader_counts(&self, leader_counts: BTreeMap<AuthorityIdentifier, u32>) {
        self.0.set_leader_counts(leader_counts);
    }

    pub fn clear(&self) {
        self.0.clear();
    }
}
