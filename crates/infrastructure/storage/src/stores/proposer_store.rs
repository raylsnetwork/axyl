//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::{
    tables::{LastProposed, LastProposedByAuthority},
    ProposerKey, StoreResult,
};
use rayls_infrastructure_types::{AuthorityIdentifier, Database, DbTxMut, Header};

/// The last proposal key - always 0.
pub const LAST_PROPOSAL_KEY: ProposerKey = 0;

/// Database trait for proposals (primary headers).
pub trait ProposerStore {
    /// Inserts a proposed header into the store
    fn write_last_proposed(&self, header: &Header) -> StoreResult<()>;

    /// Get the last header
    fn get_last_proposed(&self) -> StoreResult<Option<Header>>;

    /// Inserts the last proposed header by a specific authority
    fn write_last_proposed_by_authority(
        &self,
        authority_id: AuthorityIdentifier,
        header: &Header,
    ) -> StoreResult<()>;

    /// Get the last proposed header by a specific authority
    fn get_last_proposed_by_authority(
        &self,
        authority_id: AuthorityIdentifier,
    ) -> StoreResult<Option<Header>>;
}

impl<DB: Database> ProposerStore for DB {
    fn write_last_proposed(&self, header: &Header) -> StoreResult<()> {
        self.with_write_txn(|txn| txn.insert::<LastProposed>(&LAST_PROPOSAL_KEY, header))
    }

    fn get_last_proposed(&self) -> StoreResult<Option<Header>> {
        self.get::<LastProposed>(&LAST_PROPOSAL_KEY)
    }

    fn write_last_proposed_by_authority(
        &self,
        authority_id: AuthorityIdentifier,
        header: &Header,
    ) -> StoreResult<()> {
        self.with_write_txn(|txn| txn.insert::<LastProposedByAuthority>(&authority_id, header))
    }

    fn get_last_proposed_by_authority(
        &self,
        authority_id: AuthorityIdentifier,
    ) -> StoreResult<Option<Header>> {
        self.get::<LastProposedByAuthority>(&authority_id)
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
