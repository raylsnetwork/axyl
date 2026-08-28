use crate::tables::Votes;
use rayls_infrastructure_types::{AuthorityIdentifier, Database, DbTxMut, Vote, VoteInfo};

/// The impl for the last votes digests per authority
pub trait VoteDigestStore {
    /// Insert the vote's basic details into the database for the corresponding
    /// header author key.
    fn write_vote(&self, vote: &Vote) -> eyre::Result<()>;

    /// Read the vote info based on the provided corresponding header author key
    fn read_vote_info(&self, header_author: &AuthorityIdentifier)
        -> eyre::Result<Option<VoteInfo>>;
}

impl<DB: Database> VoteDigestStore for DB {
    /// Insert the vote's basic details into the database for the corresponding
    /// header author key.
    fn write_vote(&self, vote: &Vote) -> eyre::Result<()> {
        self.with_write_txn(|txn| txn.insert::<Votes>(vote.origin(), &vote.into()))
    }

    /// Read the vote info based on the provided corresponding header author key
    fn read_vote_info(
        &self,
        header_author: &AuthorityIdentifier,
    ) -> eyre::Result<Option<VoteInfo>> {
        self.get::<Votes>(header_author)
    }
}
