//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache};
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlockHash, CommittedSubDag, ConsensusHeader, Database, DbTx, DbTxMut,
    Epoch, Round,
};
use std::{cmp::max, collections::HashMap};
use tracing::debug;

/// Implement persistent storage of the consensus chain.
/// Uses DB tables:
///   - ConsensusBlocks
///   - ConsensusBlockNumbersByDigest
///   - ConsensusBlocksCache
pub trait ConsensusStore: Clone {
    /// Persist the sub dag to the consensus chain for some storage tests.
    /// This uses garbage parent hash and number and is ONLY for testing.
    /// As a test only function this will panic if unable to write the sub dag
    /// to the consensus chain
    fn write_subdag_for_test(&self, number: u64, sub_dag: CommittedSubDag);

    /// Clear the consensus chain, ONLY for testing.
    /// Will panic on an error.
    fn clear_consensus_chain_for_test(&self);

    /// Load the last committed round of each validator.
    fn read_last_committed(&self, epoch: Epoch) -> HashMap<AuthorityIdentifier, Round>;

    /// Returns the latest subdag committed. If none is committed yet, then
    /// None is returned instead.
    fn get_latest_sub_dag(&self) -> Option<CommittedSubDag>;

    /// Reads from storage the latest commit sub dag from the epoch where its
    /// ReputationScores are marked as "final". If none exists then this
    /// method returns `None`.
    fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> Option<CommittedSubDag>;

    /// Get a canonical ConsensusHeader by hash.
    fn get_canonical_consensus_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader>;
    /// Gets a ConsensusHeader by hash (canonical or cache).
    fn get_consensus_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader>;
    /// Get a ConsensusHeader by number (canonical or cache).
    fn get_consensus_by_number(&self, number: u64) -> Option<ConsensusHeader>;
}

impl<DB: Database> ConsensusStore for DB {
    fn write_subdag_for_test(&self, number: u64, sub_dag: CommittedSubDag) {
        let header = ConsensusHeader { number, sub_dag, ..Default::default() };
        self.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&header.number, &header)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
            Ok(())
        })
        .expect("error saving a consensus header to persistant storage!");
    }

    fn clear_consensus_chain_for_test(&self) {
        self.with_write_txn(|txn| {
            txn.clear_table::<ConsensusBlocks>()?;
            txn.clear_table::<ConsensusBlockNumbersByDigest>()?;
            Ok(())
        })
        .expect("failed to clear consensus blocks");
    }

    fn read_last_committed(&self, epoch: Epoch) -> HashMap<AuthorityIdentifier, Round> {
        let blocks: Vec<ConsensusHeader> = self
            .with_read_txn(|txn| {
                Ok(txn.reverse_iter::<ConsensusBlocks>().take(50).map(|(_, block)| block).collect())
            })
            .unwrap_or_default();

        let mut res = HashMap::new();
        for block in blocks {
            if block.sub_dag.leader_epoch() != epoch {
                continue;
            }

            let id = block.sub_dag.leader.origin().clone();
            let round = block.sub_dag.leader_round();
            res.entry(id).and_modify(|r| *r = max(*r, round)).or_insert(round);

            for c in &block.sub_dag.certificates {
                res.entry(c.origin().clone())
                    .and_modify(|r| *r = max(*r, c.round()))
                    .or_insert_with(|| c.round());
            }
        }
        res
    }

    fn get_latest_sub_dag(&self) -> Option<CommittedSubDag> {
        self.last_record::<ConsensusBlocks>().map(|(_, block)| block.sub_dag)
    }

    fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> Option<CommittedSubDag> {
        self.with_read_txn(|txn| {
            for (_, block) in txn.reverse_iter::<ConsensusBlocks>() {
                let commit = block.sub_dag;

                // ignore previous epochs
                if commit.leader_epoch() < epoch {
                    debug!("No final reputation scores have been found");
                    return Ok(None);
                }

                // Found a final of schedule score, return immediately
                if commit.reputation_score.final_of_schedule {
                    debug!(
                        "Found latest final reputation scores: {:?} from commit",
                        commit.reputation_score,
                    );
                    return Ok(Some(commit));
                }
            }

            debug!("No final reputation scores have been found");
            Ok(None)
        })
        .unwrap_or(None)
    }

    fn get_canonical_consensus_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader> {
        let number = self.get::<ConsensusBlockNumbersByDigest>(&hash).ok().flatten()?;
        self.get::<ConsensusBlocks>(&number)
            .ok()
            .flatten()
            .and_then(|block| (block.digest() == hash).then_some(block))
    }

    fn get_consensus_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader> {
        let number = self.get::<ConsensusBlockNumbersByDigest>(&hash).ok().flatten()?;
        // Guard digest == hash per row, before falling back: a present-but-divergent canonical
        // row at this number must not mask a cache row whose digest actually equals `hash`, else a
        // header the node holds is reported missing.
        self.get::<ConsensusBlocks>(&number).ok().flatten().filter(|b| b.digest() == hash).or_else(
            || {
                self.get::<ConsensusBlocksCache>(&number)
                    .ok()
                    .flatten()
                    .filter(|b| b.digest() == hash)
            },
        )
    }

    fn get_consensus_by_number(&self, number: u64) -> Option<ConsensusHeader> {
        self.get::<ConsensusBlocks>(&number)
            .ok()
            .flatten()
            .or_else(|| self.get::<ConsensusBlocksCache>(&number).ok().flatten())
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
