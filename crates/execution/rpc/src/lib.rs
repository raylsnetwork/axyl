// SPDX-License-Identifier: BUSL-1.1
//! RPC request handle for state sync requests from peers.

mod error;
mod rpc_ext;

use rayls_infrastructure_types::{
    BlockHash, BlsPublicKey, ConsensusHeader, Epoch, EpochCertificate, EpochRecord, RecordBinding,
    Round,
};
pub use rpc_ext::{RaylsNetworkRpcExt, RaylsNetworkRpcExtApiServer};
use serde::{Deserialize, Serialize};

/// Role of the local node within the current committee.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Full CVV actively voting in the current committee.
    ActiveCvv,
    /// Staked CVV catching up; allowed to sync past GC and rejoin.
    InactiveCvv,
    /// Follower not in the committee (staked or unstaked).
    Observer,
}

/// Snapshot of the local node's sync state — returned by `rayls_nodeStatus`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    /// Role of this node in the current committee.
    pub role: NodeRole,
    /// True when this node is ready to participate / has finished catching up.
    /// `ActiveCvv` is always true (promotion gate guarantees it); `InactiveCvv` is
    /// always false (still catching up by definition); `Observer` is true once
    /// the local consensus tip reaches the gossipped network tip.
    pub is_caught_up: bool,
    /// Consensus epoch this node is currently operating in.
    pub epoch: Epoch,
    /// Last round committed by the local DAG.
    pub committed_round: Round,
    /// Current primary round produced by the local DAG.
    pub primary_round: Round,
    /// Garbage-collection round boundary on the local DAG.
    pub gc_round: Round,
    /// Number of the latest canonical (executed) block.
    pub last_canonical_block: u64,
}

/// Trait used to get primary data for our RPC extension (rayls namespace).
pub trait EngineToPrimary {
    /// Retrieve the latest consensus block.
    fn get_latest_consensus_block(&self) -> ConsensusHeader;
    /// Retrieve the consensus block by number.
    fn consensus_block_by_number(&self, number: u64) -> Option<ConsensusHeader>;
    /// Retrieve the consensus block by hash.
    fn consensus_block_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader>;
    /// Get an epoch header if found.
    fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> Option<(EpochRecord, EpochCertificate)>;
    /// What the certificate of the certified record of `epoch` signs besides the record.
    fn epoch_record_binding(&self, epoch: Epoch) -> Option<RecordBinding>;
    /// The group key of `epoch` as this node knows it: `Some(None)` for aggregate certificates,
    /// `None` when unknown.
    fn epoch_group_key(&self, epoch: Epoch) -> Option<Option<BlsPublicKey>>;
    /// Snapshot of the local node's role and sync progress.
    fn node_status(&self) -> NodeStatus;
}
