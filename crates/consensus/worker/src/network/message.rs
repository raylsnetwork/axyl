//! Messages sent between workers.

use rayls_consensus_network::{PeerExchangeMap, RLMessage};
use rayls_infrastructure_types::{Batch, BlockHash, Bytes, SealedBatch};
use serde::{Deserialize, Serialize};

/// Worker messages on the gossip network.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerGossip {
    /// A new batch is available by digest.
    Batch(BlockHash),
    /// Transactions published so a committee member can include them in a batch.
    ///
    /// `Vec<Bytes>` is bcs-identical to `Vec<Vec<u8>>` (each element is a length-prefixed byte run
    /// either way), so this is not a wire-shape change: un-upgraded peers decode it unchanged.
    Txn(Vec<Bytes>),
}

impl RLMessage for WorkerRequest {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        match self {
            Self::PeerExchange { peers } => Some(peers.clone()),
            _ => None,
        }
    }
}
impl RLMessage for WorkerResponse {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        None
    }
}

/// Requests from Worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerRequest {
    /// Send a new batch to a peer.
    ReportBatch {
        /// The sealed batch that this worker is reporting.
        sealed_batch: SealedBatch,
    },
    /// Request batches by digest from a peer.
    RequestBatches {
        /// The digests of the requested batches.
        batch_digests: Vec<BlockHash>,
        /// Maximum expected response size.
        max_response_size: usize,
    },
    /// Exchange peer information.
    ///
    /// This "request" is sent to peers when this node disconnects
    /// due to excess peers. The peer exchange is intended to support
    /// discovery.
    PeerExchange {
        /// The peer information being exchanged.
        peers: PeerExchangeMap,
    },
    /// Forward transactions directly to the committee member that owns their senders' slots.
    ///
    /// Appended last: bcs is positional, so this is wire-safe as long as un-upgraded peers never
    /// receive it. Senders gate it on the `SenderAffinityLoadBalancing` fork and otherwise publish
    /// on the txn gossip topic. `Bytes` is bcs-identical to `Vec<u8>` (both a length-prefixed byte
    /// run).
    SubmitTxns {
        /// The forwarded transactions as encoded bytes: sender-contiguous, nonce-ascending runs so
        /// each sender's chain pools in order from one message.
        transactions: Vec<Bytes>,
    },
}

impl From<PeerExchangeMap> for WorkerRequest {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}

/// Response to worker requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponse {
    /// Status 200 response when a peer accepts a proposed batch.
    ReportBatch,
    /// Provided the requested batches.
    RequestBatches(Vec<Batch>),
    /// Exchange peer information.
    PeerExchange {
        /// The peer information being exchanged.
        peers: PeerExchangeMap,
    },
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    Error(WorkerRPCError),
    /// The ack for [`WorkerRequest::SubmitTxns`], listing the hashes the owner rejected as stale.
    ///
    /// Appended after `Error`: bcs encodes the variant index positionally, so inserting before
    /// `Error` would shift its index and break error decoding against un-upgraded peers on every
    /// RPC path. Everything not listed was accepted or already known, so the sender stops
    /// re-forwarding only the stale hashes.
    SubmitTxns {
        /// Hashes the owner rejected as nonce-too-low (already executed).
        stale: Vec<BlockHash>,
    },
}

impl WorkerResponse {
    /// Returns `true` if the response is an application-layer error.
    pub fn is_err(&self) -> bool {
        matches!(self, WorkerResponse::Error(_))
    }
}

impl From<WorkerRPCError> for WorkerResponse {
    fn from(value: WorkerRPCError) -> Self {
        Self::Error(value)
    }
}

/// Application-layer error returned while handling a worker request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkerRPCError(pub String);

impl From<PeerExchangeMap> for WorkerResponse {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::{encode, try_decode};

    #[test]
    fn worker_response_error_keeps_bcs_index_3() {
        // Appending SubmitTxns AFTER Error must not shift Error's positional bcs discriminant: an
        // un-upgraded peer decodes Error (index 3) on every RPC error path, so a shift breaks error
        // decoding across versions.
        let err = WorkerResponse::Error(WorkerRPCError("boom".into()));
        assert_eq!(encode(&err)[0], 3, "Error must stay bcs variant index 3");
        assert_eq!(
            encode(&WorkerResponse::SubmitTxns { stale: vec![] })[0],
            4,
            "SubmitTxns is appended last, at index 4"
        );

        // Error round-trips as itself, not misread as SubmitTxns.
        let decoded: WorkerResponse = try_decode(&encode(&err)).unwrap();
        assert!(matches!(decoded, WorkerResponse::Error(WorkerRPCError(s)) if s == "boom"));
    }

    #[test]
    fn worker_request_submit_txns_is_appended_last() {
        assert_eq!(encode(&WorkerRequest::SubmitTxns { transactions: vec![] })[0], 3);
    }
}
