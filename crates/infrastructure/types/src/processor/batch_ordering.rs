//! Per-authority batch sequence ordering with parking for out-of-order batches.

use std::{collections::BTreeMap, sync::Arc};

use crate::{Address, Batch, Epoch, PreparedBatch, WorkerId, B256};
use serde::{Deserialize, Serialize};

/// Maximum number of parked batches per authority before forced out-of-order execution.
pub const MAX_PARKED_PER_AUTHORITY: usize = 32;

/// Result of attempting to accept a batch into the ordering state.
#[derive(Debug)]
pub enum AcceptResult {
    /// Batch is in-order or the first from this authority - execute immediately.
    InOrder(PreparedBatch),
    /// Batch was parked, waiting for its predecessor.
    Parked,
    /// Parking limit reached - forced out-of-order execution.
    OverflowForced(PreparedBatch),
}

/// Per-authority ordering state.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuthoritySeqState {
    /// The highest seq executed for this authority; `None` until the first batch is seen.
    pub last_executed_seq: Option<u64>,
    /// Batches waiting for their predecessor, keyed by seq.
    pub parked: BTreeMap<u64, PreparedBatch>,
}

/// Batch ordering state with epoch tracking.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BatchOrderingState {
    /// The epoch these ordering states belong to.
    pub epoch: Epoch,
    /// Per-authority ordering state, keyed by ECDSA address.
    pub authorities: BTreeMap<Address, AuthoritySeqState>,
}

/// A parked batch persisted by reference.
///
/// Carries every [`PreparedBatch`] field except the batch body, which is reloaded from the
/// `Batches` table on restart - a committed batch's row outlives the reboot, so persisting the
/// transaction bytes in the ordering blob only duplicates them. Dropping them keeps `persist`
/// cheap even in the degraded regime that parks the per-authority limit on every output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParkedRef {
    /// Digest addressing the batch body in the `Batches` table.
    pub batch_digest: B256,
    /// ECDSA address of the authority.
    pub beneficiary: Address,
    /// The ConsensusHeader digest.
    pub output_digest: B256,
    /// The output nonce (epoch << 32 | round).
    pub output_nonce: u64,
    /// Commit timestamp from the output.
    pub timestamp: u64,
    /// The epoch from the output (for gas limit calc).
    pub epoch: Epoch,
    /// Worker ID from the batch.
    pub worker_id: WorkerId,
    /// Original batch index in the subdag.
    pub batch_index: usize,
    /// True when this batch was drained from the parking area.
    pub drained: bool,
    /// Block gas limit.
    pub gas_limit: u64,
}

impl From<&PreparedBatch> for ParkedRef {
    fn from(prepared: &PreparedBatch) -> Self {
        Self {
            batch_digest: prepared.batch_digest,
            beneficiary: prepared.beneficiary,
            output_digest: prepared.output_digest,
            output_nonce: prepared.output_nonce,
            timestamp: prepared.timestamp,
            epoch: prepared.epoch,
            worker_id: prepared.worker_id,
            batch_index: prepared.batch_index,
            drained: prepared.drained,
            gas_limit: prepared.gas_limit,
        }
    }
}

impl ParkedRef {
    /// Pairs the reference with its reloaded batch body to rebuild the full [`PreparedBatch`].
    pub fn into_prepared(self, batch: Arc<Batch>) -> PreparedBatch {
        PreparedBatch {
            batch,
            batch_digest: self.batch_digest,
            beneficiary: self.beneficiary,
            output_digest: self.output_digest,
            output_nonce: self.output_nonce,
            timestamp: self.timestamp,
            epoch: self.epoch,
            worker_id: self.worker_id,
            batch_index: self.batch_index,
            drained: self.drained,
            gas_limit: self.gas_limit,
        }
    }
}

/// Per-authority ordering state as persisted: parked batches held by reference.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StoredAuthoritySeqState {
    /// The highest seq executed for this authority; `None` until the first batch is seen.
    pub last_executed_seq: Option<u64>,
    /// Parked batches by seq, held by digest reference.
    pub parked: BTreeMap<u64, ParkedRef>,
}

/// Batch ordering state as persisted: the on-disk form of [`BatchOrderingState`] that stores
/// parked batches by digest instead of by value.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StoredBatchOrderingState {
    /// The epoch these ordering states belong to.
    pub epoch: Epoch,
    /// Per-authority ordering state, keyed by ECDSA address.
    pub authorities: BTreeMap<Address, StoredAuthoritySeqState>,
}

impl From<&BatchOrderingState> for StoredBatchOrderingState {
    fn from(state: &BatchOrderingState) -> Self {
        Self {
            epoch: state.epoch,
            authorities: state
                .authorities
                .iter()
                .map(|(addr, auth)| {
                    (
                        *addr,
                        StoredAuthoritySeqState {
                            last_executed_seq: auth.last_executed_seq,
                            parked: auth
                                .parked
                                .iter()
                                .map(|(seq, prepared)| (*seq, ParkedRef::from(prepared)))
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode, try_decode, Batch, ExecHeader};

    fn legacy_state_with_parked() -> BatchOrderingState {
        let batch = Batch::new_for_test(vec![vec![0u8; 64]], ExecHeader::default(), 0, 0, 7);
        let digest = batch.digest();
        let prepared = PreparedBatch {
            batch: Arc::new(batch),
            batch_digest: digest,
            beneficiary: Address::from([9u8; 20]),
            output_digest: B256::ZERO,
            output_nonce: 0,
            timestamp: 0,
            epoch: 3,
            worker_id: 0,
            batch_index: 0,
            drained: false,
            gas_limit: 30_000_000,
        };
        let mut state = BatchOrderingState { epoch: 3, ..Default::default() };
        state.authorities.insert(
            prepared.beneficiary,
            AuthoritySeqState { last_executed_seq: Some(6), parked: [(7, prepared)].into() },
        );
        state
    }

    /// The restart read tells the two on-disk formats apart by decode success: a legacy (by-value)
    /// blob with any parked entry must fail the compact decode and pass the legacy one, and an
    /// empty blob must decode either way. Otherwise the read would misinterpret one format as the
    /// other instead of falling back.
    #[test]
    fn stored_and_legacy_ordering_blobs_are_distinguishable_by_decode() {
        let legacy_bytes = encode(&legacy_state_with_parked());
        assert!(
            try_decode::<StoredBatchOrderingState>(&legacy_bytes).is_err(),
            "a legacy blob with parked entries must not decode as the compact format"
        );
        assert!(
            try_decode::<BatchOrderingState>(&legacy_bytes).is_ok(),
            "the legacy fallback must decode the legacy blob"
        );

        let empty_bytes = encode(&BatchOrderingState { epoch: 5, ..Default::default() });
        assert_eq!(
            try_decode::<StoredBatchOrderingState>(&empty_bytes).expect("empty decodes").epoch,
            5,
            "an all-empty blob is byte-identical across formats"
        );
    }
}
