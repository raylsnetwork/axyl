//! Single-block execution and empty-block construction.

use crate::{
    error::{EngineResult, RLEngineError},
    gas,
};
use rayls_execution_evm::{
    in_flight::InFlightTracker,
    reth_env::{RethEnv, TxValidationCounts},
    ExecutedBlock,
};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, payload::RLPayload, Bytes, ConsensusOutput, SealedHeader,
    B256, MIN_PROTOCOL_BASE_FEE,
};
use tracing::{debug, error};

/// An executed block whose header is obtainable only by settling its dropped-tx marks.
///
/// A batch tx that executes as nonce-too-high stays pooled and nonce-contiguous, so the pool's
/// membership reconcile never releases its in-flight mark; the witness makes the release a
/// precondition of using the header instead of a call every caller must remember.
#[must_use = "settle the dropped-tx marks to obtain the executed header"]
pub(crate) struct UnsettledExecution {
    header: SealedHeader,
    validation_counts: TxValidationCounts,
}

impl UnsettledExecution {
    /// Releases the marks of the txs this execution dropped as nonce-too-high and yields the
    /// executed header with its validation counts.
    ///
    /// `None` is for blocks that carry no batch txs (the empty block), where there is nothing to
    /// release.
    pub(crate) fn settle(
        self,
        tracker: Option<&InFlightTracker>,
    ) -> (SealedHeader, TxValidationCounts) {
        if let Some(tracker) = tracker {
            if !self.validation_counts.nonce_too_high_details.is_empty() {
                tracker.release_dropped(
                    self.validation_counts.nonce_too_high_details.iter().map(|d| d.tx_hash),
                );
            }
        }
        (self.header, self.validation_counts)
    }
}

/// Execute a batch payload and collect the resulting block.
pub(crate) fn execute_payload(
    payload: RLPayload,
    transactions: &[Bytes],
    executed_blocks: &mut Vec<ExecutedBlock>,
    reth_env: &RethEnv,
) -> EngineResult<UnsettledExecution> {
    let (next_canonical_block, validation_counts) =
        reth_env.build_block_from_batch_payload(payload, transactions, &executed_blocks[..])?;
    debug!(target: "engine", ?next_canonical_block, "worker's block executed");

    let canonical_header = next_canonical_block.recovered_block.clone_sealed_header();
    executed_blocks.push(next_canonical_block);

    Ok(UnsettledExecution { header: canonical_header, validation_counts })
}

/// Build and execute an empty block (no batches) for the given output.
pub(crate) fn execute_empty_block(
    canonical_header: SealedHeader,
    output: &ConsensusOutput,
    output_digest: B256,
    gas_accumulator: &GasAccumulator,
    executed_blocks: &mut Vec<ExecutedBlock>,
    reth_env: &RethEnv,
    close_epoch: Option<B256>,
) -> EngineResult<SealedHeader> {
    let base_fee_per_gas =
        gas::resolve_base_fee(reth_env, &canonical_header, MIN_PROTOCOL_BASE_FEE);
    let gas_limit = canonical_header.gas_limit;
    let leader = output.leader().origin();
    let beneficiary = gas_accumulator
        .get_authority_address(leader)
        .ok_or(RLEngineError::UnknownAuthority(leader.clone()))
        .inspect_err(|e| error!(target: "engine", ?e, "failed to find leader's execution address for empty block"))?;

    let payload = RLPayload {
        parent_header: canonical_header,
        beneficiary,
        nonce: output.nonce(),
        batch_index: 0,
        timestamp: output.committed_at(),
        batch_digest: B256::ZERO,
        consensus_header_digest: output_digest,
        base_fee_per_gas,
        gas_limit,
        mix_hash: output_digest,
        close_epoch,
        worker_id: 0,
    };

    let (header, _) = execute_payload(payload, &[], executed_blocks, reth_env)?.settle(None);
    Ok(header)
}
