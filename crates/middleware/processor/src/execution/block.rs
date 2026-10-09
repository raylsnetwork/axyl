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
    gas_accumulator::GasAccumulator, payload::RLPayload, Address, Bytes, ConsensusOutput,
    SealedHeader, B256, MIN_PROTOCOL_BASE_FEE,
};
use tracing::{debug, error};

/// An executed block whose header is obtainable only by settling its dropped-tx marks.
///
/// A batch tx that executes as nonce-too-high stays pooled and nonce-contiguous, so the pool's
/// membership reconcile never touches its in-flight mark; the witness makes the hold a
/// precondition of using the header instead of a call every caller must remember.
#[must_use = "settle the dropped-tx marks to obtain the executed header"]
pub(crate) struct UnsettledExecution {
    header: SealedHeader,
    validation_counts: TxValidationCounts,
    /// Senders whose state nonce this block advanced, releasing their held (dropped) successors.
    executed_senders: Vec<Address>,
}

impl UnsettledExecution {
    /// Releases the held marks of every sender this block advanced, holds the marks of the txs it
    /// dropped as nonce-too-high, and yields the executed header with its validation counts.
    ///
    /// Release runs first: a hash dropped in this block is behind a nonce this block did not
    /// execute, so it stays held even when the same sender advanced here.
    ///
    /// `None` is for blocks that carry no batch txs (the empty block), where there is nothing to
    /// settle.
    pub(crate) fn settle(
        self,
        tracker: Option<&InFlightTracker>,
    ) -> (SealedHeader, TxValidationCounts) {
        if let Some(tracker) = tracker {
            if !self.executed_senders.is_empty() {
                tracker.release_advanced(self.executed_senders);
            }
            let dropped = &self.validation_counts.nonce_too_high_details;
            if !dropped.is_empty() {
                tracker.hold_dropped(dropped.iter().map(|d| (d.sender, d.tx_hash)));
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
    let executed_senders = next_canonical_block.recovered_block.senders().to_vec();
    executed_blocks.push(next_canonical_block);

    Ok(UnsettledExecution { header: canonical_header, validation_counts, executed_senders })
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

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_execution_evm::{
        in_flight::{DuePolicy, InFlightTracker},
        reth_env::NonceTooHighDetail,
    };
    use rayls_infrastructure_types::ExecHeader;
    use std::time::Duration;

    /// Settling holds the block's nonce-too-high hashes and releases the held hashes of every
    /// sender the block advanced, so a dropped successor is re-sealed exactly when it can execute.
    #[test]
    fn settle_holds_dropped_hashes_and_releases_advanced_senders() {
        let tracker = InFlightTracker::new();
        let sealing = tracker.arm_sealing(DuePolicy::ttl(Duration::from_secs(60)));
        let (a, b) = (Address::random(), Address::random());
        let (dropped_a, held_b) = (B256::random(), B256::random());
        sealing.mark([dropped_a, held_b]);
        tracker.hold_dropped([(b, held_b)]);

        let mut validation_counts = TxValidationCounts::default();
        validation_counts.nonce_too_high_details.push(NonceTooHighDetail {
            tx_hash: dropped_a,
            sender: a,
            tx_nonce: 5,
            state_nonce: 3,
        });
        let execution = UnsettledExecution {
            header: SealedHeader::seal_slow(ExecHeader::default()),
            validation_counts,
            executed_senders: vec![b],
        };
        execution.settle(Some(&tracker));

        assert!(tracker.is_in_flight(&dropped_a), "a dropped hash is held, not re-sealable");
        assert!(!tracker.is_in_flight(&held_b), "the advanced sender's held hash is released");
    }
}
