//! Per-output execution against the archive `RethEnv`, gated on state-root match.

use crate::{
    error::{ReplayError, ReplayResult},
    integrity::{check_replay_anchor, EpochAnchor},
    plan::PlanBlock,
};
use rayls_execution_evm::{reth_env::RethEnv, ExecutedBlock};
use rayls_infrastructure_types::{payload::RLPayload, Bytes, SealedHeader};
use std::collections::BTreeMap;
use tracing::warn;

/// Execute every block of one consensus output on top of `parent`, mirroring the
/// live orchestrator: each block builds on the accumulating in-output ancestors
/// via `local_chain`, and `finish_executing_output` runs once for the whole output.
///
/// Returns the new canonical tip and how many blocks diverged from the snapshot.
pub async fn execute_output_group(
    archive_evm: &RethEnv,
    plans: &[PlanBlock],
    parent: SealedHeader,
    verify_every_block: bool,
    anchors: &BTreeMap<u64, EpochAnchor>,
) -> ReplayResult<(SealedHeader, u64)> {
    let mut executed_blocks: Vec<ExecutedBlock> = Vec::with_capacity(plans.len());
    let mut canonical = parent;
    let mut diverged_count = 0u64;

    for plan in plans {
        let (beneficiary, worker_id, transactions): (_, u16, &[Bytes]) =
            if let Some(batch) = plan.batch.as_ref() {
                (batch.beneficiary, batch.worker_id, batch.transactions.as_slice())
            } else {
                (plan.plan_header.beneficiary, 0u16, &[][..])
            };

        // difficulty packs `(batch_index << 16) | worker_id`; recover batch_index
        let difficulty_raw = plan.plan_header.difficulty.to::<u128>();
        let batch_index = (difficulty_raw >> 16) as usize;
        let base_fee_per_gas = plan.plan_header.base_fee_per_gas.unwrap_or(0);
        let gas_limit = plan.plan_header.gas_limit;

        // move the parent header into the payload to avoid a per-block SealedHeader
        // clone (Header carries a 256-byte bloom); canonical is reassigned from
        // our_header at the end of the iteration before the next read
        let payload = RLPayload {
            parent_header: canonical,
            beneficiary,
            nonce: plan.plan_header.nonce.into(),
            batch_index,
            timestamp: plan.plan_header.timestamp,
            batch_digest: plan.batch_digest,
            consensus_header_digest: plan.output_digest,
            base_fee_per_gas,
            gas_limit,
            mix_hash: plan.plan_header.mix_hash,
            close_epoch: plan.close_epoch,
            worker_id,
        };

        if plan.close_epoch.is_some() {
            archive_evm
                .flush_persistence()
                .await
                .map_err(|e| ReplayError::ArchiveEnv(e.to_string()))?;
        }
        // build on the accumulating in-output ancestors, matching live execute_payload;
        // the installed canonical-root oracle re-derives a divergent root from leaves
        let (next_block, _validation_counts) = archive_evm
            .build_block_from_batch_payload(payload, transactions, &executed_blocks[..])
            .map_err(|e| ReplayError::ArchiveEnv(e.to_string()))?;

        let our_header = next_block.recovered_block.clone_sealed_header();

        let diverged = (verify_every_block || plan.close_epoch.is_some())
            && our_header.state_root != plan.plan_header.state_root;
        if diverged {
            diverged_count += 1;
            warn!(
                target: "rayls_replay::replay",
                block_number = our_header.number,
                close_epoch = plan.close_epoch.is_some(),
                ours = ?our_header.state_root,
                expected = ?plan.plan_header.state_root,
                "state root diverged from snapshot (unhealed)"
            );
        }

        // a committed epoch boundary must match the consensus DB's BFT-signed anchor
        if let Some(anchor) = anchors.get(&our_header.number) {
            check_replay_anchor(anchor, &our_header);
        }

        if our_header.hash() != plan.plan_header.hash() {
            return Err(ReplayError::BlockHashMismatch {
                block_number: our_header.number,
                ours: our_header.hash(),
                expected: plan.plan_header.hash(),
            });
        }

        canonical = our_header;
        executed_blocks.push(next_block);
    }

    // one finish per output, mirroring the live orchestrator's per-output finalize
    archive_evm
        .finish_executing_output(executed_blocks)
        .map_err(|e| ReplayError::ArchiveEnv(e.to_string()))?;
    archive_evm
        .finalize_block(canonical.clone())
        .map_err(|e| ReplayError::ArchiveEnv(e.to_string()))?;

    Ok((canonical, diverged_count))
}
