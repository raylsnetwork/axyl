//! Plan derivation: read snapshot headers and consensus-DB batches into a
//! deterministic execution plan.

use crate::error::{ReplayError, ReplayResult};
use rayls_execution_evm::reth_env::SnapshotReader;
use rayls_infrastructure_storage::tables::Batches;
use rayls_infrastructure_types::{Address, Batch, Database, SealedHeader, B256};
use std::collections::BTreeMap;

/// A single planned block derived from the snapshot.
///
/// Empty / leader-reward blocks (`batch_digest == B256::ZERO`) carry `None`
/// for `batch`; everything `execute_plan_block` needs comes from `plan_header`
/// directly. Content blocks resolve `batch` from consensus-db.
#[derive(Debug)]
pub struct PlanBlock {
    /// Snapshot's authoritative sealed header for this block.
    pub plan_header: SealedHeader,
    /// `parent_beacon_block_root` == ConsensusHeader digest.
    pub output_digest: B256,
    /// `mix_hash XOR output_digest`. `B256::ZERO` for empty / leader-reward.
    pub batch_digest: B256,
    /// `Some` for content blocks, `None` for empty rounds.
    pub batch: Option<Batch>,
    /// `Some(extra_data)` at epoch-boundary blocks, `None` otherwise.
    pub close_epoch: Option<B256>,
}

/// Header-derived digests for one block.
///
/// `close_epoch` is `plan_header.extra_data`, which by Rayls header schema
/// carries `keccak256(BLS_leader_signatures)` at epoch-boundary blocks and is
/// empty otherwise. Forwarding it to `RLPayload.close_epoch` drives the
/// `ConsensusRegistry.concludeEpoch` system call exactly as live executed it.
struct HeaderDigests {
    output_digest: B256,
    batch_digest: B256,
    close_epoch: Option<B256>,
}

impl HeaderDigests {
    /// Derive the consensus digests carried by a snapshot header.
    fn from_header(header: &SealedHeader) -> Self {
        let output_digest = header.parent_beacon_block_root.unwrap_or(B256::ZERO);
        let batch_digest = header.mix_hash ^ output_digest;
        // mirror the canonical decode in evm/config.rs: only a 32-byte extra_data
        // is a close-epoch digest, so a malformed header yields None, never a panic
        let close_epoch = if header.extra_data.len() == 32 {
            Some(B256::from_slice(header.extra_data.as_ref()))
        } else {
            None
        };
        Self { output_digest, batch_digest, close_epoch }
    }
}

/// Resolve `start..=end` to a window of `PlanBlock`s in ascending order.
///
/// Reads the header range in one snapshot call and resolves every content
/// block's batch in a single consensus-DB `multi_get`, amortizing transaction
/// setup over the window. Returns fewer blocks than requested when the snapshot
/// tip falls inside the window (the caller's signal to stop).
pub fn derive_plan_window<DB: Database>(
    snapshot: &SnapshotReader,
    consensus_store: &DB,
    start: u64,
    end: u64,
) -> ReplayResult<Vec<PlanBlock>> {
    let headers = snapshot
        .blocks_for_range(start..=end)
        .map_err(|e| ReplayError::SnapshotEnv(e.to_string()))?;
    if headers.is_empty() {
        return Ok(Vec::new());
    }

    let digests: Vec<HeaderDigests> = headers.iter().map(HeaderDigests::from_header).collect();

    // resolve all content-block batches (non-zero digest) in one read txn;
    // multi_get preserves key order so results align with the filtered headers
    let batch_keys: Vec<B256> =
        digests.iter().map(|d| d.batch_digest).filter(|d| *d != B256::ZERO).collect();
    let resolved = consensus_store
        .multi_get::<Batches>(&batch_keys)
        .map_err(|e| ReplayError::ConsensusDb(e.to_string()))?;
    let mut resolved = resolved.into_iter();

    let mut plans = Vec::with_capacity(headers.len());
    for (plan_header, d) in headers.into_iter().zip(digests) {
        let batch = if d.batch_digest == B256::ZERO {
            None
        } else {
            // multi_get folds genuine read errors into None; either way a
            // missing content batch is a fatal abort at this block
            Some(resolved.next().flatten().ok_or_else(|| ReplayError::MissingBatch {
                block_number: plan_header.number,
                batch_digest: d.batch_digest,
            })?)
        };
        plans.push(PlanBlock {
            plan_header,
            output_digest: d.output_digest,
            batch_digest: d.batch_digest,
            batch,
            close_epoch: d.close_epoch,
        });
    }
    Ok(plans)
}

/// Read the snapshot block's committed withdrawals as the live close-epoch
/// tally. `build_withdrawals` encodes each leader count as a withdrawal amount,
/// so the withdrawals are the authoritative tally to reproduce on replay.
pub(crate) fn snapshot_close_epoch_tally(
    snapshot: &SnapshotReader,
    block_number: u64,
) -> ReplayResult<BTreeMap<Address, u32>> {
    let withdrawals = snapshot
        .block_withdrawals(block_number)
        .map_err(|e| ReplayError::SnapshotEnv(e.to_string()))?
        .ok_or(ReplayError::MissingHeader { block_number })?;
    Ok(withdrawals.into_iter().map(|(address, amount)| (address, amount as u32)).collect())
}
