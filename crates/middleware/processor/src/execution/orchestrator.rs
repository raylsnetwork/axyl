//! Execution orchestrator: consensus output to finalized EVM blocks.

use crate::{
    batch::BatchOrdering,
    error::EngineResult,
    execution::block::{execute_empty_block, execute_payload},
    gas,
};
use rayls_execution_evm::{
    chainspec::RaylsHardforks,
    payload::BuildArguments,
    reth_env::{RethEnv, TxValidationCounts},
    ExecutedBlock,
};
use rayls_infrastructure_types::{
    batch_tracker::BatchTracker, executed_batch_registry::ExecutedBatchRegistry,
    gas_accumulator::GasAccumulator, AcceptResult, Address, Batch, CameFrom, Database, Epoch,
    Hash as _, PreparedBatch, SealedHeader, B256,
};
use std::sync::Arc;
use tracing::{debug, info, trace, warn};

/// Shared services for the execution pipeline.
#[derive(Debug, Clone)]
pub struct Processor<DB: Database> {
    pub(crate) reth_env: RethEnv,
    pub(crate) gas_accumulator: GasAccumulator,
    pub(crate) batch_tracker: Option<Arc<BatchTracker>>,
    pub(crate) executed_batch_registry: ExecutedBatchRegistry,
    pub(crate) batch_ordering: BatchOrdering<DB>,
    pub(crate) gas_limit: u64,
}

impl<DB: Database> Processor<DB> {
    /// Create a new [`Processor`] with the given shared services.
    pub fn new(
        reth_env: RethEnv,
        gas_accumulator: GasAccumulator,
        batch_tracker: Option<Arc<BatchTracker>>,
        executed_batch_registry: ExecutedBatchRegistry,
        batch_ordering: BatchOrdering<DB>,
        gas_limit: u64,
    ) -> Self {
        Self {
            reth_env,
            gas_accumulator,
            batch_tracker,
            executed_batch_registry,
            batch_ordering,
            gas_limit,
        }
    }

    /// Set the batch lifecycle tracker.
    pub fn set_batch_tracker(&mut self, tracker: Arc<BatchTracker>) {
        self.batch_tracker = Some(tracker);
    }

    /// Shared reth execution environment.
    pub fn reth_env(&self) -> &RethEnv {
        &self.reth_env
    }

    /// Optional batch lifecycle tracker.
    pub fn batch_tracker(&self) -> Option<&Arc<BatchTracker>> {
        self.batch_tracker.as_ref()
    }

    /// Check if BatchDigestV2 behavior is active at the next block after `parent`.
    fn is_batch_digest_v2_active(&self, parent_block_number: u64) -> bool {
        self.reth_env.rayls_chain_spec().is_batch_digest_v2_active_at_block(parent_block_number + 1)
    }

    /// Check if EmptyOutputBlock behavior is active at the next block after `parent`.
    fn is_empty_output_block_active(&self, parent_block_number: u64) -> bool {
        self.reth_env
            .rayls_chain_spec()
            .is_empty_output_block_active_at_block(parent_block_number + 1)
    }

    /// Execute consensus output to extend the canonical chain.
    pub fn execute_consensus_output(
        &self,
        args: BuildArguments,
        came_from: CameFrom,
    ) -> EngineResult<SealedHeader> {
        let BuildArguments { reth_env, mut output, parent_header: mut canonical_header } = args;
        reth_env.check_persistence_completion();
        debug!(target: "engine", ?output, "executing output");
        let output_start = std::time::Instant::now();

        self.gas_accumulator.rewards_counter().inc_leader_count(output.leader().origin());

        let output_digest: B256 = output.digest().into();
        let batches = output.flatten_batches();
        let epoch = output.leader().epoch();

        let batch_digest_v2 = self.is_batch_digest_v2_active(canonical_header.number);

        let capacity = batches.len().max(1);
        let mut executed_blocks = Vec::with_capacity(capacity);

        (canonical_header, executed_blocks) = self.drain_and_execute_epoch_parked(
            epoch,
            canonical_header,
            executed_blocks,
            batch_digest_v2,
        )?;

        // Blocks produced by epoch-drain belong to the PREVIOUS epoch's parked batches, executed
        // before this output. Track them so the every-output-builds-a-block fallback below fires
        // when this output itself contributes nothing.
        let blocks_before_output = executed_blocks.len();

        {
            let (parent_epoch, parent_round) =
                RethEnv::deconstruct_nonce(canonical_header.nonce.into());
            let output_nonce = output.nonce();
            let (output_epoch, output_round) = RethEnv::deconstruct_nonce(output_nonce);
            trace!(
                target: "engine",
                parent_block_number = canonical_header.number,
                parent_nonce_epoch = parent_epoch,
                parent_nonce_round = parent_round,
                output_number = output.number,
                output_nonce,
                output_epoch,
                output_round,
                num_batches = batches.len(),
                "execute_consensus_output starting"
            );
        }

        debug_assert_eq!(
            batches.len(),
            output.batch_digests.len(),
            "uneven number of sealed blocks from batches and batch digests"
        );

        let num_output_batches = batches.len();

        let output_ctx = OutputContext {
            digest: output_digest,
            nonce: output.nonce(),
            timestamp: output.committed_at(),
            epoch,
        };

        let batch_ctx = BatchContext {
            batches,
            arc_batches: std::mem::take(&mut output.batches)
                .into_iter()
                .map(|cb| (cb.address, cb.batches.into_iter().map(Arc::new).collect()))
                .collect(),
            digests: output.batch_digests.iter().copied().collect(),
        };

        let collected = self.collect_executable_batches(batch_ctx, &output_ctx, batch_digest_v2);

        let close_epoch_value = output.close_epoch.then(|| output.keccak_leader_sigs());

        // close_epoch only goes to the batch at the last output position, matching the old
        // close_epoch_for_last_batch() consumption semantics.
        let last_output_pos = num_output_batches.checked_sub(1);
        let last_current_output_idx = last_output_pos
            .and_then(|pos| collected.iter().position(|b| !b.drained && b.batch_index == pos));

        for (i, prepared) in collected.iter().enumerate() {
            let close_epoch =
                if Some(i) == last_current_output_idx { close_epoch_value } else { None };

            let (header, validation_counts) = self.execute_prepared_batch(
                prepared,
                canonical_header,
                close_epoch,
                &mut executed_blocks,
            )?;
            canonical_header = header;

            // V2: retry batches where all txs had nonce_too_high
            if batch_digest_v2
                && prepared.batch.transactions.len() == validation_counts.nonce_too_high as usize
            {
                self.executed_batch_registry.drop_digest(prepared.batch_digest);
            }
        }

        // Every output must produce at least one block. When this output contributed none (no
        // batches, all deduped, or all parked) build a fallback empty block at its own position. A
        // close_epoch output carries `close_epoch_value` into that block. A parked batch still
        // executes LATER as its own block once its seq becomes consecutive.
        let output_produced_block = executed_blocks.len() > blocks_before_output;
        let close_epoch_unconsumed = output.close_epoch && last_current_output_idx.is_none();
        // Hardfork-gated. Post-fork (EmptyOutputBlock): every output yields >=1 block — emit a
        // fallback whenever THIS output produced none, or a close_epoch batch was parked/drained.
        // Pre-fork: reproduce the EXACT pre-`e44a028` branch structure so replaying existing
        // history is bit-identical (the difference is the drained-parked case, where the old code
        // emitted no own block for the output). The old "all batches parked, no fallback" case
        // produced no block at all — still safe because epoch-drained blocks kept the call
        // non-empty for `finish_executing_output`.
        let empty_output_block_active = self.is_empty_output_block_active(canonical_header.number);
        let output_had_batches = num_output_batches > 0;
        let produce_empty = if empty_output_block_active {
            !output_produced_block || close_epoch_unconsumed
        } else if collected.is_empty() {
            !output_had_batches
                || (output.close_epoch && (batch_digest_v2 || executed_blocks.is_empty()))
        } else {
            output.close_epoch && last_current_output_idx.is_none() && batch_digest_v2
        };
        if produce_empty {
            if output_produced_block {
                // close_epoch output whose current-position batch was parked/drained: emit the
                // close_epoch block after the drained blocks so the epoch boundary is recorded.
                warn!(
                    target: "engine",
                    output_number = output.number,
                    "close_epoch batch not executed at its position, building fallback close block"
                );
            }
            canonical_header = execute_empty_block(
                canonical_header,
                &output,
                output_digest,
                &self.gas_accumulator,
                &mut executed_blocks,
                &reth_env,
                close_epoch_value,
            )?;
        }

        // Pre-fork parity ONLY: `4ecb0f9` persisted ordering and returned WITHOUT calling
        // finish_executing_output when an output produced no block. Post-fork the gate above
        // always emits a fallback block, so an empty `executed_blocks` is an invariant violation
        // we deliberately let panic in finish_executing_output rather than swallow.
        if !empty_output_block_active && executed_blocks.is_empty() {
            warn!(
                target: "engine",
                output_number = output.number,
                "no blocks executed for this output"
            );
            self.batch_ordering.persist();
            return Ok(canonical_header);
        }

        reth_env.finish_executing_output(executed_blocks)?;
        reth_env.finalize_block(canonical_header.clone())?;
        self.batch_ordering.persist();

        let output_elapsed = output_start.elapsed();
        info!(
            target: "engine",
            came_from = %came_from,
            output_number = output.number,
            output_ms = %output_elapsed.as_millis(),
            head = canonical_header.number,
            // Which authority's certificate Bullshark picked as leader for this commit. Every
            // validator logs `spawning proposal task` for its *own* header each round, so those
            // lines cannot identify the leader - it is only decided here, at commit. Without
            // this, attributing a commit to an authority means reading the certificate out of
            // the node database after the fact.
            leader = %output.leader().origin(),
            leader_round = output.leader_round(),
            epoch,
            "output executed"
        );

        if let Some(tracker) = &self.batch_tracker {
            tracker.output_executed(output.number);
        }

        Ok(canonical_header)
    }

    /// Drain and execute all parked batches from the previous epoch on epoch change.
    fn drain_and_execute_epoch_parked(
        &self,
        epoch: Epoch,
        mut canonical_header: SealedHeader,
        mut executed_blocks: Vec<ExecutedBlock>,
        batch_digest_v2: bool,
    ) -> EngineResult<(SealedHeader, Vec<ExecutedBlock>)> {
        let epoch_parked = self.batch_ordering.drain_epoch(epoch);

        for parked in epoch_parked {
            if batch_digest_v2 {
                // V2: dedup check on epoch drain (parked batches not pre-registered)
                if !self
                    .executed_batch_registry
                    .try_register(parked.batch_digest, parked.output_digest)
                {
                    if let Some(tracker) = &self.batch_tracker {
                        tracker.batch_deduped(parked.batch_digest);
                    }
                    continue;
                }
            }
            // V1: no dedup check (batches were registered at park time)

            let (header, _) =
                self.execute_prepared_batch(&parked, canonical_header, None, &mut executed_blocks)?;
            canonical_header = header;
        }

        Ok((canonical_header, executed_blocks))
    }

    /// Collect executable batches from the current output, applying dedup and seq ordering.
    fn collect_executable_batches(
        &self,
        batch_ctx: BatchContext,
        output_ctx: &OutputContext,
        batch_digest_v2: bool,
    ) -> Vec<PreparedBatch> {
        let mut executable_batches = Vec::with_capacity(batch_ctx.batches.len());

        for (batch_index, (cert_idx, batch_idx_in_cert)) in
            batch_ctx.batches.into_iter().enumerate()
        {
            let batch_digest = batch_ctx.digests[batch_index];

            if self.executed_batch_registry.contains(&batch_digest) {
                if let Some(tracker) = &self.batch_tracker {
                    tracker.batch_deduped(batch_digest);
                }
                info!(
                    target: "executed_batch_registry",
                    ?batch_digest,
                    output_digest = ?output_ctx.digest,
                    "skipping duplicate batch digest"
                );
                continue;
            }

            let (beneficiary, ref cert_batches) = batch_ctx.arc_batches[cert_idx];
            let batch = &cert_batches[batch_idx_in_cert];

            if batch_digest_v2 {
                // V2: register only after accept (parked batches can be retried)
                let prepared = PreparedBatch {
                    batch: Arc::clone(batch),
                    batch_digest,
                    beneficiary,
                    output_digest: output_ctx.digest,
                    output_nonce: output_ctx.nonce,
                    timestamp: output_ctx.timestamp,
                    epoch: output_ctx.epoch,
                    worker_id: batch.worker_id,
                    batch_index,
                    drained: false,
                    gas_limit: self.gas_limit,
                };

                let prepared =
                    match self.batch_ordering.try_accept(beneficiary, batch.seq, prepared) {
                        AcceptResult::Parked => {
                            if let Some(tracker) = &self.batch_tracker {
                                tracker.batch_parked(batch_digest);
                            }
                            continue;
                        }
                        AcceptResult::InOrder(p) | AcceptResult::OverflowForced(p) => p,
                    };

                if !self.executed_batch_registry.try_register(batch_digest, output_ctx.digest) {
                    if let Some(tracker) = &self.batch_tracker {
                        tracker.batch_deduped(batch_digest);
                    }
                    continue;
                }

                executable_batches.push(prepared);

                self.batch_ordering.drain_consecutive(
                    beneficiary,
                    &mut executable_batches,
                    &self.executed_batch_registry,
                    self.batch_tracker.as_ref(),
                    true,
                );
            } else {
                // V1: register before parking so reproposed copies are rejected
                if !self.executed_batch_registry.try_register(batch_digest, output_ctx.digest) {
                    if let Some(tracker) = &self.batch_tracker {
                        tracker.batch_deduped(batch_digest);
                    }
                    continue;
                }

                let prepared = PreparedBatch {
                    batch: Arc::clone(batch),
                    batch_digest,
                    beneficiary,
                    output_digest: output_ctx.digest,
                    output_nonce: output_ctx.nonce,
                    timestamp: output_ctx.timestamp,
                    epoch: output_ctx.epoch,
                    worker_id: batch.worker_id,
                    batch_index,
                    drained: false,
                    gas_limit: self.gas_limit,
                };

                let prepared =
                    match self.batch_ordering.try_accept(beneficiary, batch.seq, prepared) {
                        AcceptResult::Parked => {
                            if let Some(tracker) = &self.batch_tracker {
                                tracker.batch_parked(batch_digest);
                            }
                            continue;
                        }
                        AcceptResult::InOrder(p) | AcceptResult::OverflowForced(p) => p,
                    };

                executable_batches.push(prepared);

                self.batch_ordering.drain_consecutive(
                    beneficiary,
                    &mut executable_batches,
                    &self.executed_batch_registry,
                    self.batch_tracker.as_ref(),
                    false,
                );
            }
        }

        executable_batches
    }

    /// Execute a prepared batch as an EVM block, reporting metrics and updating gas state.
    fn execute_prepared_batch(
        &self,
        prepared: &PreparedBatch,
        canonical_header: SealedHeader,
        close_epoch: Option<B256>,
        executed_blocks: &mut Vec<ExecutedBlock>,
    ) -> EngineResult<(SealedHeader, TxValidationCounts)> {
        if let Some(tracker) = &self.batch_tracker {
            tracker.batch_executing(prepared.batch_digest, prepared.batch.transactions.len());
        }

        let base_fee_per_gas = gas::resolve_base_fee(
            &self.reth_env,
            &canonical_header,
            prepared.batch.base_fee_per_gas,
        );
        let payload = prepared.build_payload(canonical_header, close_epoch, base_fee_per_gas);

        let (header, validation_counts) = execute_payload(
            payload,
            &prepared.batch.transactions,
            executed_blocks,
            &self.reth_env,
        )?;
        report_batch_execution(
            self.batch_tracker.as_ref(),
            prepared.batch_digest,
            header.number,
            &validation_counts,
        );

        self.gas_accumulator.inc_block(prepared.worker_id, header.gas_used, header.gas_limit);
        gas::update_base_fee_after_block(&self.reth_env, &self.gas_accumulator, &header);

        Ok((header, validation_counts))
    }
}

/// Report batch execution metrics to the tracker.
fn report_batch_execution(
    batch_tracker: Option<&Arc<BatchTracker>>,
    batch_digest: B256,
    block_number: u64,
    counts: &TxValidationCounts,
) {
    if let Some(tracker) = batch_tracker {
        tracker.batch_executed(batch_digest, block_number);
        let details: Vec<_> = counts
            .nonce_too_high_details
            .iter()
            .map(|d| (d.tx_hash, d.sender, d.tx_nonce, d.state_nonce))
            .collect();
        tracker.tx_validation_counts(
            &rayls_infrastructure_types::batch_tracker::TxValidationReport {
                digest: batch_digest,
                nonce_too_high: counts.nonce_too_high,
                nonce_too_low: counts.nonce_too_low,
                other: counts.other,
                sender_nonce_ranges: counts.sender_nonce_ranges.clone(),
                nonce_too_high_details: details,
            },
        );
    }
}

/// Flattened batch data from a consensus output, ready for collection.
struct BatchContext {
    /// Flattened `(cert_idx, batch_idx_in_cert)` indices.
    batches: Vec<(usize, usize)>,
    /// Batch data wrapped in Arcs, grouped by certificate.
    arc_batches: Vec<(Address, Vec<Arc<Batch>>)>,
    /// Digests for each batch in order.
    digests: Vec<B256>,
}

/// Consensus output metadata shared across all batches in an output.
struct OutputContext {
    digest: B256,
    nonce: u64,
    timestamp: u64,
    epoch: Epoch,
}

/// Backward-compatible free function that constructs a [`Processor`] and delegates.
///
/// External callers (batch-builder tests) use this entry point.
pub fn execute_consensus_output<DB: Database>(
    args: BuildArguments,
    gas_accumulator: GasAccumulator,
    batch_tracker: Option<Arc<BatchTracker>>,
    executed_batch_registry: ExecutedBatchRegistry,
    batch_ordering: BatchOrdering<DB>,
    gas_limit: u64,
) -> EngineResult<SealedHeader> {
    let processor = Processor::new(
        args.reth_env.clone(),
        gas_accumulator,
        batch_tracker,
        executed_batch_registry,
        batch_ordering,
        gas_limit,
    );
    processor.execute_consensus_output(args, CameFrom::FreeFn)
}
