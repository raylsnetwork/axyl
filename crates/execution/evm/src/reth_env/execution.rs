use crate::{
    error::{RaylsRethError, RaylsRethResult},
    evm::RaylsEvmConfig,
    reth_env::{
        types::reth_recover_raw_transactions, NonceTooHighDetail, RethEnv, SparseRootFn,
        TxValidationCounts,
    },
    traits::RaylsPrimitives,
    FixedBytes,
};
use alloy::{
    consensus::{BlockHeader as _, Transaction as _},
    eips::eip1898::BlockWithParent,
};
use alloy_evm::{revm::context_interface::result::InvalidTransaction, Evm};
use rayls_infrastructure_types::{
    payload::RLPayload, BlockNumHash, RecoveredBlock, TransactionSigned, B256, U256,
};
use reth_chain_state::{
    ComputedTrieData, DeferredTrieData, ExecutedBlock, LazyOverlay, MemoryOverlayStateProvider,
    NewCanonicalChain,
};
use reth_engine_tree::tree::{ExecutionEnv, StateProviderBuilder};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::BlockExecutor,
    execute::{BlockAssembler, BlockAssemblerInput, BlockBuilder, BlockExecutionOutput},
    ConfigureEvm,
};
use reth_primitives::Recovered;
use reth_provider::{
    providers::OverlayStateProviderFactory, AccountReader as _, DatabaseProviderFactory,
    LatestStateProvider, StateProviderBox,
};
use reth_revm::{
    cached::CachedReads, database::StateProviderDatabase,
    db::states::bundle_state::BundleRetention, State,
};
use reth_trie::{
    changesets::compute_trie_changesets, trie_cursor::InMemoryTrieCursorFactory,
    updates::TrieUpdatesSorted, HashedPostStateSorted, TrieInput,
};
use reth_trie_db::DatabaseTrieCursorFactory;
use std::sync::Arc;
use tracing::{debug, info, warn};

impl RethEnv {
    /// Whether every transaction in `transactions` is still pending against the latest state —
    /// i.e. each tx's nonce is at or ahead of its sender's current account nonce.
    ///
    /// "Pending" here is `tx.nonce >= account.nonce` (at or ahead — the txn hasn't been mined yet);
    /// the typical retryable case is strictly ahead (nonce-too-high), but a tx exactly at the
    /// current nonce also counts as pending.
    ///
    /// Used by restart dedup reconstruction to tell two empty blocks apart: a batch whose txns are
    /// still pending produced an empty block only because they aren't yet executable and is
    /// genuinely retryable; a batch whose txns are already mined (nonce-too-low) is done and must
    /// stay deduped, else a restart re-executes it and forks the chain.
    ///
    /// Conservative: returns `false` for an empty input, if any tx is already mined, or on any
    /// decode/state error — never reports "retryable" unless it can positively prove it.
    pub fn batch_txns_all_pending(&self, transactions: &[Vec<u8>]) -> bool {
        if transactions.is_empty() {
            return false;
        }
        let Ok(state) = self.latest() else { return false };
        // `None` batch_digest: recovery errors here won't carry the digest in their log context,
        // which is fine — an undecodable tx just makes the batch non-retryable below.
        for res in reth_recover_raw_transactions(None, transactions) {
            let Ok(recovered) = res else { return false };
            let sender = recovered.signer();
            let account_nonce = match state.basic_account(&sender) {
                Ok(Some(account)) => account.nonce,
                Ok(None) => 0,
                Err(_) => return false,
            };
            // already mined / superseded -> not retryable
            if recovered.nonce() < account_nonce {
                return false;
            }
        }
        true
    }

    /// Construct a canonical block from a worker's block that reached consensus.
    ///
    /// State root computation uses a 3-tier strategy:
    /// - **Tier 1** (sparse trie): concurrent with execution via PayloadProcessor
    /// - **Tier 2** (parallel root): parallel trie traversal after execution (Tier 1 fallback)
    /// - **Tier 3** (parallel root): same algorithm, parent still in memory
    pub fn build_block_from_batch_payload(
        &self,
        payload: RLPayload,
        transactions: &[Vec<u8>],
        local_chain: &[ExecutedBlock],
    ) -> RaylsRethResult<(ExecutedBlock, TxValidationCounts)> {
        let parent_header = payload.parent_header.clone();
        debug!(target: "engine", ?parent_header, "retrieving state for next block");
        let state_provider: StateProviderBox = {
            let db_provider = self.blockchain_provider.database_provider_ro()?;
            let latest: StateProviderBox = Box::new(LatestStateProvider::new(db_provider));
            if local_chain.is_empty() {
                let canonical_in_memory_state = self.canonical_in_memory_state();
                Box::new(canonical_in_memory_state.state_provider(parent_header.hash(), latest))
            } else {
                let anchor_hash = local_chain[0].recovered_block.header().parent_hash;
                let canonical_in_memory_state = self.canonical_in_memory_state();
                let historical: StateProviderBox =
                    Box::new(canonical_in_memory_state.state_provider(anchor_hash, latest));
                let in_memory: Vec<_> = local_chain.iter().rev().cloned().collect();
                Box::new(MemoryOverlayStateProvider::new(historical, in_memory))
            }
        };
        let state = StateProviderDatabase::new(&state_provider);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .without_state_clear()
            .build();

        debug!(
            target: "engine",
            parent = ?parent_header.num_hash(),
            "building new payload"
        );

        // collect these totals to report at the end
        let mut total_fees = U256::ZERO;

        // copy in case of error
        let batch_digest = payload.batch_digest;

        // The cache uses get_canonical_block_number() (an atomic on
        // ChainInfoTracker) as its key.  update_chain() inserts blocks into
        // CIM's maps but does NOT advance that atomic — set_canonical_head()
        // only runs later in finish_executing_output().  So for blocks 2..M
        // in a round the cache returns stale input missing prior blocks'
        // hashed_state + trie_updates.  Extend explicitly with local_chain.
        let ancestor_start = std::time::Instant::now();
        let ancestor_input = if local_chain.is_empty() {
            self.cached_ancestor_trie_input()
        } else {
            let base = self.cached_ancestor_trie_input();
            let mut extended = (*base).clone();
            let sorted_data: Vec<_> =
                local_chain.iter().map(|b| (b.hashed_state(), b.trie_updates())).collect();
            for (ref hs, ref tu) in &sorted_data {
                extended.state.extend_from_sorted(hs);
                extended.nodes.extend_from_sorted(tu);
            }
            Arc::new(extended)
        };
        let ancestor_elapsed = ancestor_start.elapsed();

        // Sort the ancestor overlay and share via Arc. Archive replay reuses a base
        // memoized per head advance, merging only the in-output delta; the live path
        // re-sorts every block.
        #[cfg(feature = "archive-replay")]
        let (ancestor_sorted_state, ancestor_sorted_nodes) = {
            let (base_state, base_nodes) = self.cached_sorted_ancestor_input();
            if local_chain.is_empty() {
                (base_state, base_nodes)
            } else {
                let mut state = (*base_state).clone();
                let mut nodes = (*base_nodes).clone();
                for b in local_chain {
                    state.extend_ref_and_sort(&b.hashed_state());
                    nodes.extend_ref_and_sort(&b.trie_updates());
                }
                (Arc::new(state), Arc::new(nodes))
            }
        };
        #[cfg(not(feature = "archive-replay"))]
        let (ancestor_sorted_state, ancestor_sorted_nodes) = (
            Arc::new(ancestor_input.state.clone_into_sorted()),
            Arc::new(ancestor_input.nodes.clone_into_sorted()),
        );

        // Recover transactions early so prewarming can use them in parallel
        let recovered_transactions =
            reth_recover_raw_transactions(Some(batch_digest), transactions);
        let prewarm_txs: Vec<Recovered<TransactionSigned>> =
            recovered_transactions.iter().filter_map(|r| r.as_ref().ok().cloned()).collect();

        // Tier 1 setup: always attempt sparse trie regardless of whether parent
        // is in memory or on disk. The overlay factory now carries both trie nodes
        // and hashed state from ancestors, so the sparse trie can read correct
        // in-memory branch nodes even when the DB has stale data after persistence.
        let sparse_setup = match self.spawn_sparse_trie_task(
            parent_header.hash(),
            parent_header.state_root,
            prewarm_txs,
            Arc::clone(&ancestor_input),
            Arc::clone(&ancestor_sorted_state),
            Arc::clone(&ancestor_sorted_nodes),
        ) {
            Ok((state_hook, sparse_root_fn)) => Some((state_hook, sparse_root_fn)),
            Err(err) => {
                debug!(target: "engine", %err, "Tier 1 setup failed, will use Tier 2");
                None
            }
        };

        // NOTE: gas fix in inspector gated by PrecompileGasFix hardfork
        let evm_env = self.evm_config.next_evm_env(&parent_header, &payload)?;
        let evm = self.evm_config.evm_factory.create_evm_with_native_erc20_only(
            &mut db,
            evm_env,
            self.evm_config.chain_spec().clone(),
        );
        let ctx = self.evm_config.context_for_next_block(&parent_header, payload)?;
        let ctx_for_assembler = ctx.clone();
        let mut builder = self.evm_config.create_block_builder(evm, &parent_header, ctx);

        // attach state_hook to the executor if Tier 1 is active — the hook
        // sends per-transaction state diffs to the multiproof/sparse trie task
        let sparse_root_fn = if let Some((state_hook, sparse_root_fn)) = sparse_setup {
            builder.executor_mut().set_state_hook(Some(state_hook));
            Some(sparse_root_fn)
        } else {
            None
        };

        builder.apply_pre_execution_changes().inspect_err(|err| {
            warn!(target: "engine", %err, "failed to apply pre-execution changes");
        })?;

        let basefee = builder.evm_mut().block().basefee;

        let mut validation_counts = TxValidationCounts::default();
        let mut committed_txs: Vec<Recovered<TransactionSigned>> = Vec::new();

        for recovered_res in recovered_transactions {
            let recovered = match recovered_res {
                Ok(tx) => tx,
                Err(err) => {
                    // allow transaction errors (ie - bad sigs)
                    debug!(target: "engine", %err, ?batch_digest, "skipping invalid transaction in batch");
                    validation_counts.other += 1;
                    continue;
                }
            };

            // track per-sender nonce range across all txs in this batch
            validation_counts.observe_nonce(recovered.signer(), recovered.nonce());

            let gas_used = match builder.execute_transaction(recovered.clone()) {
                Ok(gas_used) => gas_used,
                Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                    error,
                    ..
                })) => {
                    if error.is_nonce_too_low() {
                        validation_counts.nonce_too_low += 1;
                    } else if let Some(InvalidTransaction::NonceTooHigh { tx, state }) =
                        error.as_invalid_tx_err()
                    {
                        validation_counts.nonce_too_high += 1;
                        validation_counts.nonce_too_high_details.push(NonceTooHighDetail {
                            tx_hash: *recovered.tx_hash(),
                            sender: recovered.signer(),
                            tx_nonce: *tx,
                            state_nonce: *state,
                        });
                    } else {
                        validation_counts.other += 1;
                    }
                    continue;
                }
                // this is an error that we should treat as fatal for this attempt
                Err(err) => return Err(err.into()),
            };

            committed_txs.push(recovered.clone());

            // update add to total fees
            let miner_fee = recovered
                .effective_tip_per_gas(basefee)
                .expect("fee is always valid; execution succeeded");
            total_fees += U256::from(miner_fee) * U256::from(gas_used);
        }

        // Phase 2: finish the executor — runs post-execution system calls
        // (epoch closing) and drops the state_hook, which signals the sparse
        // trie to begin finalizing
        let finish_start = std::time::Instant::now();
        let executor = builder.into_executor();
        let (evm, execution_result) = executor.finish()?;
        let (_db_ref, evm_env) = evm.finish();

        // Phase 3: merge all state transitions into the bundle
        db.merge_transitions(BundleRetention::Reverts);

        // Phase 4: compute hashed post state from bundle
        let hashed_state = state_provider.hashed_post_state(&db.bundle_state);

        // Phase 5: compute state root — SDK handles sparse trie internally
        let mut tier_label;
        let (state_root, trie_updates) = if let Some(sparse_root_fn) = sparse_root_fn {
            tier_label = "sparse";
            match sparse_root_fn() {
                Ok((root, updates)) => (root, updates),
                Err(err) => {
                    warn!(target: "engine", %err, "sparse trie failed, falling back to serial state root");
                    tier_label = "serial-fallback";
                    state_provider
                        .state_root_with_updates(hashed_state.clone())
                        .map_err(|e| RaylsRethError::EVMCustom(e.to_string()))?
                }
            }
        } else {
            tier_label = "serial";
            state_provider
                .state_root_with_updates(hashed_state.clone())
                .map_err(|e| RaylsRethError::EVMCustom(e.to_string()))?
        };

        // archive replay re-derives a divergent root from leaves (a re-execution
        // artifact); inert on the live path, where no oracle is installed
        #[cfg(feature = "archive-replay")]
        let (state_root, trie_updates) = self.heal_divergent_state_root(
            parent_header.number + 1,
            state_root,
            trie_updates,
            &hashed_state,
            &ancestor_input.state,
        )?;

        // Phase 7: assemble the block
        let (transactions, senders): (Vec<_>, Vec<_>) =
            committed_txs.into_iter().map(|tx| tx.into_parts()).unzip();

        let block = self.evm_config.block_assembler().assemble_block(BlockAssemblerInput::<
            <RaylsEvmConfig as ConfigureEvm>::BlockExecutorFactory,
        >::new(
            evm_env,
            ctx_for_assembler,
            &parent_header,
            transactions,
            &execution_result,
            &db.bundle_state,
            &*state_provider,
            state_root,
        ))?;
        let block = RecoveredBlock::new_unhashed(block, senders);
        let finish_elapsed = finish_start.elapsed();

        let block_num: u64 = block.number();
        info!(
            target: "engine",
            block_num,
            ?batch_digest,
            tier = tier_label,
            local_chain_len = local_chain.len(),
            ancestor_ms = %ancestor_elapsed.as_millis(),
            finish_ms = %finish_elapsed.as_millis(),
            "block built"
        );
        let execution_output =
            BlockExecutionOutput { result: execution_result, state: db.take_bundle() };
        let trie_updates_sorted = Arc::new(trie_updates.into_sorted());

        // compute trie changesets (old node values before this block) and cache
        // them for the parallel state root overlay. This is cheap — just cursor
        // reads of old trie node values, no state root recomputation.
        {
            if let Ok(db_provider) = self.blockchain_provider.database_provider_ro() {
                let db_cursor_factory = DatabaseTrieCursorFactory::new(db_provider.tx_ref());
                let cursor_factory =
                    InMemoryTrieCursorFactory::new(db_cursor_factory, &ancestor_sorted_nodes);
                if let Ok(changesets) =
                    compute_trie_changesets(&cursor_factory, &trie_updates_sorted)
                {
                    self.changeset_cache.insert(block.hash(), block.number(), Arc::new(changesets));
                }
            }
        }

        let trie_data = ComputedTrieData::without_trie_input(
            Arc::new(hashed_state.into_sorted()),
            trie_updates_sorted,
        );
        let res: ExecutedBlock<RaylsPrimitives> =
            ExecutedBlock::new(Arc::new(block), Arc::new(execution_output), trie_data);

        // Insert block into CanonicalInMemoryState immediately so spawn_sparse_trie_task() can
        // resolve parent state for the next block via state_by_block_hash().
        // The canonical head update and notification still happen in
        // finish_executing_output().
        self.canonical_in_memory_state()
            .update_chain(NewCanonicalChain::Commit { new: vec![res.clone()] });

        // Populate PayloadProcessor's cross-block ExecutionCache with this
        // block's BundleState so the next block's spawn() starts with warm
        // account/storage/bytecode caches instead of hitting MDBX.
        {
            let block_hash = res.recovered_block.hash();
            let block_with_parent = BlockWithParent::new(
                parent_header.hash(),
                BlockNumHash::new(block_num, block_hash),
            );
            self.payload_processor
                .lock()
                .on_inserted_executed_block(block_with_parent, &res.execution_output.state);
        }

        Ok((res, validation_counts))
    }

    /// Spawn sparse trie background tasks via [`PayloadProcessor`].
    ///
    /// Return the state_hook (to attach to the block executor) and a
    /// type-erased closure that stops prewarming and blocks on the
    /// sparse trie result. The closure is called after `executor.finish()`
    /// drops the hook, which signals the sparse trie to finalize.
    fn spawn_sparse_trie_task(
        &self,
        parent_hash: B256,
        parent_state_root: B256,
        recovered_txs: Vec<Recovered<TransactionSigned>>,
        trie_input: Arc<TrieInput>,
        sorted_state: Arc<HashedPostStateSorted>,
        sorted_nodes: Arc<TrieUpdatesSorted>,
    ) -> RaylsRethResult<(Box<dyn reth_evm::OnStateHook>, SparseRootFn)> {
        let provider_builder = StateProviderBuilder::new(
            self.blockchain_provider.clone(),
            parent_hash,
            None, // ancestor data provided via overlay_factory instead
        );

        let transaction_count = recovered_txs.len();
        let env = ExecutionEnv {
            evm_env: Default::default(),
            hash: B256::ZERO,
            parent_hash,
            parent_state_root,
            transaction_count,
            withdrawals: None,
        };

        let txs: (
            Vec<Recovered<TransactionSigned>>,
            fn(
                Recovered<TransactionSigned>,
            ) -> Result<Recovered<TransactionSigned>, core::convert::Infallible>,
        ) = (recovered_txs, Ok);

        let overlay_factory = OverlayStateProviderFactory::new(
            self.blockchain_provider.clone(),
            self.changeset_cache.clone(),
        )
        .with_lazy_overlay(if trie_input.state.is_empty() && trie_input.nodes.is_empty() {
            None
        } else {
            let computed = ComputedTrieData::without_trie_input(sorted_state, sorted_nodes);
            Some(LazyOverlay::new(parent_hash, vec![DeferredTrieData::ready(computed)]))
        });

        let mut processor = self.payload_processor.lock();
        let mut handle =
            processor.spawn(env, txs, provider_builder, overlay_factory, &self.tree_config, None);
        drop(processor); // release the mutex before computation

        // extract state_hook before capturing handle in the closure
        let state_hook: Box<dyn reth_evm::OnStateHook> = Box::new(handle.state_hook());

        // type-erased closure that stops prewarming, then blocks on the sparse
        // trie result, catching panics to allow Tier 2 fallback
        let sparse_root_fn: SparseRootFn = Box::new(move || {
            handle.stop_prewarming_execution();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handle
                    .state_root()
                    .map(|outcome| (outcome.state_root, outcome.trie_updates))
                    .map_err(|e| format!("{e}"))
            }))
            .unwrap_or_else(|panic| Err(format!("sparse trie task panicked: {panic:?}")))
        });

        debug!(target: "engine", %parent_hash, "Tier 1: spawned sparse trie task");
        Ok((state_hook, sparse_root_fn))
    }

    /// Set canonical head and broadcast chain notifications.
    ///
    /// Blocks are already in [`CanonicalInMemoryState`] (inserted per-block in
    /// `build_block_from_batch_payload` so `spawn_sparse_trie_task` can resolve
    /// parent state). This method updates the canonical head pointer and
    /// notifies subscribers.
    pub fn finish_executing_output(&self, blocks: Vec<ExecutedBlock>) -> RaylsRethResult<()> {
        let canonical_in_memory_state = self.canonical_in_memory_state();

        // Extract info from the last block before moving
        let last = blocks.last().expect("finish_executing_output called with empty blocks");
        let sealed_head = last.recovered_block.clone_sealed_header();
        let tx_count = last.recovered_block.transaction_count();

        info!(
            target: "engine",
            first=?blocks.first().map(|b| b.recovered_block.num_hash()),
            last=?blocks.last().map(|b| b.recovered_block.num_hash()),
            count = blocks.len(),
            "finalizing output — updating head + notifications",
        );

        // Build notification from blocks (cheap — blocks contain Arcs).
        // Blocks are already in CIM from build_block_from_batch_payload.
        let notification = NewCanonicalChain::Commit { new: blocks }.to_chain_notification();
        canonical_in_memory_state.set_canonical_head(sealed_head.clone());

        let (epoch, round) =
            Self::deconstruct_nonce(<FixedBytes<8> as Into<u64>>::into(sealed_head.nonce));
        info!(
            target: "engine",
            "canonical head for epoch {:?} round {:?}: {:?} - {:?}, txs: {:?}",
            epoch,
            round,
            sealed_head.number,
            sealed_head.hash(),
            tx_count,
        );

        // Broadcast canonical update (txpool maintenance subscribes to these events).
        canonical_in_memory_state.notify_canon_state(notification);

        Ok(())
    }

    /// Helper to deconstruct block nonce into epoch and round.
    ///
    /// Delegates to the canonical codec in `rayls_infrastructure_types::nonce` so the encode side
    /// (`Header::nonce`) and every decode site share one definition of the layout.
    pub fn deconstruct_nonce(nonce: u64) -> (u32, u32) {
        rayls_infrastructure_types::nonce::unpack_nonce(nonce)
    }

    /// Get the epoch and round from the current execution tip.
    ///
    /// Returns (epoch, round, block_number) tuple.
    /// Returns (0, 0, 0) if no blocks have been executed yet.
    pub fn execution_tip_epoch_round(&self) -> RaylsRethResult<(u32, u32, u64)> {
        let head = self.lookup_head()?;
        let nonce: u64 = head.nonce.into();
        let (epoch, round) = Self::deconstruct_nonce(nonce);
        Ok((epoch, round, head.number))
    }
}
