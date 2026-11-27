use crate::{
    error::{RaylsRethError, RaylsRethResult},
    reth_env::RethEnv,
};
use alloy::consensus::BlockHeader as _;
use rayls_infrastructure_types::SealedHeader;
use reth_chain_state::{CanonicalInMemoryState, ExecutedBlock};
use reth_engine_tree::persistence::PersistenceResult;
use reth_provider::CanonChainTracker;
use reth_trie::TrieInput;
use std::sync::Arc;
use tracing::{debug, error, info, trace, warn};

impl RethEnv {
    /// Finalize block (header) executed from consensus output and update chain info.
    ///
    /// Updates in-memory finalized/safe state and triggers deferred persistence
    /// when the canonical head exceeds `last_persisted + threshold` blocks —
    /// matching reth's engine tree `should_persist()` logic.
    pub fn finalize_block(&self, header: SealedHeader) -> RaylsRethResult<()> {
        let mut state = self.persistence_state.lock();
        state.latest_finalized_number = header.number;
        if let Err(e) =
            self.persistence_handle.save_finalized_block_number(state.latest_finalized_number)
        {
            error!(target: "persistence", %e, "failed to send finalized number to persistence service");
        }
        self.blockchain_provider.set_finalized(header.clone());

        state.latest_safe_number = header.number;
        if let Err(e) = self.persistence_handle.save_safe_block_number(state.latest_safe_number) {
            error!(target: "persistence", %e, "failed to send safe number to persistence service");
        }
        self.blockchain_provider.set_safe(header.clone());

        // persist when canonical head is far enough ahead of last persisted block
        // and no persistence is already in-flight (matching reth's advance_persistence)
        if !state.in_progress() && state.should_persist(header.number) {
            let blocks = Self::get_canonical_blocks_to_persist(
                &self.canonical_in_memory_state(),
                state.last_persisted_block.number,
            );
            if !blocks.is_empty() {
                let count = blocks.len();
                let (tx, rx) = crossbeam_channel::bounded(1);
                self.persistence_handle
                    .save_blocks(blocks, tx)
                    .expect("periodic persistence service dead — cannot send blocks to MDBX");
                trace!(
                    target: "persistence",
                    count,
                    last_persisted = state.last_persisted_block.number,
                    canonical_head = header.number,
                    "periodic persist triggered"
                );
                state.pending_rx = Some(rx);
                state.pending_started_at = Some(std::time::Instant::now());
            }
        }

        Ok(())
    }

    /// Poll the persistence thread for completion and clean up in-memory state.
    ///
    /// Non-blocking: if persistence is still in progress, returns immediately.
    pub fn check_persistence_completion(&self) {
        let mut state = self.persistence_state.lock();
        if let Some(rx) = state.pending_rx.as_mut() {
            match rx.try_recv() {
                Ok(PersistenceResult { last_block: Some(persisted), .. }) => {
                    let elapsed = state.pending_started_at.map(|t| t.elapsed());
                    info!(
                        target: "persistence",
                        ?persisted,
                        ?elapsed,
                        "periodic persist completed"
                    );
                    self.canonical_in_memory_state().remove_persisted_blocks(persisted);
                    self.changeset_cache.evict(persisted.number);
                    state.last_persisted_block = persisted;
                    state.pending_rx = None;
                    state.pending_started_at = None;
                }
                Ok(PersistenceResult { last_block: None, .. }) => {
                    let elapsed = state.pending_started_at.map(|t| t.elapsed());
                    info!(
                        target: "persistence",
                        ?elapsed,
                        "periodic persist completed with no blocks"
                    );
                    state.pending_rx = None;
                    state.pending_started_at = None;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // still in progress
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    error!(target: "persistence", "persistence channel closed unexpectedly");
                    state.pending_rx = None;
                    state.pending_started_at = None;
                }
            }
        }
    }

    /// Extract canonical blocks from in-memory state that need to be persisted.
    ///
    /// Iterates by block number from `last_persisted + 1` to the canonical
    /// head, using `state_by_number()` to look up each block. Blocks
    /// inserted via batch `update_chain()` DO have parent links, but we
    /// iterate by number for consistency with the persistence range.
    fn get_canonical_blocks_to_persist(
        canonical_in_memory_state: &CanonicalInMemoryState,
        last_persisted_block_number: u64,
    ) -> Vec<ExecutedBlock> {
        let canonical_head = canonical_in_memory_state.get_canonical_block_number();
        let count = canonical_head.saturating_sub(last_persisted_block_number) as usize;
        let mut blocks_to_persist = Vec::with_capacity(count);

        for number in (last_persisted_block_number + 1)..=canonical_head {
            if let Some(block_state) = canonical_in_memory_state.state_by_number(number) {
                blocks_to_persist.push(block_state.block());
            } else {
                warn!(
                    target: "persistence",
                    number,
                    "block missing from in-memory state during persist extraction"
                );
            }
        }

        debug!(
            target: "persistence",
            count = blocks_to_persist.len(),
            last_persisted_block_number,
            canonical_head,
            "extracted blocks to persist from canonical in-memory state",
        );
        blocks_to_persist.sort_unstable_by_key(|b| b.recovered_block.number());
        blocks_to_persist
    }

    /// Return cached ancestor trie input, recomputing only when `last_persisted`
    /// changes and extending incrementally when new blocks are appended.
    ///
    /// Within a single consensus round several blocks are built in sequence.
    /// Each block advances `canonical_head`, but the set of unpersisted
    /// ancestors only grows by one block. Instead of recomputing the full
    /// `TrieInput` from scratch on every head change, we:
    ///
    /// 1. **Exact hit** — return an `Arc::clone` (zero-cost).
    /// 2. **Incremental extend** — append only the new blocks' state via `Arc::make_mut` (zero-cost
    ///    when refcount is 1, COW otherwise).
    /// 3. **Full recompute** — only when `last_persisted` changes (rare).
    pub(super) fn cached_ancestor_trie_input(&self) -> Arc<TrieInput> {
        let last_persisted = self.persistence_state.lock().last_persisted_block.number;
        let canonical_in_memory_state = self.canonical_in_memory_state();
        let canonical_head = canonical_in_memory_state.get_canonical_block_number();
        let mut cache = self.ancestor_trie_cache.lock();

        if let Some((ref mut cached_persisted, ref mut cached_head, ref mut arc_input)) = *cache {
            if *cached_persisted == last_persisted {
                if *cached_head == canonical_head {
                    debug!(
                        target: "persistence",
                        canonical_head,
                        "reusing cached ancestor trie input",
                    );
                    return Arc::clone(arc_input);
                }
                if *cached_head < canonical_head {
                    // Incremental: extend with blocks added since last cache
                    // update. Arc::make_mut is zero-cost when refcount == 1 (the
                    // common case since blocks are built sequentially).
                    let new_blocks: Vec<_> = ((*cached_head + 1)..=canonical_head)
                        .filter_map(|n| {
                            canonical_in_memory_state.state_by_number(n).map(|bs| bs.block())
                        })
                        .collect();
                    let delta = new_blocks.len();
                    let input = Arc::make_mut(arc_input);
                    let sorted_data: Vec<_> =
                        new_blocks.iter().map(|b| (b.hashed_state(), b.trie_updates())).collect();
                    for (ref hs, ref tu) in &sorted_data {
                        input.state.extend_from_sorted(hs);
                        input.nodes.extend_from_sorted(tu);
                    }
                    debug!(
                        target: "persistence",
                        prev_head = *cached_head,
                        canonical_head,
                        delta,
                        "incrementally extended ancestor trie input",
                    );
                    *cached_head = canonical_head;
                    return Arc::clone(arc_input);
                }
            }
        }

        // Full recompute — first call or persistence changed the base.
        let input = Arc::new(self.compute_ancestor_trie_input());
        *cache = Some((last_persisted, canonical_head, Arc::clone(&input)));
        input
    }

    /// Build a [`TrieInput`] from all in-memory ancestor blocks that have not
    /// yet been persisted to the database.
    ///
    /// The state root computation reads
    /// existing trie nodes from the database via [`ConsistentDbView`].  With
    /// deferred persistence the database trie lags behind the canonical head,
    /// so the incremental root sees a stale base trie and only the current
    /// block's diff — producing a wrong (but deterministically wrong) state
    /// root.  The root stays *consistently* wrong across validators until the
    /// first async persistence flush completes on some validators but not
    /// others, at which point the base tries diverge and the network forks.
    ///
    /// This method collects `hashed_state` + `trie_updates` from every
    /// non-persisted block (oldest first) and packs them into a [`TrieInput`]
    /// that is prepended to the current block's input — matching reth's engine
    /// tree `compute_trie_input` pattern.
    pub(super) fn compute_ancestor_trie_input(&self) -> TrieInput {
        let last_persisted = self.persistence_state.lock().last_persisted_block.number;

        let cim = self.canonical_in_memory_state();
        let canonical_head = cim.get_canonical_block_number();

        // Collect block states so their Arc refs stay alive for the iterator.
        let ancestor_blocks: Vec<_> = ((last_persisted + 1)..=canonical_head)
            .filter_map(|n| cim.state_by_number(n).map(|bs| bs.block()))
            .collect();

        let sorted_data: Vec<_> =
            ancestor_blocks.iter().map(|b| (b.hashed_state(), b.trie_updates())).collect();
        let mut input = TrieInput::default();
        for (ref hs, ref tu) in &sorted_data {
            input.state.extend_from_sorted(hs);
            input.nodes.extend_from_sorted(tu);
        }

        debug!(
            target: "persistence",
            last_persisted,
            canonical_head,
            ancestor_count = ancestor_blocks.len(),
            "computed ancestor trie input for state root",
        );

        input
    }

    /// Flush all unpersisted blocks to disk.
    ///
    /// Used at epoch boundaries to ensure all blocks are persisted before
    /// clearing the consensus DB. Extracts blocks from [`CanonicalInMemoryState`]
    /// by walking the canonical chain down to `last_persisted_block`.
    pub async fn flush_persistence(&self) -> RaylsRethResult<()> {
        let flush_persistence_start = std::time::Instant::now();

        let persistence_state = self.persistence_state.clone();
        let persistence_handle = self.persistence_handle.clone();
        let canonical_in_memory_state = self.canonical_in_memory_state();
        let changeset_cache = self.changeset_cache.clone();

        let result: RaylsRethResult<()> = tokio::task::spawn_blocking(move || {
            let mut state = persistence_state.lock();

            if let Some(rx) = state.pending_rx.take() {
                match rx.recv() {
                    Ok(PersistenceResult { last_block: Some(persisted), .. }) => {
                        info!(target: "persistence", ?persisted, "in-flight persistence completed");
                        canonical_in_memory_state.remove_persisted_blocks(persisted);
                        changeset_cache.evict(persisted.number);
                        state.last_persisted_block = persisted;
                    }
                    Ok(PersistenceResult { last_block: None, .. }) => {
                        info!(target: "persistence", "in-flight persistence completed with no blocks");
                    }
                    Err(e) => {
                        warn!(target: "persistence", %e, "in-flight persistence channel dropped");
                    }
                }
                state.pending_started_at = None;
            }

            let blocks = RethEnv::get_canonical_blocks_to_persist(
                &canonical_in_memory_state,
                state.last_persisted_block.number,
            );

            let total_blocks_to_persist = blocks.len();
            if total_blocks_to_persist > 0 {
                let (tx, rx) = crossbeam_channel::bounded(1);
                persistence_handle.save_blocks(blocks, tx).map_err(|e| {
                    RaylsRethError::EVMCustom(format!(
                        "flush_persistence: persistence service unavailable: {e}"
                    ))
                })?;

                match rx.recv() {
                    Ok(PersistenceResult { last_block: Some(persisted), .. }) => {
                        info!(
                            target: "persistence",
                            ?persisted,
                            blocks = total_blocks_to_persist,
                            "flush blocks persisted"
                        );
                        canonical_in_memory_state.remove_persisted_blocks(persisted);
                        changeset_cache.evict(persisted.number);
                        state.last_persisted_block = persisted;
                    }
                    Ok(PersistenceResult { last_block: None, .. }) => {
                        info!(target: "persistence", "flush blocks empty");
                    }
                    Err(e) => {
                        return Err(RaylsRethError::EVMCustom(format!(
                            "flush_persistence: persistence channel dropped: {e}"
                        )));
                    }
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| {
            RaylsRethError::EVMCustom(format!("flush_persistence: spawn_blocking failed: {e}"))
        })?;

        result?;

        info!(target: "engine", elapsed=?flush_persistence_start.elapsed(), "flush_persistence completed");

        Ok(())
    }
}
