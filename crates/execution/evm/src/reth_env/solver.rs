//! From-leaves state-root re-derivation for archive replay.
//!
//! Re-execution can rebuild a same-rooted but non-canonical stored trie-node set,
//! so the incremental root miscomputes. Rebuilding from hashed leaves ignores
//! stored nodes and reproduces the canonical root.

use crate::{error::RaylsRethResult, reth_env::RethEnv};
use rayls_infrastructure_types::B256;
use reth_provider::{DatabaseProviderFactory, ProviderError};
use reth_trie::{
    hashed_cursor::HashedPostStateCursorFactory,
    trie_cursor::noop::NoopTrieCursorFactory,
    updates::{TrieUpdates, TrieUpdatesSorted},
    HashedPostState, HashedPostStateSorted, StateRoot,
};
use reth_trie_db::DatabaseHashedCursorFactory;
use std::sync::Arc;
use tracing::{debug, warn};

/// Per-block canonical state-root lookup for archive replay.
///
/// Maps a block number to the snapshot's authoritative state root, or `None`
/// when the snapshot has no header for it. Installed via
/// [`RethEnv::set_canonical_root_oracle`].
pub type CanonicalRootOracle = Box<dyn Fn(u64) -> Option<B256> + Send + Sync>;

impl RethEnv {
    /// Return the sorted base ancestor overlay (the non-persisted window), memoized by
    /// `(last_persisted, canonical_head)`. Mirrors [`Self::cached_ancestor_trie_input`] over
    /// sorted types in three tiers: (1) an exact hit clones the two cached `Arc`s; (2) a head
    /// advance merges the newly-canonical blocks' deltas with `extend_ref_and_sort` (a linear
    /// merge) instead of re-sorting the window; (3) a `last_persisted` change sorts the base
    /// overlay once.
    ///
    /// Lock ordering: `persistence_state` is read before the sorted-cache lock, and
    /// `cached_ancestor_trie_input` runs only on the recompute path with the lock released.
    pub(super) fn cached_sorted_ancestor_input(
        &self,
    ) -> (Arc<HashedPostStateSorted>, Arc<TrieUpdatesSorted>) {
        let last_persisted = self.persistence_state.lock().last_persisted_block.number;
        let cim = self.canonical_in_memory_state();
        let head = cim.get_canonical_block_number();

        // Tiers 1-2 under the cache lock. `cached_ancestor_trie_input` (which locks
        // `persistence_state` and `ancestor_trie_cache`) is deliberately NOT called
        // here, so the sorted-cache lock never sits above those in the hierarchy.
        {
            let mut cache = self.ancestor_sorted_cache.lock();
            if let Some((ref cached_persisted, ref mut cached_head, ref mut state, ref mut nodes)) =
                *cache
            {
                if *cached_persisted == last_persisted && *cached_head <= head {
                    // Tier 2: extend by the blocks canonicalized since the last
                    // update, oldest first so newer blocks override older keys.
                    if *cached_head < head {
                        let from = *cached_head + 1;
                        let sorted_state = Arc::make_mut(state);
                        let sorted_nodes = Arc::make_mut(nodes);
                        let mut delta = 0usize;
                        // a None from state_by_number silently skips that block's
                        // delta; safe only because every in-output-group block stays
                        // in CanonicalInMemoryState until the group is flushed
                        for n in from..=head {
                            if let Some(bs) = cim.state_by_number(n) {
                                let b = bs.block();
                                sorted_state.extend_ref_and_sort(&b.hashed_state());
                                sorted_nodes.extend_ref_and_sort(&b.trie_updates());
                                delta += 1;
                            }
                        }
                        *cached_head = head;
                        debug!(
                            target: "persistence",
                            prev_head = from - 1,
                            head,
                            delta,
                            "incrementally extended sorted ancestor input",
                        );
                    }
                    // Tier 1 falls through here too (cached_head == head: no extend).
                    return (Arc::clone(state), Arc::clone(nodes));
                }
            }
        }

        // Tier 3: sort the non-persisted overlay once (lock released above).
        let base = self.cached_ancestor_trie_input();
        let sorted_state = Arc::new(base.state.clone_into_sorted());
        let sorted_nodes = Arc::new(base.nodes.clone_into_sorted());
        *self.ancestor_sorted_cache.lock() =
            Some((last_persisted, head, Arc::clone(&sorted_state), Arc::clone(&sorted_nodes)));
        (sorted_state, sorted_nodes)
    }

    /// Re-derive a block's state root from leaves when the incremental root
    /// disagrees with the snapshot's canonical root.
    ///
    /// Re-execution rebuilds a same-rooted but non-canonical stored-node set
    /// (the live producer's node set was shaped by its own restart history);
    /// the incremental path trusts those nodes and can miscompute. A from-leaves
    /// rebuild ignores stored nodes and is history-independent. Returns the
    /// re-derived `(root, updates)` iff they match canonical, otherwise the
    /// original pair (with a warning) so a genuine content divergence surfaces.
    pub(super) fn heal_divergent_state_root(
        &self,
        block_number: u64,
        state_root: B256,
        trie_updates: TrieUpdates,
        block_hashed: &HashedPostState,
        ancestor_state: &HashedPostState,
    ) -> RaylsRethResult<(B256, TrieUpdates)> {
        let Some(expected) =
            self.canonical_root_oracle.get().and_then(|oracle| oracle(block_number))
        else {
            return Ok((state_root, trie_updates));
        };
        if expected == state_root {
            return Ok((state_root, trie_updates));
        }

        let (from_leaves_root, from_leaves_updates) =
            self.from_leaves_root_with_updates(ancestor_state, block_hashed)?;

        if from_leaves_root == expected {
            warn!(
                target: "engine",
                block = block_number,
                incremental = ?state_root,
                from_leaves = ?from_leaves_root,
                "incremental state root diverged on re-execution; re-derived from leaves"
            );
            Ok((from_leaves_root, from_leaves_updates))
        } else {
            warn!(
                target: "engine",
                block = block_number,
                incremental = ?state_root,
                from_leaves = ?from_leaves_root,
                expected = ?expected,
                "from-leaves re-derivation did not match canonical; possible content divergence"
            );
            Ok((state_root, trie_updates))
        }
    }

    /// Rebuild the state root and trie updates from the hashed leaves only,
    /// over the ancestor overlay merged with this block's hashed state.
    ///
    /// Uses a noop trie cursor so no stored/overlay trie node is reused; a stale
    /// stored node therefore cannot corrupt the result.
    fn from_leaves_root_with_updates(
        &self,
        ancestor_state: &HashedPostState,
        block_hashed: &HashedPostState,
    ) -> RaylsRethResult<(B256, TrieUpdates)> {
        let mut combined = ancestor_state.clone();
        combined.extend_ref(block_hashed);
        let combined_sorted = combined.into_sorted();
        let provider = self.blockchain_provider.database_provider_ro()?;
        let hashed_cf = HashedPostStateCursorFactory::new(
            DatabaseHashedCursorFactory::new(provider.tx_ref()),
            &combined_sorted,
        );
        let (root, updates) = StateRoot::new(NoopTrieCursorFactory::default(), hashed_cf)
            .root_with_updates()
            .map_err(ProviderError::from)?;
        Ok((root, updates))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        reth_env::RethEnv, system_calls::ConsensusRegistry, test_utils::TransactionFactory,
        ExecutedBlock,
    };
    use alloy::primitives::utils::parse_ether;
    use rand::{rngs::StdRng, SeedableRng as _};
    use rayls_infrastructure_config::NodeInfo;
    use rayls_infrastructure_types::{
        generate_proof_of_possession_bls, payload::RLPayload, Address, BlsKeypair, BlsSignature,
        Bytes, Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput, GenesisAccount,
        NodeP2pInfo, ReputationScores, SealedHeader, SignatureVerificationState, TaskManager, B256,
        U256,
    };
    use reth_chainspec::ChainSpec as RethChainSpec;
    use reth_primitives_traits::Account;
    use reth_trie::{HashedPostState, HashedStorage};
    use std::{sync::Arc, time::Duration};
    use tempfile::TempDir;

    /// Build an empty-batch consensus output for a given round/epoch/subdag.
    fn consensus_output(round: u32, epoch: u32, subdag_index: u64) -> ConsensusOutput {
        let mut leader = Certificate::default();
        leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(),
        ));
        leader.header_mut_for_test().created_at = rayls_infrastructure_types::now();
        leader.header.round = round;
        leader.header.epoch = epoch;
        ConsensusOutput {
            sub_dag: CommittedSubDag::new(
                vec![leader.clone(), Certificate::default()],
                leader,
                subdag_index,
                ReputationScores::default(),
                None,
            )
            .into(),
            close_epoch: false,
            batches: Default::default(),
            batch_digests: Default::default(),
            parent_hash: ConsensusHeader::default().digest(),
            number: subdag_index,
            extra: Default::default(),
        }
    }

    /// Build a block, advance the canonical head, but DO NOT persist or finalize.
    ///
    /// Leaves the block in `CanonicalInMemoryState` as a non-persisted ancestor
    /// so both `compute_ancestor_trie_input` and the cache see it.
    fn execute_and_advance_head(
        reth_env: &RethEnv,
        payload: RLPayload,
        transactions: Vec<Bytes>,
    ) -> eyre::Result<ExecutedBlock> {
        let (block, _counts) =
            reth_env.build_block_from_batch_payload(payload, &transactions, &[])?;
        reth_env.finish_executing_output(vec![block.clone()])?;
        Ok(block)
    }

    /// Construct the consensus-registry genesis chain spec plus a funded EOA.
    fn registry_chain_with_funded_eoa() -> eyre::Result<(Arc<RethChainSpec>, TransactionFactory)> {
        let eoa = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(77));
        let validators: Vec<_> = (0..4u64)
            .map(|i| {
                let addr = Address::from_slice(&[(i as u8 + 1) * 0x11; 20]);
                let mut rng = StdRng::seed_from_u64(i);
                let bls = BlsKeypair::generate(&mut rng);
                let pop = generate_proof_of_possession_bls(&bls, &addr).expect("pop");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls.public(),
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: addr,
                    proof_of_possession: pop,
                }
            })
            .collect();
        let owner = Address::from_slice(&[0xAA; 20]);
        let stake_cfg = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochDuration: 86_400,
        };
        let funded_genesis = rayls_infrastructure_types::test_genesis().extend_accounts([(
            eoa.address(),
            GenesisAccount::default().with_balance(U256::from(parse_ether("1_000_000").unwrap())),
        )]);
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators,
            funded_genesis,
            stake_cfg,
            owner,
            owner,
            vec![(owner, U256::from(parse_ether("1_000_000").unwrap()))],
        )?;
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        Ok((chain, eoa))
    }

    /// Build an adversarial hashed-state diff exercising a destroyed account, a
    /// storage wipe, and a normal account + slot update.
    fn adversarial_block_hashed() -> HashedPostState {
        let destroyed = B256::repeat_byte(0xDE);
        let wiped = B256::repeat_byte(0xC0);
        let updated = B256::repeat_byte(0xA1);
        let normal_account =
            Account { nonce: 7, balance: U256::from(123_456u64), bytecode_hash: None };
        HashedPostState::default()
            .with_accounts([(destroyed, None), (updated, Some(normal_account))])
            .with_storages([
                (wiped, HashedStorage::new(true)),
                (
                    updated,
                    HashedStorage::from_iter(false, [(B256::repeat_byte(0x01), U256::from(42u64))]),
                ),
            ])
    }

    /// The archive replay sorted-base optimization sorts the base overlay once
    /// and merges each per-block delta via `extend_ref_and_sort`. Assert that
    /// yields the same sorted state as sorting the full (base + delta) overlay
    /// from scratch, even with overlapping keys, a resurrected destroyed
    /// account, and merged storage. A divergence here would fork replay.
    #[test]
    fn archive_sorted_base_plus_delta_equals_full_sort() {
        let base = adversarial_block_hashed();

        // delta overlaps base keys so the "other wins" precedence is exercised
        let destroyed = B256::repeat_byte(0xDE);
        let updated = B256::repeat_byte(0xA1);
        let fresh = B256::repeat_byte(0xF5);
        let reborn = Account { nonce: 9, balance: U256::from(999u64), bytecode_hash: None };
        let delta = HashedPostState::default()
            .with_accounts([(destroyed, Some(reborn)), (updated, None), (fresh, Some(reborn))])
            .with_storages([(
                updated,
                HashedStorage::from_iter(
                    false,
                    [
                        (B256::repeat_byte(0x01), U256::from(7u64)),
                        (B256::repeat_byte(0x02), U256::from(8u64)),
                    ],
                ),
            )]);

        // incremental: sort the base once, then merge the sorted delta on top
        let mut incremental = base.clone_into_sorted();
        incremental.extend_ref_and_sort(&delta.clone_into_sorted());

        // from-scratch: extend the unsorted overlay then sort (the live formula)
        let from_scratch = {
            let mut combined = base.clone();
            combined.extend_ref(&delta);
            combined.clone_into_sorted()
        };

        assert_eq!(
            incremental, from_scratch,
            "archive merged sorted state must equal the full from-scratch sort"
        );
    }

    /// Assert `cached_sorted_ancestor_input`'s incremental extend (tier 2) stays
    /// byte-identical to a full from-scratch re-sort across head advances; a
    /// divergence would fork archive replay.
    #[tokio::test]
    async fn cached_sorted_ancestor_input_incremental_extend_matches_full_sort() -> eyre::Result<()>
    {
        let (chain, mut eoa) = registry_chain_with_funded_eoa()?;
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("sorted-incremental-test");
        let reth_env = tokio::time::timeout(
            Duration::from_secs(60),
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None),
        )
        .await??;

        let sink = Address::from_slice(&[0x42; 20]);
        let mut parent: SealedHeader = chain.sealed_genesis_header();

        // Each iteration advances the head by one block, then calls the cache:
        // round 1 is a tier-3 miss; rounds 2..=4 hit tier-2 (cached_head < head).
        for round in 1..=4u32 {
            let tx = eoa.create_eip1559_encoded(
                chain.clone(),
                None,
                100,
                Some(sink),
                U256::from(1_000u64),
                Default::default(),
            );
            let output = consensus_output(round, 0, round as u64);
            let payload = RLPayload::new_for_test(parent.clone(), &output);
            let block = execute_and_advance_head(&reth_env, payload, vec![tx])?;
            parent = block.recovered_block.clone_sealed_header();

            let (cached_state, cached_nodes) = reth_env.cached_sorted_ancestor_input();

            // ground truth: full sort of the current unsorted non-persisted overlay
            let base = reth_env.cached_ancestor_trie_input();
            let expected_state = base.state.clone_into_sorted();
            let expected_nodes = base.nodes.clone_into_sorted();

            assert_eq!(*cached_state, expected_state, "round {round}: sorted state diverged");
            assert_eq!(*cached_nodes, expected_nodes, "round {round}: sorted nodes diverged");
        }

        Ok(())
    }

    /// Assert the from-leaves heal output is byte-identical whether the ancestor
    /// state is sourced from a fresh CIM walk (current heal) or the cached input
    /// (proposed optimization), across empty and non-empty local-chain shapes.
    #[tokio::test]
    async fn heal_ancestor_state_equivalence_with_destroyed_account_and_wipe() -> eyre::Result<()> {
        let (chain, mut eoa) = registry_chain_with_funded_eoa()?;
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("solver-equivalence-test");
        let reth_env = tokio::time::timeout(
            Duration::from_secs(60),
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None),
        )
        .await??;

        // sink address for value transfers so each block carries real state diffs
        let sink = Address::from_slice(&[0x42; 20]);

        // build >=3 non-persisted ancestor blocks, advancing the head each time so
        // the cache exercises full-recompute then incremental-extend
        let mut parent: SealedHeader = chain.sealed_genesis_header();
        for round in 1..=3u32 {
            let tx = eoa.create_eip1559_encoded(
                chain.clone(),
                None,
                100,
                Some(sink),
                U256::from(1_000u64),
                Default::default(),
            );
            let output = consensus_output(round, 0, round as u64);
            let payload = RLPayload::new_for_test(parent.clone(), &output);
            let block = execute_and_advance_head(&reth_env, payload, vec![tx])?;
            parent = block.recovered_block.clone_sealed_header();
        }

        let block_hashed = adversarial_block_hashed();

        // sub-case (a): empty local_chain
        {
            let local_chain: Vec<ExecutedBlock> = Vec::new();
            let mut fresh = reth_env.compute_ancestor_trie_input();
            for b in &local_chain {
                fresh.state.extend_from_sorted(&b.hashed_state());
            }
            let (root_a, upd_a) =
                reth_env.from_leaves_root_with_updates(&fresh.state, &block_hashed)?;

            let base = reth_env.cached_ancestor_trie_input();
            let mut extended = (*base).clone();
            for b in &local_chain {
                extended.state.extend_from_sorted(&b.hashed_state());
                extended.nodes.extend_from_sorted(&b.trie_updates());
            }
            let (root_b, upd_b) =
                reth_env.from_leaves_root_with_updates(&extended.state, &block_hashed)?;

            assert_eq!(root_a, root_b, "empty local_chain: roots diverge");
            assert_eq!(upd_a, upd_b, "empty local_chain: trie updates diverge");
        }

        // sub-case (b): non-empty local_chain - tail blocks built WITHOUT
        // advancing the head, each passing prior tail blocks as local_chain.
        {
            let mut local_chain: Vec<ExecutedBlock> = Vec::new();
            let mut tail_parent = parent.clone();
            for round in 4..=5u32 {
                let tx = eoa.create_eip1559_encoded(
                    chain.clone(),
                    None,
                    100,
                    Some(sink),
                    U256::from(2_000u64),
                    Default::default(),
                );
                let output = consensus_output(round, 0, round as u64);
                let payload = RLPayload::new_for_test(tail_parent.clone(), &output);
                let (block, _counts) =
                    reth_env.build_block_from_batch_payload(payload, &[tx], &local_chain)?;
                tail_parent = block.recovered_block.clone_sealed_header();
                local_chain.push(block);
            }

            let mut fresh = reth_env.compute_ancestor_trie_input();
            for b in &local_chain {
                fresh.state.extend_from_sorted(&b.hashed_state());
            }
            let (root_a, upd_a) =
                reth_env.from_leaves_root_with_updates(&fresh.state, &block_hashed)?;

            let base = reth_env.cached_ancestor_trie_input();
            let mut extended = (*base).clone();
            for b in &local_chain {
                extended.state.extend_from_sorted(&b.hashed_state());
                extended.nodes.extend_from_sorted(&b.trie_updates());
            }
            let (root_b, upd_b) =
                reth_env.from_leaves_root_with_updates(&extended.state, &block_hashed)?;

            assert_eq!(root_a, root_b, "non-empty local_chain: roots diverge");
            assert_eq!(upd_a, upd_b, "non-empty local_chain: trie updates diverge");
        }

        Ok(())
    }
}
