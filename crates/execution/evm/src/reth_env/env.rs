use crate::{
    evm::RaylsEvmConfig, persistence, reth_env::types::SharedPayloadProcessor, traits::RaylsNode,
};
use rayls_infrastructure_types::{BlockNumber, TaskSpawner};
use reth::builder::NodeConfig;
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_engine_primitives::TreeConfig;
use reth_engine_tree::persistence::PersistenceHandle as RethPersistenceHandle;
use reth_ethereum_primitives::EthPrimitives;
use reth_provider::{providers::BlockchainProvider, ProviderFactory};
use reth_trie::TrieInput;
#[cfg(feature = "archive-replay")]
use reth_trie::{updates::TrieUpdatesSorted, HashedPostStateSorted};
use reth_trie_db::ChangesetCache;
use std::sync::Arc;

/// This is a wrapped abstraction around Reth.
///
/// It should allow the rayls app to access the required functionality without
/// leaking Reth internals all over the codebase (this makes staying up to date
/// VERY time consuming).
#[derive(Clone)]
pub struct RethEnv {
    /// The type that holds all information needed to launch the node's engine.
    ///
    /// The [NodeConfig] is reth-specific and holds many helper functions that
    /// help Rayls stay in-sync with the Ethereum community.
    pub(crate) node_config: NodeConfig<RethChainSpec>,
    /// Type that fetches data from the database.
    pub(crate) blockchain_provider: BlockchainProvider<RaylsNode>,
    /// Provider factory for direct storage operations such as pipeline unwind.
    pub(crate) provider_factory: ProviderFactory<RaylsNode>,
    /// The type to configure the EVM for execution.
    pub(crate) evm_config: RaylsEvmConfig,
    /// The type to spawn tasks.
    pub(crate) task_spawner: TaskSpawner,
    /// Handle for sending blocks to reth's background persistence service.
    pub(crate) persistence_handle: RethPersistenceHandle<EthPrimitives>,
    /// Shared state tracking pending persistence operations.
    ///
    /// Lock ordering: acquire `persistence_state` BEFORE `ancestor_trie_cache`
    /// to prevent deadlocks (see `cached_ancestor_trie_input`).
    pub(crate) persistence_state: Arc<parking_lot::Mutex<persistence::PersistenceState>>,
    /// Concurrent payload processor for sparse trie state root computation.
    pub(crate) payload_processor: SharedPayloadProcessor,
    /// Engine tree configuration for state root task parameters.
    pub(crate) tree_config: TreeConfig,
    /// Cached ancestor trie input keyed by (last_persisted, canonical_head).
    ///
    /// Within a single consensus round multiple blocks share the same ancestor
    /// chain. We cache the (expensive) trie input computation and extend it
    /// incrementally when new blocks are added to the chain. Full recomputation
    /// only occurs when `last_persisted` changes (persistence completed).
    ///
    /// The `Arc<TrieInput>` allows cheap sharing with providers and the sparse
    /// trie task. `Arc::make_mut` is used for zero-cost in-place extension
    /// when the refcount is 1 (the common case, since blocks are built
    /// sequentially).
    ///
    /// Lock ordering: acquire `persistence_state` BEFORE this lock.
    pub(crate) ancestor_trie_cache:
        Arc<parking_lot::Mutex<Option<(BlockNumber, BlockNumber, Arc<TrieInput>)>>>,
    /// Shared changeset cache for parallel state root computation.
    ///
    /// Accumulates per-block trie changesets (old node values) so that
    /// `OverlayStateProviderFactory` can reconstruct the correct trie state
    /// even when the DB is stale from deferred persistence.
    pub(crate) changeset_cache: ChangesetCache,
    /// Canonical state-root oracle, populated only for archive replay.
    ///
    /// Returns the snapshot's authoritative state root for a block number so a
    /// divergent incremental root (a re-execution artifact) can be re-derived
    /// from leaves. Empty on the live/Observer path, where the heal never runs.
    /// Settable at most once: a second `set` is silently ignored by `OnceLock`.
    #[cfg(feature = "archive-replay")]
    pub(crate) canonical_root_oracle:
        Arc<std::sync::OnceLock<crate::reth_env::solver::CanonicalRootOracle>>,
    /// Memoized sorted base ancestor overlay for archive replay, keyed by
    /// `(last_persisted, canonical_head)`, so blocks in an output group reuse one
    /// sort and merge only their delta. See [`RethEnv::cached_sorted_ancestor_input`].
    ///
    /// Lock ordering: acquire `persistence_state` before this lock.
    #[cfg(feature = "archive-replay")]
    pub(crate) ancestor_sorted_cache: Arc<
        parking_lot::Mutex<
            Option<(BlockNumber, BlockNumber, Arc<HashedPostStateSorted>, Arc<TrieUpdatesSorted>)>,
        >,
    >,
}

impl std::fmt::Debug for RethEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RethEnv, config: {:?}", self.node_config)
    }
}
