use crate::{
    evm::{initialize_erc20_precompile, RaylsEvmConfig},
    native_erc20::{Erc20Precompile, Erc20TokenConfig, ERC20_PRECOMPILE_ADDRESS},
    persistence,
    reth_env::{types::set_basefee_address, RethConfig, RethDb, RethEnv},
    traits::RaylsNode,
    RaylsChainSpec,
};
use rayls_infrastructure_types::{
    Address, BuildMetadata, RaylsNetwork, TaskManager, TaskSpawner, B256,
};
use rayls_middleware_rewards::RewardsCounter;
use reth::{args::DatadirArgs, builder::NodeConfig, dirs::MaybePlatformPath};
use reth_chainspec::{ChainSpec as RethChainSpec, EthChainSpec};
use reth_config::config::StageConfig;
use reth_consensus::noop::NoopConsensus;
use reth_db::{init_db, DatabaseEnv};
use reth_db_common::init::init_genesis_with_settings;
use reth_downloaders::{bodies::noop::NoopBodiesDownloader, headers::noop::NoopHeaderDownloader};
use reth_engine_primitives::DEFAULT_PERSISTENCE_THRESHOLD;
use reth_engine_tree::tree::{precompile_cache::PrecompileCacheMap, PayloadProcessor};
use reth_node_core::args::{EngineArgs, StorageArgs};
use reth_provider::{
    providers::{BlockchainProvider, RocksDBBuilder, StaticFileProvider},
    BlockNumReader, ChainSpecProvider, DatabaseProviderFactory, ProviderFactory,
    RocksDBProviderFactory, StorageSettingsCache,
};
use reth_prune_types::PruneModes;
use reth_stages::{sets::DefaultStages, PipelineBuilder, PipelineTarget};
use reth_static_file::StaticFileProducer;
use reth_trie_db::ChangesetCache;
use std::{path::Path, sync::Arc};
use tokio::sync::{oneshot, watch};
use tracing::{debug, error, info, warn};

impl RethEnv {
    /// Create a new Reth DB.
    /// Break this out so this can be created upfront and used even on a
    /// restart (when catching up for instance).
    pub fn new_database<P: AsRef<Path>>(
        reth_config: &RethConfig,
        db_path: P,
    ) -> eyre::Result<RethDb> {
        let db_path = db_path.as_ref();
        info!(target: "rayls::reth", path = ?db_path, "opening database");
        Ok(Arc::new(init_db(db_path, reth_config.0.db.database_args())?))
    }

    /// Produce a new wrapped Reth environment from a config, DB path and task manager.
    ///
    /// This method MUST be called from within a tokio runtime.
    /// It is async to support pipeline-based unwind if database inconsistency is detected.
    pub async fn new(
        reth_config: &RethConfig,
        task_manager: &TaskManager,
        database: RethDb,
        basefee_address: Option<Address>,
        rewards_counter: RewardsCounter,
        build_metadata: &BuildMetadata,
        network: Option<RaylsNetwork>,
        min_base_fee: Option<u64>,
    ) -> eyre::Result<Self> {
        let node_config = reth_config.0.clone();
        let mut builder = RaylsChainSpec::builder(Arc::clone(&node_config.chain));
        if let Some(network) = network {
            builder = builder.rayls_hardforks(network);
        }
        if let Some(min_fee) = min_base_fee {
            builder = builder.min_base_fee(min_fee);
        }
        let chain_spec = Arc::new(builder.build());
        let evm_config = RaylsEvmConfig::new(chain_spec.clone(), rewards_counter.clone());
        let task_spawner = task_manager.get_spawner();
        let runtime = reth_tasks::Runtime::with_existing_handle(tokio::runtime::Handle::current())?;
        let provider_factory = Self::init_provider_factory(
            &node_config,
            chain_spec,
            database.clone(),
            &task_spawner,
            runtime.clone(),
            rewards_counter,
        )
        .await?;
        let blockchain_provider = BlockchainProvider::new(provider_factory.clone())?;
        set_basefee_address(basefee_address);

        // Initialize the Native ERC-20 precompile with chain configuration
        let chain_id = node_config.chain.chain_id();
        let erc20_precompile =
            Erc20Precompile::new(Erc20TokenConfig::default(), ERC20_PRECOMPILE_ADDRESS, chain_id);
        if initialize_erc20_precompile(erc20_precompile).is_err() {
            debug!(target: "rayls::execution", "Native ERC-20 precompile already initialized");
        } else {
            info!(target: "rayls::execution", address=?ERC20_PRECOMPILE_ADDRESS, %chain_id, "Initialized Native ERC-20 precompile");
        }

        // initialize deferred persistence
        let last_persisted = blockchain_provider.database_provider_ro()?.best_block_number()?;

        let (persistence_handle, _sync_metrics_tx) =
            persistence::spawn_persistence(provider_factory.clone(), node_config.prune_config());
        let persistence_state =
            Arc::new(parking_lot::Mutex::new(persistence::PersistenceState::new(
                last_persisted,
                node_config.engine.persistence_threshold,
                database.clone(),
            )));

        // start reth execution-layer metrics on a separate endpoint
        if let Some(reth_metrics_socket) = node_config.metrics.prometheus {
            rayls_execution_metrics::start_reth_metrics_server(
                reth_metrics_socket,
                runtime.clone(),
                &provider_factory,
                node_config.datadir().pprof_dumps(),
                "Axyl",
                build_metadata,
            )
            .await?;
        }

        // Build TreeConfig from EngineArgs (converts cross_block_cache_size MB → bytes).
        let tree_config = node_config.engine.tree_config();

        // Construct the payload processor for concurrent state root computation.
        let payload_processor = Arc::new(parking_lot::Mutex::new(PayloadProcessor::new(
            runtime.clone(),
            evm_config.clone(),
            &tree_config,
            PrecompileCacheMap::default(),
        )));

        Ok(Self {
            node_config,
            blockchain_provider,
            #[cfg(feature = "archive-replay")]
            provider_factory,
            evm_config,
            task_spawner,
            persistence_handle,
            persistence_state,
            payload_processor,
            tree_config,
            ancestor_trie_cache: Arc::new(parking_lot::Mutex::new(None)),
            changeset_cache: ChangesetCache::new(),
            #[cfg(feature = "archive-replay")]
            canonical_root_oracle: Arc::new(std::sync::OnceLock::new()),
            #[cfg(feature = "archive-replay")]
            ancestor_sorted_cache: Arc::new(parking_lot::Mutex::new(None)),
        })
    }

    /// Create a new temp RethEnv using a specified chain spec.
    pub async fn new_for_temp_chain<P: AsRef<Path>>(
        chain: Arc<RethChainSpec>,
        db_path: P,
        task_manager: &TaskManager,
        rewards: Option<RewardsCounter>,
    ) -> eyre::Result<Self> {
        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(db_path.as_ref().to_path_buf()),
                // default static path should resolve to: `DEFAULT_ROOT_DIR/<CHAIN_ID>/static_files`
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            ..NodeConfig::default()
        };
        let reth_config = RethConfig(node_config);
        let database = Self::new_database(&reth_config, db_path)?;
        Self::new(
            &reth_config,
            task_manager,
            database,
            None,
            rewards.unwrap_or_default(),
            &BuildMetadata::default(),
            None,
            None,
        )
        .await
    }

    /// Create a RethEnv tailored for archive-replay workloads, anchored at a
    /// rayls datadir. Mirrors [`Self::new_for_temp_chain`] but applies the
    /// network's hardfork schedule, the same `basefee_address` + `min_base_fee`
    /// the production node uses, and selects the v2 storage layout
    /// (`static_files/` + RocksDB) so the rebuilt archive is bit-compatible
    /// with snapshots produced by nodes running `--storage.v2`. Pruning is
    /// DISABLED (default `NodeConfig::default()` has `prune_config() == None`),
    /// producing a full archive.
    ///
    /// `rayls_datadir` is the rayls root, holding `db/` + `static_files/` +
    /// `rocksdb/` as siblings (matching the standard node's layout).
    #[cfg(feature = "archive-replay")]
    pub async fn new_for_archive_replay<P: AsRef<Path>>(
        chain: Arc<RethChainSpec>,
        rayls_datadir: P,
        task_manager: &TaskManager,
        network: RaylsNetwork,
        basefee_address: Option<Address>,
        min_base_fee: Option<u64>,
        storage_v2: bool,
        persistence_threshold: Option<u64>,
        rewards_counter: RewardsCounter,
    ) -> eyre::Result<Self> {
        let rayls_datadir = rayls_datadir.as_ref();
        let db_path = rayls_datadir.join("db");
        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(rayls_datadir.to_path_buf()),
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            storage: StorageArgs { v2: storage_v2 },
            engine: EngineArgs {
                persistence_threshold: persistence_threshold
                    .unwrap_or(DEFAULT_PERSISTENCE_THRESHOLD),
                // sequential replay re-executes the same txs microseconds later on
                // the same caches, so speculative prewarming is wasted work
                prewarming_disabled: true,
                ..Default::default()
            },
            ..NodeConfig::default()
        };
        let reth_config = RethConfig(node_config);
        let database = Self::new_database(&reth_config, &db_path)?;
        Self::new(
            &reth_config,
            task_manager,
            database,
            basefee_address,
            rewards_counter,
            &BuildMetadata::default(),
            Some(network),
            min_base_fee,
        )
        .await
    }

    /// Initialize the provider factory with consistency check and auto-repair.
    pub(crate) async fn init_provider_factory(
        node_config: &NodeConfig<RethChainSpec>,
        chain_spec: Arc<RaylsChainSpec>,
        database: Arc<DatabaseEnv>,
        task_spawner: &TaskSpawner,
        runtime: reth_tasks::Runtime,
        rewards_counter: RewardsCounter,
    ) -> eyre::Result<ProviderFactory<RaylsNode>> {
        let datadir = node_config.datadir();
        // Wrap ChainSpec in RaylsChainSpec for static base fee
        let rocksdb_provider = RocksDBBuilder::new(datadir.rocksdb())
            .with_default_tables()
            .with_metrics()
            .with_statistics()
            .build()?;
        let mut provider_factory = ProviderFactory::new(
            database,
            chain_spec,
            StaticFileProvider::read_write(datadir.static_files())?,
            rocksdb_provider,
            runtime,
        )?;

        provider_factory.set_storage_settings_cache(node_config.storage_settings());

        if let Some(prune_config) = node_config.prune_config() {
            provider_factory = provider_factory.with_prune_modes(prune_config.segments);
        }

        let (rocksdb_unwind, static_file_unwind) = provider_factory.check_consistency()?;

        // heal RocksDB history shards left ahead of MDBX by incomplete snapshot restores
        Self::heal_rocksdb_history_after_snapshot(&provider_factory)?;

        let unwind_block = [rocksdb_unwind, static_file_unwind].into_iter().flatten().min();

        if let Some(target_block) = unwind_block {
            // panic instead of unwinding to block 0
            assert_ne!(
                target_block, 0,
                "A storage consistency check would trigger an unwind to block 0"
            );

            info!(
                target: "rayls::reth",
                target_block,
                "Executing pipeline unwind after failed storage consistency check"
            );

            Self::execute_pipeline_unwind(
                &provider_factory,
                PipelineTarget::Unwind(target_block),
                task_spawner,
                rewards_counter,
            )
            .await?;
        }

        // init_genesis_with_settings writes HashedAccounts/HashedStorages via
        // insert_genesis_hashes and derives the trie via compute_state_root,
        // so no post-init rehashing is needed for v1 or v2.
        let genesis_hash =
            init_genesis_with_settings(&provider_factory, node_config.storage_settings())?;
        debug!(target: "rayls::execution", chain=%node_config.chain.chain, ?genesis_hash, "Initialized genesis");

        Ok(provider_factory)
    }

    /// Execute pipeline unwind using reth's DefaultStages with noop downloaders.
    async fn execute_pipeline_unwind(
        provider_factory: &ProviderFactory<RaylsNode>,
        unwind_target: PipelineTarget,
        task_spawner: &TaskSpawner,
        rewards_counter: RewardsCounter,
    ) -> eyre::Result<()> {
        let (_tip_tx, tip_rx) = watch::channel(B256::ZERO);
        let prune_modes = PruneModes::default();
        let stage_config = StageConfig::default();

        // build unwind-only pipeline with noop downloaders
        let pipeline = PipelineBuilder::default()
            .add_stages(DefaultStages::new(
                provider_factory.clone(),
                tip_rx,
                Arc::new(NoopConsensus::default()),
                NoopHeaderDownloader::default(),
                NoopBodiesDownloader::default(),
                RaylsEvmConfig::new(provider_factory.chain_spec(), rewards_counter),
                stage_config,
                prune_modes.clone(),
                None,
            ))
            .build(
                provider_factory.clone(),
                StaticFileProducer::new(provider_factory.clone(), prune_modes),
            );

        // non-critical: completing Ok must not trigger TaskManager shutdown
        let (tx, rx) = oneshot::channel();
        reth_tasks::TaskSpawner::spawn_blocking_task(
            task_spawner,
            Box::pin(async move {
                let (_, result) = pipeline.run_as_fut(Some(unwind_target)).await;
                let _ = tx.send(result);
            }),
        );

        rx.await?.inspect_err(|err| {
            error!(target: "rayls::reth", unwind_target = %unwind_target, %err, "Pipeline unwind failed")
        })?;

        info!(target: "rayls::reth", "Pipeline unwind complete");
        Ok(())
    }

    /// Repair stale RocksDB history indices left by non-atomic snapshot restore.
    ///
    /// Snapshots taken between the RocksDB and MDBX commit phases leave history
    /// shards beyond the canonical tip. Re-execution then panics with `UnsortedInput`
    /// when appending duplicate block numbers to `RoaringTreemap` shards.
    fn heal_rocksdb_history_after_snapshot(
        provider_factory: &ProviderFactory<RaylsNode>,
    ) -> eyre::Result<()> {
        let canonical_tip = provider_factory.database_provider_ro()?.last_block_number()?;
        let rocksdb = provider_factory.rocksdb_provider();

        // scan AccountsHistory for shards beyond the canonical tip
        let mut stale_accounts: Vec<Address> = Vec::new();
        {
            let mut last_addr: Option<Address> = None;
            for entry in rocksdb.iter::<reth_db::tables::AccountsHistory>()? {
                let (key, value) = entry?;
                let stale = if key.highest_block_number == u64::MAX {
                    value.max().is_some_and(|m| m > canonical_tip)
                } else {
                    key.highest_block_number > canonical_tip
                };
                if stale && last_addr.as_ref() != Some(&key.key) {
                    stale_accounts.push(key.key);
                    last_addr = Some(key.key);
                }
            }
        }

        // scan StoragesHistory similarly
        let mut stale_storage: Vec<(Address, B256)> = Vec::new();
        {
            let mut last_slot: Option<(Address, B256)> = None;
            for entry in rocksdb.iter::<reth_db::tables::StoragesHistory>()? {
                let (key, value) = entry?;
                let stale = if key.sharded_key.highest_block_number == u64::MAX {
                    value.max().is_some_and(|m| m > canonical_tip)
                } else {
                    key.sharded_key.highest_block_number > canonical_tip
                };
                if stale {
                    let slot = (key.address, key.sharded_key.key);
                    if last_slot.as_ref() != Some(&slot) {
                        stale_storage.push(slot);
                        last_slot = Some(slot);
                    }
                }
            }
        }

        if stale_accounts.is_empty() && stale_storage.is_empty() {
            return Ok(());
        }

        warn!(
            target: "rayls::reth",
            stale_accounts = stale_accounts.len(),
            stale_storage_slots = stale_storage.len(),
            canonical_tip,
            "Healing stale RocksDB history indices from snapshot restore"
        );

        let mut batch = rocksdb.batch();
        for addr in &stale_accounts {
            batch.unwind_account_history_to(*addr, canonical_tip)?;
        }
        for (addr, storage_key) in &stale_storage {
            batch.unwind_storage_history_to(*addr, *storage_key, canonical_tip)?;
        }
        batch.commit()?;

        info!(target: "rayls::reth", "RocksDB history indices healed successfully");
        Ok(())
    }

    /// Unwind the persisted chain down to `target_block`, reverting MDBX, static
    /// files, and RocksDB so a subsequent run resumes from the target.
    ///
    /// For offline tooling: call on a freshly opened env before any block
    /// building, then restart the process. The in-memory canonical state is not
    /// refreshed in place, so the new tip is only observed on the next open.
    #[cfg(feature = "archive-replay")]
    pub async fn unwind_to(
        &self,
        target_block: u64,
        rewards_counter: RewardsCounter,
    ) -> eyre::Result<()> {
        let current_tip = self.last_block_number()?;
        if target_block >= current_tip {
            info!(
                target: "rayls::reth",
                target_block,
                current_tip,
                "unwind target at or above tip; nothing to unwind"
            );
            return Ok(());
        }
        if target_block == 0 {
            return Err(eyre::eyre!("refusing to unwind to block 0; clear the datadir instead"));
        }
        info!(target: "rayls::reth", target_block, current_tip, "unwinding archive chain");
        Self::execute_pipeline_unwind(
            &self.provider_factory,
            PipelineTarget::Unwind(target_block),
            &self.task_spawner,
            rewards_counter,
        )
        .await?;
        info!(target: "rayls::reth", target_block, "unwind complete");
        Ok(())
    }

    /// Seed the genesis account history for accounts that produce no changesets.
    ///
    /// `IndexAccountHistoryStage` clears `AccountsHistory` on its first run and
    /// rebuilds from `AccountChangeSets`, so genesis accounts whose
    /// code/nonce/balance never change again (immutable system contracts) lose
    /// their block-0 entry entirely — historical `eth_call`/`eth_getCode`
    /// returns empty for them.
    ///
    /// This re-inserts a genesis block-0 entry for every such account via a
    /// read-merge-write so existing post-genesis shards are preserved. Accounts
    /// with more than one shard already carry post-genesis changesets and are
    /// left untouched. Only applicable to v2 (RocksDB) archives. Idempotent.
    ///
    /// Limitation: on an archive whose `AccountsHistory` was cleared and rebuilt
    /// from `AccountChangeSets` by `IndexAccountHistoryStage` (a normally-synced
    /// node — not `rayls-replay`), a *multi-shard* genesis account is skipped here
    /// and genesis has no changeset to rebuild from, so it never regains its
    /// block-0 entry. This is a non-issue for replay-built archives, where that
    /// stage never runs; account history is written once by genesis init and only
    /// appended to.
    #[cfg(feature = "archive-replay")]
    pub fn fix_genesis_account_history(&self) -> eyre::Result<()> {
        Self::fix_genesis_account_history_with(
            &self.provider_factory,
            self.node_config.storage_settings().use_hashed_state(),
        )?;
        Ok(())
    }

    /// Testable core of [`Self::fix_genesis_account_history`]. Returns the number
    /// of accounts seeded.
    #[cfg(feature = "archive-replay")]
    pub(crate) fn fix_genesis_account_history_with(
        provider_factory: &ProviderFactory<RaylsNode>,
        use_hashed_state: bool,
    ) -> eyre::Result<usize> {
        use reth_db::{models::ShardedKey, BlockNumberList};
        use reth_provider::RocksDBProviderFactory;
        use std::collections::{HashMap, HashSet};

        if !use_hashed_state {
            info!(target: "rayls::reth", "v1 (plain) storage; account history needs no fix");
            return Ok(0);
        }

        let chain_spec = provider_factory.chain_spec();
        let genesis = chain_spec.genesis();
        let block = chain_spec.genesis_header().number;

        // Single pass: collect which addresses already have a block-0 entry
        // (idempotency guard) and how many shards each address has (safety guard).
        let (already_fixed, shard_counts): (HashSet<Address>, HashMap<Address, usize>) = {
            let rocksdb = provider_factory.rocksdb_provider();
            let mut fixed = HashSet::new();
            let mut counts: HashMap<Address, usize> = HashMap::new();
            for entry in rocksdb.iter::<reth_db::tables::AccountsHistory>()? {
                let (key, value) = entry?;
                *counts.entry(key.key).or_insert(0) += 1;
                if value.contains(block) {
                    fixed.insert(key.key);
                }
            }
            (fixed, counts)
        };

        if !already_fixed.is_empty() {
            info!(
                target: "rayls::reth",
                already_fixed = already_fixed.len(),
                "idempotency: skipping accounts already present in AccountsHistory at genesis block"
            );
        }

        // Accounts with more than one shard have post-genesis changesets that were
        // correctly indexed by IndexAccountHistoryStage.
        let multi_shard_skipped = genesis
            .alloc
            .keys()
            .filter(|addr| shard_counts.get(*addr).copied().unwrap_or(0) > 1)
            .count();

        if multi_shard_skipped > 0 {
            info!(
                target: "rayls::reth",
                multi_shard_skipped,
                "skipping genesis accounts with multiple history shards (post-genesis changesets present)"
            );
        }

        let to_fix: Vec<Address> = genesis
            .alloc
            .keys()
            .filter(|addr| {
                !already_fixed.contains(*addr) && shard_counts.get(*addr).copied().unwrap_or(0) <= 1
            })
            .copied()
            .collect();

        if to_fix.is_empty() {
            info!(target: "rayls::reth", block, "genesis account history already fully seeded; nothing to do");
            return Ok(0);
        }

        let rocksdb = provider_factory.rocksdb_provider();
        let mut batch = rocksdb.batch();
        for addr in &to_fix {
            let key = ShardedKey::last(*addr);
            let existing = rocksdb.get::<reth_db::tables::AccountsHistory>(key.clone())?;
            let merged = match existing {
                Some(existing_list) => {
                    let blocks: Vec<u64> =
                        core::iter::once(block).chain(existing_list.iter()).collect();
                    BlockNumberList::new(blocks)
                        .map_err(|e| eyre::eyre!("failed to build block number list: {e}"))?
                }
                None => BlockNumberList::new([block]).expect("single block always fits"),
            };
            batch.put::<reth_db::tables::AccountsHistory>(key, &merged)?;
        }
        batch.commit()?;

        info!(
            target: "rayls::reth",
            accounts = to_fix.len(),
            block,
            "seeded genesis account history for v2 archive"
        );
        Ok(to_fix.len())
    }

    /// Re-key the genesis storage history to hashed keys so v2 archive reads
    /// reconstruct genesis-seeded storage instead of returning 0x0.
    ///
    /// reth's genesis writer (`insert_storage_history`) keys `StoragesHistory`
    /// by the plain slot, but the v2 read looks it up by `keccak256(slot)`, so
    /// genesis-seeded storage (e.g. the static validator set) reads as
    /// `NotYetWritten` -> 0x0. This re-emits the genesis block entry under the
    /// hashed slot.
    ///
    /// The entry is **merged into the earliest existing shard** for each
    /// `(address, hashed slot)` via a read-merge-write, rather than overwriting
    /// the open (`u64::MAX`) shard. That keeps the post-genesis history of a
    /// genesis slot that was also modified later (e.g. a balance / registry slot)
    /// and places block 0 in the shard the historical read consults for early
    /// blocks. When prepending would push the earliest shard past
    /// `NUM_OF_INDICES_IN_SHARD` (a hot genesis slot with >= that many
    /// post-genesis changes), the slot's shards are re-chunked so none exceeds
    /// the limit. Only applicable to v2 (RocksDB) archives. Idempotent.
    ///
    /// This adds the entry under the hashed key; the original plain-slot entry
    /// written by genesis init is intentionally left in place. It's never read by
    /// v2 (which looks up the hashed key) and deleting it is avoidable risk for no
    /// functional gain, so it's kept as harmless dead weight rather than removed.
    #[cfg(feature = "archive-replay")]
    pub fn fix_genesis_history(&self) -> eyre::Result<()> {
        Self::fix_genesis_history_with(
            &self.provider_factory,
            self.node_config.storage_settings().use_hashed_state(),
        )?;
        Ok(())
    }

    /// Testable core of [`Self::fix_genesis_history`]. Returns the number of
    /// storage slots re-keyed (0 when everything is already correct).
    #[cfg(feature = "archive-replay")]
    pub(crate) fn fix_genesis_history_with(
        provider_factory: &ProviderFactory<RaylsNode>,
        use_hashed_state: bool,
    ) -> eyre::Result<usize> {
        use alloy::primitives::keccak256;
        use reth_db::{
            models::{
                sharded_key::NUM_OF_INDICES_IN_SHARD, storage_sharded_key::StorageShardedKey,
            },
            BlockNumberList,
        };
        use reth_provider::RocksDBProviderFactory;

        if !use_hashed_state {
            info!(target: "rayls::reth", "v1 (plain) storage; genesis history needs no re-key");
            return Ok(0);
        }

        let chain_spec = provider_factory.chain_spec();
        let genesis = chain_spec.genesis();
        let block = chain_spec.genesis_header().number;

        let rocksdb = provider_factory.rocksdb_provider();
        let mut batch = rocksdb.batch();
        let (mut fixed, mut already, mut rechunked) = (0usize, 0usize, 0usize);

        for (addr, account) in genesis.alloc.iter() {
            let Some(storage) = account.storage.as_ref() else { continue };
            for slot in storage.keys() {
                let hashed = keccak256(slot);
                // All shards for this (address, hashed slot), ascending by highest
                // block. A cheap per-slot prefix scan, not an O(N) full-table scan.
                let shards = rocksdb.storage_history_shards(*addr, hashed)?;

                // Idempotency: a prior run already placed the genesis block.
                if shards.iter().any(|(_, list)| list.contains(block)) {
                    already += 1;
                    continue;
                }

                match shards.first() {
                    // No history yet (immutable genesis slot, never touched after
                    // genesis): open a fresh `::last` shard holding just the block.
                    None => {
                        let key = StorageShardedKey::last(*addr, hashed);
                        batch.put::<reth_db::tables::StoragesHistory>(
                            key,
                            &BlockNumberList::new([block]).expect("single block always fits"),
                        )?;
                    }
                    Some((earliest_key, earliest_list)) => {
                        // Fast path: a single shard (the open `::last`) with room —
                        // prepend the genesis block in place. The read for an early
                        // block consults this shard, so block 0 belongs here.
                        if shards.len() == 1
                            && (earliest_list.len() as usize) < NUM_OF_INDICES_IN_SHARD
                        {
                            let blocks: Vec<u64> =
                                core::iter::once(block).chain(earliest_list.iter()).collect();
                            let merged = BlockNumberList::new(blocks).map_err(|e| {
                                eyre::eyre!("failed to build block number list: {e}")
                            })?;
                            batch.put::<reth_db::tables::StoragesHistory>(
                                earliest_key.clone(),
                                &merged,
                            )?;
                        } else {
                            // Prepending would overflow the earliest shard (a hot
                            // genesis slot with a full first shard). Gather all of the
                            // slot's blocks, prepend genesis, delete the old shards and
                            // re-split into <= NUM_OF_INDICES_IN_SHARD chunks so none
                            // exceeds the limit. `all` is globally sorted: block 0 is
                            // smaller than every post-genesis change, and both the shard
                            // order and each shard's list are ascending.
                            rechunked += 1;
                            let mut all: Vec<u64> = core::iter::once(block).collect();
                            for (_, list) in &shards {
                                all.extend(list.iter());
                            }
                            // `all` must be strictly ascending for BlockNumberList /
                            // the shard split: genesis block < every post-genesis block,
                            // storage_history_shards returns shards ascending, and each
                            // shard's list is ascending. Guard that invariant.
                            debug_assert!(
                                all.windows(2).all(|w| w[0] < w[1]),
                                "re-chunk block list must be strictly ascending"
                            );
                            for (key, _) in &shards {
                                batch.delete::<reth_db::tables::StoragesHistory>(key.clone())?;
                            }
                            let mut i = 0;
                            while i < all.len() {
                                let end = (i + NUM_OF_INDICES_IN_SHARD).min(all.len());
                                let chunk = &all[i..end];
                                let key = if end == all.len() {
                                    StorageShardedKey::last(*addr, hashed)
                                } else {
                                    StorageShardedKey::new(*addr, hashed, chunk[chunk.len() - 1])
                                };
                                let list =
                                    BlockNumberList::new(chunk.iter().copied()).map_err(|e| {
                                        eyre::eyre!("failed to build block number list: {e}")
                                    })?;
                                batch.put::<reth_db::tables::StoragesHistory>(key, &list)?;
                                i = end;
                            }
                        }
                    }
                }
                fixed += 1;
            }
        }

        if fixed == 0 {
            info!(target: "rayls::reth", block, already, "genesis storage history already re-keyed; nothing to do");
            return Ok(0);
        }
        batch.commit()?;

        info!(
            target: "rayls::reth",
            slots = fixed,
            already,
            rechunked,
            block,
            "re-keyed genesis storage history to hashed keys"
        );
        Ok(fixed)
    }
}
