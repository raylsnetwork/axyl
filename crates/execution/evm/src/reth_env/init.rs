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

    /// Seed the genesis account history for accounts that produce no changesets
    /// `IndexAccountHistoryStage` clears `AccountsHistory` on its first
    /// run and rebuilds from `AccountChangeSets`.
    ///
    /// This fix re-inserts a genesis block 0 entry for every such account via a
    /// read-merge-write so existing post-genesis modification shards are
    /// preserved. Only applicable to v2 (RocksDB) archives. Idempotent.
    #[cfg(feature = "archive-replay")]
    pub fn fix_genesis_account_history(&self) -> eyre::Result<()> {
        use reth_db::{models::ShardedKey, BlockNumberList};
        use reth_provider::RocksDBProviderFactory;
        use std::collections::{HashMap, HashSet};

        if !self.node_config.storage_settings().use_hashed_state() {
            info!(target: "rayls::reth", "v1 (plain) storage; account history needs no fix");
            return Ok(());
        }

        let chain_spec = self.provider_factory.chain_spec();
        let genesis = chain_spec.genesis();
        let block = chain_spec.genesis_header().number;

        // Single pass: collect which addresses already have a block-0 entry
        // (idempotency guard) and how many shards each address has (safety guard).
        let (already_fixed, shard_counts): (HashSet<Address>, HashMap<Address, usize>) = {
            let rocksdb = self.provider_factory.rocksdb_provider();
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
            return Ok(());
        }

        let rocksdb = self.provider_factory.rocksdb_provider();
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
        Ok(())
    }

    /// Re-key the genesis storage history to hashed keys so v2 archive reads
    /// reconstruct genesis-seeded storage instead of returning 0x0.
    ///
    /// reth's genesis writer keys `StoragesHistory` by the plain slot, but the
    /// v2 read looks it up by `keccak256(slot)`, so genesis-seeded storage
    /// (e.g. the static validator set) reads as `NotYetWritten` -> 0x0. This
    /// re-emits the genesis storage history under the hashed slot via reth's
    /// public `insert_storage_history`. Idempotent; a no-op in v1 (plain) mode.
    #[cfg(feature = "archive-replay")]
    pub fn fix_genesis_history(&self) -> eyre::Result<()> {
        use alloy::{genesis::GenesisAccount, primitives::keccak256};
        use reth_db_common::init::insert_storage_history;
        use reth_provider::DBProvider;
        use std::collections::HashSet;

        if !self.node_config.storage_settings().use_hashed_state() {
            info!(target: "rayls::reth", "v1 (plain) storage; genesis history needs no re-key");
            return Ok(());
        }

        let chain_spec = self.provider_factory.chain_spec();
        let genesis = chain_spec.genesis();
        let block = chain_spec.genesis_header().number;

        // mirror reth's genesis alloc but with hashed storage keys; the address
        // stays plain (StoragesHistory keys the address plain, slot hashed).
        let mut rehashed: Vec<(Address, GenesisAccount)> = genesis
            .alloc
            .iter()
            .filter_map(|(addr, account)| {
                account.storage.as_ref().map(|storage| {
                    let hashed =
                        storage.iter().map(|(slot, value)| (keccak256(slot), *value)).collect();
                    (*addr, GenesisAccount { storage: Some(hashed), ..account.clone() })
                })
            })
            .collect();

        // Idempotency + safety guard: for each genesis (addr, hashed_slot), walk every
        // shard for that key and skip it if it already carries any block number —
        // whether that's `block` itself (a previous run already applied the fix) or
        // some other block (real post-genesis writes reached this slot). Genesis-alloc
        // offers no guarantee a slot stays static after genesis, and
        // insert_storage_history's write is an upsert of the `last` shard, not an
        // append: applying it to a slot that already has history — of either kind —
        // would silently overwrite it. Walking shards per-key keeps this bounded to the
        // genesis slots (not an O(N) scan of the whole StoragesHistory table) while
        // staying correct. Scoping the rocksdb reference ensures it's dropped before
        // the write transaction is opened below.
        let (already_fixed, has_other_history): (HashSet<(Address, B256)>, HashSet<(Address, B256)>) = {
            let rocksdb = self.provider_factory.rocksdb_provider();
            let mut fixed = HashSet::new();
            let mut other = HashSet::new();
            for (addr, account) in &rehashed {
                let Some(storage) = account.storage.as_ref() else { continue };
                for slot in storage.keys() {
                    let shards = rocksdb.storage_history_shards(*addr, *slot)?;
                    let (has_block, has_other) = shards
                        .iter()
                        .flat_map(|(_, list)| list.iter())
                        .fold((false, false), |(has_block, has_other), b| {
                            (has_block || b == block, has_other || b != block)
                        });
                    if has_block {
                        fixed.insert((*addr, *slot));
                    }
                    if has_other {
                        other.insert((*addr, *slot));
                    }
                }
            }
            (fixed, other)
        };

        if !already_fixed.is_empty() {
            info!(
                target: "rayls::reth",
                already_fixed = already_fixed.len(),
                "idempotency: skipping slots already present in StoragesHistory at genesis block"
            );
        }

        if !has_other_history.is_empty() {
            warn!(
                target: "rayls::reth",
                slots = has_other_history.len(),
                block,
                "genesis-alloc storage slots already have post-genesis history; skipping re-key \
                 to avoid overwriting it (these slots turned out not to be static)"
            );
        }

        let skip: HashSet<(Address, B256)> =
            already_fixed.union(&has_other_history).copied().collect();

        if !skip.is_empty() {
            for (addr, account) in &mut rehashed {
                if let Some(storage) = account.storage.as_mut() {
                    storage.retain(|slot, _| !skip.contains(&(*addr, *slot)));
                }
            }
            rehashed.retain(|(_, account)| account.storage.as_ref().is_some_and(|s| !s.is_empty()));
        }

        let slots: usize =
            rehashed.iter().map(|(_, a)| a.storage.as_ref().map_or(0, |s| s.len())).sum();

        if slots == 0 {
            info!(target: "rayls::reth", block, "genesis storage history already fully re-keyed; nothing to do");
            return Ok(());
        }

        let provider_rw = self.provider_factory.database_provider_rw()?;
        insert_storage_history(&provider_rw, rehashed.iter().map(|(a, acc)| (a, acc)), block)?;
        provider_rw.commit()?;
        info!(target: "rayls::reth", slots, block, "re-keyed genesis storage history to hashed keys");
        Ok(())
    }
}
