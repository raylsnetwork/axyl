use crate::{
    error::RaylsRethResult,
    reth_env::{types::BlockWithSenders, ChainSpec, RethEnv},
    RaylsChainSpec, WorkerTxPool,
};
use alloy::primitives::Address;
use rayls_infrastructure_types::{
    BlockHashOrNumber, BlockNumHash, BlockNumber, ExecHeader, SealedBlock, SealedHeader,
    TaskSpawner, B256,
};
use reth_chain_state::CanonicalInMemoryState;
use reth_chainspec::BaseFeeParams;
use reth_eth_wire::BlockHashNumber;
use reth_provider::{
    BlockIdReader as _, BlockNumReader, BlockReader, CanonStateNotificationStream,
    CanonStateSubscriptions as _, ChainStateBlockReader, DatabaseProviderFactory,
    HeaderProvider as _, StateProviderBox, StateProviderFactory, TransactionVariant,
};
use std::{ops::RangeInclusive, sync::Arc};

#[cfg(feature = "archive-replay")]
use reth_primitives_traits::Block as _;

impl RethEnv {
    /// Initialize a new transaction pool for worker.
    pub fn init_txn_pool(&self) -> eyre::Result<WorkerTxPool> {
        self.init_txn_pool_with_in_flight(crate::in_flight::InFlightTracker::new())
    }

    /// Initialize the worker transaction pool around a caller-owned in-flight tracker, so the
    /// same tracker can be handed to the executor engine, which releases the marks of
    /// execution-dropped transactions.
    pub fn init_txn_pool_with_in_flight(
        &self,
        in_flight: crate::in_flight::InFlightTracker,
    ) -> eyre::Result<WorkerTxPool> {
        WorkerTxPool::new(
            &self.node_config,
            &self.task_spawner,
            &self.blockchain_provider,
            in_flight,
        )
    }

    /// Return a channel receiver that will return each canonical block in turn.
    pub fn canonical_block_stream(&self) -> CanonStateNotificationStream {
        self.blockchain_provider.canonical_state_stream()
    }

    /// Return a reference to the [TaskSpawner] for spawning tasks.
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.task_spawner
    }

    /// Return the chainspec for this instance.
    pub fn chainspec(&self) -> ChainSpec {
        ChainSpec(self.node_config.chain.clone())
    }

    /// Return a reference to the Rayls chain spec with hardfork information.
    pub fn rayls_chain_spec(&self) -> &Arc<RaylsChainSpec> {
        self.evm_config.chain_spec()
    }

    /// Return the canonical in-memory state.
    pub fn canonical_in_memory_state(&self) -> CanonicalInMemoryState {
        self.blockchain_provider.canonical_in_memory_state()
    }

    /// Look up and return the sealed header for hash.
    pub fn sealed_header_by_hash(&self, hash: B256) -> RaylsRethResult<Option<SealedHeader>> {
        Ok(self.blockchain_provider.sealed_header_by_hash(hash)?)
    }

    /// Look up and return the sealed header for block number.
    pub fn sealed_header_by_number(&self, number: u64) -> RaylsRethResult<Option<SealedHeader>> {
        Ok(self.blockchain_provider.sealed_header(number)?)
    }

    /// Look up and return the sealed block for number.
    pub fn sealed_block_by_number(&self, number: u64) -> RaylsRethResult<Option<SealedBlock>> {
        Ok(self
            .blockchain_provider
            .sealed_block_with_senders(
                BlockHashOrNumber::Number(number),
                TransactionVariant::NoHash,
            )?
            .map(|b| b.clone_sealed_block()))
    }

    /// Look up and return the sealed header (with senders) for hash.
    pub fn sealed_block_with_senders(
        &self,
        id: BlockHashOrNumber,
    ) -> RaylsRethResult<Option<BlockWithSenders>> {
        Ok(self.blockchain_provider.sealed_block_with_senders(id, TransactionVariant::NoHash)?)
    }

    /// Return the blocks with senders for a range of block numbers.
    pub fn block_with_senders_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> RaylsRethResult<Vec<BlockWithSenders>> {
        Ok(self.blockchain_provider.block_with_senders_range(range)?)
    }

    /// Return the blocks for a range of block numbers.
    pub fn blocks_for_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> RaylsRethResult<Vec<SealedHeader>> {
        Ok(self.blockchain_provider.sealed_headers_range(range)?)
    }

    /// Return the head header from the reth db.
    pub fn lookup_head(&self) -> RaylsRethResult<SealedHeader> {
        let head = self.node_config.lookup_head(&self.blockchain_provider)?;
        let header = self
            .blockchain_provider
            .sealed_header(head.number)?
            .expect("Failed to retrieve sealed header from head's block number");
        Ok(header)
    }

    /// If a debug max round is set then return it.
    pub fn get_debug_max_round(&self) -> Option<u64> {
        self.node_config.debug.max_block
    }

    /// Helper to get the gas price based on the provider's latest header.
    pub fn get_gas_price(&self) -> RaylsRethResult<u128> {
        let header = self.lookup_head()?;
        Ok(header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default().into())
    }

    /// Return the execution header for hash if available.
    pub fn header(&self, hash: B256) -> RaylsRethResult<Option<ExecHeader>> {
        Ok(self.blockchain_provider.header(hash)?)
    }

    /// Return the execution header for block number if available.
    pub fn header_by_number(&self, block_num: u64) -> RaylsRethResult<Option<ExecHeader>> {
        Ok(self.blockchain_provider.header_by_number(block_num)?)
    }

    /// Return the finalized execution header if available.
    pub fn finalized_header(&self) -> RaylsRethResult<Option<ExecHeader>> {
        let finalized_block_num_hash =
            self.blockchain_provider.finalized_block_num_hash().unwrap_or_default();
        if let Some(finalized_block_num_hash) = finalized_block_num_hash {
            Ok(self.blockchain_provider.header(finalized_block_num_hash.hash)?)
        } else {
            Ok(None)
        }
    }

    /// Return the latest canonical block number.
    pub fn last_block_number(&self) -> RaylsRethResult<u64> {
        Ok(self.blockchain_provider.last_block_number().unwrap_or(0))
    }

    /// Return the block number and hash for the current canonical tip.
    ///
    /// This checks the canonical-in-memory-state.
    pub fn canonical_tip(&self) -> SealedHeader {
        self.blockchain_provider.canonical_in_memory_state().get_canonical_head()
    }

    /// If available return the finalized block number and hash.
    ///
    /// This checks the canonical-in-memory-state.
    pub fn finalized_block_num_hash(&self) -> RaylsRethResult<Option<BlockNumHash>> {
        Ok(self.blockchain_provider.finalized_block_num_hash()?)
    }

    /// Returns the block number of the last finalized block.
    pub fn last_finalized_block_number(&self) -> RaylsRethResult<u64> {
        Ok(self
            .blockchain_provider
            .database_provider_ro()?
            .last_finalized_block_number()?
            .unwrap_or(0))
    }

    /// Return the block number and hash of the finalized block on node startup.
    ///
    /// This method adds additional fallbacks to ensure genesis is used when the network is starting
    /// because the genesis block is not initialized as `finalized`. Nodes that start on genesis
    /// will resync with the network if it exists.
    pub fn finalized_block_hash_number_for_startup(&self) -> RaylsRethResult<BlockHashNumber> {
        let hash = self
            .blockchain_provider
            .finalized_block_hash()?
            .unwrap_or_else(|| self.node_config.chain.sealed_genesis_header().hash());
        let number = self.blockchain_provider.finalized_block_number()?.unwrap_or_default();
        Ok(BlockHashNumber { hash, number })
    }

    /// Provide the state for the latest block in this instance.
    pub fn latest(&self) -> RaylsRethResult<StateProviderBox> {
        Ok(self.blockchain_provider.latest()?)
    }

    /// Return block `number`'s withdrawals as `(address, amount)` pairs, reading
    /// the body without recovering transaction senders. `None` if the block is
    /// absent; an empty vec if the block has no withdrawals.
    #[cfg(feature = "archive-replay")]
    pub fn block_withdrawals(&self, number: u64) -> RaylsRethResult<Option<Vec<(Address, u64)>>> {
        Ok(self.blockchain_provider.block(BlockHashOrNumber::Number(number))?.map(|b| {
            b.body()
                .withdrawals
                .as_ref()
                .map(|w| w.iter().map(|wd| (wd.address, wd.amount)).collect::<Vec<_>>())
                .unwrap_or_default()
        }))
    }

    /// Install the canonical state-root oracle used by archive replay.
    ///
    /// The first install wins: `OnceLock::set` silently ignores later calls, so
    /// the oracle must be set at most once per `RethEnv` lifetime.
    #[cfg(feature = "archive-replay")]
    pub fn set_canonical_root_oracle(&self, oracle: crate::reth_env::solver::CanonicalRootOracle) {
        let _ = self.canonical_root_oracle.set(oracle);
    }
}
