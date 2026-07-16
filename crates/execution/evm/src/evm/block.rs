//! The types that build blocks for EVM execution.

use crate::{
    chainspec::RaylsHardforks,
    error::{RaylsRethError, RaylsRethResult},
    evm::hardforks,
    system_calls::{
        ConsensusRegistry::{self, RewardInfo, ValidatorInfo, ValidatorStatus},
        RewardDistributor, CONSENSUS_REGISTRY_ADDRESS, REWARD_DISTRIBUTOR_ADDRESS, SYSTEM_ADDRESS,
    },
    RaylsChainSpec,
};
use alloy::{
    consensus::{proofs, Block, BlockBody, Transaction, TxReceipt, TxType},
    eips::{eip2935::HISTORY_STORAGE_ADDRESS, eip4788::BEACON_ROOTS_ADDRESS, eip7685::Requests},
    sol_types::SolCall as _,
};
use alloy_evm::{block::StateChangeSource, eth::EthTxResult, tx::RecoveredTx as _, Database, Evm};
use rand::{rngs::StdRng, seq::IteratorRandom, Rng as _, SeedableRng as _};
use rayls_infrastructure_types::{
    rewards::build_withdrawals, Address, Bytes, Encodable2718, ExecHeader, Receipt, SolValue,
    TransactionSigned, Withdrawals, B256, EMPTY_WITHDRAWALS, U256,
};
use rayls_middleware_rewards::RewardsCounter;
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::{BlockExecutor, BlockExecutorFactory, ExecutableTx, InternalBlockExecutionError},
    eth::receipt_builder::{ReceiptBuilder, ReceiptBuilderCtx},
    execute::{BlockAssembler, BlockAssemblerInput},
    FromRecoveredTx, FromTxWithEncoded, OnStateHook,
};
use reth_primitives::logs_bloom;
use reth_primitives_traits::proofs::calculate_withdrawals_root;
use reth_provider::BlockExecutionResult;
use reth_revm::{
    context::result::{ExecutionResult, ResultAndState},
    context_interface::Block as EvmBlockTr,
    db::states::bundle_state::BundleRetention,
    DatabaseCommit as _, State,
};
use std::{collections::BTreeMap, sync::Arc};
use tracing::{debug, error, info, trace};

/// Context for Rayls block execution.
#[derive(Debug, Clone)]
pub struct RaylsBlockExecutionCtx {
    /// Parent block hash.
    pub parent_hash: B256,
    /// Parent beacon block root - the digest of the `ConsensusHeader`.
    pub parent_beacon_block_root: Option<B256>,
    /// The index for the batch.
    pub nonce: u64,
    /// Ommers hash for the block header.
    ///
    /// Pre-BatchDigestV2: carries the batch digest.
    /// Post-BatchDigestV2: set to EMPTY_OMMER_ROOT_HASH (go-ethereum compat).
    pub ommers_hash: B256,
    /// Requests hash for the block header.
    ///
    /// Pre-BatchDigestV2: set to EMPTY_REQUESTS_HASH.
    /// Post-BatchDigestV2: carries the batch digest.
    pub requests_hash: Option<B256>,
    /// Keccak hash of the bls signature for the leader certificate.
    ///
    /// Executor makes closing epoch system call when this if included.
    /// The hash is stored in the `extra_data` field so clients know when the
    /// closing epoch call was made.
    pub close_epoch: Option<B256>,
    /// Difficulty- this contains the worker id and batch index:
    /// `U256::from(payload.batch_index << 16 | payload.worker_id as usize)`
    pub difficulty: U256,
    /// Counter that resolves leader counts at epoch boundary.
    pub rewards_counter: RewardsCounter,
    /// Pre-computed close-epoch leader tally. Populated at ctx construction
    /// when `close_epoch.is_some()`; `None` otherwise. Read by both the
    /// system-call site at `finish` and the withdrawals site at `assemble_block`.
    pub close_epoch_tally: Option<BTreeMap<Address, u32>>,
}

impl RaylsBlockExecutionCtx {
    /// Checks if the batch_index stored in the difficulty field is zero
    /// which indicates the first batch in the executed output from consensus.
    ///
    /// The difficulty field packs two values using bit operations:
    /// `difficulty = U256::from(batch_index << 16 | worker_id as usize)`
    ///
    /// This creates a bit layout where:
    /// - Bits 0-15 (lower 16 bits): worker_id (max value 65535)
    /// - Bits 16+     (upper bits): batch_index
    ///
    /// Since worker_id can only occupy the lower 16 bits (max value 2^16 - 1 = 65535),
    /// if the entire difficulty value is less than 2^16 (65536), then no bits are set
    /// in positions 16 or higher. This mathematically guarantees that batch_index = 0.
    ///
    /// This approach avoids bit shifting operations and provides an efficient
    /// zero-check without extracting the actual batch_index value.
    ///
    /// # Example
    /// ```
    /// // If difficulty = 0x00001234, then:
    /// // - worker_id = 0x1234 (bits 0-15)
    /// // - batch_index = 0x0000 (bits 16+)
    /// // Since 0x1234 < 0x10000 (65536), batch_index is 0
    /// ```
    ///
    /// This is used during execution to write the consensus header hash
    /// to `BEACON_ROOTS` contract (eip4788).
    fn first_batch(&self) -> bool {
        self.difficulty < U256::from(65536)
    }
}

/// Block executor for Ethereum.
pub(crate) struct RaylsBlockExecutor<Evm, Spec, R: ReceiptBuilder> {
    /// Reference to the specification object.
    spec: Spec,
    /// Context for block execution.
    pub ctx: RaylsBlockExecutionCtx,
    /// Inner EVM.
    evm: Evm,
    /// Receipt builder.
    receipt_builder: R,

    /// Receipts of executed transactions.
    receipts: Vec<R::Receipt>,
    /// Total gas used by transactions in this block.
    gas_used: u64,
    /// Hook for per-transaction state change notifications
    state_hook: Option<Box<dyn OnStateHook>>,
}

impl<Evm: std::fmt::Debug, Spec: std::fmt::Debug, R: ReceiptBuilder> std::fmt::Debug
    for RaylsBlockExecutor<Evm, Spec, R>
where
    R::Receipt: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaylsBlockExecutor")
            .field("spec", &self.spec)
            .field("ctx", &self.ctx)
            .field("evm", &self.evm)
            .field("receipts", &self.receipts)
            .field("gas_used", &self.gas_used)
            .field("state_hook", &self.state_hook.as_ref().map(|_| ".."))
            .finish()
    }
}

impl<'db, Evm, Spec, R, DB> RaylsBlockExecutor<Evm, Spec, R>
where
    DB: Database + 'db,
    DB::Error: core::fmt::Display,
    Evm: alloy_evm::Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
    >,
    Spec: EthereumHardforks + RaylsHardforks,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
{
    /// Creates a new [`RaylsBlockExecutor`]
    pub(crate) fn new(
        evm: Evm,
        ctx: RaylsBlockExecutionCtx,
        spec: Spec,
        receipt_builder: R,
    ) -> Self {
        Self {
            evm,
            ctx,
            receipts: Vec::new(),
            gas_used: 0,
            spec,
            receipt_builder,
            state_hook: None,
        }
    }

    /// Increase the beneficiary account balance and withdraw from governance safe.
    ///
    /// This must be called once per epoch, before the conclude epoch call.
    fn apply_consensus_block_rewards(
        &mut self,
        rewards: BTreeMap<Address, u32>,
    ) -> RaylsRethResult<()> {
        let calldata = self.generate_apply_incentives_calldata(
            rewards.iter().map(|(address, count)| (*address, *count)).collect(),
        )?;

        trace!(target: "engine", ?calldata, "apply incentives calldata");

        // execute system call to consensus registry
        let res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", "error applying consensus block rewards contract call: {:?}", e);
                return Err(RaylsRethError::EVMCustom(format!(
                    "applying consensus block rewards failed: {e}"
                )));
            }
        };

        // return error if closing epoch call failed
        if !res.result.is_success() {
            // execution failed
            error!(target: "engine", "failed applying consensus block rewards call: {:?}", res.result);
            return Err(RaylsRethError::EVMCustom(
                "failed applying consensus block rewards".to_string(),
            ));
        }
        trace!(target: "engine", ?res, "applying consensus block rewards");

        // notify state hook of state changes
        if let Some(hook) = &mut self.state_hook {
            hook.on_state(StateChangeSource::Transaction(self.receipts.len()), &res.state);
        }

        // commit the changes
        self.evm.db_mut().commit(res.state);

        Ok(())
    }

    /// Apply the closing epoch call to ConsensusRegistry.
    fn apply_closing_epoch_contract_call(&mut self, randomness: B256) -> RaylsRethResult<()> {
        debug!(target: "engine", ?randomness, "applying closing contract call");
        let calldata = self.generate_conclude_epoch_calldata(randomness)?;
        trace!(target: "engine", ?calldata, "close epoch calldata");

        // execute system call to consensus registry
        let res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", "error executing closing epoch contract call: {:?}", e);
                return Err(RaylsRethError::EVMCustom(format!(
                    "epoch closing execution failed: {e}"
                )));
            }
        };

        trace!(target: "engine", ?res, "transact system call for conclude epoch");

        // return error if closing epoch call failed
        if !res.result.is_success() {
            // execution failed
            error!(target: "engine", "failed to apply closing epoch call: {:?}", res.result);
            return Err(RaylsRethError::EVMCustom("failed to close epoch".to_string()));
        }

        trace!(target: "engine", "closing epoch logs:\n{:?}", res.result.logs());

        // notify state hook of state changes
        if let Some(hook) = &mut self.state_hook {
            hook.on_state(StateChangeSource::Transaction(self.receipts.len()), &res.state);
        }

        // commit the changes
        self.evm.db_mut().commit(res.state);
        Ok(())
    }

    /// Distribute pending RLS rewards to validators at epoch end.
    ///
    /// Calls `RewardDistributor.distributeRewards()` which reads performance
    /// weights set by `applyIncentives` and distributes accumulated RLS.
    /// Succeeds silently when no rewards are pending.
    fn apply_reward_distribution(&mut self) -> RaylsRethResult<()> {
        debug!(target: "engine", "applying reward distribution");

        let calldata: Bytes = RewardDistributor::distributeRewardsCall {}.abi_encode().into();

        trace!(target: "engine", ?calldata, "distribute rewards calldata");

        let res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            REWARD_DISTRIBUTOR_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                error!(target: "engine", "error executing reward distribution call: {:?}", e);
                return Err(RaylsRethError::EVMCustom(format!(
                    "reward distribution execution failed: {e}"
                )));
            }
        };

        if !res.result.is_success() {
            error!(target: "engine", "failed to apply reward distribution: {:?}", res.result);
            return Err(RaylsRethError::EVMCustom("failed to distribute rewards".to_string()));
        }

        trace!(target: "engine", "reward distribution logs:\n{:?}", res.result.logs());

        // notify state hook of state changes
        if let Some(hook) = &mut self.state_hook {
            hook.on_state(StateChangeSource::Transaction(self.receipts.len()), &res.state);
        }

        // commit the changes
        self.evm.db_mut().commit(res.state);
        Ok(())
    }

    /// Generate calldata for updating the ConsensusRegistry to conclude the epoch.
    fn generate_conclude_epoch_calldata(&mut self, randomness: B256) -> RaylsRethResult<Bytes> {
        // shuffle all validators for new committee
        let mut new_committee = self.shuffle_new_committee(randomness)?;

        if new_committee.is_empty() {
            let epoch = self.extract_epoch_from_nonce(self.ctx.nonce);
            error!(
                target: "engine",
                nonce = self.ctx.nonce,
                epoch,
                "concludeEpoch called with EMPTY committee - aborting to prevent contract revert"
            );
            return Err(RaylsRethError::EVMCustom(format!(
                "concludeEpoch: empty committee at epoch {epoch} - \
                 see 'NO ACTIVE VALIDATORS' log for validator statuses"
            )));
        }

        // sort addresses in ascending order (0x0...0xf)
        new_committee.sort();
        info!(target: "engine", committee_size = new_committee.len(), "concludeEpoch committee");

        // encode the call to bytes with method selector and args
        let bytes = ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();

        Ok(bytes)
    }

    /// Generate calldata for applying incentives when concluding the epoch.
    fn generate_apply_incentives_calldata(
        &mut self,
        reward_infos: Vec<(Address, u32)>,
    ) -> RaylsRethResult<Bytes> {
        debug!(target: "engine", ?reward_infos, "applying incentives");

        // encode the call to bytes with method selector and args
        let bytes = ConsensusRegistry::applyIncentivesCall {
            rewardInfos: reward_infos
                .iter()
                .map(|(address, count)| RewardInfo {
                    validatorAddress: *address,
                    consensusHeaderCount: U256::from(*count),
                })
                .collect(),
        }
        .abi_encode()
        .into();

        Ok(bytes)
    }

    /// Read eligible validators from latest state and shuffle the committee deterministically.
    fn shuffle_new_committee(&mut self, randomness: B256) -> RaylsRethResult<Vec<Address>> {
        let new_committee_size = self.next_committee_size()?;

        // read all active validators from consensus registry
        let all_active_validators = self.get_active_validators()?;

        info!(
            target: "engine",
            new_committee_size,
            active_validators = all_active_validators.len(),
            "shuffle_new_committee: read active validators"
        );

        if all_active_validators.is_empty() {
            // query ALL validators (Any status) to understand their statuses
            let all_validators = self.get_all_validators()?;

            error!(
                target: "engine",
                total_validators = all_validators.len(),
                statuses = ?all_validators
                    .iter()
                    .map(|v| (v.validatorAddress, v.currentStatus))
                    .collect::<Vec<_>>(),
                "NO ACTIVE VALIDATORS - dumping all validator statuses"
            );
        }

        // create seed from hashed bls agg signature
        let mut seed = [0; 32];
        seed.copy_from_slice(randomness.as_slice());
        trace!(target: "engine", ?seed, "seed after");

        // used as deterministic randomness
        let mut rng = StdRng::from_seed(seed);

        // 1) separate active and pending validators
        // 2) check if active length is sufficient
        // 3) if missing, randomly select from the pending validators
        let (pending_exit, mut active_validators): (Vec<_>, Vec<_>) = all_active_validators
            .into_iter()
            .partition(|v| v.currentStatus == ValidatorStatus::PendingExit);

        let active_validator_count = active_validators.len();
        let mut validators_for_shuffle = if active_validator_count >= new_committee_size {
            // enough active validators for next committee
            active_validators
        } else {
            // NOTE: already checked if active_validator_count >= new_committee_size above
            let num_missing = new_committee_size - active_validator_count;

            // randomly take enough pending exit validators to reach new committee size
            let random_pending = pending_exit.into_iter().choose_multiple(&mut rng, num_missing);
            active_validators.extend(random_pending);
            active_validators
        };

        // simple Fisher-Yates shuffle
        for i in (1..validators_for_shuffle.len()).rev() {
            let j = rng.random_range(0..=i);
            validators_for_shuffle.swap(i, j);
        }

        debug!(target: "engine",  "validators post-shuffle {:?}", validators_for_shuffle);

        let mut new_committee =
            validators_for_shuffle.into_iter().map(|v| v.validatorAddress).collect::<Vec<_>>();

        // trim the shuffled committee to maintain correct size
        new_committee.truncate(new_committee_size);

        trace!(target: "engine",  ?new_committee_size, ?new_committee, "truncated shuffle for new committee");

        Ok(new_committee)
    }

    /// Return the next committee size.
    ///
    /// This is isolated into a function and requires a fork to change.
    fn next_committee_size(&mut self) -> RaylsRethResult<usize> {
        // retrieve the current committee size
        let epoch = self.extract_epoch_from_nonce(self.ctx.nonce);

        // query contract's currentEpoch to detect desync
        let contract_epoch: u32 = self.get_current_epoch_number()?;
        info!(
            target: "engine",
            nonce_epoch = epoch,
            contract_epoch,
            "next_committee_size: epoch comparison"
        );

        if contract_epoch != epoch {
            error!(
                target: "engine",
                nonce_epoch = epoch,
                contract_epoch,
                "EPOCH DESYNC: contract currentEpoch != nonce epoch"
            );
        }

        let current_committee: Vec<ValidatorInfo> = self.get_epoch_committee_validators(epoch)?;

        info!(
            target: "engine",
            epoch,
            committee_size = current_committee.len(),
            "next_committee_size: read committee for epoch"
        );

        // this will fail on-chain if incorrect
        Ok(current_committee.len())
    }

    /// Extract the epoch number from a header's nonce.
    fn extract_epoch_from_nonce(&self, nonce: u64) -> u32 {
        rayls_infrastructure_types::nonce::unpack_nonce(nonce).0
    }

    /// Applies the pre-block call to the EIP-4788 consensus root contract (cancun).
    fn apply_consensus_root_contract_call(&mut self) -> Result<(), BlockExecutionError> {
        if !self.spec.is_cancun_active_at_timestamp(self.evm.block().timestamp().saturating_to()) {
            return Ok(());
        }

        let parent_beacon_block_root = self
            .ctx
            .parent_beacon_block_root
            .ok_or(BlockValidationError::MissingParentBeaconBlockRoot)?;

        trace!(target: "engine", block_number=?self.evm.block().number(), ?parent_beacon_block_root, "evaluating parent root");

        // if the block number is zero (genesis block) then the parent beacon block root must
        // be 0x0 and no system transaction may occur as per EIP-4788
        if self.evm.block().number() == U256::ZERO {
            if !parent_beacon_block_root.is_zero() {
                return Err(BlockValidationError::CancunGenesisParentBeaconBlockRootNotZero {
                    parent_beacon_block_root,
                }
                .into());
            }

            return Ok(());
        }

        let mut res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            BEACON_ROOTS_ADDRESS,
            parent_beacon_block_root.0.into(),
        ) {
            Ok(res) => res,
            Err(e) => {
                error!(target: "engine", "failed to apply consensus root contract call: {:?}", e);
                return Err(BlockValidationError::BeaconRootContractCall {
                    parent_beacon_block_root: Box::new(parent_beacon_block_root),
                    message: e.to_string(),
                }
                .into());
            }
        };

        // NOTE: revm currently marks the caller and block beneficiary accounts as "touched"
        // after the above transact calls, and includes them in the result.
        //
        // Cleanup state here to make sure that changeset only includes the changed
        // contract storage.
        res.state.retain(|addr, _| *addr == BEACON_ROOTS_ADDRESS);
        trace!(target: "engine", ?res, "retained state");

        // notify state hook of state changes
        if let Some(hook) = &mut self.state_hook {
            hook.on_state(StateChangeSource::Transaction(self.receipts.len()), &res.state);
        }

        self.evm.db_mut().commit(res.state);

        Ok(())
    }

    /// Applies the pre-block call to the EIP-2935 blockhashes contract (pectra).
    fn apply_blockhashes_contract_call(&mut self) -> Result<(), BlockExecutionError> {
        trace!(target: "engine", "applying blockhashes contract call");
        if !self.spec.is_prague_active_at_timestamp(self.evm.block().timestamp().saturating_to()) {
            return Ok(());
        }

        // if the block number is zero (genesis block) then no system transaction may occur as per
        // EIP-2935
        if self.evm.block().number() == U256::ZERO {
            return Ok(());
        }

        let mut result_and_state = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            HISTORY_STORAGE_ADDRESS,
            self.ctx.parent_hash.into(),
        ) {
            Ok(res) => res,
            Err(e) => {
                error!(target: "engine", "failed to apply blockhashes contract call: {:?}", e);
                return Err(
                    BlockValidationError::BlockHashContractCall { message: e.to_string() }.into()
                );
            }
        };

        trace!(target: "engine", "result and state before: \n{:#?}", result_and_state);
        // NOTE: revm currently marks the caller and block beneficiary accounts as "touched"
        // after the above transact calls, and includes them in the result.
        //
        // Cleanup state here to make sure that changeset only includes the changed
        // contract storage.
        result_and_state.state.retain(|addr, _| *addr == HISTORY_STORAGE_ADDRESS);
        trace!(target: "engine", "result and state after: \n{:#?}", result_and_state);

        // notify state hook of state changes
        if let Some(hook) = &mut self.state_hook {
            hook.on_state(
                StateChangeSource::Transaction(self.receipts.len()),
                &result_and_state.state,
            );
        }

        self.evm.db_mut().commit(result_and_state.state);

        Ok(())
    }

    /// get's validators with ValidatorStatus::Active from ConsensusRegistry precomplie
    fn get_active_validators(&mut self) -> RaylsRethResult<Vec<ValidatorInfo>> {
        self.get_validators(ValidatorStatus::Active)
    }

    /// get's validators with ValidatorStatus::Any from ConsensusRegistry precomplie
    fn get_all_validators(&mut self) -> RaylsRethResult<Vec<ValidatorInfo>> {
        self.get_validators(ValidatorStatus::Any)
    }

    /// get's validators with given status from ConsensusRegistry precomplie
    fn get_validators(&mut self, status: ValidatorStatus) -> RaylsRethResult<Vec<ValidatorInfo>> {
        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsCall { status: status.into() }.abi_encode().into();
        self.get_consensus_registry_data(calldata)
    }

    /// get's epoch committee validators for given epoch from ConsensusRegistry precomplie
    fn get_epoch_committee_validators(
        &mut self,
        epoch: u32,
    ) -> RaylsRethResult<Vec<ValidatorInfo>> {
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.get_consensus_registry_data(calldata)
    }

    /// get's current epoch number from ConsensusRegistry precomplie
    fn get_current_epoch_number(&mut self) -> RaylsRethResult<u32> {
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        self.get_consensus_registry_data(calldata)
    }

    /// get's parsed data from ConsensusRegistry precomplie
    fn get_consensus_registry_data<T>(&mut self, calldata: Bytes) -> RaylsRethResult<T>
    where
        T: SolValue + From<<T::SolType as alloy::sol_types::SolType>::RustType>,
    {
        self.get_precompile_data(CONSENSUS_REGISTRY_ADDRESS, calldata)
    }

    /// get's parsed data from given precomplie
    fn get_precompile_data<T>(&mut self, contract: Address, calldata: Bytes) -> RaylsRethResult<T>
    where
        T: SolValue + From<<T::SolType as alloy::sol_types::SolType>::RustType>,
    {
        let state = self.read_state_on_chain(SYSTEM_ADDRESS, contract, calldata)?;

        let res = alloy::sol_types::SolValue::abi_decode(&state)?;

        Ok(res)
    }

    /// Read state on-chain.
    fn read_state_on_chain(
        &mut self,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> RaylsRethResult<Bytes> {
        // read from state
        let res = match self.evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state on chain: {}", e);
                return Err(RaylsRethError::EVMCustom(format!(
                    "failed to read state on chain: {e}"
                )));
            }
        };

        // retrieve data from execution result
        let data = match res.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => {
                // fatal error
                error!(target: "engine", "error reading state on chain: {:?}", e);
                return Err(RaylsRethError::EVMCustom(format!(
                    "error reading state on chain: {e:?}"
                )));
            }
        };

        Ok(data)
    }
}

// alloy-evm
impl<'db, DB, E, Spec, R> BlockExecutor for RaylsBlockExecutor<E, Spec, R>
where
    DB: Database + 'db,
    E: Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>,
    >,
    Spec: EthereumHardforks + RaylsHardforks,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
{
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;
    type Evm = E;
    type Result = EthTxResult<<E as Evm>::HaltReason, TxType>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag =
            self.spec.is_spurious_dragon_active_at_block(self.evm.block().number().saturating_to());
        self.evm.db_mut().set_state_clear_flag(state_clear_flag);

        // log newly activated hardforks on the first batch of an output
        if self.ctx.first_batch() {
            let block_number = self.evm.block().number().saturating_to::<u64>();
            let parent_number = block_number.saturating_sub(1);
            for fork in self.spec.newly_activated_forks(parent_number, block_number) {
                info!(
                    target: "engine",
                    ?fork,
                    block_number,
                    "Rayls hardfork activated"
                );
            }
        }

        // apply any one-shot hardfork state migrations newly activated at this block
        if self.ctx.first_batch() {
            let block_number = self.evm.block().number().saturating_to::<u64>();
            let parent_number = block_number.saturating_sub(1);
            hardforks::apply_activated_migrations(
                &self.spec,
                &mut *self.evm.db_mut(),
                &mut self.state_hook,
                self.receipts.len(),
                parent_number,
                block_number,
            )?;
        }

        // apply system calls and cleanup state
        if self.ctx.first_batch() {
            // only write consensus root once per output
            self.apply_consensus_root_contract_call()?;
        }

        // apply blockhashes cleanup state after
        self.apply_blockhashes_contract_call()?;

        Ok(())
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, tx) = tx.into_parts();

        let block_available_gas = self.evm.block().gas_limit() - self.gas_used;
        if tx.tx().gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: tx.tx().gas_limit(),
                block_available_gas,
            }
            .into());
        }

        let result = self.evm.transact(tx_env).map_err(|err| {
            let hash = tx.tx().trie_hash();
            BlockExecutionError::evm(err, hash)
        })?;

        Ok(EthTxResult {
            result,
            blob_gas_used: tx.tx().blob_gas_used().unwrap_or_default(),
            tx_type: tx.tx().tx_type(),
        })
    }

    fn commit_transaction(&mut self, output: Self::Result) -> Result<u64, BlockExecutionError> {
        let EthTxResult { result: ResultAndState { result, state }, tx_type, .. } = output;

        // notify state hook of per-transaction state changes
        if let Some(hook) = &mut self.state_hook {
            hook.on_state(StateChangeSource::Transaction(self.receipts.len()), &state);
        }

        let gas_used = result.gas_used();

        // append gas used
        self.gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx_type,
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));

        // Commit the state changes.
        self.evm.db_mut().commit(state);

        Ok(gas_used)
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        // don't support prague deposit requests
        let requests = Requests::default();

        // potentially close epoch boundary
        if let Some(randomness) = self.ctx.close_epoch {
            debug!(target: "engine", ?randomness, "ctx indicates close epoch");
            let tally = self.ctx.close_epoch_tally.clone().unwrap_or_default();
            self.apply_consensus_block_rewards(tally).map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
            })?;

            self.apply_closing_epoch_contract_call(randomness).map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
            })?;

            // best-effort, never blocks epoch close; testnet archive replay skips the
            // historical reward-distribution outage window to match canonical state
            #[cfg(feature = "archive-replay")]
            let skip_distribution =
                self.spec.is_tokenomics_outage_block(self.evm.block().number().saturating_to());
            #[cfg(not(feature = "archive-replay"))]
            let skip_distribution = false;
            if !skip_distribution {
                match self.apply_reward_distribution() {
                    Ok(()) => {}
                    Err(e) => {
                        error!(target: "engine", "reward distribution failed (non-fatal): {:?}", e);
                    }
                }
            }

            // merge transitions into bundle state
            self.evm.db_mut().merge_transitions(BundleRetention::Reverts);
        }

        Ok((
            self.evm,
            BlockExecutionResult {
                receipts: self.receipts,
                requests,
                gas_used: self.gas_used,
                blob_gas_used: 0,
            },
        ))
    }

    fn set_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.state_hook = hook;
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }

    fn receipts(&self) -> &[Self::Receipt] {
        &self.receipts
    }
}

/// Block builder for Rayls.
#[derive(Debug, Clone)]
pub struct RaylsBlockAssembler<ChainSpec = RaylsChainSpec> {
    /// The chainspec.
    pub chain_spec: Arc<ChainSpec>,
}

impl<ChainSpec> RaylsBlockAssembler<ChainSpec> {
    /// Creates a new [`RaylsBlockAssembler`].
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}

// reth-evm
impl<F, ChainSpec> BlockAssembler<F> for RaylsBlockAssembler<ChainSpec>
where
    F: for<'a> BlockExecutorFactory<
        ExecutionCtx<'a> = RaylsBlockExecutionCtx,
        Transaction = TransactionSigned,
        Receipt = Receipt,
    >,
    ChainSpec: EthChainSpec + EthereumHardforks + RaylsHardforks,
{
    type Block = Block<TransactionSigned>;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, F>,
    ) -> Result<Block<TransactionSigned>, BlockExecutionError> {
        let BlockAssemblerInput {
            evm_env,
            execution_ctx: ctx,
            transactions,
            output: BlockExecutionResult { receipts, gas_used, .. },
            state_root,
            ..
        } = input;

        let timestamp = evm_env.block_env.timestamp().saturating_to();
        let transactions_root = proofs::calculate_transaction_root(&transactions);
        let receipts_root = Receipt::calculate_receipt_root_no_memo(receipts);
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|r| r.logs()));

        // set excess blob gas 0
        let excess_blob_gas = Some(0);
        let blob_gas_used =
            Some(transactions.iter().map(|tx| tx.blob_gas_used().unwrap_or_default()).sum());

        // Rayls-specific values
        let nonce = ctx.nonce.into(); // subdag leader's nonce: ((epoch as u64) << 32) | self.round as u64
        let difficulty = ctx.difficulty; // worker id and batch index

        // use keccak256(bls_sig) if closing epoch or Bytes::default
        let extra_data = ctx.close_epoch.map(|hash| hash.to_vec().into()).unwrap_or_default();

        let (withdrawals, withdrawals_root) =
            match (ctx.close_epoch, ctx.close_epoch_tally.as_ref()) {
                (Some(_), Some(tally)) => {
                    info!(target: "engine", ?tally, "building withdrawals for closed epoch");
                    let withdrawals = build_withdrawals(tally);
                    let withdrawals_root = calculate_withdrawals_root(withdrawals.as_ref());
                    (Some(withdrawals), Some(withdrawals_root))
                }
                _ => (Some(Withdrawals::default()), Some(EMPTY_WITHDRAWALS)),
            };

        let header = ExecHeader {
            parent_hash: ctx.parent_hash,
            ommers_hash: ctx.ommers_hash,
            beneficiary: evm_env.block_env.beneficiary(),
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp,
            mix_hash: evm_env.block_env.prevrandao().unwrap_or_default(),
            nonce,
            base_fee_per_gas: Some(evm_env.block_env.basefee()),
            number: evm_env.block_env.number().saturating_to(),
            gas_limit: evm_env.block_env.gas_limit(),
            difficulty,
            gas_used: *gas_used,
            extra_data,
            parent_beacon_block_root: ctx.parent_beacon_block_root,
            blob_gas_used,
            excess_blob_gas,
            requests_hash: ctx.requests_hash,
        };

        Ok(Block {
            header,
            body: BlockBody { transactions, ommers: Default::default(), withdrawals },
        })
    }
}
