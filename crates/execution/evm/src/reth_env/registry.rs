use crate::{
    error::{RaylsRethError, RaylsRethResult},
    evm::RaylsEvm,
    reth_env::RethEnv,
    system_calls::{
        ConsensusRegistry::{self},
        EpochState, CONSENSUS_REGISTRY_ADDRESS, SYSTEM_ADDRESS,
    },
};
use alloy::{primitives::Bytes, sol_types::SolCall};
use alloy_evm::Evm;
use eyre::OptionExt;
use rayls_infrastructure_types::{Address, Epoch, ExecHeader};
use reth_evm::{ConfigureEvm, EvmFactory};
use reth_provider::{DatabaseProviderFactory, LatestStateProvider, StateProviderBox};
use reth_revm::{
    context::result::{ExecutionResult, ResultAndState},
    database::StateProviderDatabase,
    State,
};
use tracing::{error, info, trace};

impl RethEnv {
    /// Read the latest committee and epoch information from the [ConsensusRegistry] on-chain.
    ///
    /// The protocol needs the BLS pubkey for the authorities.
    /// - get current epoch info
    /// - getValidator token id by address
    /// - getValidator info by token id
    pub fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        // create EVM with latest state
        let canonical_tip = self.canonical_tip();
        trace!(target: "engine", ?canonical_tip, "retrieving epoch state from canonical tip");

        let db_provider = self.blockchain_provider.database_provider_ro()?;
        let state_provider: StateProviderBox = Box::new(LatestStateProvider::new(db_provider));

        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder()
            .with_database(state)
            .with_bundle_update()
            .without_state_clear()
            .build();
        trace!(target: "engine", state=?db.bundle_state, hashes=?db.block_hashes, "retrieving epoch state from canonical tip");
        let mut rayls_evm = self
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.evm_config.evm_env(&canonical_tip)?);

        // current epoch number
        let epoch = self.get_current_epoch_number(&mut rayls_evm)?;

        // current epoch info
        let epoch_info = self.get_current_epoch_info(&mut rayls_evm)?;
        info!(target: "engine", ?epoch, ?epoch_info, "retrieved epoch info from canonical tip for next epoch");

        // closing timestamp for previous epoch; falls back to genesis when the
        // target block predates a snapshot-bootstrapped chain.
        let epoch_start = self
            .header_by_number(epoch_info.blockHeight.saturating_sub(1))?
            .or(self.header_by_number(0)?)
            .ok_or_eyre("failed to retrieve closing epoch information")?
            .timestamp;

        // retrieve the committee
        let validators = self.get_committee_validators_by_epoch(epoch, &mut rayls_evm)?;
        let epoch_state = EpochState { epoch, epoch_info, validators, epoch_start };
        info!(target: "engine", ?epoch_state, "returning epoch state from canonical tip");

        Ok(epoch_state)
    }

    /// Read the latest committee and epoch information from the [ConsensusRegistry] on-chain.
    pub fn validators_for_epoch(
        &self,
        epoch: u32,
    ) -> eyre::Result<Vec<ConsensusRegistry::ValidatorInfo>> {
        // create EVM with latest state
        let canonical_tip = self.canonical_tip();
        info!(
            target: "engine",
            epoch,
            canonical_tip_num = canonical_tip.number,
            canonical_tip_hash = ?canonical_tip.hash(),
            "validators_for_epoch: reading from canonical tip",
        );

        let db_provider = self.blockchain_provider.database_provider_ro()?;
        let state_provider: StateProviderBox = Box::new(LatestStateProvider::new(db_provider));
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder()
            .with_database(state)
            .with_bundle_update()
            .without_state_clear()
            .build();
        let mut rayls_evm = self
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.evm_config.evm_env(&canonical_tip)?);

        let result = self.get_committee_validators_by_epoch(epoch, &mut rayls_evm)?;

        info!(
            target: "engine",
            epoch,
            validator_count = result.len(),
            "validators_for_epoch: contract returned validators",
        );
        Ok(result)
    }

    /// Extract the epoch number from a header's nonce.
    pub fn extract_epoch_from_header(header: &ExecHeader) -> Epoch {
        rayls_infrastructure_types::nonce::unpack_nonce(header.nonce.into()).0
    }

    /// Read the current epoch number from the [ConsensusRegistry] on-chain.
    fn get_current_epoch_number<DB>(&self, evm: &mut RaylsEvm<DB>) -> eyre::Result<u32>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        self.call_consensus_registry::<_, u32>(evm, calldata)
    }

    /// Read the current epoch info from the [ConsensusRegistry] on-chain.
    fn get_current_epoch_info<DB>(
        &self,
        evm: &mut RaylsEvm<DB>,
    ) -> eyre::Result<ConsensusRegistry::EpochInfo>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into();
        self.call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(evm, calldata)
    }

    /// Retrieve all `ValidatorInfo` in the committee for the provided epoch.
    fn get_committee_validators_by_epoch<DB>(
        &self,
        epoch: Epoch,
        evm: &mut RaylsEvm<DB>,
    ) -> eyre::Result<Vec<ConsensusRegistry::ValidatorInfo>>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(evm, calldata)
    }

    /// Helper function to call `ConsensusRegistry` state on-chain.
    pub(crate) fn call_consensus_registry<DB, T>(
        &self,
        evm: &mut RaylsEvm<DB>,
        calldata: Bytes,
    ) -> eyre::Result<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state =
            self.read_state_on_chain(evm, SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;

        // retrieve data from state
        match state.result {
            ExecutionResult::Success { output, .. } => {
                let data = output.into_data();
                // use SolValue to decode the result
                let decoded = alloy::sol_types::SolValue::abi_decode(&data)?;
                Ok(decoded)
            }
            e => Err(eyre::eyre!("failed to read validators from state: {e:?}")),
        }
    }

    /// Read state on-chain.
    fn read_state_on_chain<DB>(
        &self,
        evm: &mut RaylsEvm<DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> RaylsRethResult<ResultAndState>
    where
        DB: alloy_evm::Database,
    {
        // read from state
        let res = match evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state: {}", e);
                return Err(RaylsRethError::EVMCustom(format!(
                    "system call failed reading state: {e}"
                )));
            }
        };

        Ok(res)
    }
}
