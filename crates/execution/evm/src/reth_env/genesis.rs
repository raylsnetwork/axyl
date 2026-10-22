use crate::{
    chainspec::RaylsChainSpec,
    evm::RaylsEvmConfig,
    reth_env::RethEnv,
    system_calls::{
        ConsensusRegistry::{self},
        DelegationPool, FeeAggregator, NativeTokenController, RLSAccumulator, RLSToken,
        RewardDistributor, CONSENSUS_REGISTRY_ADDRESS, DELEGATION_POOL_ADDRESS,
        FEE_AGGREGATOR_ADDRESS, NATIVE_TOKEN_CONTROLLER_ADDRESS, REWARD_DISTRIBUTOR_ADDRESS,
        RLS_ACCUMULATOR_ADDRESS, RLS_ADDRESS,
    },
};
use alloy::{
    hex::{self, ToHexExt},
    sol_types::{SolCall, SolConstructor},
};
use alloy_evm::Evm;
use eyre::OptionExt;
use rayls_infrastructure_config::{
    GenesisDealing, NodeInfo, BLSG1_JSON, CONSENSUS_REGISTRY_JSON, DELEGATION_POOL_IMPL_ADDRESS,
    DELEGATION_POOL_JSON, DKG_MANAGER_ADDRESS, DKG_MANAGER_JSON, ERC1967PROXY_JSON,
    FEE_AGGREGATOR_IMPL_ADDRESS, FEE_AGGREGATOR_JSON, NATIVE_TOKEN_CONTROLLER_IMPL_ADDRESS,
    NATIVE_TOKEN_CONTROLLER_JSON, REWARD_DISTRIBUTOR_IMPL_ADDRESS, REWARD_DISTRIBUTOR_JSON,
    RLS_ACCUMULATOR_IMPL_ADDRESS, RLS_ACCUMULATOR_JSON, RLS_IMPL_ADDRESS, RLS_JSON,
    USDR_PRECOMPILE_ADDRESS,
};
use rayls_infrastructure_types::{address, Address, Bytes, Genesis, GenesisAccount, B256, U256};
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_evm::{ConfigureEvm, EvmFactory};
use reth_revm::{
    bytecode::Bytecode as RevmBytecode,
    context::result::ResultAndState,
    db::{states::bundle_state::BundleRetention, BundleState, CacheDB, EmptyDBTyped},
    state::AccountInfo,
    DatabaseCommit, State,
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use tracing::debug;

/// Throwaway EOA used as the deployer for the upgradable precompiles
const PRECOMPILE_DEPLOYER: Address = address!("d35150d35150d35150d35150d35150d35150d351");

impl RethEnv {
    /// Return a genesis alloc with the consensus registry and UUPS precompile
    /// proxies deployed, their storage captured from a pre-genesis EVM sim.
    pub fn create_consensus_registry_genesis_accounts(
        validators: Vec<NodeInfo>,
        genesis: Genesis,
        initial_stake_config: ConsensusRegistry::StakeConfig,
        owner_address: Address,
        network_admin: Address,
        rls_prefunds: Vec<(Address, U256)>,
    ) -> eyre::Result<Genesis> {
        // the genesis validators' encryption keys, registered in DkgManager from genesis
        let dkg_manager_storage = dkg_manager_genesis_storage(&validators)?;
        // each genesis validator's DKG submitter gets a native balance for its transactions
        let dkg_submitters: Vec<(Address, GenesisAccount)> = validators
            .iter()
            .filter(|v| !v.dkg_submitter.is_zero())
            .map(|v| {
                let balance = U256::from(crate::reth_env::dkg_manager::GENESIS_SUBMITTER_BALANCE);
                (v.dkg_submitter, GenesisAccount::default().with_balance(balance))
            })
            .collect();
        let total_stake_for_transfer = initial_stake_config
            .stakeAmount
            .checked_mul(U256::from(validators.len()))
            .ok_or_eyre("overflow computing total validator stake")?;
        let total_rls_prefund = rls_prefunds
            .iter()
            .try_fold(U256::ZERO, |acc, (_, amt)| acc.checked_add(*amt))
            .ok_or_eyre("overflow summing --rls-accounts balances")?;
        let total_rls_supply = total_stake_for_transfer
            .checked_add(total_rls_prefund)
            .ok_or_eyre("overflow computing total RLS supply")?;

        // Helper: guarantees the sim exercises the same Solidity
        // semantics as the final chain.
        let deployed_runtime = |json: &str, label: &str| -> eyre::Result<Vec<u8>> {
            let binding = Self::fetch_value_from_json_str(json, Some("deployedBytecode.object"))?;
            hex::decode(binding.as_str().ok_or_else(|| eyre::eyre!("invalid {label} json"))?)
                .map_err(Into::into)
        };

        let rls_impl_runtimecode = deployed_runtime(RLS_JSON, "rls")?;
        let ntc_impl_runtimecode =
            deployed_runtime(NATIVE_TOKEN_CONTROLLER_JSON, "native_token_controller")?;
        let fa_impl_runtimecode = deployed_runtime(FEE_AGGREGATOR_JSON, "fee_aggregator")?;
        let rd_impl_runtimecode = deployed_runtime(REWARD_DISTRIBUTOR_JSON, "reward_distributor")?;
        let dp_impl_runtimecode = deployed_runtime(DELEGATION_POOL_JSON, "delegation_pool")?;
        let acc_impl_runtimecode = deployed_runtime(RLS_ACCUMULATOR_JSON, "rls_accumulator")?;

        // Runtime bytecode for the ERC1967 proxy
        let proxy_runtimecode_binding =
            Self::fetch_value_from_json_str(ERC1967PROXY_JSON, Some("deployedBytecode.object"))?;
        let proxy_runtimecode =
            hex::decode(proxy_runtimecode_binding.as_str().ok_or_eyre("invalid proxy json")?)?;

        // ERC-1967 implementation slot = keccak256("eip1967.proxy.implementation") - 1
        let erc1967_impl_slot = B256::new([
            0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d,
            0xb9, 0x8d, 0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50,
            0x5d, 0x38, 0x2b, 0xbc,
        ]);

        // chain spec for evm_env: only hardfork/chain-id info matters, alloc is
        // irrelevant because we seed the tmp DB directly below.
        let base_chain: Arc<RethChainSpec> = Arc::new(genesis.clone().into());
        let tmp_chain: Arc<RaylsChainSpec> = Arc::new(RaylsChainSpec::builder(base_chain).build());
        let evm_config = RaylsEvmConfig::new(tmp_chain.clone(), Default::default());
        let sealed_header = tmp_chain.sealed_genesis_header();
        let evm_env = evm_config.evm_env(&sealed_header)?;

        // seed the in-memory DB with impl bytecodes (for proxy delegatecalls),
        // the RLS proxy shim (so the sim's RLS calls land on real impl code),
        // and the precompile deployer EOA's balance.
        let mut cache_db = CacheDB::new(EmptyDBTyped::<core::convert::Infallible>::new());

        let seed_code = |db: &mut CacheDB<EmptyDBTyped<core::convert::Infallible>>,
                         addr: Address,
                         bytecode: &[u8]| {
            let code = RevmBytecode::new_raw(alloy::primitives::Bytes::copy_from_slice(bytecode));
            let code_hash = code.hash_slow();
            db.insert_account_info(
                addr,
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash,
                    account_id: None,
                    code: Some(code),
                },
            );
        };

        seed_code(&mut cache_db, RLS_IMPL_ADDRESS, &rls_impl_runtimecode);
        seed_code(&mut cache_db, NATIVE_TOKEN_CONTROLLER_IMPL_ADDRESS, &ntc_impl_runtimecode);
        seed_code(&mut cache_db, FEE_AGGREGATOR_IMPL_ADDRESS, &fa_impl_runtimecode);
        seed_code(&mut cache_db, REWARD_DISTRIBUTOR_IMPL_ADDRESS, &rd_impl_runtimecode);
        seed_code(&mut cache_db, DELEGATION_POOL_IMPL_ADDRESS, &dp_impl_runtimecode);
        seed_code(&mut cache_db, RLS_ACCUMULATOR_IMPL_ADDRESS, &acc_impl_runtimecode);
        seed_code(&mut cache_db, RLS_ADDRESS, &proxy_runtimecode);

        // RLS proxy's ERC-1967 impl slot → points to RLS_IMPL_ADDRESS
        cache_db
            .insert_account_storage(
                RLS_ADDRESS,
                U256::from_be_bytes(erc1967_impl_slot.0),
                U256::from_be_slice(RLS_IMPL_ADDRESS.as_slice()),
            )
            .expect("infallible on EmptyDB");

        // precompile deployer EOA balance
        cache_db.insert_account_info(
            PRECOMPILE_DEPLOYER,
            AccountInfo {
                balance: U256::from(10).pow(U256::from(24)),
                nonce: 0,
                ..Default::default()
            },
        );

        let mut db = State::builder()
            .with_database(cache_db)
            .with_bundle_update()
            .without_state_clear()
            .build();

        // deploy blsg1 library (owner nonce 0)
        let blsg1_address = {
            let mut rayls_evm = evm_config.evm_factory().create_evm(&mut db, evm_env.clone());
            let blsg1_initcode_binding =
                Self::fetch_value_from_json_str(BLSG1_JSON, Some("bytecode.object"))?;
            let blsg1_initcode =
                hex::decode(blsg1_initcode_binding.as_str().ok_or_eyre("invalid blsg1 json")?)?;
            let ResultAndState { result, state } =
                rayls_evm.transact_pre_genesis_create(owner_address, blsg1_initcode.into())?;
            debug!(target: "engine", "create blsg1 library result:\n{:#?}", result);
            rayls_evm.db_mut().commit(state);
            owner_address.create(0)
        };

        // deploy RLS implementation (owner nonce 1, discarded; only bumps the
        // owner's nonce so the next CREATE lands at owner.create(2))
        {
            let rls_initcode_binding =
                Self::fetch_value_from_json_str(RLS_JSON, Some("bytecode.object"))?;
            let rls_initcode =
                hex::decode(rls_initcode_binding.as_str().ok_or_eyre("invalid rls json")?)?;
            let mut rayls_evm = evm_config.evm_factory().create_evm(&mut db, evm_env.clone());
            let ResultAndState { result, state } =
                rayls_evm.transact_pre_genesis_create(owner_address, rls_initcode.into())?;
            debug!(target: "engine", "deploy RLS implementation result:\n{:#?}", result);
            rayls_evm.db_mut().commit(state);
        }

        // deploy RLS proxy via ERC1967Proxy(impl, initialize(owner, owner, total_supply))
        // (owner nonce 2)
        {
            let proxy_initcode_binding =
                Self::fetch_value_from_json_str(ERC1967PROXY_JSON, Some("bytecode.object"))?;
            let proxy_initcode =
                hex::decode(proxy_initcode_binding.as_str().ok_or_eyre("invalid proxy json")?)?;
            let initialize_calldata = RLSToken::initializeCall {
                admin: owner_address,
                treasury: owner_address,
                initialSupply: total_rls_supply,
            }
            .abi_encode();
            let proxy_constructor_args = alloy::sol_types::SolValue::abi_encode_params(&(
                RLS_IMPL_ADDRESS,
                alloy::primitives::Bytes::from(initialize_calldata),
            ));
            let mut proxy_deploy = proxy_initcode;
            proxy_deploy.extend(proxy_constructor_args);
            let mut rayls_evm = evm_config.evm_factory().create_evm(&mut db, evm_env.clone());
            let ResultAndState { result, state } =
                rayls_evm.transact_pre_genesis_create(owner_address, proxy_deploy.into())?;
            debug!(target: "engine", "deploy RLS proxy result:\n{:#?}", result);
            rayls_evm.db_mut().commit(state);
        }

        let tmp_rls_proxy_address = owner_address.create(2);

        // transfer total_stake from owner (treasury) to CONSENSUS_REGISTRY_ADDRESS
        {
            let transfer_calldata = RLSToken::transferCall {
                to: CONSENSUS_REGISTRY_ADDRESS,
                amount: total_stake_for_transfer,
            }
            .abi_encode();
            let mut rayls_evm = evm_config.evm_factory().create_evm(&mut db, evm_env.clone());
            let ResultAndState { result, state } = rayls_evm.transact_pre_genesis_call(
                owner_address,
                tmp_rls_proxy_address,
                transfer_calldata.into(),
            )?;
            debug!(target: "engine", "transfer RLS to ConsensusRegistry result:\n{:#?}", result);
            rayls_evm.db_mut().commit(state);
        }

        // transfer RLS from owner (treasury) to each --rls-accounts entry
        for (recipient, amount) in &rls_prefunds {
            let transfer_calldata =
                RLSToken::transferCall { to: *recipient, amount: *amount }.abi_encode();
            let mut rayls_evm = evm_config.evm_factory().create_evm(&mut db, evm_env.clone());
            let ResultAndState { result, state } = rayls_evm.transact_pre_genesis_call(
                owner_address,
                tmp_rls_proxy_address,
                transfer_calldata.into(),
            )?;
            debug!(target: "engine", ?recipient, ?amount, "pre-genesis RLS prefund transfer: {:#?}", result);
            rayls_evm.db_mut().commit(state);
        }

        // prepare registry deployment
        let (validators, proofs): (Vec<_>, Vec<_>) = validators
            .iter()
            .map(|v| {
                let validator = ConsensusRegistry::ValidatorInfo {
                    blsPubkey: v.bls_public_key.to_bytes().into(),
                    validatorAddress: v.execution_address,
                    activationEpoch: 0,
                    exitEpoch: 0,
                    currentStatus: ConsensusRegistry::ValidatorStatus::Active,
                    isRetired: false,
                    isDelegated: false,
                    stakeVersion: 0,
                };
                let proof = ConsensusRegistry::ProofOfPossession {
                    uncompressedPubkey: v.bls_public_key.serialize().into(),
                    uncompressedSignature: v.proof_of_possession.serialize().into(),
                };

                (validator, proof)
            })
            .unzip();

        debug!(target: "engine", ?initial_stake_config, "calling constructor for consensus registry");

        let constructor_args = ConsensusRegistry::constructorCall {
            rls_: RLS_ADDRESS,
            genesisConfig_: initial_stake_config,
            initialValidators_: validators,
            proofsOfPossession: proofs,
            owner_: owner_address,
        }
        .abi_encode();

        let registry_initcode_binding =
            Self::fetch_value_from_json_str(CONSENSUS_REGISTRY_JSON, Some("bytecode.object"))?;
        let registry_initcode_str =
            registry_initcode_binding.as_str().ok_or_eyre("invalid registry json")?;
        // link the BlsG1 library address into the registry bytecode
        let linked_registry_initcode =
            Self::link_solidity_library(registry_initcode_str, &blsg1_address.encode_hex())?;

        let mut create_registry = linked_registry_initcode;
        create_registry.extend(constructor_args);

        // Registry with BLS PoPs exceeds EIP-170, so raise the code size limit
        // for this tmp chain only.
        let mut registry_evm_env = evm_env.clone();
        registry_evm_env.cfg_env.limit_contract_code_size = Some(0x12000000);

        let (tmp_registry_address, registry_deployed_runtimecode) = {
            let mut rayls_evm = evm_config.evm_factory().create_evm(&mut db, registry_evm_env);
            let ResultAndState { result, state } =
                rayls_evm.transact_pre_genesis_create(owner_address, create_registry.into())?;
            debug!(target: "engine", "create consensus registry result:\n{:#?}", result);

            // Extract the actual deployed bytecode from the CREATE result.
            // This bytecode has immutables (like `_rls`) correctly filled in by the constructor.
            // Using the artifact's `deployedBytecode.object` would leave `_rls = address(0)`,
            // causing `safeTransferFrom` to revert when validators try to stake.
            let deployed_bytecode =
                result.output().ok_or_eyre("ConsensusRegistry CREATE had no output")?.to_vec();

            rayls_evm.db_mut().commit(state);

            // ConsensusRegistry is the final CREATE by owner on the tmp chain:
            //   0: BlsG1, 1: RLS impl, 2: RLS proxy, 3: transfer to ConsensusRegistry, 4..(4+N-1):
            // N transfers for --rls-accounts entries, registry CREATE.
            let registry_create_nonce = 4u64 + rls_prefunds.len() as u64;
            (owner_address.create(registry_create_nonce), deployed_bytecode)
        };

        // ── upgradable precompile proxies ────────────────────────────────────
        let proxy_initcode_binding =
            Self::fetch_value_from_json_str(ERC1967PROXY_JSON, Some("bytecode.object"))?;
        let proxy_initcode =
            hex::decode(proxy_initcode_binding.as_str().ok_or_eyre("invalid proxy json")?)?;

        let deploy_proxy = |db: &mut State<_>,
                            impl_addr: Address,
                            initialize_calldata: Vec<u8>,
                            label: &str|
         -> eyre::Result<Vec<u8>> {
            let proxy_constructor_args = alloy::sol_types::SolValue::abi_encode_params(&(
                impl_addr,
                alloy::primitives::Bytes::from(initialize_calldata),
            ));
            let mut proxy_deploy = proxy_initcode.clone();
            proxy_deploy.extend(proxy_constructor_args);
            let mut rayls_evm = evm_config.evm_factory().create_evm(db, evm_env.clone());
            let ResultAndState { result, state } =
                rayls_evm.transact_pre_genesis_create(PRECOMPILE_DEPLOYER, proxy_deploy.into())?;
            let deployed = result.output().ok_or_eyre("proxy CREATE had no output")?.to_vec();
            debug!(target: "engine", label, "deploy proxy result:\n{:#?}", result);
            rayls_evm.db_mut().commit(state);
            Ok(deployed)
        };

        let call_admin = |db: &mut State<_>,
                          target: Address,
                          calldata: Vec<u8>,
                          label: &str|
         -> eyre::Result<()> {
            let mut rayls_evm = evm_config.evm_factory().create_evm(db, evm_env.clone());
            let ResultAndState { result, state } =
                rayls_evm.transact_pre_genesis_call(network_admin, target, calldata.into())?;
            debug!(target: "engine", label, "admin call result:\n{:#?}", result);
            rayls_evm.db_mut().commit(state);
            Ok(())
        };

        // deployer_nonce 0: NativeTokenController proxy
        let ntc_proxy_code = deploy_proxy(
            &mut db,
            NATIVE_TOKEN_CONTROLLER_IMPL_ADDRESS,
            NativeTokenController::initializeCall { admin: network_admin }.abi_encode(),
            "NativeTokenController",
        )?;
        let tmp_ntc_proxy = PRECOMPILE_DEPLOYER.create(0);

        // deployer_nonce 1: FeeAggregator proxy
        let fa_proxy_code = deploy_proxy(
            &mut db,
            FEE_AGGREGATOR_IMPL_ADDRESS,
            FeeAggregator::initializeCall {
                rlsToken_: RLS_ADDRESS,
                algebraRouter_: Address::ZERO,
                rewardDistributor_: REWARD_DISTRIBUTOR_ADDRESS,
                ecosystemTreasury_: Address::ZERO,
                burnAddress_: address!("000000000000000000000000000000000000dead"),
                usdrToken_: USDR_PRECOMPILE_ADDRESS,
                config_: FeeAggregator::DistributionConfig {
                    validatorPoolBps: U256::from(5000),
                    ecosystemBps: U256::ZERO,
                    burnBps: U256::from(5000),
                },
                admin_: network_admin,
            }
            .abi_encode(),
            "FeeAggregator",
        )?;
        let tmp_fa_proxy = PRECOMPILE_DEPLOYER.create(1);

        // deployer_nonce 2: RewardDistributor proxy
        let rd_proxy_code = deploy_proxy(
            &mut db,
            REWARD_DISTRIBUTOR_IMPL_ADDRESS,
            RewardDistributor::initializeCall {
                rls_: RLS_ADDRESS,
                feeAggregator_: FEE_AGGREGATOR_ADDRESS,
                consensusRegistry_: CONSENSUS_REGISTRY_ADDRESS,
                delegationPool_: DELEGATION_POOL_ADDRESS,
                admin_: network_admin,
            }
            .abi_encode(),
            "RewardDistributor",
        )?;
        let tmp_rd_proxy = PRECOMPILE_DEPLOYER.create(2);

        // deployer_nonce 3: DelegationPool proxy
        let dp_proxy_code = deploy_proxy(
            &mut db,
            DELEGATION_POOL_IMPL_ADDRESS,
            DelegationPool::initializeCall {
                rls_: RLS_ADDRESS,
                consensusRegistry_: CONSENSUS_REGISTRY_ADDRESS,
                admin_: network_admin,
                config_: DelegationPool::DelegationConfig {
                    minDelegation: U256::from(10).pow(U256::from(18)), // 1 RLS
                    maxDelegation: U256::from(500_000_000u64) * U256::from(10).pow(U256::from(18)),
                    maxValidatorDelegation: U256::from(500_000_000u64)
                        * U256::from(10).pow(U256::from(18)),
                    unbondingEpochs: 14,
                    commissionDelayEpochs: 7,
                },
            }
            .abi_encode(),
            "DelegationPool",
        )?;
        let tmp_dp_proxy = PRECOMPILE_DEPLOYER.create(3);

        // deployer_nonce 4: RLSAccumulator proxy (calls forceApprove on RLS internally)
        let acc_proxy_code = deploy_proxy(
            &mut db,
            RLS_ACCUMULATOR_IMPL_ADDRESS,
            RLSAccumulator::initializeCall {
                rls_: RLS_ADDRESS,
                rewardDistributor_: REWARD_DISTRIBUTOR_ADDRESS,
                admin_: network_admin,
            }
            .abi_encode(),
            "RLSAccumulator",
        )?;
        let tmp_acc_proxy = PRECOMPILE_DEPLOYER.create(4);

        // Wire RewardDistributor → RLSAccumulator (from admin, which holds DEFAULT_ADMIN_ROLE).
        call_admin(
            &mut db,
            tmp_rd_proxy,
            RewardDistributor::setAccumulatorCall { newAccumulator: RLS_ACCUMULATOR_ADDRESS }
                .abi_encode(),
            "setAccumulator",
        )?;

        // Wire DelegationPool → RewardDistributor.
        call_admin(
            &mut db,
            tmp_dp_proxy,
            DelegationPool::setRewardDistributorCall {
                newRewardDistributor: REWARD_DISTRIBUTOR_ADDRESS,
            }
            .abi_encode(),
            "setRewardDistributor",
        )?;

        // single final merge: we only need the post-sim bundle state, no reverts
        // or intermediate unwinds to preserve.
        db.merge_transitions(BundleRetention::PlainState);
        let BundleState { state, contracts, reverts, state_size, reverts_size } = db.take_bundle();

        debug!(target: "engine", "contracts:\n{:#?}", contracts);
        debug!(target: "engine", "reverts:\n{:#?}", reverts);
        debug!(target: "engine", "state_size:{:#?}", state_size);
        debug!(target: "engine", "reverts_size:{:#?}", reverts_size);

        // construct real genesis using known values & tmp chain storage result
        let tmp_rls_impl_address = owner_address.create(1);

        let capture_storage = |addr: Address| -> Option<BTreeMap<B256, B256>> {
            state.get(&addr).map(|account| {
                account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
            })
        };

        // RLSAccumulator.initialize() calls `IERC20(RLS_ADDRESS).forceApprove(RD, MAX)`
        let tmp_rls_proxy_storage = capture_storage(tmp_rls_proxy_address);
        let tmp_registry_storage = capture_storage(tmp_registry_address);
        let tmp_ntc_storage = capture_storage(tmp_ntc_proxy);
        let tmp_fa_storage = capture_storage(tmp_fa_proxy);
        let tmp_rd_storage = capture_storage(tmp_rd_proxy);
        let tmp_dp_storage = capture_storage(tmp_dp_proxy);
        let tmp_acc_storage = capture_storage(tmp_acc_proxy);

        let rls_proxy_runtimecode_binding =
            Self::fetch_value_from_json_str(ERC1967PROXY_JSON, Some("deployedBytecode.object"))?;
        let rls_proxy_runtimecode =
            hex::decode(rls_proxy_runtimecode_binding.as_str().ok_or_eyre("invalid proxy json")?)?;

        // Use the actual deployed bytecode captured from the CREATE result.
        // The library (BlsG1) is already linked and immutables are already filled in.
        let registry_runtimecode = registry_deployed_runtimecode;

        let blsg1_runtimecode_binding =
            Self::fetch_value_from_json_str(BLSG1_JSON, Some("deployedBytecode.object"))?;
        let blsg1_runtimecode =
            hex::decode(blsg1_runtimecode_binding.as_str().ok_or_eyre("invalid blsg1 json")?)?;

        debug!(target: "engine",
            tmp_rls_impl = ?tmp_rls_impl_address,
            tmp_rls_proxy = ?tmp_rls_proxy_address,
            tmp_registry = ?tmp_registry_address,
            "genesis deployment addresses"
        );

        let rls_proxy_code = alloy::primitives::Bytes::from(rls_proxy_runtimecode);

        // DkgManager starts with the genesis validators' encryption keys, and funds for refunds
        let dkg_manager_runtimecode = deployed_runtime(DKG_MANAGER_JSON, "dkg_manager")?;
        let dkg_manager_balance =
            U256::from(crate::reth_env::dkg_manager::GENESIS_DKG_MANAGER_BALANCE);

        let genesis = genesis.extend_accounts([
            (blsg1_address, GenesisAccount::default().with_code(Some(blsg1_runtimecode.into()))),
            (
                DKG_MANAGER_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(dkg_manager_runtimecode.into()))
                    .with_storage(Some(dkg_manager_storage))
                    .with_balance(dkg_manager_balance),
            ),
            // RLS implementation — stateless, just bytecode
            (
                RLS_IMPL_ADDRESS,
                GenesisAccount::default().with_code(Some(rls_impl_runtimecode.into())),
            ),
            // RLS proxy — holds all token state (balances, allowances set by
            // RLSAccumulator.initialize's forceApprove, ERC1967 impl slot)
            (
                RLS_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(rls_proxy_code))
                    .with_storage(tmp_rls_proxy_storage),
            ),
            // ConsensusRegistry — no native balance; RLS ERC-20 balance tracked in RLS storage
            (
                CONSENSUS_REGISTRY_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(registry_runtimecode.into()))
                    .with_storage(tmp_registry_storage),
            ),
            // Five UUPS precompile proxies
            (
                NATIVE_TOKEN_CONTROLLER_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(ntc_proxy_code.into()))
                    .with_storage(tmp_ntc_storage),
            ),
            (
                FEE_AGGREGATOR_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(fa_proxy_code.into()))
                    .with_storage(tmp_fa_storage),
            ),
            (
                REWARD_DISTRIBUTOR_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(rd_proxy_code.into()))
                    .with_storage(tmp_rd_storage),
            ),
            (
                DELEGATION_POOL_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(dp_proxy_code.into()))
                    .with_storage(tmp_dp_storage),
            ),
            (
                RLS_ACCUMULATOR_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(acc_proxy_code.into()))
                    .with_storage(tmp_acc_storage),
            ),
        ]);
        let genesis = genesis.extend_accounts(dkg_submitters);

        Ok(genesis)
    }

    /// Links a library address into contract bytecode
    /// Replaces Solidity's `__$<34 chars of library hash>$__` placeholder
    pub fn link_solidity_library(
        bytecode_hex: &str,
        library_address: &str,
    ) -> eyre::Result<Vec<u8>> {
        const PLACEHOLDER_PREFIX: &str = "__$";
        const PLACEHOLDER_SUFFIX: &str = "$__";
        const PLACEHOLDER_LEN: usize = 40; // __$ + 34 chars + $__
        let library_address_unprefixed =
            library_address.strip_prefix("0x").unwrap_or(library_address);
        let mut result = String::with_capacity(bytecode_hex.len());
        let mut chars = bytecode_hex.chars().peekable();

        while let Some(ch) = chars.next() {
            // check if we're at the start of a placeholder
            if ch == '_' && chars.peek() == Some(&'_') {
                let mut potential_placeholder = String::from("_");

                // collect the next 39 characters (we already have the first _)
                for _ in 1..PLACEHOLDER_LEN {
                    if let Some(next_ch) = chars.next() {
                        potential_placeholder.push(next_ch);
                    } else {
                        break;
                    }
                }

                // check it matches the placeholder pattern
                if potential_placeholder.starts_with(PLACEHOLDER_PREFIX)
                    && potential_placeholder.ends_with(PLACEHOLDER_SUFFIX)
                    && potential_placeholder.len() == PLACEHOLDER_LEN
                {
                    // it's a valid placeholder, replace with address
                    result.push_str(library_address_unprefixed);
                } else {
                    // not a placeholder, add the characters back
                    result.push_str(&potential_placeholder);
                }
            } else {
                result.push(ch);
            }
        }

        hex::decode(result).map_err(Into::into)
    }

    /// Fetches json info from the given string
    ///
    /// If a key is specified, return the corresponding nested object.
    /// Otherwise return the entire JSON
    /// With a generic this could be adjusted to handle YAML also
    pub fn fetch_value_from_json_str(json_content: &str, key: Option<&str>) -> eyre::Result<Value> {
        let json: Value = serde_json::from_str(json_content)?;
        let result = match key {
            Some(path) => {
                let key: Vec<&str> = path.split('.').collect();
                let mut current_value = &json;
                for &k in &key {
                    current_value =
                        current_value.get(k).ok_or_else(|| eyre::eyre!("key '{}' not found", k))?;
                }
                current_value.clone()
            }
            None => json,
        };

        Ok(result)
    }
}

// ── Greenfield genesis fixes ─────────────────────────────────────────
//
// Two fixes the sim can't produce:
//   1. UUPS impls need `__self` patched to the well-known address (it's an immutable, not state).
//      Pre-patched `.bin` files replace the yaml'd bytecode; `_initialized = MAX` locks the impl.
//   2. `RLS._allowances[ACC][RD] = MAX`. ACC.initialize's `forceApprove` runs in the sim, but
//      `msg.sender` there is the tmp ACC proxy, not `RLS_ACCUMULATOR_ADDRESS`, so it writes the
//      wrong slot.

/// OpenZeppelin Initializable ERC-7201 slot.
/// `keccak256(abi.encode(uint256(keccak256("openzeppelin.storage.Initializable")) - 1)) & ~0xff`
const INITIALIZABLE_SLOT: B256 = B256::new([
    0xf0, 0xc5, 0x7e, 0x16, 0x84, 0x0d, 0xf0, 0x40, 0xf1, 0x50, 0x88, 0xdc, 0x2f, 0x81, 0xfe, 0x39,
    0x1c, 0x39, 0x23, 0xbe, 0xc7, 0x3e, 0x23, 0xa9, 0x66, 0x2e, 0xfc, 0x9c, 0x22, 0x9c, 0x6a, 0x00,
]);

/// `type(uint64).max` — the value `_disableInitializers()` writes.
const INITIALIZED_MAX: B256 = B256::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff,
]);

/// `RLS._allowances[RLSAccumulator][RewardDistributor]`, derived from OZ ERC20's
/// ERC-7201 base + 1 (allowances mapping) and the two participant addresses:
/// `keccak256(abi.encode(RewardDistributor, keccak256(abi.encode(RLSAccumulator, ERC20_BASE +
/// 1))))`
const RLS_ALLOWANCE_SLOT: B256 = B256::new([
    0xa8, 0x6c, 0x96, 0x62, 0xf5, 0xbb, 0xd1, 0x41, 0x9e, 0x01, 0xb9, 0x20, 0xe7, 0x03, 0x4d, 0xd5,
    0xf6, 0x8f, 0x6d, 0x93, 0xdd, 0x1a, 0x17, 0x52, 0xef, 0xe6, 0xe7, 0x79, 0x17, 0x00, 0x58, 0xba,
]);

/// `type(uint256).max` — the allowance value.
const U256_MAX: B256 = B256::new([0xff; 32]);

const FEE_AGGREGATOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("../evm/hardforks/bytecodes/fee_aggregator_impl.bin");
const DELEGATION_POOL_IMPL_BYTECODE: &[u8] =
    include_bytes!("../evm/hardforks/bytecodes/delegation_pool_impl.bin");
const REWARD_DISTRIBUTOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("../evm/hardforks/bytecodes/reward_distributor_impl.bin");
const NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE: &[u8] =
    include_bytes!("../evm/hardforks/bytecodes/native_token_controller_impl.bin");
const RLS_IMPL_BYTECODE: &[u8] = include_bytes!("../evm/hardforks/bytecodes/rls_impl.bin");
const RLS_ACCUMULATOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("../evm/hardforks/bytecodes/rls_accumulator_impl.bin");

/// Six UUPS impl addresses paired with their pre-patched runtime bytecode.
fn uups_impls() -> [(Address, &'static [u8]); 6] {
    [
        (FEE_AGGREGATOR_IMPL_ADDRESS, FEE_AGGREGATOR_IMPL_BYTECODE),
        (DELEGATION_POOL_IMPL_ADDRESS, DELEGATION_POOL_IMPL_BYTECODE),
        (REWARD_DISTRIBUTOR_IMPL_ADDRESS, REWARD_DISTRIBUTOR_IMPL_BYTECODE),
        (NATIVE_TOKEN_CONTROLLER_IMPL_ADDRESS, NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE),
        (RLS_IMPL_ADDRESS, RLS_IMPL_BYTECODE),
        (RLS_ACCUMULATOR_IMPL_ADDRESS, RLS_ACCUMULATOR_IMPL_BYTECODE),
    ]
}

/// Patch the six UUPS impls with `__self`-fixed bytecode + locked initializer,
/// and write the `RLSAccumulator → RewardDistributor` allowance slot on RLS.
pub fn apply_greenfield_fixes(alloc: &mut BTreeMap<Address, GenesisAccount>) {
    for (addr, bytecode) in uups_impls() {
        let mut storage = BTreeMap::new();
        storage.insert(INITIALIZABLE_SLOT, INITIALIZED_MAX);
        alloc.insert(
            addr,
            GenesisAccount::default()
                .with_code(Some(Bytes::copy_from_slice(bytecode)))
                .with_storage(Some(storage)),
        );
    }

    let rls = alloc.entry(RLS_ADDRESS).or_insert_with(GenesisAccount::default);
    let mut rls_storage = rls.storage.clone().unwrap_or_default();
    rls_storage.insert(RLS_ALLOWANCE_SLOT, U256_MAX);
    rls.storage = Some(rls_storage);
}

#[cfg(test)]
mod greenfield_tests {
    use super::*;

    #[test]
    fn applies_six_impls_plus_rls_allowance() {
        let mut alloc: BTreeMap<Address, GenesisAccount> = BTreeMap::new();
        apply_greenfield_fixes(&mut alloc);

        for (addr, expected_bytecode) in uups_impls() {
            let account = alloc.get(&addr).expect("impl missing");
            let code = account.code.as_ref().expect("impl has no code");
            assert_eq!(code.as_ref(), expected_bytecode, "impl {addr} bytecode");
            let storage = account.storage.as_ref().expect("impl has no storage");
            assert_eq!(
                storage.get(&INITIALIZABLE_SLOT),
                Some(&INITIALIZED_MAX),
                "impl {addr} _initialized"
            );
        }

        let rls_storage = alloc
            .get(&RLS_ADDRESS)
            .and_then(|a| a.storage.as_ref())
            .expect("RLS account missing or no storage");
        assert_eq!(rls_storage.get(&RLS_ALLOWANCE_SLOT), Some(&U256_MAX));
    }

    #[test]
    fn rls_allowance_merges_with_existing_storage() {
        let unrelated_slot = B256::new([0x11; 32]);
        let unrelated_value = B256::new([0x22; 32]);
        let mut existing = BTreeMap::new();
        existing.insert(unrelated_slot, unrelated_value);
        let mut alloc = BTreeMap::new();
        alloc.insert(RLS_ADDRESS, GenesisAccount::default().with_storage(Some(existing)));

        apply_greenfield_fixes(&mut alloc);

        let rls_storage =
            alloc.get(&RLS_ADDRESS).and_then(|a| a.storage.as_ref()).expect("RLS storage missing");
        assert_eq!(rls_storage.get(&unrelated_slot), Some(&unrelated_value));
        assert_eq!(rls_storage.get(&RLS_ALLOWANCE_SLOT), Some(&U256_MAX));
    }
}

/// `DkgManager` storage registering each genesis validator's encryption key, after checking its
/// proof of knowledge against the validator's BLS key and that no two validators bring the same
/// key: the key's bytes in `encryptionKeys` (slot 0, a `mapping(address => bytes)`), its owner in
/// `keyOwners` (slot 1, `mapping(bytes32 => address)` by the key's keccak hash), and no
/// registration block, which the contract reads as a genesis key. A validator without a key in
/// its node-info is left out and registers on-chain.
pub(crate) fn dkg_manager_genesis_storage(
    validators: &[NodeInfo],
) -> eyre::Result<BTreeMap<B256, B256>> {
    use alloy::primitives::keccak256;
    use rayls_infrastructure_types::nidkg::{
        encryption_key_context, EncryptionKey, EncryptionKeyProof,
    };
    let mut storage = BTreeMap::new();
    for validator in validators.iter().filter(|v| !v.dkg_encryption_key.is_empty()) {
        let address = validator.execution_address;
        let key = EncryptionKey::from_bytes(&validator.dkg_encryption_key)
            .ok_or_else(|| eyre::eyre!("validator {address}: bad dkg encryption key"))?;
        let proof: EncryptionKeyProof =
            rayls_infrastructure_types::try_decode(&validator.dkg_key_proof)
                .map_err(|e| eyre::eyre!("validator {address}: bad dkg key proof: {e}"))?;
        eyre::ensure!(
            key.verify(&validator.bls_public_key, &encryption_key_context(), &proof),
            "validator {address}: dkg key proof does not verify against its BLS key"
        );
        // a `bytes` value of 32 bytes or more: its slot holds `2 * len + 1`, the data fills
        // consecutive slots from `keccak256(slot)`, the last one left-aligned
        let bytes = key.to_bytes();
        let owner_slot =
            keccak256([keccak256(bytes).0, U256::from(1).to_be_bytes::<32>()].concat());
        eyre::ensure!(
            storage.insert(owner_slot, address.into_word()).is_none(),
            "validator {address}: dkg encryption key already brought by another validator"
        );
        let slot = keccak256([address.into_word().0, [0u8; 32]].concat());
        storage.insert(slot, B256::from(U256::from(bytes.len() * 2 + 1)));
        let data = U256::from_be_bytes(keccak256(slot).0);
        for (i, chunk) in bytes.chunks(32).enumerate() {
            let mut word = [0u8; 32];
            word[..chunk.len()].copy_from_slice(chunk);
            storage.insert(B256::from(data + U256::from(i)), B256::from(word));
        }
    }
    Ok(storage)
}

/// Check the genesis validators' dealings of epoch 0's key and return their commitment, the hash
/// `DkgManager.genesisDealings` holds: every dealer a distinct genesis validator, signed by its
/// BLS key, every dealing verified against the genesis committee (every member with an
/// encryption key, in committee order) at the committee's quorum threshold, and at least a third
/// of the committee dealing, as for any epoch's key. `dealings` come back sorted by dealer, the
/// order of the file and of the commitment.
pub fn verify_genesis_dealings(
    validators: &[NodeInfo],
    chain_id: u64,
    dealings: &mut Vec<GenesisDealing>,
) -> eyre::Result<B256> {
    use rayls_infrastructure_types::{
        dkg::dealers_needed,
        nidkg::{dealing_context, dealing_message, Dealing, EncryptionKey, Recipient},
        quorum_threshold, try_decode,
    };
    eyre::ensure!(
        validators.len() <= super::dkg_manager::MAX_KEYED_COMMITTEE,
        "{} genesis validators: a committee above {} gets no threshold key",
        validators.len(),
        super::dkg_manager::MAX_KEYED_COMMITTEE
    );
    let mut committee: Vec<&NodeInfo> = validators.iter().collect();
    committee.sort_by(|a, b| a.bls_public_key.cmp(&b.bls_public_key));
    let recipients = committee
        .iter()
        .enumerate()
        .map(|(position, validator)| {
            let key =
                EncryptionKey::from_bytes(&validator.dkg_encryption_key).ok_or_else(|| {
                    eyre::eyre!(
                    "validator {} has no dkg encryption key: epoch 0 cannot have a threshold key",
                    validator.execution_address
                )
                })?;
            Ok(Recipient { position: position as u32, key })
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    let threshold = quorum_threshold(committee.len() as u64) as usize;
    dealings.sort_by_key(|dealing| dealing.dealer);
    eyre::ensure!(
        dealings.windows(2).all(|pair| pair[0].dealer != pair[1].dealer),
        "two genesis dealings from one dealer"
    );
    eyre::ensure!(
        dealings.len() >= dealers_needed(committee.len()),
        "{} genesis dealings, epoch 0's key needs {}",
        dealings.len(),
        dealers_needed(committee.len())
    );
    for entry in dealings.iter() {
        let dealer = committee
            .iter()
            .find(|validator| validator.execution_address == entry.dealer)
            .ok_or_else(|| {
                eyre::eyre!("genesis dealer {} is not a genesis validator", entry.dealer)
            })?;
        let message = dealing_message(chain_id, 0, entry.dealer, &entry.dealing);
        eyre::ensure!(
            entry.signature.verify_raw(&message, &dealer.bls_public_key),
            "genesis dealing of {} is not signed by its BLS key",
            entry.dealer
        );
        let dealing: Dealing = try_decode(&entry.dealing)
            .map_err(|e| eyre::eyre!("genesis dealing of {} does not decode: {e}", entry.dealer))?;
        dealing
            .verify(&dealing_context(chain_id, 0, entry.dealer), threshold, &recipients)
            .map_err(|e| eyre::eyre!("genesis dealing of {} does not verify: {e}", entry.dealer))?;
    }
    Ok(genesis_dealings_commitment(dealings))
}

/// The commitment to genesis dealings sorted by dealer: the keccak hash of their BCS encoding.
pub fn genesis_dealings_commitment(dealings: &[GenesisDealing]) -> B256 {
    alloy::primitives::keccak256(rayls_infrastructure_types::encode(&dealings))
}

/// `genesis` with `DkgManager.genesisDealings` (slot 8) set to `commitment`.
pub fn with_genesis_dealings(mut genesis: Genesis, commitment: B256) -> eyre::Result<Genesis> {
    let account = genesis
        .alloc
        .get_mut(&DKG_MANAGER_ADDRESS)
        .ok_or_else(|| eyre::eyre!("genesis has no DkgManager"))?;
    account
        .storage
        .get_or_insert_with(Default::default)
        .insert(B256::from(U256::from(8)), commitment);
    Ok(genesis)
}
