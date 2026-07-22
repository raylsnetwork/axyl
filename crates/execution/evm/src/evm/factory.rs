//! The factory to create EVM environments.

use super::{RaylsBlockExecutionCtx, RaylsBlockExecutor, RaylsEvm, RaylsEvmContext};
use crate::{
    chainspec::RaylsChainSpec,
    native_erc20::{
        abi::{eip3009, erc20, Erc20Selector},
        precompile::Erc20Precompile,
        CompositeInspector, NativeErc20Inspector, BASE_ERROR_GAS, ERC20_PRECOMPILE_ADDRESS,
    },
};
use alloy::sol_types::SolCall;
use alloy_evm::{precompiles::DynPrecompile, Database};
use parking_lot::RwLock;
use rayls_infrastructure_types::{Receipt, TransactionSigned, U256};
use reth_evm::{
    block::{BlockExecutorFactory, BlockExecutorFor},
    eth::{
        receipt_builder::{AlloyReceiptBuilder, ReceiptBuilder},
        spec::{EthExecutorSpec, EthSpec},
    },
    precompiles::{PrecompileInput, PrecompilesMap},
    EvmEnv, EvmFactory, FromRecoveredTx, FromTxWithEncoded,
};
use reth_revm::{
    context::{
        result::{EVMError, HaltReason},
        BlockEnv, CfgEnv, TxEnv,
    },
    inspector::NoOpInspector,
    precompile::{
        PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult, PrecompileSpecId,
        Precompiles,
    },
    primitives::hardfork::SpecId,
    Context, Inspector, MainBuilder, MainContext, State,
};
use std::sync::{Arc, OnceLock};
use tracing::{debug, trace};

/// Decode ABI call parameters with standardized error handling.
#[inline]
fn decode_call<T: SolCall>(data: &[u8]) -> Result<T, PrecompileError> {
    T::abi_decode(data).map_err(|e| PrecompileError::other(format!("ABI decode failed: {e:?}")))
}

/// Global ERC-20 precompile instance.
///
/// This must be initialized before creating any EVMs that need Native ERC-20 support.
/// Use [`initialize_erc20_precompile`] to initialize.
pub(crate) static ERC20_PRECOMPILE_INSTANCE: OnceLock<Arc<RwLock<Erc20Precompile>>> =
    OnceLock::new();

/// Initialize the global ERC-20 precompile instance.
///
/// This should be called during node startup with the appropriate configuration.
/// Returns `Ok(())` if initialization succeeded, or `Err` if already initialized.
pub(crate) fn initialize_erc20_precompile(
    precompile: Erc20Precompile,
) -> Result<(), Arc<RwLock<Erc20Precompile>>> {
    ERC20_PRECOMPILE_INSTANCE.set(Arc::new(RwLock::new(precompile)))
}

/// Get the global ERC-20 precompile instance.
///
/// Returns `None` if not yet initialized.
/// Commented out because it is currently not in use
// pub(crate) fn get_erc20_precompile() -> Option<Arc<RwLock<Erc20Precompile>>> {
//     ERC20_PRECOMPILE_INSTANCE.get().cloned()
// }

/// Create a DynPrecompile for the Native ERC-20 wrapper.
///
/// This is used for eth_call simulations where the inspector path is not available.
/// ERC-20 precompile ID for the DynPrecompile registration.
const ERC20_PRECOMPILE_ID: PrecompileId =
    PrecompileId::Custom(std::borrow::Cow::Borrowed("NativeERC20Wrapper"));

fn create_erc20_dyn_precompile() -> DynPrecompile {
    DynPrecompile::new(ERC20_PRECOMPILE_ID, move |mut input: PrecompileInput<'_>| {
        // reject native value: revm transfers value to 0x0400 before precompile dispatch and would
        // otherwise commit it permanently
        if input.value != U256::ZERO {
            return PrecompileResult::Err(PrecompileError::other(
                "ERC-20 precompile rejects native value transfer",
            ));
        }

        let precompile = match ERC20_PRECOMPILE_INSTANCE.get() {
            Some(p) => p.clone(),
            None => {
                return PrecompileResult::Err(PrecompileError::other(
                    "ERC-20 precompile not initialized",
                ));
            }
        };

        if input.data.len() < 4 {
            return PrecompileResult::Err(PrecompileError::other("Invalid input: too short"));
        }

        let selector = Erc20Selector::from_calldata(input.data)
            .ok_or_else(|| PrecompileError::other("Unknown function selector"))?;

        // reject state mutation under STATICCALL: revm enforces is_static only at opcode dispatch,
        // not journal layer
        if input.is_static && selector.is_state_mutating() {
            return PrecompileResult::Err(PrecompileError::other(
                "ERC-20 state mutation forbidden under static call",
            ));
        }

        // Lock the precompile for reading
        let precompile_guard = precompile.read();

        // Route to appropriate handler based on selector
        trace!(target: "erc20_precompile", ?selector, "dispatch");
        let result = match selector {
            // Metadata - no state access needed
            Erc20Selector::Name => precompile_guard.handle_name(),
            Erc20Selector::Symbol => precompile_guard.handle_symbol(),
            Erc20Selector::Decimals => precompile_guard.handle_decimals(),
            Erc20Selector::TotalSupply => {
                precompile_guard.handle_total_supply(&mut input.internals)
            }

            // Queries
            Erc20Selector::BalanceOf => {
                let params = decode_call::<erc20::balanceOfCall>(input.data)?;
                precompile_guard.handle_balance_of(&mut input.internals, params.account)
            }
            Erc20Selector::Allowance => {
                let params = decode_call::<erc20::allowanceCall>(input.data)?;
                precompile_guard.handle_allowance(
                    &mut input.internals,
                    params.owner,
                    params.spender,
                )
            }

            // State mutations
            Erc20Selector::Transfer => {
                let params = decode_call::<erc20::transferCall>(input.data)?;
                precompile_guard.handle_transfer(
                    &mut input.internals,
                    input.caller,
                    params.to,
                    params.amount,
                )
            }
            Erc20Selector::Approve => {
                let params = decode_call::<erc20::approveCall>(input.data)?;
                precompile_guard.handle_approve(
                    &mut input.internals,
                    input.caller,
                    params.spender,
                    params.amount,
                )
            }
            Erc20Selector::TransferFrom => {
                let params = decode_call::<erc20::transferFromCall>(input.data)?;
                precompile_guard.handle_transfer_from(
                    &mut input.internals,
                    input.caller,
                    params.from,
                    params.to,
                    params.amount,
                )
            }
            Erc20Selector::Mint => {
                let params = decode_call::<erc20::mintCall>(input.data)?;
                precompile_guard.handle_mint(
                    &mut input.internals,
                    input.caller,
                    params.to,
                    params.amount,
                )
            }
            Erc20Selector::Burn => {
                let params = decode_call::<erc20::burnCall>(input.data)?;
                precompile_guard.handle_burn(&mut input.internals, input.caller, params.amount)
            }
            Erc20Selector::BurnFrom => {
                let params = decode_call::<erc20::burnFromCall>(input.data)?;
                precompile_guard.handle_burn_from(
                    &mut input.internals,
                    input.caller,
                    params.account,
                    params.amount,
                )
            }

            // EIP-3009 meta-transactions
            Erc20Selector::TransferWithAuthorization => {
                let params = decode_call::<eip3009::TransferWithAuthorizationCall>(input.data)?;
                precompile_guard.handle_transfer_with_authorization(
                    &mut input.internals,
                    params.from,
                    params.to,
                    params.value,
                    params.validAfter,
                    params.validBefore,
                    params.nonce,
                    params.v,
                    params.r.into(),
                    params.s.into(),
                )
            }
            Erc20Selector::ReceiveWithAuthorization => {
                let params = decode_call::<eip3009::ReceiveWithAuthorizationCall>(input.data)?;
                precompile_guard.handle_receive_with_authorization(
                    &mut input.internals,
                    input.caller,
                    params.from,
                    params.to,
                    params.value,
                    params.validAfter,
                    params.validBefore,
                    params.nonce,
                    params.v,
                    params.r,
                    params.s,
                )
            }
            Erc20Selector::CancelAuthorization => {
                let params = decode_call::<eip3009::CancelAuthorizationCall>(input.data)?;
                precompile_guard.handle_cancel_authorization(
                    &mut input.internals,
                    params.authorizer,
                    params.nonce,
                    params.v,
                    params.r,
                    params.s,
                )
            }
        };

        // gate gas_used on input.gas so PrecompilesMap::run never trips its underflow assert
        match result {
            Ok(output) => {
                if output.gas_used > input.gas {
                    return Err(PrecompileError::OutOfGas);
                }
                Ok(PrecompileOutput::new(output.gas_used, output.bytes))
            }
            Err(err) => {
                let charge = BASE_ERROR_GAS.min(input.gas);
                Ok(PrecompileOutput::new_reverted(charge, err.into()))
            }
        }
    })
}

/// Create a PrecompilesMap with the Native ERC-20 precompile registered.
fn create_precompiles_with_erc20(spec_id: SpecId) -> PrecompilesMap {
    let mut precompiles_map =
        PrecompilesMap::from_static(Precompiles::new(PrecompileSpecId::from_spec_id(spec_id)));

    // Only register the ERC-20 precompile if it's been initialized
    if ERC20_PRECOMPILE_INSTANCE.get().is_some() {
        let erc20_dyn_precompile = create_erc20_dyn_precompile();
        precompiles_map.apply_precompile(&ERC20_PRECOMPILE_ADDRESS, |_| Some(erc20_dyn_precompile));
        debug!("Registered Native ERC-20 precompile at {:?}", ERC20_PRECOMPILE_ADDRESS);
    }

    precompiles_map
}

/// Factory producing [`RaylsEvm`].
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct RaylsEvmFactory;

impl RaylsEvmFactory {
    /// Create an EVM with CompositeInspector (NativeErc20Inspector + custom inspector).
    ///
    /// This method wraps any inspector with NativeErc20Inspector to ensure ERC-20
    /// state-modifying operations (transfer, approve, transferFrom) are properly
    /// intercepted and handled.
    ///
    /// This should be the default EVM creation path when you need inspector support
    /// for actual transaction execution with persistent state changes.
    pub fn create_evm_with_native_erc20_inspector<DB, I>(
        &self,
        db: DB,
        input: EvmEnv,
        inner_inspector: I,
        chain_spec: Arc<RaylsChainSpec>,
    ) -> RaylsEvm<DB, CompositeInspector<I>, PrecompilesMap>
    where
        DB: Database,
        I: Inspector<RaylsEvmContext<DB>>,
    {
        // Ensure the precompile instance is initialized
        let precompile_instance = ERC20_PRECOMPILE_INSTANCE
            .get()
            .expect("ERC20_PRECOMPILE_INSTANCE must be initialized before creating EVMs with Native ERC-20 inspector")
            .clone();

        // Create NativeErc20Inspector with chain spec for hardfork-gated gas fix
        let native_erc20_inspector = NativeErc20Inspector::new(precompile_instance, chain_spec);

        // Wrap both inspectors in CompositeInspector
        let composite = CompositeInspector::new(native_erc20_inspector, inner_inspector);

        debug!(
            "Creating EVM with CompositeInspector (NativeErc20Inspector + custom) for ERC-20 state handling"
        );

        // Use the trait method with composite inspector
        let spec_id = input.cfg_env.spec;
        RaylsEvm {
            inner: Context::mainnet()
                .with_block(input.block_env)
                .with_cfg(input.cfg_env)
                .with_db(db)
                .build_mainnet_with_inspector(composite)
                .with_precompiles(create_precompiles_with_erc20(spec_id)),
            inspect: true,
        }
    }

    /// Create an EVM with only NativeErc20Inspector (no additional inspector).
    ///
    /// Convenience method when you only need ERC-20 inspection without other inspectors.
    pub fn create_evm_with_native_erc20_only<DB>(
        &self,
        db: DB,
        input: EvmEnv,
        chain_spec: Arc<RaylsChainSpec>,
    ) -> RaylsEvm<DB, CompositeInspector<NoOpInspector>, PrecompilesMap>
    where
        DB: Database,
    {
        self.create_evm_with_native_erc20_inspector(db, input, NoOpInspector, chain_spec)
    }
}

impl EvmFactory for RaylsEvmFactory {
    type Evm<DB: Database, I: Inspector<RaylsEvmContext<DB>>> = RaylsEvm<DB, I, Self::Precompiles>;
    type Context<DB: Database> = Context<BlockEnv, TxEnv, CfgEnv, DB>;
    type Tx = TxEnv;
    type Error<DBError: core::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        let spec_id = input.cfg_env.spec;
        RaylsEvm {
            inner: Context::mainnet()
                .with_block(input.block_env)
                .with_cfg(input.cfg_env)
                .with_db(db)
                .build_mainnet_with_inspector(NoOpInspector)
                .with_precompiles(create_precompiles_with_erc20(spec_id)),
            inspect: false,
        }
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        let spec_id = input.cfg_env.spec;
        RaylsEvm {
            inner: Context::mainnet()
                .with_block(input.block_env)
                .with_cfg(input.cfg_env)
                .with_db(db)
                .build_mainnet_with_inspector(inspector)
                .with_precompiles(create_precompiles_with_erc20(spec_id)),
            inspect: true,
        }
    }
}

/// Ethereum block executor factory.
#[derive(Debug, Clone)]
pub struct RaylsBlockExecutorFactory<
    R = AlloyReceiptBuilder,
    Spec = EthSpec,
    EvmFactory = RaylsEvmFactory,
> {
    /// Receipt builder.
    receipt_builder: R,
    /// Chain specification.
    spec: Spec,
    /// EVM factory.
    evm_factory: EvmFactory,
    /// Provider factory for RocksDB access (needed by genesis storage migration).
    pub(crate) provider_factory: Option<Arc<reth_provider::ProviderFactory<crate::traits::RaylsNode>>>,
}

// alloy-evm
impl<R, Spec, EvmFactory> RaylsBlockExecutorFactory<R, Spec, EvmFactory> {
    /// Creates a new [`RaylsBlockExecutorFactory`] with the given spec, [`EvmFactory`], and
    /// [`ReceiptBuilder`].
    pub fn new(receipt_builder: R, spec: Spec, evm_factory: EvmFactory) -> Self {
        Self { receipt_builder, spec, evm_factory, provider_factory: None }
    }

    /// Creates a new [`RaylsBlockExecutorFactory`] with a provider factory for RocksDB access.
    pub fn new_with_provider_factory(
        receipt_builder: R,
        spec: Spec,
        evm_factory: EvmFactory,
        provider_factory: Option<Arc<reth_provider::ProviderFactory<crate::traits::RaylsNode>>>,
    ) -> Self {
        Self { receipt_builder, spec, evm_factory, provider_factory }
    }

    /// Exposes the receipt builder.
    pub const fn receipt_builder(&self) -> &R {
        &self.receipt_builder
    }

    /// Exposes the chain specification.
    pub const fn spec(&self) -> &Spec {
        &self.spec
    }

    /// Exposes the EVM factory.
    pub const fn evm_factory(&self) -> &EvmFactory {
        &self.evm_factory
    }
}

// alloy-evm
impl<R, Spec, EvmF> BlockExecutorFactory for RaylsBlockExecutorFactory<R, Spec, EvmF>
where
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
    Spec: EthExecutorSpec + crate::chainspec::RaylsHardforks,
    EvmF: EvmFactory<Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>>,
    Self: 'static,
{
    type EvmFactory = EvmF;
    type ExecutionCtx<'a> = RaylsBlockExecutionCtx;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        &self.evm_factory
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EvmF::Evm<&'a mut State<DB>, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<EvmF::Context<&'a mut State<DB>>> + 'a,
    {
        RaylsBlockExecutor::new_with_provider_factory(
            evm, ctx, &self.spec, &self.receipt_builder,
            self.provider_factory.clone(),
        )
    }
}
