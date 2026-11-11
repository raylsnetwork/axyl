//! Custom handler to override EVM basefees.

use crate::{
    reth_env::types::basefee_address,
    rls_flavour::{balance_slot, transfer_log, RLS_FEE_RECIPIENT, RLS_TOKEN},
};
use rayls_infrastructure_types::{Address, TxKind};
use reth_revm::{
    context_interface::{
        journaled_state::account::JournaledAccountTr,
        result::{HaltReason, InvalidTransaction},
        Block, ContextTr, JournalTr, LocalContextTr, Transaction,
    },
    handler::{post_execution, pre_execution, EvmTr, FrameResult, FrameTr, Handler},
    inspector::{InspectorEvmTr, InspectorHandler},
    interpreter::{interpreter::EthInterpreter, interpreter_action::FrameInit, InterpreterResult},
    primitives::{KECCAK_EMPTY, U256},
    Database, Inspector,
};

/// Gas and value of an RLS-flavour transaction, charged in RLS instead of native USDr.
///
/// The transaction itself runs with a zero gas price and zero value, so revm moves no USDr; the
/// handler moves RLS around it.
#[derive(Debug, Clone, Copy)]
pub(super) struct RlsGas {
    /// RLS charged per unit of gas.
    pub(super) price: u128,
    /// RLS sent to the recipient.
    pub(super) value: U256,
}

/// The handler that executes Rayls evm types.
///
/// Overwrites basefee logic, and charges RLS-flavour transactions in RLS.
pub(super) struct RaylsEvmHandler<EVM> {
    /// Address for basefees
    basefee_address: Address,
    /// Set for RLS-flavour transactions.
    rls: Option<RlsGas>,
    _phantom: core::marker::PhantomData<EVM>,
}

impl<EVM> RaylsEvmHandler<EVM> {
    fn new(basefee_address: Address) -> Self {
        Self { basefee_address, rls: None, _phantom: core::marker::PhantomData }
    }

    /// A handler that charges gas and value in RLS.
    pub(super) fn with_rls(rls: Option<RlsGas>) -> Self {
        Self { rls, ..Self::default() }
    }
}

impl<EVM> Default for RaylsEvmHandler<EVM> {
    fn default() -> Self {
        RaylsEvmHandler::new(basefee_address())
    }
}

/// Adds `delta` to `owner`'s RLS balance, or takes it away. Fails if the balance is short.
fn move_rls<J: JournalTr>(
    journal: &mut J,
    owner: Address,
    delta: U256,
    credit: bool,
) -> Result<(), RlsMoveError<<J::Database as Database>::Error>> {
    journal.load_account(RLS_TOKEN).map_err(RlsMoveError::Db)?;
    let slot = balance_slot(owner);
    let balance = journal.sload(RLS_TOKEN, slot).map_err(RlsMoveError::Db)?.data;
    let new_balance = if credit {
        balance.saturating_add(delta)
    } else {
        balance.checked_sub(delta).ok_or(RlsMoveError::Short { balance, needed: delta })?
    };
    journal.sstore(RLS_TOKEN, slot, new_balance).map_err(RlsMoveError::Db)?;
    Ok(())
}

enum RlsMoveError<DbError> {
    Db(DbError),
    Short { balance: U256, needed: U256 },
}

impl<EVM> Handler for RaylsEvmHandler<EVM>
where
    EVM: EvmTr<
        Context: ContextTr<Journal: JournalTr, Local: LocalContextTr>,
        Precompiles: reth_revm::handler::PrecompileProvider<
            EVM::Context,
            Output = InterpreterResult,
        >,
        Frame: FrameTr<FrameResult = FrameResult, FrameInit = FrameInit>,
    >,
{
    type Evm = EVM;
    type Error = reth_revm::context_interface::result::EVMError<
        <<EVM::Context as ContextTr>::Db as Database>::Error,
        InvalidTransaction,
    >;
    type HaltReason = HaltReason;

    // RLS flavour: take the maximum gas fee and the value in RLS before execution
    fn validate_against_state_and_deduct_caller(
        &self,
        evm: &mut Self::Evm,
    ) -> Result<(), Self::Error> {
        // nonce and code checks; no USDr moves since the gas price and value are zero
        pre_execution::validate_against_state_and_deduct_caller::<_, Self::Error>(evm.ctx())?;

        let Some(rls) = self.rls else { return Ok(()) };
        let (_, tx, _, journal, _, _) = evm.ctx().all_mut();
        let caller = tx.caller();
        let max_fee = U256::from(tx.gas_limit()) * U256::from(rls.price);

        let recipient = match tx.kind() {
            TxKind::Call(to) => Some(to),
            TxKind::Create => None,
        };
        if !rls.value.is_zero() {
            // msg.value stays USDr inside the EVM, so RLS value only goes to accounts without code
            let Some(to) = recipient else {
                return Err(InvalidTransaction::Str(
                    "RLS value cannot fund a contract creation".into(),
                )
                .into());
            };
            if journal.load_account_with_code(to)?.data.info.code_hash != KECCAK_EMPTY {
                return Err(InvalidTransaction::Str(
                    "RLS value can only go to an account without code".into(),
                )
                .into());
            }
        }

        let Some(needed) = max_fee.checked_add(rls.value) else {
            return Err(InvalidTransaction::OverflowPaymentInTransaction.into());
        };
        match move_rls(journal, caller, needed, false) {
            Ok(()) => {}
            Err(RlsMoveError::Db(err)) => return Err(err.into()),
            Err(RlsMoveError::Short { balance, needed }) => {
                return Err(InvalidTransaction::LackOfFundForMaxFee {
                    fee: Box::new(needed),
                    balance: Box::new(balance),
                }
                .into())
            }
        }
        if let Some(to) = recipient.filter(|_| !rls.value.is_zero()) {
            if let Err(RlsMoveError::Db(err)) = move_rls(journal, to, rls.value, true) {
                return Err(err.into());
            }
            journal.log(transfer_log(caller, to, rls.value));
        }
        Ok(())
    }

    // RLS flavour: return the RLS for unused gas
    fn reimburse_caller(
        &self,
        evm: &mut Self::Evm,
        exec_result: &mut <<Self::Evm as EvmTr>::Frame as FrameTr>::FrameResult,
    ) -> Result<(), Self::Error> {
        post_execution::reimburse_caller(evm.ctx(), exec_result.gas(), U256::ZERO)?;

        let Some(rls) = self.rls else { return Ok(()) };
        let gas = exec_result.gas();
        let unused = U256::from(gas.remaining() + gas.refunded() as u64);
        let (_, tx, _, journal, _, _) = evm.ctx().all_mut();
        let caller = tx.caller();
        if let Err(RlsMoveError::Db(err)) =
            move_rls(journal, caller, unused * U256::from(rls.price), true)
        {
            return Err(err.into());
        }
        Ok(())
    }

    // overwrite the default basefee logic
    fn reward_beneficiary(
        &self,
        evm: &mut Self::Evm,
        exec_result: &mut <<Self::Evm as EvmTr>::Frame as FrameTr>::FrameResult,
    ) -> Result<(), Self::Error> {
        let gas = exec_result.gas();
        let gas_used = gas.spent_sub_refunded() as u128;

        // read block and tx info through destructuring to avoid borrow issues
        let (block, tx, _, journal, _, _) = evm.ctx().all_mut();
        let beneficiary = block.beneficiary();
        let basefee = block.basefee() as u128;
        let effective_gas_price = tx.effective_gas_price(basefee);

        // RLS flavour: the gas paid in RLS goes to staker rewards
        if let Some(rls) = self.rls {
            if let Err(RlsMoveError::Db(err)) = move_rls(
                journal,
                RLS_FEE_RECIPIENT,
                U256::from(gas_used) * U256::from(rls.price),
                true,
            ) {
                return Err(err.into());
            }
            return Ok(());
        }

        // transfer fee to coinbase/beneficiary.
        // Basefee amount of gas is redirected.
        let coinbase_gas_price = effective_gas_price.saturating_sub(basefee);

        // reward coinbase with priority fee
        journal
            .load_account_mut(beneficiary)?
            .incr_balance(U256::from(coinbase_gas_price * gas_used));

        // Send the base fee portion to a basefee account for later processing
        // (offchain).
        journal
            .load_account_mut(self.basefee_address)?
            .incr_balance(U256::from(basefee * gas_used));

        Ok(())
    }
}

impl<EVM> InspectorHandler for RaylsEvmHandler<EVM>
where
    EVM: InspectorEvmTr<
        Inspector: Inspector<<<Self as Handler>::Evm as EvmTr>::Context, EthInterpreter>,
        Context: ContextTr<Journal: JournalTr, Local: LocalContextTr>,
        Precompiles: reth_revm::handler::PrecompileProvider<
            EVM::Context,
            Output = InterpreterResult,
        >,
    >,
{
    type IT = EthInterpreter;
}
