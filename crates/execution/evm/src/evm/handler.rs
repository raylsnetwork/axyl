//! Custom handler to override EVM basefees.

use crate::{
    reth_env::types::basefee_address,
    rls_flavour::{balance_slot, is_precompile_range, transfer_log, RLS_FEE_RECIPIENT, RLS_TOKEN},
};
use rayls_infrastructure_types::{Address, TxKind};
use reth_revm::{
    context_interface::{
        journaled_state::account::JournaledAccountTr,
        result::{EVMError, HaltReason, InvalidTransaction},
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
    /// RLS charged per unit of gas: the effective price, `min(max_fee, base fee + tip)`.
    pub(super) price: u128,
    /// The signed max fee per gas, in RLS. The sender must hold `gas_limit × max_fee + value`
    /// (EIP-1559's affordability rule, which the pool applies too), though only `price` is taken.
    pub(super) max_fee: u128,
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

/// The error type the RLS balance helpers return: the handler's own error type.
type RlsError<J> = EVMError<<<J as JournalTr>::Database as Database>::Error, InvalidTransaction>;

/// Reads `owner`'s RLS balance slot.
fn rls_balance<J: JournalTr>(journal: &mut J, owner: Address) -> Result<U256, RlsError<J>> {
    journal.load_account(RLS_TOKEN)?;
    Ok(journal.sload(RLS_TOKEN, balance_slot(owner))?.data)
}

/// Whether the RLS token is paused. A paused token refuses every RLS-flavour transaction, gas
/// included, as `RLS._update` would.
fn rls_paused<J: JournalTr>(journal: &mut J) -> Result<bool, RlsError<J>> {
    journal.load_account(RLS_TOKEN)?;
    Ok(crate::rls_flavour::is_paused(
        journal.sload(RLS_TOKEN, crate::rls_flavour::PAUSED_SLOT)?.data,
    ))
}

/// Takes `amount` from `owner`'s RLS. A short balance makes the transaction invalid, like a short
/// USDr balance for gas.
fn debit_rls<J: JournalTr>(
    journal: &mut J,
    owner: Address,
    amount: U256,
) -> Result<(), RlsError<J>> {
    let balance = rls_balance(journal, owner)?;
    let Some(new_balance) = balance.checked_sub(amount) else {
        return Err(InvalidTransaction::LackOfFundForMaxFee {
            fee: Box::new(amount),
            balance: Box::new(balance),
        }
        .into());
    };
    journal.sstore(RLS_TOKEN, balance_slot(owner), new_balance)?;
    Ok(())
}

/// Adds `amount` to `owner`'s RLS. Overflow cannot happen within RLS's supply cap; if it ever did,
/// it is reported instead of saturating, since this is consensus code.
fn credit_rls<J: JournalTr>(
    journal: &mut J,
    owner: Address,
    amount: U256,
) -> Result<(), RlsError<J>> {
    let balance = rls_balance(journal, owner)?;
    let new_balance = balance
        .checked_add(amount)
        // an invalid transaction is skipped; a custom error would fail the whole block
        .ok_or_else(|| {
            EVMError::Transaction(InvalidTransaction::Str("RLS balance overflow".into()))
        })?;
    journal.sstore(RLS_TOKEN, balance_slot(owner), new_balance)?;
    Ok(())
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
    type Error = EVMError<<<EVM::Context as ContextTr>::Db as Database>::Error, InvalidTransaction>;
    type HaltReason = HaltReason;

    // RLS flavour: take the upfront gas cost (gas limit × effective price) and the value in RLS
    // before execution, once the sender is shown to afford gas limit × max fee + value
    fn validate_against_state_and_deduct_caller(
        &self,
        evm: &mut Self::Evm,
    ) -> Result<(), Self::Error> {
        // nonce and code checks; no USDr moves since the gas price and value are zero
        pre_execution::validate_against_state_and_deduct_caller::<_, Self::Error>(evm.ctx())?;

        let Some(rls) = self.rls else { return Ok(()) };
        let (_, tx, _, journal, _, _) = evm.ctx().all_mut();
        if rls_paused(journal)? {
            return Err(InvalidTransaction::Str("RLS is paused".into()).into());
        }
        let caller = tx.caller();
        let gas_limit = U256::from(tx.gas_limit());
        let upfront_gas = gas_limit * U256::from(rls.price);
        let max_gas = gas_limit * U256::from(rls.max_fee);

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
            // revm applies a type-4 transaction's authorizations after this hook, so the code
            // check below would see `to` before its own delegation: refuse value on type 4
            if tx.authorization_list_len() > 0 {
                return Err(InvalidTransaction::Str(
                    "RLS value cannot be sent in an EIP-7702 transaction".into(),
                )
                .into());
            }
            // precompiles have no code hash but can fail, and this value moves before execution
            // and is not reverted with the call, so they are refused too (the pool does the same)
            if is_precompile_range(to)
                || journal.load_account_with_code(to)?.data.info.code_hash != KECCAK_EMPTY
            {
                return Err(InvalidTransaction::Str(
                    "RLS value can only go to an account without code that is not a precompile"
                        .into(),
                )
                .into());
            }
        }

        let (Some(max_cost), Some(upfront)) =
            (max_gas.checked_add(rls.value), upfront_gas.checked_add(rls.value))
        else {
            return Err(InvalidTransaction::OverflowPaymentInTransaction.into());
        };
        let balance = rls_balance(journal, caller)?;
        if balance < max_cost {
            return Err(InvalidTransaction::LackOfFundForMaxFee {
                fee: Box::new(max_cost),
                balance: Box::new(balance),
            }
            .into());
        }
        debit_rls(journal, caller, upfront)?;
        if let Some(to) = recipient.filter(|_| !rls.value.is_zero()) {
            credit_rls(journal, to, rls.value)?;
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
        credit_rls(journal, caller, unused * U256::from(rls.price))
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
            let fee = U256::from(gas_used) * U256::from(rls.price);
            if fee.is_zero() {
                // simulation with no gas price: nothing paid, no log
                return Ok(());
            }
            credit_rls(journal, RLS_FEE_RECIPIENT, fee)?;
            // One Transfer for the gas actually paid, so balances rebuilt from events add up for
            // successful transactions (the up-front debit and the refund net out to this). A
            // reverted or halted transaction keeps no logs, so its gas (and any RLS value, which
            // moved before execution) has no Transfer: an open item for indexers.
            journal.log(transfer_log(tx.caller(), RLS_FEE_RECIPIENT, fee));
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
