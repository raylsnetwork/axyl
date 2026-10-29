//! Native ERC-20 Inspector for intercepting precompile calls.
//!
//! This inspector intercepts calls to the ERC-20 precompile address (0x0400) and
//! routes them to the appropriate handler methods. It also emits Transfer events
//! for native ETH transfers to provide unified event tracking.

use parking_lot::RwLock;
use std::{ops::Range, sync::Arc};

use crate::{
    chainspec::{RaylsChainSpec, RaylsHardforks},
    evm::RaylsEvmContext,
    native_erc20::{
        abi::{eip3009, erc20, Erc20Selector},
        precompile::{ERC20Error, Erc20Precompile, PrecompileOutput, StateAccessAdapter},
        BASE_ERROR_GAS, ERC20_PRECOMPILE_ADDRESS,
    },
};
use alloy::{
    primitives::{Address, Bytes, Log, U256},
    sol_types::SolCall,
};
use reth_revm::{
    bytecode::Bytecode,
    context_interface::{journaled_state::account::JournaledAccountTr, ContextTr, JournalTr},
    inspector::Inspector,
    interpreter::{
        interpreter::EthInterpreter, CallInputs, CallOutcome, CreateInputs, CreateOutcome,
        CreateScheme, Gas, InstructionResult, Interpreter, InterpreterResult,
    },
    primitives::{StorageKey, StorageValue},
    state::Account,
    Database,
};
use tracing::{debug, error};

/// Emit unified ERC-20 Transfer events for native value transfers,
/// contract creations, and precompile account code management.
/// Pre-PrecompileGasFix: also handles ERC-20 ops directly (legacy gas).
/// Post-PrecompileGasFix: defers ERC-20 ops to DynPrecompile.
#[derive(Debug, Clone)]
pub struct NativeErc20Inspector {
    precompile: Arc<RwLock<Erc20Precompile>>,
    chain_spec: Arc<RaylsChainSpec>,
}

/// Build a `CallOutcome` that reverts with the given reason and the standard precompile error gas
/// charge.
fn reject_outcome(reason: &[u8], inputs: &CallInputs) -> CallOutcome {
    CallOutcome {
        result: InterpreterResult {
            result: InstructionResult::Revert,
            output: Bytes::copy_from_slice(reason),
            gas: Gas::new(BASE_ERROR_GAS.min(inputs.gas_limit)),
        },
        memory_offset: inputs.return_memory_offset.clone(),
        was_precompile_called: true,
        precompile_call_logs: vec![],
    }
}

impl NativeErc20Inspector {
    /// Create a new NativeErc20Inspector.
    #[inline]
    pub fn new(precompile: Arc<RwLock<Erc20Precompile>>, chain_spec: Arc<RaylsChainSpec>) -> Self {
        Self { precompile, chain_spec }
    }

    /// Set minimal bytecode on the precompile account so revm persists storage.
    fn ensure_precompile_code<DB: Database>(&self, context: &mut RaylsEvmContext<DB>)
    where
        DB::Error: 'static,
    {
        let needs_code = match context.journal_mut().load_account(ERC20_PRECOMPILE_ADDRESS) {
            Ok(acc) => {
                acc.data.info.code.is_none()
                    || acc.data.info.code.as_ref().is_some_and(|c| c.is_empty())
            }
            Err(_) => return,
        };

        if needs_code {
            let minimal_code = Bytecode::new_raw(Bytes::from_static(&[0x00]));
            let code_hash = minimal_code.hash_slow();
            if let Ok(mut acc) = context.journal_mut().load_account_mut(ERC20_PRECOMPILE_ADDRESS) {
                acc.set_code(code_hash, minimal_code);
            }
        }

        context.journal_mut().touch_account(ERC20_PRECOMPILE_ADDRESS);
    }

    /// Build a successful CallOutcome with legacy gas accounting.
    ///
    /// BUG: `Gas::new(gas_used)` sets `remaining = gas_used`, not `spent = gas_used`.
    /// When the parent frame recovers gas via `erase_cost(remaining)`, it only gets
    /// back `gas_used` (e.g. 400) instead of the full subcall budget minus cost
    /// (e.g. 73,329). The difference is silently lost. For a 100k gas transaction,
    /// a single `balanceOf` subcall leaks ~94% of the parent's gas budget.
    ///
    /// The correct pattern (used by `PrecompilesMap::run`) is:
    ///   `Gas::new(gas_limit)` then `record_cost(gas_used)`
    /// which sets `remaining = gas_limit - gas_used`, returning unspent gas properly.
    ///
    /// Post-PrecompileGasFix, the inspector returns `None` and lets the DynPrecompile
    /// handle execution with correct gas. This function is kept only to replay
    /// pre-fork blocks with identical (broken) behavior.
    #[deprecated(note = "use DynPrecompile path post-PrecompileGasFix")]
    #[allow(deprecated)]
    #[inline]
    fn success_outcome(output: PrecompileOutput, memory_offset: Range<usize>) -> CallOutcome {
        CallOutcome {
            result: InterpreterResult {
                result: InstructionResult::Return,
                output: output.bytes,
                gas: Gas::new(output.gas_used),
            },
            memory_offset,
            precompile_call_logs: vec![],
            was_precompile_called: true,
        }
    }

    /// Handle transfer(address to, uint256 amount) with balance modification and event emission.
    #[inline]
    fn handle_transfer<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &mut CallInputs,
        reject_empty_debits: bool,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        debug!("Handling transfer(address to, uint256 amount) - delegating to precompile");
        let calldata = inputs.input.bytes(context);
        let params = erc20::transferCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("transfer"))?;

        let output = self.precompile.read().handle_transfer(
            context,
            inputs.caller,
            params.to,
            params.amount,
            reject_empty_debits,
        )?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle balanceOf(address account) with direct balance query.
    #[inline]
    fn handle_balance_of<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        let calldata = inputs.input.bytes(context);
        let params = erc20::balanceOfCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("Invalid parameters: balanceOf"))?;

        let output = self.precompile.read().handle_balance_of(context, params.account)?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle allowance(address owner, address spender) with storage read.
    #[inline]
    fn handle_allowance<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        let calldata = inputs.input.bytes(context);
        let params = erc20::allowanceCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("Invalid parameters: allowance"))?;

        let output =
            self.precompile.read().handle_allowance(context, params.owner, params.spender)?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle approve(address spender, uint256 amount) with storage write and event emission.
    #[inline]
    fn handle_approve<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        let calldata = inputs.input.bytes(context);
        let params = erc20::approveCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("Invalid parameters: approve"))?;

        let output = self.precompile.read().handle_approve(
            context,
            inputs.caller,
            params.spender,
            params.amount,
        )?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle transferFrom(address from, address to, uint256 amount).
    #[inline]
    fn handle_transfer_from<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
        reject_empty_debits: bool,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        let calldata = inputs.input.bytes(context);
        let params = erc20::transferFromCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("Invalid parameters: transferFrom"))?;

        let output = self.precompile.read().handle_transfer_from(
            context,
            inputs.caller,
            params.from,
            params.to,
            params.amount,
            reject_empty_debits,
        )?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle mint(address to, uint256 amount) with access control and event emission.
    #[inline]
    fn handle_mint<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        debug!("Handling mint(address to, uint256 amount) - delegating to precompile");
        let calldata = inputs.input.bytes(context);
        let params = erc20::mintCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("Invalid parameters: mint"))?;

        let output =
            self.precompile.read().handle_mint(context, inputs.caller, params.to, params.amount)?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle burn(uint256 amount) - ERC20Burnable compliant burn from caller's balance.
    #[inline]
    fn handle_burn<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
        reject_empty_debits: bool,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        debug!(target: "native_erc20_inspector", "handle_burn: entry point");
        let calldata = inputs.input.bytes(context);
        let params = erc20::burnCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("Invalid parameters: burn"))?;

        let caller = inputs.caller;
        debug!(target: "native_erc20_inspector", "handle_burn: caller={:?} amount={:?}", caller, params.amount);

        let result =
            self.precompile.read().handle_burn(context, caller, params.amount, reject_empty_debits);

        match &result {
            Ok(output) => {
                debug!(target: "native_erc20_inspector", "handle_burn: success, gas_used={}", output.gas_used)
            }
            Err(e) => debug!(target: "native_erc20_inspector", "handle_burn: error={:?}", e),
        }

        #[allow(deprecated)]
        Ok(Self::success_outcome(result?, inputs.return_memory_offset.clone()))
    }

    /// Handle burnFrom(address account, uint256 amount) - ERC20Burnable compliant burn with
    /// allowance.
    #[inline]
    fn handle_burn_from<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
        reject_empty_debits: bool,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        debug!("Handling burnFrom(address account, uint256 amount) - delegating to precompile");
        let calldata = inputs.input.bytes(context);
        let params = erc20::burnFromCall::abi_decode(&calldata)
            .map_err(|_| ERC20Error::other("Invalid parameters: burnFrom"))?;

        let output = self.precompile.read().handle_burn_from(
            context,
            inputs.caller,
            params.account,
            params.amount,
            reject_empty_debits,
        )?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle transferWithAuthorization (EIP-3009).
    #[inline]
    fn handle_transfer_with_authorization<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
        reject_empty_debits: bool,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        let calldata = inputs.input.bytes(context);
        let params = eip3009::TransferWithAuthorizationCall::abi_decode(&calldata)
            .map_err(|e| ERC20Error::other(format!("Failed to decode params: {:?}", e)))?;

        let output = self
            .precompile
            .read()
            .handle_transfer_with_authorization(
                context,
                params.from,
                params.to,
                params.value,
                params.validAfter,
                params.validBefore,
                params.nonce,
                params.v,
                params.r.into(),
                params.s.into(),
                reject_empty_debits,
            )
            .inspect_err(|e| {
                error!(target: "native_erc20_inspector", "Error calling transferWithAuthorization: {:?}", e);
            })?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle receiveWithAuthorization (EIP-3009).
    #[inline]
    fn handle_receive_with_authorization<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
        reject_empty_debits: bool,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        let calldata = inputs.input.bytes(context);
        let params = eip3009::ReceiveWithAuthorizationCall::abi_decode(&calldata)
            .map_err(|e| ERC20Error::other(format!("Failed to decode params: {:?}", e)))?;

        let output = self.precompile.read().handle_receive_with_authorization(
            context,
            inputs.caller,
            params.from,
            params.to,
            params.value,
            params.validAfter,
            params.validBefore,
            params.nonce,
            params.v,
            params.r,
            params.s,
            reject_empty_debits,
        )?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }

    /// Handle cancelAuthorization (EIP-3009).
    #[inline]
    fn handle_cancel_authorization<DB: Database>(
        &self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
    ) -> Result<CallOutcome, ERC20Error>
    where
        DB::Error: 'static,
    {
        let calldata = inputs.input.bytes(context);
        let params = eip3009::CancelAuthorizationCall::abi_decode(&calldata)
            .map_err(|e| ERC20Error::other(format!("Failed to decode params: {:?}", e)))?;

        let output = self.precompile.read().handle_cancel_authorization(
            context,
            params.authorizer,
            params.nonce,
            params.v,
            params.r,
            params.s,
        )?;

        #[allow(deprecated)]
        Ok(Self::success_outcome(output, inputs.return_memory_offset.clone()))
    }
}

/// Implement StateAccessAdapter for RaylsEvmContext to allow precompile to access state.
impl<DB> StateAccessAdapter for RaylsEvmContext<DB>
where
    DB: Database,
    DB::Error: 'static,
{
    fn perform_load_account(
        &mut self,
        address: Address,
    ) -> Result<reth_revm::interpreter::StateLoad<&Account>, Box<dyn std::error::Error>> {
        self.journal_mut()
            .load_account(address)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    fn perform_set_balance(
        &mut self,
        address: Address,
        balance: U256,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.journal_mut()
            .load_account_mut(address)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?
            .set_balance(balance);
        Ok(())
    }

    fn perform_sload(
        &mut self,
        contract_address: Address,
        key: StorageKey,
    ) -> Result<reth_revm::interpreter::StateLoad<StorageValue>, Box<dyn std::error::Error>> {
        self.journal_mut()
            .sload(contract_address, key)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    fn perform_sstore(
        &mut self,
        contract_address: Address,
        key: StorageKey,
        value: StorageValue,
    ) -> Result<
        reth_revm::interpreter::StateLoad<reth_revm::interpreter::SStoreResult>,
        Box<dyn std::error::Error>,
    > {
        self.journal_mut()
            .sstore(contract_address, key, value)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    fn perform_touch_account(&mut self, address: Address) {
        self.journal_mut().touch_account(address);
    }

    fn perform_log(&mut self, log: Log) {
        self.journal_mut().log(log);
    }

    fn get_block_timestamp(&self) -> U256 {
        self.block.timestamp
    }
}

impl<DB> Inspector<RaylsEvmContext<DB>, EthInterpreter> for NativeErc20Inspector
where
    DB: Database,
    DB::Error: 'static,
{
    fn initialize_interp(
        &mut self,
        _interp: &mut Interpreter<EthInterpreter>,
        _context: &mut RaylsEvmContext<DB>,
    ) {
        // Initialize interpreter if needed
    }

    fn step(
        &mut self,
        _interp: &mut Interpreter<EthInterpreter>,
        _context: &mut RaylsEvmContext<DB>,
    ) {
        // Step through execution if needed
    }

    fn step_end(
        &mut self,
        _interp: &mut Interpreter<EthInterpreter>,
        _context: &mut RaylsEvmContext<DB>,
    ) {
        // End of step handling if needed
    }

    fn log(&mut self, _context: &mut RaylsEvmContext<DB>, _log: Log) {
        // Log handling if needed
    }

    fn call(
        &mut self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        if inputs.target_address != ERC20_PRECOMPILE_ADDRESS {
            return None;
        }

        // post-fork: let PrecompilesMap::run handle execution with correct gas.
        // Touch the account so the journal tracks and static calls return data through the
        // precompile handler
        if self.chain_spec.is_precompile_gas_fix_active_at_block(context.block.number.to::<u64>()) {
            context.journal_mut().touch_account(ERC20_PRECOMPILE_ADDRESS);
            return None;
        }

        // pre-fork: ensure precompile account has code so storage persists
        self.ensure_precompile_code(context);

        // pre-fork: legacy inspector path (broken gas, matches history)
        let reject_empty_debits = self
            .chain_spec
            .is_eip161_empty_account_guard_active_at_block(context.block.number.to::<u64>());
        let calldata = inputs.input.bytes(context);
        let selector = Erc20Selector::from_calldata(&calldata)?;

        // precompile is non-payable; journal layer doesn't enforce is_static for custom precompiles
        if inputs.value.transfer().is_some_and(|v| !v.is_zero()) {
            return Some(reject_outcome(
                b"ERC-20 precompile rejects native value transfer",
                inputs,
            ));
        }
        if inputs.is_static && selector.is_state_mutating() {
            return Some(reject_outcome(
                b"ERC-20 state mutation forbidden under static call",
                inputs,
            ));
        }

        let result = match selector {
            Erc20Selector::Transfer => self.handle_transfer(context, inputs, reject_empty_debits),
            Erc20Selector::BalanceOf => self.handle_balance_of(context, inputs),
            Erc20Selector::Allowance => self.handle_allowance(context, inputs),
            Erc20Selector::Approve => self.handle_approve(context, inputs),
            Erc20Selector::TransferFrom => {
                self.handle_transfer_from(context, inputs, reject_empty_debits)
            }
            Erc20Selector::Mint => self.handle_mint(context, inputs),
            Erc20Selector::Burn => self.handle_burn(context, inputs, reject_empty_debits),
            Erc20Selector::BurnFrom => self.handle_burn_from(context, inputs, reject_empty_debits),
            Erc20Selector::TransferWithAuthorization => {
                self.handle_transfer_with_authorization(context, inputs, reject_empty_debits)
            }
            Erc20Selector::ReceiveWithAuthorization => {
                self.handle_receive_with_authorization(context, inputs, reject_empty_debits)
            }
            Erc20Selector::CancelAuthorization => self.handle_cancel_authorization(context, inputs),
            _ => return None,
        };

        match result {
            Ok(outcome) => Some(outcome),
            Err(err) => Some(CallOutcome {
                result: InterpreterResult {
                    result: InstructionResult::Revert,
                    output: err.into(),
                    gas: Gas::new(BASE_ERROR_GAS.min(inputs.gas_limit)),
                },
                memory_offset: inputs.return_memory_offset.clone(),
                was_precompile_called: true,
                precompile_call_logs: vec![],
            }),
        }
    }

    fn call_end(
        &mut self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CallInputs,
        outcome: &mut CallOutcome,
    ) {
        // Only process successful calls
        if !outcome.result.result.is_ok() {
            return;
        }

        // emit Transfer event for native value transfers (not to precompile --
        // the precompile handlers emit their own events directly)
        if let Some(transfer_value) = inputs.value.transfer() {
            if transfer_value > U256::ZERO && inputs.target_address != ERC20_PRECOMPILE_ADDRESS {
                let event_log = Erc20Precompile::encode_transfer_event(
                    ERC20_PRECOMPILE_ADDRESS,
                    inputs.caller,
                    inputs.target_address,
                    transfer_value,
                );
                context.journaled_state.log(event_log);
            }
        }
    }

    fn create(
        &mut self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &mut CreateInputs,
    ) -> Option<CreateOutcome> {
        // Check if ETH is being sent with contract creation
        if inputs.value() > U256::ZERO {
            // Calculate the new contract address (CREATE2 or CREATE)
            let _new_contract_address = match inputs.scheme() {
                CreateScheme::Create => {
                    // CREATE: address = keccak256(rlp([sender, nonce]))[12:]
                    let sender_nonce = context
                        .journal_mut()
                        .load_account(inputs.caller())
                        .map(|acc| acc.data.info.nonce)
                        .unwrap_or(0);

                    // Use alloy's address calculation
                    inputs.caller().create(sender_nonce.saturating_sub(1))
                }
                CreateScheme::Create2 { salt } => {
                    // CREATE2: address = keccak256(0xff ++ sender ++ salt ++
                    // keccak256(init_code))[12:]
                    inputs.caller().create2_from_code(salt.to_be_bytes(), inputs.init_code())
                }
                CreateScheme::Custom { .. } => {
                    // Custom creation scheme - don't process, let it proceed normally
                    return None;
                }
            };
        }

        // Let the creation proceed normally
        None
    }

    fn create_end(
        &mut self,
        context: &mut RaylsEvmContext<DB>,
        inputs: &CreateInputs,
        outcome: &mut CreateOutcome,
    ) {
        debug!(
            target: "native_erc20_inspector",
            "create_end() invoked: result={:?}, value={}, address={:?}, caller={:?}",
            outcome.result.result,
            inputs.value(),
            outcome.address,
            inputs.caller()
        );

        if !outcome.result.result.is_ok() {
            debug!(
                target: "native_erc20_inspector",
                "create_end(): creation failed with {:?}, skipping Transfer event",
                outcome.result.result
            );
            return;
        }

        // Check if ETH was sent with contract creation
        if let Some(addr) = outcome.address {
            if inputs.value() > U256::ZERO {
                debug!(
                    target: "native_erc20_inspector",
                    "create_end(): emitting Transfer event from {:?} to {:?} for {} wei",
                    inputs.caller(),
                    addr,
                    inputs.value()
                );
                let transfer_event = Erc20Precompile::encode_transfer_event(
                    ERC20_PRECOMPILE_ADDRESS,
                    inputs.caller(),
                    addr,
                    inputs.value(),
                );
                context.journaled_state.log(transfer_event);
                debug!(
                    target: "native_erc20_inspector",
                    "create_end(): Transfer event logged successfully"
                );
            } else {
                debug!(
                    target: "native_erc20_inspector",
                    "create_end(): value is zero, skipping Transfer event"
                );
            }
        } else {
            debug!(
                target: "native_erc20_inspector",
                "create_end(): no address in outcome, skipping Transfer event"
            );
        }
    }

    fn selfdestruct(&mut self, _contract: Address, _target: Address, _value: U256) {
        // Handle selfdestruct if needed
    }
}

#[cfg(test)]
mod tests {
    use crate::native_erc20::{
        precompile::{Erc20Precompile, Erc20TokenConfig},
        ERC20_PRECOMPILE_ADDRESS,
    };

    fn test_precompile() -> Erc20Precompile {
        Erc20Precompile::new(Erc20TokenConfig::default(), ERC20_PRECOMPILE_ADDRESS, 1)
    }

    // ── Metadata handlers always succeed (invariant for staticcall safety) ──

    #[test]
    fn decimals_returns_18_for_default_config() {
        let precompile = test_precompile();
        let output = precompile.handle_decimals().expect("decimals must not fail");
        assert_eq!(output.bytes.len(), 32, "ABI-encoded uint8 must be 32 bytes");
        assert_eq!(output.bytes[31], 18, "default decimals must be 18");
    }

    #[test]
    fn name_returns_usd_rayls_for_default_config() {
        let precompile = test_precompile();
        let output = precompile.handle_name().expect("name must not fail");
        // ABI-encoded string: offset (32) + length (32) + padded data
        assert!(output.bytes.len() >= 64);
        let len = alloy::primitives::U256::from_be_slice(&output.bytes[32..64]).to::<usize>();
        let name = std::str::from_utf8(&output.bytes[64..64 + len]).unwrap();
        assert_eq!(name, "USD Rayls");
    }

    #[test]
    fn symbol_returns_usdr_for_default_config() {
        let precompile = test_precompile();
        let output = precompile.handle_symbol().expect("symbol must not fail");
        assert!(output.bytes.len() >= 64);
        let len = alloy::primitives::U256::from_be_slice(&output.bytes[32..64]).to::<usize>();
        let symbol = std::str::from_utf8(&output.bytes[64..64 + len]).unwrap();
        assert_eq!(symbol, "USDr");
    }

    #[test]
    fn metadata_handlers_are_infallible() {
        // These handlers take no state access and must always succeed.
        // This is the invariant that ensures contract staticcalls to
        // decimals()/name()/symbol() never revert via the DynPrecompile path.
        let precompile = test_precompile();
        assert!(precompile.handle_name().is_ok());
        assert!(precompile.handle_symbol().is_ok());
        assert!(precompile.handle_decimals().is_ok());
    }

    #[test]
    fn metadata_handlers_with_custom_config() {
        let config = Erc20TokenConfig {
            name: "Custom Token".to_string(),
            symbol: "CTK".to_string(),
            decimals: 6,
            version: "2.0.0".to_string(),
        };
        let precompile = Erc20Precompile::new(config, ERC20_PRECOMPILE_ADDRESS, 42);

        let decimals_out = precompile.handle_decimals().unwrap();
        assert_eq!(decimals_out.bytes[31], 6);

        let name_out = precompile.handle_name().unwrap();
        let len = alloy::primitives::U256::from_be_slice(&name_out.bytes[32..64]).to::<usize>();
        let name = std::str::from_utf8(&name_out.bytes[64..64 + len]).unwrap();
        assert_eq!(name, "Custom Token");

        let symbol_out = precompile.handle_symbol().unwrap();
        let len = alloy::primitives::U256::from_be_slice(&symbol_out.bytes[32..64]).to::<usize>();
        let symbol = std::str::from_utf8(&symbol_out.bytes[64..64 + len]).unwrap();
        assert_eq!(symbol, "CTK");
    }
}
