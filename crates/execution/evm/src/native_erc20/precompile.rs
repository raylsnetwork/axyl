//! Core ERC-20 precompile implementation.
//!
//! This module provides the main [`Erc20Precompile`] struct that implements all
//! ERC-20 operations including standard methods, mint/burn, and EIP-3009 meta-transactions.

use crate::native_erc20::{
    abi::{
        eip3009, encode_bool, encode_revert_reason, encode_string, encode_uint256, encode_uint8,
        erc20, verify_cancel_authorization, verify_transfer_authorization,
    },
    storage::{allowance_slot, authorization_nonce_slot, total_supply_slot},
    MINTING_MODULE_ADDRESS,
};
use alloy::{
    primitives::{Address, Bytes, Log, B256, KECCAK256_EMPTY, U256},
    sol_types::SolEvent,
};
use alloy_evm::traits::EvmInternals;
use reth_revm::{
    interpreter::{SStoreResult, StateLoad},
    primitives::{StorageKey, StorageValue},
    state::Account,
};
use serde::Serialize;
use std::error::Error;
use thiserror::Error;
use tracing::{debug, error, trace};

/// Configuration for the ERC-20 token metadata.
#[derive(Debug, Clone)]
pub struct Erc20TokenConfig {
    /// Token name (e.g., "USD Rayls").
    pub name: String,
    /// Token symbol (e.g., "USDr").
    pub symbol: String,
    /// Token decimals (typically 18).
    pub decimals: u8,
    /// EIP-712 domain version string.
    pub version: String,
}

impl Default for Erc20TokenConfig {
    fn default() -> Self {
        Self {
            name: "USD Rayls".to_string(),
            symbol: "USDr".to_string(),
            decimals: 18,
            version: "1.0.0".to_string(),
        }
    }
}

/// The main ERC-20 precompile struct.
#[derive(Debug, Clone)]
pub struct Erc20Precompile {
    /// Token name.
    pub name: String,
    /// Token symbol.
    pub symbol: String,
    /// Token decimals.
    pub decimals: u8,
    /// Precompile contract address.
    pub contract_address: Address,

    // EIP-3009
    /// EIP-712 domain version.
    pub version: String,
    /// Chain ID for EIP-712 domain.
    pub chain_id: u64,

    /// EIP-712 domain for signature verification (constructed once at initialization).
    pub domain: alloy::sol_types::Eip712Domain,
}

/// Trait for abstracting state access operations.
///
/// This allows the precompile to work with both `EvmInternals` (for DynPrecompile/eth_call)
/// and `RaylsEvmContext<DB>` (for Inspector/transaction execution).
pub trait StateAccessAdapter {
    /// Load an account from state (read-only).
    fn perform_load_account(
        &mut self,
        address: Address,
    ) -> Result<StateLoad<&Account>, Box<dyn std::error::Error>>;

    /// Get an account's balance.
    fn perform_get_balance(
        &mut self,
        address: Address,
    ) -> Result<U256, Box<dyn std::error::Error>> {
        Ok(self.perform_load_account(address)?.data.info.balance)
    }

    /// Set an account's balance via the journal (loads account mutably, then sets balance).
    fn perform_set_balance(
        &mut self,
        address: Address,
        balance: U256,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// Load a storage value.
    fn perform_sload(
        &mut self,
        contract_address: Address,
        key: StorageKey,
    ) -> Result<StateLoad<StorageValue>, Box<dyn std::error::Error>>;

    /// Store a storage value.
    fn perform_sstore(
        &mut self,
        contract_address: Address,
        key: StorageKey,
        value: StorageValue,
    ) -> Result<StateLoad<SStoreResult>, Box<dyn std::error::Error>>;

    /// Mark an account as touched.
    fn perform_touch_account(&mut self, address: Address);

    /// Emit a log.
    fn perform_log(&mut self, log: Log);

    /// Get the current block timestamp.
    fn get_block_timestamp(&self) -> U256;
}

impl StateAccessAdapter for EvmInternals<'_> {
    fn perform_load_account(
        &mut self,
        address: Address,
    ) -> Result<StateLoad<&Account>, Box<dyn Error>> {
        self.load_account(address).map_err(Into::into)
    }

    fn perform_set_balance(
        &mut self,
        address: Address,
        balance: U256,
    ) -> Result<(), Box<dyn Error>> {
        self.load_account_mut(address)
            .map_err(|e| Box::new(e) as Box<dyn Error>)?
            .set_balance(balance);
        Ok(())
    }

    fn perform_sload(
        &mut self,
        address: Address,
        key: StorageKey,
    ) -> Result<StateLoad<StorageValue>, Box<dyn Error>> {
        self.sload(address, key).map_err(Into::into)
    }

    fn perform_sstore(
        &mut self,
        address: Address,
        key: StorageKey,
        value: StorageValue,
    ) -> Result<StateLoad<SStoreResult>, Box<dyn Error>> {
        self.sstore(address, key, value).map_err(Into::into)
    }

    fn perform_touch_account(&mut self, address: Address) {
        let _ = self.touch_account(address);
    }

    fn perform_log(&mut self, log: Log) {
        self.log(log);
    }

    fn get_block_timestamp(&self) -> U256 {
        self.block_timestamp()
    }
}

impl Erc20Precompile {
    fn get_allowance<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        owner: Address,
        spender: Address,
    ) -> Result<StorageValue, ERC20Error> {
        // Calculate storage slot for allowance
        let slot = allowance_slot(owner, spender);
        // Read allowance from storage at the precompile address
        let state = state_access
            .perform_sload(self.contract_address, slot)
            .map_err(|e| ERC20Error::read_error(ERC20Resource::Allowance, e))?;
        Ok(state.data)
    }

    fn get_balance<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        address: Address,
    ) -> Result<U256, ERC20Error> {
        state_access
            .perform_get_balance(address)
            .map_err(|e| ERC20Error::read_error(ERC20Resource::Account(address), e))
    }

    fn set_balance<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        address: Address,
        balance: U256,
    ) -> Result<(), ERC20Error> {
        state_access
            .perform_set_balance(address, balance)
            .map_err(|e| ERC20Error::write_error(ERC20Resource::Account(address), e))
    }

    fn get_total_supply<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
    ) -> Result<StorageValue, ERC20Error> {
        let total_supply_slot_key = total_supply_slot();
        let total_supply = state_access
            .perform_sload(self.contract_address, total_supply_slot_key)
            .map_err(|e| ERC20Error::read_error(ERC20Resource::TotalSupply, e))?;
        Ok(total_supply.data)
    }

    fn set_allowance<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        owner: Address,
        spender: Address,
        amount: U256,
    ) -> Result<StateLoad<SStoreResult>, ERC20Error> {
        // Note: Precompile account is warmed in inspector before this handler is called
        // Calculate storage slot for allowance
        let slot = allowance_slot(owner, spender);

        // Write allowance to storage at the precompile's address
        state_access
            .perform_sstore(self.contract_address, slot, amount)
            .map_err(|e| ERC20Error::write_error(ERC20Resource::Allowance, e))
    }

    fn set_total_supply<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        amount: U256,
    ) -> Result<StateLoad<SStoreResult>, ERC20Error> {
        let total_supply_slot_key = total_supply_slot();
        state_access
            .perform_sstore(self.contract_address, total_supply_slot_key, amount)
            .map_err(|e| ERC20Error::write_error(ERC20Resource::TotalSupply, e))
    }

    fn check_approver(&self, approver: Address) -> Result<(), ERC20Error> {
        if approver == Address::ZERO {
            return Err(ERC20Error::ERC20InvalidApprover(Address::ZERO));
        }
        Ok(())
    }

    fn check_spender(&self, spender: Address) -> Result<(), ERC20Error> {
        if spender == Address::ZERO {
            return Err(ERC20Error::ERC20InvalidSpender(Address::ZERO));
        }
        Ok(())
    }

    fn check_balance(
        &self,
        sender: Address,
        balance: U256,
        needed: U256,
    ) -> Result<(), ERC20Error> {
        if balance < needed {
            trace!(target: "erc20_precompile", ?sender, ?balance, ?needed, "insufficient balance");
            return Err(ERC20Error::ERC20InsufficientBalance(sender, balance, needed));
        }
        Ok(())
    }

    /// Reject a debit that would leave `account` EIP-161 empty
    /// (`balance == 0 && nonce == 0 && code_hash == KECCAK256_EMPTY`).
    ///
    /// A `Loaded` account that becomes empty inside a transaction makes revm's commit hit
    /// `unreachable!("Wrong state transition, touch empty is not possible from Loaded")` and
    /// halts the chain. The check runs before any balance write, so a rejected transfer leaves
    /// the state untouched.
    fn check_not_left_empty<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        account: Address,
        new_balance: U256,
    ) -> Result<(), ERC20Error> {
        if !new_balance.is_zero() {
            return Ok(());
        }
        let (nonce, code_hash) = {
            let loaded = state_access
                .perform_load_account(account)
                .map_err(|e| ERC20Error::read_error(ERC20Resource::Account(account), e))?;
            (loaded.data.info.nonce, loaded.data.info.code_hash)
        };
        if nonce == 0 && code_hash == KECCAK256_EMPTY {
            trace!(
                target: "erc20_precompile",
                ?account,
                "debit would leave the account EIP-161 empty; rejecting"
            );
            return Err(ERC20Error::TransferWouldEmptyAccount(account));
        }
        Ok(())
    }

    fn check_allowance(
        &self,
        spender: Address,
        allowance: U256,
        needed: U256,
        is_unlimited: bool,
    ) -> Result<(), ERC20Error> {
        if !is_unlimited && allowance < needed {
            return Err(ERC20Error::ERC20InsufficientAllowance(spender, allowance, needed));
        }
        Ok(())
    }

    fn check_whitelist(&self, caller: Address) -> Result<(), ERC20Error> {
        if caller != MINTING_MODULE_ADDRESS {
            return Err(ERC20Error::NotWhitelisted(caller));
        }
        Ok(())
    }

    fn check_sender(&self, sender: Address) -> Result<(), ERC20Error> {
        if sender == Address::ZERO {
            return Err(ERC20Error::ERC20InvalidSender(Address::ZERO));
        }
        Ok(())
    }

    fn check_receiver(&self, receiver: Address) -> Result<(), ERC20Error> {
        if receiver == Address::ZERO {
            return Err(ERC20Error::ERC20InvalidReceiver(Address::ZERO));
        }
        Ok(())
    }

    /// Transfer balance between accounts (shared logic for transfer and transferFrom).
    fn transfer_balance<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        sender: Address,
        receiver: Address,
        amount: U256,
    ) -> Result<(), ERC20Error> {
        // Debit sender
        {
            let balance = self.get_balance(state_access, sender)?;
            self.check_balance(sender, balance, amount)?;
            let new_balance = balance.checked_sub(amount).ok_or_else(|| {
                ERC20Error::overflow_error(ERC20Resource::Balance, ERC20Method::Transfer)
            })?;
            self.check_not_left_empty(state_access, sender, new_balance)?;
            self.set_balance(state_access, sender, new_balance)?;
        }
        state_access.perform_touch_account(sender);

        // Credit receiver
        {
            let balance = self.get_balance(state_access, receiver)?;
            let new_balance = balance.checked_add(amount).ok_or_else(|| {
                ERC20Error::overflow_error(ERC20Resource::Balance, ERC20Method::Transfer)
            })?;
            self.set_balance(state_access, receiver, new_balance)?;
        }
        state_access.perform_touch_account(receiver);
        Ok(())
    }
}

impl Erc20Precompile {
    /// Create a new ERC-20 precompile instance.
    pub fn new(config: Erc20TokenConfig, contract_address: Address, chain_id: u64) -> Self {
        // Construct EIP-712 domain once at initialization
        let domain = alloy::sol_types::Eip712Domain {
            name: Some(config.name.clone().into()),
            version: Some(config.version.clone().into()),
            chain_id: Some(U256::from(chain_id)),
            verifying_contract: Some(contract_address),
            salt: None,
        };

        Self {
            name: config.name,
            symbol: config.symbol,
            decimals: config.decimals,
            contract_address,
            chain_id,
            version: config.version,
            domain,
        }
    }

    /// Create a precompile with default native token configuration.
    pub fn default_native(contract_address: Address, chain_id: u64) -> Self {
        Self::new(Erc20TokenConfig::default(), contract_address, chain_id)
    }

    /// Create an ERC-20 precompile instance from genesis configuration.
    ///
    /// Note: Total supply is initialized in EVM storage at genesis time,
    /// not in this precompile instance. This method no longer requires
    /// genesis allocations as the total supply is read directly from
    /// storage when needed.
    pub fn from_genesis(
        contract_address: Address,
        chain_id: u64,
        config: Erc20TokenConfig,
    ) -> Self {
        Self::new(config, contract_address, chain_id)
    }

    /// Handle name() - returns the token name.
    pub(crate) fn handle_name(&self) -> Result<PrecompileOutput, ERC20Error> {
        Ok(PrecompileOutput::new(GAS_NAME, encode_string(&self.name)))
    }

    /// Handle symbol() - returns the token symbol.
    pub(crate) fn handle_symbol(&self) -> Result<PrecompileOutput, ERC20Error> {
        Ok(PrecompileOutput::new(GAS_SYMBOL, encode_string(&self.symbol)))
    }

    /// Handle decimals() - returns the token decimals.
    pub(crate) fn handle_decimals(&self) -> Result<PrecompileOutput, ERC20Error> {
        Ok(PrecompileOutput::new(GAS_DECIMALS, encode_uint8(self.decimals)))
    }

    /// Handle totalSupply() - returns the total supply from storage.
    pub fn handle_total_supply<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", "handle_total_supply called");
        // Read total supply from storage (initialized at genesis)
        let total_supply_slot_key = total_supply_slot();
        let total_supply = state_access
            .perform_sload(self.contract_address, total_supply_slot_key)
            .map_err(|e| ERC20Error::read_error(ERC20Resource::TotalSupply, e))?
            .data;

        Ok(PrecompileOutput::new(GAS_TOTAL_SUPPLY, encode_uint256(total_supply)))
    }

    /// Handle balanceOf(address) - returns the native balance of the address.
    pub fn handle_balance_of<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        address: Address,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", "handle_balance_of called with address {:?}", address);
        let balance = self.get_balance(state_access, address)?;

        // Encode balance as uint256
        Ok(PrecompileOutput::new(GAS_BALANCE_QUERY, encode_uint256(balance)))
    }

    /// Handle allowance(address,address) - returns the allowance from storage.
    pub fn handle_allowance<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        owner: Address,
        spender: Address,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", "handle_allowance called with address {:?} {:?}", owner, spender);
        let allowance = self.get_allowance(state_access, owner, spender)?;

        // Encode allowance as uint256
        Ok(PrecompileOutput::new(GAS_ALLOWANCE_QUERY, encode_uint256(allowance)))
    }

    /// Handle transfer(address,uint256) - transfers native balance between accounts.
    pub fn handle_transfer<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        sender: Address,
        receiver: Address,
        amount: U256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", ?sender, ?receiver, ?amount, "handle_transfer");

        // ERC20 compliance: validate sender and receiver addresses
        self.check_sender(sender)?;
        self.check_receiver(receiver)?;

        if amount.is_zero() {
            return Ok(PrecompileOutput::new(GAS_TRANSFER, encode_bool(true)));
        }

        // Self-transfer check
        if sender == receiver {
            return Ok(PrecompileOutput::new(GAS_TRANSFER, encode_bool(true)));
        }

        self.transfer_balance(state_access, sender, receiver, amount)?;

        // emit Transfer event
        let event = Self::encode_transfer_event(self.contract_address, sender, receiver, amount);
        state_access.perform_log(event);

        Ok(PrecompileOutput::new(GAS_TRANSFER, encode_bool(true)))
    }

    /// Handle approve(address,uint256) - sets allowance in storage.
    pub fn handle_approve<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        owner: Address,
        spender: Address,
        amount: U256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", "handle_approve called with address {:?} {:?} {:?}", owner, spender, amount);

        self.check_approver(owner)?;
        self.check_spender(spender)?;

        // Per ERC-20 spec: approve MUST set allowance even for self-approval (owner == spender)
        // No special case - always write to storage
        self.set_allowance(state_access, owner, spender, amount)?;

        // Mark precompile account as touched to ensure storage change persists
        state_access.perform_touch_account(self.contract_address);

        // Emit Approval event
        let event_log = Self::encode_approval_event(self.contract_address, owner, spender, amount);
        state_access.perform_log(event_log);

        // Return success (bool true)
        Ok(PrecompileOutput::new(GAS_APPROVE, encode_bool(true)))
    }

    /// Handle transferFrom(address,address,uint256) - transfers with allowance check.
    pub fn handle_transfer_from<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        spender: Address,
        sender: Address,
        receiver: Address,
        amount: U256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", ?spender, ?sender, ?receiver, ?amount, "handle_transfer_from");

        // ERC20 compliance: validate sender and receiver addresses
        self.check_sender(sender)?;
        self.check_receiver(receiver)?;

        if amount.is_zero() {
            return Ok(PrecompileOutput::new(GAS_TRANSFER_FROM, encode_bool(true)));
        }

        // CRITICAL: Handle allowance BEFORE self-transfer optimization
        // Per ERC-20 spec, transferFrom MUST decrease allowance even for self-transfers
        let current_spender_allowance = self.get_allowance(state_access, sender, spender)?;

        // Check if allowance is unlimited (U256::MAX) - no need to decrease
        let is_unlimited = current_spender_allowance == U256::MAX;

        self.check_allowance(spender, current_spender_allowance, amount, is_unlimited)?;

        // Self-transfer optimization: Skip balance updates (but allowance already handled above)
        if sender == receiver {
            // Still need to decrease allowance (unless unlimited)
            if !is_unlimited {
                let new_spender_allowance =
                    current_spender_allowance.checked_sub(amount).ok_or_else(|| {
                        ERC20Error::overflow_error(ERC20Resource::Allowance, ERC20Method::Transfer)
                    })?;

                self.set_allowance(state_access, sender, spender, new_spender_allowance)?;

                // Mark precompile account as touched to ensure storage change persists
                state_access.perform_touch_account(self.contract_address);
            }
            let event =
                Self::encode_transfer_event(self.contract_address, sender, receiver, amount);
            state_access.perform_log(event);
            return Ok(PrecompileOutput::new(GAS_TRANSFER_FROM, encode_bool(true)));
        }

        self.transfer_balance(state_access, sender, receiver, amount)?;

        // decrease allowance (unless unlimited)
        if !is_unlimited {
            let new_spender_allowance =
                current_spender_allowance.checked_sub(amount).ok_or_else(|| {
                    ERC20Error::overflow_error(ERC20Resource::Allowance, ERC20Method::Transfer)
                })?;

            self.set_allowance(state_access, sender, spender, new_spender_allowance)?;
            state_access.perform_touch_account(self.contract_address);
        }

        let event = Self::encode_transfer_event(self.contract_address, sender, receiver, amount);
        state_access.perform_log(event);
        Ok(PrecompileOutput::new(GAS_TRANSFER_FROM, encode_bool(true)))
    }

    /// Handle mint(address,uint256) - mints new tokens (access controlled).
    pub fn handle_mint<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        caller: Address,
        receiver: Address,
        amount: U256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", "handle_mint called with address {:?} {:?} {:?}", caller, receiver, amount);
        self.check_whitelist(caller)?;

        // ERC20 compliance: cannot mint to zero address
        self.check_receiver(receiver)?;

        if amount.is_zero() {
            return Err(ERC20Error::zero_amount_error(ERC20Method::Mint));
        }

        let current_total_supply = self.get_total_supply(state_access)?;

        // Calculate new total supply (check for overflow)
        let new_total_supply = current_total_supply.checked_add(amount).ok_or_else(|| {
            ERC20Error::overflow_error(ERC20Resource::TotalSupply, ERC20Method::Mint)
        })?;

        self.set_total_supply(state_access, new_total_supply)?;

        // Mark precompile account as touched to ensure storage change persists
        state_access.perform_touch_account(self.contract_address);

        // Load and update recipient balance
        {
            let receiver_balance = self.get_balance(state_access, receiver)?;

            // Check for overflow and calculate new balance
            let new_receiver_balance = receiver_balance.checked_add(amount).ok_or_else(|| {
                ERC20Error::overflow_error(ERC20Resource::Balance, ERC20Method::Mint)
            })?;

            // Update recipient balance
            self.set_balance(state_access, receiver, new_receiver_balance)?;
        }
        state_access.perform_touch_account(receiver);

        // Emit Mint event
        let mint_event = Self::encode_mint_event(self.contract_address, receiver, amount);
        state_access.perform_log(mint_event);

        // Emit Transfer event
        let transfer_event =
            Self::encode_transfer_event(self.contract_address, Address::ZERO, receiver, amount);
        state_access.perform_log(transfer_event);

        // Return success (bool true)
        Ok(PrecompileOutput::new(GAS_MINT, encode_bool(true)))
    }

    /// Handle burn(uint256) - burns tokens from caller's balance (access controlled).
    pub fn handle_burn<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        caller: Address,
        amount: U256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", ?caller, ?amount, "handle_burn");

        self.check_whitelist(caller)?;

        let sender = caller;

        // Cannot burn from zero address
        if sender.is_zero() {
            return Err(ERC20Error::ERC20InvalidSender(Address::ZERO));
        }

        if amount.is_zero() {
            return Err(ERC20Error::zero_amount_error(ERC20Method::Burn));
        }

        // Get sender balance BEFORE any modifications
        let sender_balance = self.get_balance(state_access, sender)?;
        trace!(target: "erc20_precompile", ?sender_balance, "burn: loaded sender balance");

        self.check_balance(sender, sender_balance, amount)?;
        self.check_not_left_empty(state_access, sender, sender_balance - amount)?;

        // Update total supply first (same order as mint)
        let current_total_supply = self.get_total_supply(state_access)?;
        let new_total_supply = current_total_supply.checked_sub(amount).ok_or_else(|| {
            ERC20Error::overflow_error(ERC20Resource::TotalSupply, ERC20Method::Burn)
        })?;
        trace!(target: "erc20_precompile", ?current_total_supply, ?new_total_supply, "burn: updating total supply");

        self.set_total_supply(state_access, new_total_supply)?;
        state_access.perform_touch_account(self.contract_address);

        // Update sender balance
        {
            let new_sender_balance = sender_balance.checked_sub(amount).ok_or_else(|| {
                ERC20Error::overflow_error(ERC20Resource::Balance, ERC20Method::Burn)
            })?;
            self.set_balance(state_access, sender, new_sender_balance)?;
            trace!(target: "erc20_precompile", ?new_sender_balance, "burn: updated sender balance");
        }
        state_access.perform_touch_account(sender);

        // Emit events
        let burn_event = Self::encode_burn_event(self.contract_address, sender, amount);
        state_access.perform_log(burn_event);

        let transfer_event =
            Self::encode_transfer_event(self.contract_address, sender, Address::ZERO, amount);
        state_access.perform_log(transfer_event);

        Ok(PrecompileOutput::new(GAS_BURN, encode_bool(true)))
    }

    /// Handle burnFrom(address,uint256) - burns from another account with allowance (access
    /// controlled).
    pub fn handle_burn_from<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        caller: Address,
        sender: Address,
        amount: U256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        debug!(target: "erc20_precompile", ?caller, ?sender, ?amount, "handle_burn_from");

        // Whitelist check required for ALL callers (including minting module)
        self.check_whitelist(caller)?;

        // Cannot burn from zero address
        if sender.is_zero() {
            return Err(ERC20Error::ERC20InvalidSender(Address::ZERO));
        }

        if amount.is_zero() {
            return Err(ERC20Error::zero_amount_error(ERC20Method::Burn));
        }

        // ERC20Burnable compliance: burnFrom ALWAYS requires allowance
        // Even whitelisted callers must have allowance from the account being burned from
        let current_allowance = self.get_allowance(state_access, sender, caller)?;
        let is_unlimited = current_allowance == U256::MAX;
        self.check_allowance(caller, current_allowance, amount, is_unlimited)?;
        trace!(target: "erc20_precompile", ?current_allowance, "burn_from: allowance check passed");

        // Get account balance BEFORE any modifications
        let account_balance = self.get_balance(state_access, sender)?;
        trace!(target: "erc20_precompile", ?account_balance, "burn_from: loaded account balance");

        self.check_balance(sender, account_balance, amount)?;
        self.check_not_left_empty(state_access, sender, account_balance - amount)?;

        // Update total supply first (same order as mint)
        let current_total_supply = self.get_total_supply(state_access)?;
        let new_total_supply = current_total_supply.checked_sub(amount).ok_or_else(|| {
            ERC20Error::overflow_error(ERC20Resource::TotalSupply, ERC20Method::Burn)
        })?;
        trace!(target: "erc20_precompile", ?current_total_supply, ?new_total_supply, "burn_from: updating total supply");

        self.set_total_supply(state_access, new_total_supply)?;
        state_access.perform_touch_account(self.contract_address);

        // Update account balance
        {
            let new_balance = account_balance.checked_sub(amount).ok_or_else(|| {
                ERC20Error::overflow_error(ERC20Resource::Balance, ERC20Method::Burn)
            })?;
            self.set_balance(state_access, sender, new_balance)?;
            trace!(target: "erc20_precompile", ?new_balance, "burn_from: updated account balance");
        }
        state_access.perform_touch_account(sender);

        // Decrease allowance (unless unlimited)
        if !is_unlimited {
            let new_allowance = current_allowance.checked_sub(amount).ok_or_else(|| {
                ERC20Error::overflow_error(ERC20Resource::Allowance, ERC20Method::Burn)
            })?;
            self.set_allowance(state_access, sender, caller, new_allowance)?;
            state_access.perform_touch_account(self.contract_address);
            trace!(target: "erc20_precompile", ?new_allowance, "burn_from: allowance updated");
        }

        // Emit events
        let burn_event = Self::encode_burn_event(self.contract_address, sender, amount);
        state_access.perform_log(burn_event);

        let transfer_event =
            Self::encode_transfer_event(self.contract_address, sender, Address::ZERO, amount);
        state_access.perform_log(transfer_event);

        Ok(PrecompileOutput::new(GAS_BURN, encode_bool(true)))
    }

    /// Encode a Transfer event log.
    pub fn encode_transfer_event(
        contract_address: Address,
        from: Address,
        to: Address,
        value: U256,
    ) -> Log {
        Log {
            address: contract_address,
            data: erc20::Transfer { from, to, value }.encode_log_data(),
        }
    }

    fn encode_approval_event(
        contract_address: Address,
        owner: Address,
        spender: Address,
        value: U256,
    ) -> Log {
        Log {
            address: contract_address,
            data: erc20::Approval { owner, spender, value }.encode_log_data(),
        }
    }

    fn encode_mint_event(contract_address: Address, to: Address, value: U256) -> Log {
        Log { address: contract_address, data: erc20::Mint { to, value }.encode_log_data() }
    }

    fn encode_burn_event(contract_address: Address, from: Address, value: U256) -> Log {
        Log { address: contract_address, data: erc20::Burn { from, value }.encode_log_data() }
    }

    // region: EIP-3009: "Transfer with Authorization"
    fn encode_authorization_used_event(
        contract_address: Address,
        authorizer: Address,
        nonce: B256,
    ) -> Log {
        Log {
            address: contract_address,
            data: eip3009::AuthorizationUsed { authorizer, nonce }.encode_log_data(),
        }
    }

    fn encode_authorization_canceled_event(
        contract_address: Address,
        authorizer: Address,
        nonce: B256,
    ) -> Log {
        Log {
            address: contract_address,
            data: eip3009::AuthorizationCanceled { authorizer, nonce }.encode_log_data(),
        }
    }
    // endregion: EIP-3009: "Transfer with Authorization"
}

// region: EIP-3009: "Transfer with Authorization"
impl Erc20Precompile {
    /// Check if an authorization nonce has been used.
    pub fn is_authorization_nonce_used<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        authorizer: Address,
        nonce: B256,
    ) -> Result<bool, ERC20Error> {
        let slot = authorization_nonce_slot(authorizer, nonce);
        let value = state_access
            .perform_sload(self.contract_address, slot)
            .map_err(|e| ERC20Error::read_error(ERC20Resource::Nonce, e))?;
        Ok(value.data != U256::ZERO)
    }

    /// Mark an authorization nonce as used.
    pub fn mark_nonce_used<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        authorizer: Address,
        nonce: B256,
    ) -> Result<(), ERC20Error> {
        let slot = authorization_nonce_slot(authorizer, nonce);

        state_access
            .perform_sstore(self.contract_address, slot, U256::ONE)
            .map_err(|e| ERC20Error::write_error(ERC20Resource::Nonce, e))?;
        state_access.perform_touch_account(self.contract_address);
        Ok(())
    }

    /// Handle transferWithAuthorization - EIP-3009 meta-transaction.
    ///
    /// Note: The caller address is not validated per EIP-3009 spec - anyone can submit
    /// a valid authorization on behalf of the signer.
    #[allow(clippy::too_many_arguments)]
    pub fn handle_transfer_with_authorization<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        sender: Address,
        receiver: Address,
        amount: U256,
        valid_after: U256,
        valid_before: U256,
        nonce: B256,
        v: u8,
        r: U256,
        s: U256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        // 1. Time window validation
        let current_timestamp = state_access.get_block_timestamp();
        if current_timestamp < valid_after {
            error!(target: "erc20_precompile", "Authorization not yet valid");
            return Err(ERC20Error::AuthorizationNotYetValid(valid_after, current_timestamp));
        }

        if current_timestamp >= valid_before {
            error!(target: "erc20_precompile", "Authorization expired");
            return Err(ERC20Error::AuthorizationExpired(valid_before, current_timestamp));
        }

        // 2. Nonce validation
        if self.is_authorization_nonce_used(state_access, sender, nonce)? {
            error!(target: "erc20_precompile", "Authorization already used");
            return Err(ERC20Error::AuthorizationAlreadyUsed(nonce));
        }

        // 3. Signature validation
        let signer = verify_transfer_authorization(
            &self.domain,
            sender,
            receiver,
            amount,
            valid_after,
            valid_before,
            nonce,
            v,
            r,
            s,
        )
        .inspect_err(|e| {
            error!(target: "erc20_precompile", "Invalid signer: {:?}", e);
        })?;

        // 4. Validate signer
        if signer != sender {
            error!(target: "erc20_precompile", signer=?signer, sender=?sender , "Invalid Authorizer");
            return Err(ERC20Error::InvalidAuthorizer(sender, signer));
        }

        // 5. Mark nonce as used
        self.mark_nonce_used(state_access, sender, nonce).inspect_err(|e| {
            error!(target: "erc20_precompile", "Failed to mark nonce as used: {:?}", e);
        })?;

        // 6. Execute transfer
        let transfer_result =
            self.handle_transfer(state_access, sender, receiver, amount).inspect_err(
                |e| error!(target: "erc20_precompile", "Failed to execute transfer: {:?}", e),
            )?;

        // 7. Emit AuthorizationUsed event
        let auth_event =
            Self::encode_authorization_used_event(self.contract_address, sender, nonce);

        state_access.perform_log(auth_event);
        debug!(target: "erc20_precompile", "Transfer with authorization successful");
        Ok(PrecompileOutput { gas_used: GAS_TRANSFER_WITH_AUTHORIZATION, ..transfer_result })
    }

    /// Handle receiveWithAuthorization - EIP-3009 receiver-initiated transfer.
    #[allow(clippy::too_many_arguments)]
    pub fn handle_receive_with_authorization<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        caller: Address,
        sender: Address,
        receiver: Address,
        amount: U256,
        valid_after: U256,
        valid_before: U256,
        nonce: B256,
        v: u8,
        r: B256,
        s: B256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        if caller != receiver {
            return Err(ERC20Error::CallerNotReceiver(caller, receiver));
        }

        // 1. Time window validation
        let current_timestamp = state_access.get_block_timestamp();
        if current_timestamp < valid_after {
            return Err(ERC20Error::AuthorizationNotYetValid(valid_after, current_timestamp));
        }

        if current_timestamp >= valid_before {
            return Err(ERC20Error::AuthorizationExpired(valid_before, current_timestamp));
        }

        // 2. Nonce validation
        if self.is_authorization_nonce_used(state_access, sender, nonce)? {
            return Err(ERC20Error::AuthorizationAlreadyUsed(nonce));
        }

        // 3. Signature validation
        let signer = verify_transfer_authorization(
            &self.domain,
            sender,
            receiver,
            amount,
            valid_after,
            valid_before,
            nonce,
            v,
            r.into(),
            s.into(),
        )?;

        // 4. Validate signer
        if signer != sender {
            return Err(ERC20Error::InvalidAuthorizer(sender, signer));
        }

        // 5. Mark nonce as used
        self.mark_nonce_used(state_access, sender, nonce)?;

        // 6. Execute transfer
        let transfer_result = self.handle_transfer(state_access, sender, receiver, amount)?;

        // 7. Emit AuthorizationUsed event
        let auth_event =
            Self::encode_authorization_used_event(self.contract_address, sender, nonce);

        state_access.perform_log(auth_event);
        Ok(PrecompileOutput { gas_used: GAS_RECEIVE_WITH_AUTHORIZATION, ..transfer_result })
    }

    /// Handle cancelAuthorization - EIP-3009 authorization cancellation.
    ///
    /// Note: The caller address is not validated per EIP-3009 spec - only the authorizer's
    /// signature is verified.
    pub fn handle_cancel_authorization<T: StateAccessAdapter>(
        &self,
        state_access: &mut T,
        authorizer: Address,
        nonce: B256,
        v: u8,
        r: B256,
        s: B256,
    ) -> Result<PrecompileOutput, ERC20Error> {
        // 1. Verify nonce
        if self.is_authorization_nonce_used(state_access, authorizer, nonce)? {
            error!(target: "erc20_precompile", "Authorization already used");
            return Err(ERC20Error::AuthorizationAlreadyUsed(nonce));
        }

        // 2. Verify signer
        let signer =
            verify_cancel_authorization(&self.domain, authorizer, nonce, v, r.into(), s.into())?;
        if signer != authorizer {
            return Err(ERC20Error::InvalidAuthorizer(authorizer, signer));
        }

        // 3. Mark nonce as canceled
        self.mark_nonce_used(state_access, authorizer, nonce)?;

        // 4. Emit AuthorizationCanceled event
        let auth_event =
            Self::encode_authorization_canceled_event(self.contract_address, authorizer, nonce);
        state_access.perform_log(auth_event);

        Ok(PrecompileOutput::new(GAS_CANCEL_AUTHORIZATION, encode_bool(true)))
    }
}
// endregion: EIP-3009: "Transfer with Authorization"

/// Precompile output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrecompileOutput {
    /// Gas used.
    pub gas_used: u64,
    /// Return data.
    pub bytes: Bytes,
}

impl PrecompileOutput {
    /// Create new precompile output.
    pub fn new(gas_used: u64, bytes: Bytes) -> Self {
        Self { gas_used, bytes }
    }
}

/// ERC-20 error types.
#[derive(Error, Serialize, Debug)]
pub enum ERC20Error {
    /// Invalid sender address.
    #[error("Invalid sender: {0}")]
    ERC20InvalidSender(Address),
    /// Invalid receiver address.
    #[error("Invalid receiver: {0}")]
    ERC20InvalidReceiver(Address),
    /// Insufficient balance for transfer.
    #[error("Insufficient balance for {0}: have {1}, need {2}")]
    ERC20InsufficientBalance(Address, U256, U256),
    /// Invalid approver address.
    #[error("Invalid approver: {0}")]
    ERC20InvalidApprover(Address),
    /// Invalid spender address.
    #[error("Invalid spender: {0}")]
    ERC20InvalidSpender(Address),
    /// Insufficient allowance for transfer.
    #[error("Insufficient allowance for {0}: have {1}, need {2}")]
    ERC20InsufficientAllowance(Address, U256, U256),
    /// Caller not whitelisted for mint/burn.
    #[error("Not whitelisted: {0}")]
    NotWhitelisted(Address),
    /// Generic ERC-20 error.
    #[error("ERC20 Error: {0}")]
    ERC20Other(String),
    /// Debit would leave the account EIP-161 empty, which revm cannot commit.
    #[error("Transfer would leave account {0} empty (balance 0, nonce 0, no code)")]
    TransferWouldEmptyAccount(Address),

    // EIP-3009: "Transfer with Authorization"
    /// Authorization not yet valid.
    #[error("Authorization not yet valid: validAfter {0}, currentTime {1}")]
    AuthorizationNotYetValid(U256, U256),
    /// Authorization expired.
    #[error("Authorization expired: validBefore: {0}, currentTime: {1}")]
    AuthorizationExpired(U256, U256),
    /// Authorization nonce already used.
    #[error("Authorization already used: nonce: {0}")]
    AuthorizationAlreadyUsed(B256),
    /// Invalid authorizer (signer mismatch).
    #[error("Invalid authorizer: sender: {0}, signer: {1}")]
    InvalidAuthorizer(Address, Address),
    /// Invalid signature.
    #[error("Invalid signature")]
    InvalidSignature,
    /// Caller is not the receiver (for receiveWithAuthorization).
    #[error("Caller is not receiver: caller: {0}, receiver: {1}")]
    CallerNotReceiver(Address, Address),
    /// Signature malleability detected.
    #[error("Signature malleability detected")]
    SignatureMalleability,
}

#[derive(Debug)]
/// ERC-20 resource types for error context.
pub enum ERC20Resource {
    /// Allowance storage.
    Allowance,
    /// Account balance.
    Account(Address),
    /// Total supply storage.
    TotalSupply,
    /// Sender address.
    Sender,
    /// Receiver address.
    Receiver,
    /// Balance value.
    Balance,
    /// EIP-3009 nonce.
    Nonce,
}

impl std::fmt::Display for ERC20Resource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Allowance => write!(f, "Allowance"),
            Self::Account(addr) => write!(f, "Account({})", addr),
            Self::TotalSupply => write!(f, "TotalSupply"),
            Self::Sender => write!(f, "Sender"),
            Self::Receiver => write!(f, "Receiver"),
            Self::Balance => write!(f, "Balance"),
            Self::Nonce => write!(f, "Nonce"),
        }
    }
}

#[derive(Debug)]
/// ERC-20 method names for error context.
pub enum ERC20Method {
    /// approve().
    Approve,
    /// transfer().
    Transfer,
    /// transferFrom().
    TransferFrom,
    /// mint().
    Mint,
    /// burn() / burnFrom().
    Burn,
    /// burnFrom().
    BurnFrom,
}

impl std::fmt::Display for ERC20Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Approve => write!(f, "Approve"),
            Self::Transfer => write!(f, "Transfer"),
            Self::TransferFrom => write!(f, "TransferFrom"),
            Self::Mint => write!(f, "Mint"),
            Self::Burn => write!(f, "Burn"),
            Self::BurnFrom => write!(f, "BurnFrom"),
        }
    }
}

impl ERC20Error {
    /// Create a generic error.
    pub fn other(msg: impl Into<String>) -> Self {
        Self::ERC20Other(msg.into())
    }

    /// Create an initialization error.
    pub fn initialize_error(resource: ERC20Resource, error: Box<dyn Error>) -> Self {
        Self::other(format!("Failed to initialize {}: {}", resource, error))
    }

    /// Create a read error.
    pub fn read_error(resource: ERC20Resource, error: Box<dyn Error>) -> Self {
        Self::ERC20Other(format!("Failed to read {}: {}", resource, error))
    }

    /// Create a write error.
    pub fn write_error(resource: ERC20Resource, error: Box<dyn Error>) -> Self {
        Self::ERC20Other(format!("Failed to write {}: {}", resource, error))
    }

    /// Create an overflow error.
    pub fn overflow_error(resource: ERC20Resource, method: ERC20Method) -> Self {
        Self::ERC20Other(format!("{} overflow during {}", resource, method))
    }

    /// Create a zero amount error.
    pub fn zero_amount_error(method: ERC20Method) -> Self {
        Self::ERC20Other(format!("Cannot {} zero amount", method))
    }
}

impl From<ERC20Error> for Bytes {
    fn from(value: ERC20Error) -> Self {
        encode_revert_reason(&value.to_string())
    }
}

/// Gas costs for ERC-20 operations.
/// These are based on standard ERC-20 gas costs and EVM storage operations.
pub const GAS_NAME: u64 = 100; // Pure function
pub const GAS_SYMBOL: u64 = 100; // Pure function
pub const GAS_DECIMALS: u64 = 100; // Pure function
pub const GAS_TOTAL_SUPPLY: u64 = 2_100; // Cold SLOAD
pub const GAS_BALANCE_QUERY: u64 = 400; // Warm SLOAD (account balance)
pub const GAS_ALLOWANCE_QUERY: u64 = 2_100; // Cold SLOAD
pub const GAS_TRANSFER: u64 = 9_000; // Two balance updates + log
pub const GAS_APPROVE: u64 = 5_000; // One SSTORE + log
pub const GAS_TRANSFER_FROM: u64 = 12_000; // Three updates (from, to, allowance) + log
pub const GAS_MINT: u64 = 10_000; // Balance update + total supply SSTORE + 2 logs (Mint + Transfer)
pub const GAS_BURN: u64 = 10_000; // Balance update + total supply SSTORE + 2 logs (Burn + Transfer)

// region: EIP-3009: "Transfer with Authorization"
// Breakdown:
//   - 2 SLOAD (nonce check): 2,100
//   - 1 SSTORE (nonce set): 20,000 (cold)
//   - Signature verification: ~3,000
//   - Transfer logic: 9,000
//   - Event emission: 1,000
pub const GAS_TRANSFER_WITH_AUTHORIZATION: u64 = 35_000;

// Same as transferWithAuthorization + caller validation
pub const GAS_RECEIVE_WITH_AUTHORIZATION: u64 = 35_000;

// Breakdown:
//   - 1 SLOAD (nonce check): 2,100
//   - 1 SSTORE (nonce set): 20,000
//   - Signature verification: ~3,000
//   - Event emission: 375
pub const GAS_CANCEL_AUTHORIZATION: u64 = 25_000;
// endregion: EIP-3009: "Transfer with Authorization"
