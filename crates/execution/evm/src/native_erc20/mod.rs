//! Native ERC-20 Wrapper Precompile
//!
//! This module provides an ERC-20 compliant interface for the native coin at precompile
//! address 0x0400. It enables the blockchain's native token to be used as a standard
//! ERC-20 token while maintaining full compatibility with Ethereum tooling.
//!
//! # Architecture
//!
//! The implementation uses a dual-path architecture:
//! - **Inspector path**: Used for actual transactions, provides persistent state changes
//! - **DynPrecompile path**: Used for eth_call simulations, provides temporary state
//!
//! # Features
//!
//! - Full ERC-20 compliance (name, symbol, decimals, totalSupply, balanceOf, transfer, approve,
//!   allowance, transferFrom)
//! - Mint/Burn with access control (MINTING_MODULE_ADDRESS whitelist)
//! - EIP-3009 gasless transfers (transferWithAuthorization, receiveWithAuthorization,
//!   cancelAuthorization)
//! - Automatic Transfer event emission for ALL native transfers

pub mod abi;
pub mod composite_inspector;
pub mod inspector;
pub mod precompile;
pub mod storage;

#[cfg(test)]
mod eip3009_empty_account_tests;

pub use abi::{eip3009, erc20, Erc20Selector};
pub use composite_inspector::CompositeInspector;
pub use inspector::NativeErc20Inspector;
pub use precompile::{
    ERC20Error, ERC20Method, ERC20Resource, Erc20Precompile, Erc20TokenConfig, PrecompileOutput,
    StateAccessAdapter,
};
pub use storage::{allowance_slot, authorization_nonce_slot, total_supply_slot};

use alloy::primitives::{address, Address};

/// Precompile address for the Native ERC-20 Wrapper.
pub const ERC20_PRECOMPILE_ADDRESS: Address = address!("0000000000000000000000000000000000000400");

/// Address authorized to mint and burn native tokens via the ERC-20 wrapper precompile.
///
/// This should be set to the minting module address that manages the native token supply.
/// For production, this should be configurable via genesis.
pub const MINTING_MODULE_ADDRESS: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e6");

/// Gas charged on every precompile error path.
///
/// Matches the EIP-2929 `COLD_SLOAD_COST` to bound attacker-cheap revert cycling. Used by both
/// the legacy inspector and the post-fork DynPrecompile, clamped to `min(BASE_ERROR_GAS,
/// gas_limit)` before passing to revm so the consumer's `record_cost` cannot underflow.
pub(crate) const BASE_ERROR_GAS: u64 = 2_100;
