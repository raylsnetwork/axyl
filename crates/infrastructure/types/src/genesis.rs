//! Genesis helper methods.
//!
//! The yaml, chainspec, and Genesis struct are used for all
//! testing purposes.
use crate::{now, Genesis, ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE};
use alloy::{
    genesis::GenesisAccount,
    primitives::{address, U256},
};
use reth_chainspec::ChainSpec;
use std::sync::Arc;

/// test genesis
///
/// Provide a genesis for running tests.
/// With funded [TransactionFactory] default account.
/// This is usable for many unit tests but it lacks the genesis contracts and storage.
/// Go throuigh ['GenesisArgs'] to generate a complete genesis.
pub fn test_genesis() -> Genesis {
    let mut genesis = Genesis { timestamp: now(), ..Default::default() };
    set_genesis_defaults(&mut genesis);
    genesis.config.chain_id = 2017;
    let default_factory_accounts = vec![
        (
            // Default transaction factory
            address!("0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        // Various accounts used by faucet.
        (
            address!("0xe626ce81714cb7777b1bf8ad2323963fb3398ad5"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        (
            address!("0xb3fabbd1d2edde4d9ced3ce352859ce1bebf7907"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        (
            address!("0xa3478861957661b2d8974d9309646a71271d98b9"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        (
            address!("0xe69151677e5aec0b4fc0a94bfcaf20f6f0f975eb"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
    ];
    // use testnet pre-compiles
    let precompiles: Genesis =
        serde_yaml::from_str(TESTNET_GENESIS).expect("bad testnet genesis yaml data");
    let genesis = genesis.extend_accounts(precompiles.alloc);
    // overwrite any conflicting accounts with specified values
    genesis.extend_accounts(default_factory_accounts)
}

/// Set the genesis default config.
pub fn set_genesis_defaults(genesis: &mut Genesis) {
    // Configure hardforks or Reth will be cross with us...
    genesis.config.homestead_block = Some(0);
    genesis.config.eip150_block = Some(0);
    genesis.config.eip155_block = Some(0);
    genesis.config.eip158_block = Some(0);
    genesis.config.byzantium_block = Some(0);
    genesis.config.constantinople_block = Some(0);
    genesis.config.petersburg_block = Some(0);
    genesis.config.istanbul_block = Some(0);
    genesis.config.berlin_block = Some(0);
    genesis.config.london_block = Some(0);
    genesis.config.shanghai_time = Some(0);
    genesis.config.cancun_time = Some(0);
    genesis.config.prague_time = Some(0);
    genesis.config.osaka_time = None;
    // Configure some misc genesis stuff.
    // chain_id and maybe timestamp should probably be a command line option...
    genesis.timestamp = now();
    genesis.config.terminal_total_difficulty_passed = true;
    genesis.config.terminal_total_difficulty = Some(U256::from(0));
    genesis.gas_limit = ETHEREUM_BLOCK_GAS_LIMIT_56BITS;
    genesis.base_fee_per_gas = Some(MIN_PROTOCOL_BASE_FEE as u128);
}

/// test chain spec wrapped in [Arc].
pub fn test_chain_spec_arc() -> Arc<ChainSpec> {
    let chain: ChainSpec = test_genesis().into();
    Arc::new(chain)
}

/// testnet genesis
pub fn testnet_genesis() -> Genesis {
    serde_yaml::from_str(TESTNET_GENESIS).expect("serde parse valid testnet yaml")
}

/// testnet chain spec parsed from genesis.
fn _testnet_chain_spec() -> ChainSpec {
    testnet_genesis().into()
}

/// testnet chain spec parsed from genesis and wrapped in [Arc].
fn _testnet_chain_spec_arc() -> Arc<ChainSpec> {
    Arc::new(_testnet_chain_spec())
}

// The raw string for the testnet genesis, kept as a test fixture.
/// Static string for the (testnet) genesis used by the test fixtures above.
///
/// Faucet addresses:
/// - 0xe626ce81714cb7777b1bf8ad2323963fb3398ad5
/// - 0xb3fabbd1d2edde4d9ced3ce352859ce1bebf7907
/// - 0xa3478861957661b2d8974d9309646a71271d98b9
/// - 0xe69151677e5aec0b4fc0a94bfcaf20f6f0f975eb
pub const TESTNET_GENESIS: &str = include_str!("./testdata/testnet-genesis.yaml");
