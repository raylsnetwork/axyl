//! Test-utilities for execution/engine node.

use clap::Parser as _;
use core::fmt;
use rayls_execution_evm::{
    reth_env::{RethConfig, RethEnv},
    RethChainSpec,
};
use rayls_execution_faucet::FaucetArgs;
use rayls_infrastructure_config::Config;
use rayls_infrastructure_types::{
    rewards::RewardsCounter, Address, BuildMetadata, TaskManager, TimestampSec, Withdrawals, B256,
};
use rayls_middleware_orchestrator::engine::{ExecutionNode, RaylsBuilder};
use rayls_network_cli::{node::NodeCommand, NoArgs};
use std::{path::Path, str::FromStr, sync::Arc};

/// Convenience type for testing Execution Node.
pub type TestExecutionNode = ExecutionNode;

/// Convenience function for creating engine node using tempdir and optional args.
/// Defaults if params not provided:
/// - opt_authority_identifier: `AuthorityIdentifier(1)`
/// - opt_chain: `testnet`
/// - opt_address: `0x1111111111111111111111111111111111111111`
pub async fn default_test_execution_node(
    opt_chain: Option<Arc<RethChainSpec>>,
    opt_address: Option<Address>,
    tmp_dir: &Path,
    rewards: Option<RewardsCounter>,
) -> eyre::Result<TestExecutionNode> {
    let (builder, _) = execution_builder::<NoArgs>(
        opt_chain.clone(),
        opt_address,
        None, // optional args
        tmp_dir,
    )?;

    // create engine node
    let engine = if let Some(chain) = opt_chain {
        ExecutionNode::new(
            &builder,
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir, &TaskManager::default(), rewards)
                .await?,
        )?
    } else {
        ExecutionNode::new(
            &builder,
            RethEnv::new_for_test(tmp_dir, &TaskManager::default(), rewards).await?,
        )?
    };

    Ok(engine)
}

/// Create CLI command for tests calling `ExecutionNode::new`.
fn execution_builder<CliExt: clap::Args + fmt::Debug>(
    opt_chain: Option<Arc<RethChainSpec>>,
    opt_address: Option<Address>,
    opt_args: Option<Vec<&str>>,
    tmp_dir: &Path,
) -> eyre::Result<(RaylsBuilder, CliExt)> {
    let default_args = ["rayls-network", "--http", "--storage.v2"];

    // extend faucet args if provided
    let cli_args = if let Some(args) = opt_args {
        [&default_args, &args[..]].concat()
    } else {
        default_args.to_vec()
    };

    // use same approach as rayls-network binary
    let command = NodeCommand::<CliExt>::try_parse_from(cli_args)?;

    let NodeCommand { instance, ext, reth, healthcheck, .. } = command;
    let reth_command = reth;

    let mut rayls_infrastructure_config = Config::default_for_test();
    if let Some(chain) = opt_chain {
        // overwrite chain spec if passed in
        rayls_infrastructure_config.genesis = chain.genesis().clone();
    }

    // check args then use test defaults
    let address = opt_address.unwrap_or_else(|| {
        Address::from_str("0x1111111111111111111111111111111111111111").expect("address from 0x1s")
    });

    // update execution address
    rayls_infrastructure_config.node_info.execution_address = address;

    // TODO: this a temporary approach until upstream reth supports public rpc hooks
    let opt_faucet_args = None;
    let builder = RaylsBuilder::new(
        RethConfig::new(
            reth_command,
            instance,
            tmp_dir,
            true,
            Arc::new(rayls_infrastructure_config.chain_spec()),
        ),
        rayls_infrastructure_config,
        opt_faucet_args,
        None,
        healthcheck,
        BuildMetadata::default(),
    );

    Ok((builder, ext))
}

/// Create a RaylsBuilder without CLI extensions for tests that construct RethEnv separately.
pub fn execution_builder_no_args(
    opt_chain: Option<Arc<RethChainSpec>>,
    opt_address: Option<Address>,
    tmp_dir: &Path,
) -> eyre::Result<(RaylsBuilder, NoArgs)> {
    execution_builder::<NoArgs>(opt_chain, opt_address, None, tmp_dir)
}

/// Convenience function for creating engine node using tempdir and optional args.
/// Defaults if params not provided:
/// - opt_authority_identifier: `AuthorityIdentifier(1)`
/// - opt_chain: `testnet`
/// - opt_address: `0x1111111111111111111111111111111111111111`
// #[cfg(feature = "faucet")]
pub async fn faucet_test_execution_node(
    google_kms: bool,
    opt_chain: Option<Arc<RethChainSpec>>,
    opt_address: Option<Address>,
    faucet_proxy_address: Address,
    tmp_dir: &Path,
) -> eyre::Result<TestExecutionNode> {
    let faucet_args = ["--google-kms"];

    // TODO: support non-google-kms faucet
    let extended_args = if google_kms { Some(faucet_args.to_vec()) } else { None };
    // always include default expected faucet derived from `TransactionFactory::default`
    let faucet = faucet_proxy_address.to_string();
    let extended_args =
        extended_args.map(|opt| [opt, vec!["--faucet-contract", &faucet]].concat().to_vec());

    // execution builder + faucet args
    let (builder, faucet) =
        execution_builder::<FaucetArgs>(opt_chain.clone(), opt_address, extended_args, tmp_dir)?;

    // replace default builder's faucet args
    let RaylsBuilder { node_config, rayls_infrastructure_config, healthcheck, .. } = builder;
    let builder = RaylsBuilder::new(
        node_config.clone(),
        rayls_infrastructure_config,
        Some(faucet),
        None,
        healthcheck,
        BuildMetadata::default(),
    );

    // create engine node
    let reth_db = RethEnv::new_database(&node_config, tmp_dir.join("db"))?;
    let engine = ExecutionNode::new(
        &builder,
        RethEnv::new(
            &node_config,
            &TaskManager::default(),
            reth_db,
            None,
            RewardsCounter::default(),
            &BuildMetadata::default(),
            None,
            None,
            false,
        )
        .await?,
    )?;

    Ok(engine)
}

/// Optional parameters to pass to the `execute_test_batch` function.
///
/// These optional parameters are used to replace default in the batch's header if included.
#[derive(Debug, Default)]
pub struct OptionalTestBatchParams {
    /// Optional beneficiary address.
    ///
    /// Default is `Address::random()`.
    pub beneficiary_opt: Option<Address>,
    /// Optional withdrawals.
    ///
    /// Default is `Withdrawals<vec![]>` (empty).
    pub withdrawals_opt: Option<Withdrawals>,
    /// Optional timestamp.
    ///
    /// Default is `now()`.
    pub timestamp_opt: Option<TimestampSec>,
    /// Optional mix_hash.
    ///
    /// Default is `B256::random()`.
    pub mix_hash_opt: Option<B256>,
    /// Optional base_fee_per_gas.
    ///
    /// Default is [MIN_PROTOCOL_BASE_FEE], which is 7 wei.
    pub base_fee_per_gas_opt: Option<u64>,
}
