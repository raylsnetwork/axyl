//! Builder for engine to mantain generics.

use super::{node_inner::ExecutionNodeInner, RaylsBuilder};
use rayls_execution_evm::reth_env::RethEnv;
use rayls_execution_faucet::FaucetArgs;
use rayls_infrastructure_config::Config;

/// A builder that handles component initialization for the execution node.
/// Separates initialization concerns from runtime behavior.
#[derive(Debug)]
pub(super) struct ExecutionNodeBuilder {
    /// The protocol configuration.
    rayls_infrastructure_config: Config,

    /// Reth environment for executing transactions.
    reth_env: RethEnv,

    /// Optional components (for testnet only).
    opt_faucet_args: Option<FaucetArgs>,
}

impl ExecutionNodeBuilder {
    /// Start the builder with required components
    pub(super) fn new(rayls_builder: &RaylsBuilder, reth_env: RethEnv) -> Self {
        let RaylsBuilder { rayls_infrastructure_config, opt_faucet_args, .. } = rayls_builder;

        Self {
            reth_env,
            rayls_infrastructure_config: rayls_infrastructure_config.clone(),
            opt_faucet_args: opt_faucet_args.clone(),
        }
    }

    /// Build the final ExecutionNodeInner
    pub(super) fn build(self) -> eyre::Result<ExecutionNodeInner> {
        // Ensure all required components are initialized

        Ok(ExecutionNodeInner {
            reth_env: self.reth_env,
            address: *self.rayls_infrastructure_config.execution_address(),
            opt_faucet_args: self.opt_faucet_args,
            rayls_infrastructure_config: self.rayls_infrastructure_config,
            workers: Vec::default(),
            own_executed_sequence: None,
            in_flight_tracker: rayls_execution_evm::in_flight::InFlightTracker::new(),
        })
    }
}
