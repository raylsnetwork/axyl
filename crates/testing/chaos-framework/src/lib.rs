// SPDX-License-Identifier: BUSL-1.1
//! Chaos engineering framework for Axyl e2e testing.
//!
//! Provides fault injection primitives (node kills, network latency, transaction spam)
//! and integrity verifiers (block consistency, nonce monotonicity, chain liveness)
//! for testing chain resilience under adverse conditions.

// Suppress unused crate warnings for workspace dependencies used only in submodules.
#![allow(unused_crate_dependencies)]

pub mod cluster;
pub mod fault;
pub mod node;
pub mod rpc;
pub mod scenario;
pub mod verify;

use rayls_infrastructure_types::RaylsNetwork;

/// The network profile the chaos test clusters run. The ceremony's `--chain-id` and every
/// node's `--network` flag are derived from it so the pair can never drift apart.
pub const TEST_NETWORK: RaylsNetwork = RaylsNetwork::Local;

/// Chain ID the chaos test clusters run (TEST_NETWORK's chain-id).
pub const CHAIN_ID: u64 = TEST_NETWORK.chain_id();
