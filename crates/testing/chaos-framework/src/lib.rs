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

/// Chain ID of the chaos cluster: the `local` network profile (487 = 0x1e7).
pub const CHAIN_ID: u64 = rayls_infrastructure_types::RaylsNetwork::Local.chain_id();
