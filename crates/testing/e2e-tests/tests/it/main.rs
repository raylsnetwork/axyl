//! CLI integration test
// ignore for lib
#![allow(unused_crate_dependencies)]

#[cfg(feature = "dev-single-node-setup")]
mod dev;
// Multi-validator e2e suites: they spawn 4-validator local testnets, so they cannot run
// against a single-node-only dev build. Compiled/run in non-feature (production) builds only.
mod active_profile;
#[cfg(not(feature = "dev-single-node-setup"))]
mod epochs;
#[cfg(feature = "faucet")]
mod faucet;
#[cfg(not(feature = "dev-single-node-setup"))]
mod genesis_tests;
#[cfg(not(feature = "dev-single-node-setup"))]
mod restarts;

fn main() {}
