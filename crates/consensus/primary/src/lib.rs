// SPDX-License-Identifier: BUSL-1.1
//! Primary actors

mod aggregators;
mod certificate_fetcher;
mod certifier;
pub mod consensus;
mod error;
pub mod network;
mod primary;
mod proposer;
mod state_handler;
mod state_sync;
mod vote_failure_tracker;

pub use state_sync::StateSynchronizer;

#[cfg(test)]
#[path = "tests/certificate_tests.rs"]
mod certificate_tests;

pub use crate::primary::Primary;

mod consensus_bus;
pub use consensus_bus::*;

mod recently_executed_blocks;
pub use recently_executed_blocks::*;

#[cfg(feature = "test-utils")]
pub mod test_utils;
