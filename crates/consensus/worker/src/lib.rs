// SPDX-License-Identifier: BUSL-1.1
//! Worker components to create and sync batches.

mod batch_fetcher;
mod network;
mod worker;
pub use network::{
    SubmitError, SubmitRejection, WorkerNetwork, WorkerNetworkHandle, WorkerRequest, WorkerResponse,
};
pub mod quorum_waiter;

pub mod metrics;

pub use crate::{
    network::{
        error::WorkerNetworkError,
        handler::RequestHandler,
        message::{WorkerGossip, WorkerRPCError},
    },
    worker::{new_worker, Worker, CHANNEL_CAPACITY},
};

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 26;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
