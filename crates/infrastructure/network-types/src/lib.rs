// SPDX-License-Identifier: BUSL-1.1
//! Network messages for anemo communication

pub mod local;
mod notify;
mod response;
pub use notify::*;
use rayls_infrastructure_types::{Batch, BlockHash};
pub use response::*;
use std::collections::{HashMap, HashSet};

// async_trait for object safety, get rid of when possible.
#[async_trait::async_trait]
/// Worker to primary messages.
pub trait WorkerToPrimaryClient: Send + Sync + 'static {
    /// Report own batch
    async fn report_own_batch(&self, request: WorkerOwnBatchMessage) -> eyre::Result<()>;

    /// Report a batch from a peer.
    async fn report_others_batch(&self, request: WorkerOthersBatchMessage) -> eyre::Result<()>;
}

/// Dumb mock to just return Ok on calls for tests.
#[derive(Debug)]
pub struct MockWorkerToPrimary();

#[async_trait::async_trait]
impl WorkerToPrimaryClient for MockWorkerToPrimary {
    async fn report_own_batch(&self, _request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        Ok(())
    }

    async fn report_others_batch(&self, _request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        Ok(())
    }
}

/// Dumb mock that pends forever on calls for tests.
#[derive(Debug)]
pub struct MockWorkerToPrimaryHang();

#[async_trait::async_trait]
impl WorkerToPrimaryClient for MockWorkerToPrimaryHang {
    async fn report_own_batch(&self, _request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        std::future::pending().await
    }

    async fn report_others_batch(&self, _request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        std::future::pending().await
    }
}

/// Mock whose `report_own_batch` always errors, for tests of a seal whose report goes
/// unacknowledged (the proposer has stopped draining).
#[derive(Debug, Default)]
pub struct MockWorkerToPrimaryError {
    /// Count of `report_own_batch` calls, so a test can wait for the seal before asserting.
    pub attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl WorkerToPrimaryClient for MockWorkerToPrimaryError {
    async fn report_own_batch(&self, _request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        eyre::bail!("report_own_batch failed: proposer not draining (epoch teardown)")
    }

    async fn report_others_batch(&self, _request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        Ok(())
    }
}

// async_trait for object safety, get rid of when possible.
#[async_trait::async_trait]
/// Primary to worker messages.
pub trait PrimaryToWorkerClient: Send + Sync + 'static {
    /// Synchronize
    async fn synchronize(&self, message: WorkerSynchronizeMessage) -> eyre::Result<()>;

    /// Fetch batches
    async fn fetch_batches(&self, digests: HashSet<BlockHash>) -> eyre::Result<FetchBatchResponse>;
}

/// Type that can return batches.
#[derive(Default, Debug)]
pub struct MockPrimaryToWorkerClient {
    /// The batches for tests.
    pub batches: HashMap<BlockHash, Batch>,
}

#[async_trait::async_trait]
impl PrimaryToWorkerClient for MockPrimaryToWorkerClient {
    async fn synchronize(&self, _message: WorkerSynchronizeMessage) -> eyre::Result<()> {
        Ok(())
    }

    async fn fetch_batches(
        &self,
        _digests: HashSet<BlockHash>,
    ) -> eyre::Result<FetchBatchResponse> {
        Ok(FetchBatchResponse { batches: self.batches.clone() })
    }
}
