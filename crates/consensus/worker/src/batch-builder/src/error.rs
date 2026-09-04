//! Error types for the batch builder.

use std::any::Any;

use rayls_execution_evm::{PoolTransactionError, ProviderError, RethError};
use tokio::sync::{mpsc, oneshot};

/// Result alias for [`BatchBuilderError`].
pub(crate) type BatchBuilderResult<T> = Result<T, BatchBuilderError>;

/// Errors raised while building a batch or handing it to the worker.
#[derive(Debug, thiserror::Error)]
pub enum BatchBuilderError {
    /// Error from Reth
    #[error(transparent)]
    Reth(#[from] RethError),
    /// Error retrieving data from Provider.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// The next batch digest is missing.
    #[error("Missing next batch digest for recovered sealed block with senders.")]
    NextBatchDigestMissing,
    /// The block body and senders lengths don't match.
    #[error("Failed to seal block with senders - lengths don't match")]
    SealBlockWithSenders,
    /// The oneshot channel that receives the ack that the block was persisted and being proposed.
    #[error("Fatal error: failed to receive ack reply that new block was built. Shutting down...")]
    AckChannelClosed,
    /// Failed to send to the worker.
    #[error("Fatal error: failed to send built block to worker.")]
    WorkerChannelClosed,
    /// Fatal db error with worker while trying to reach quorum.
    #[error("Fatal error: batch provider db error")]
    FatalDBFailure,
    /// Error building batch because this transaction would case the batch to exceed max size (in
    /// bytes).
    #[error(
        "The transaction was not included because it would exceed the max batch size. Tx size: {0} bytes - max size: {1} bytes."
    )]
    MaxBatchSize(usize, usize),
    /// An operation that requires canonical state did not have it.
    #[error("Missing canonical state.")]
    MissingCanonical,
    /// The blocking pool failed to complete batch construction.
    #[error("batch build task failed: {0}")]
    BuildTaskFailed(#[from] tokio::task::JoinError),
}

impl BatchBuilderError {
    /// Returns whether the worker seal loop is gone, so the builder should end gracefully.
    pub(crate) fn is_worker_gone(&self) -> bool {
        matches!(self, Self::AckChannelClosed | Self::WorkerChannelClosed)
    }
}

impl From<oneshot::error::RecvError> for BatchBuilderError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::AckChannelClosed
    }
}

impl<T> From<mpsc::error::SendError<T>> for BatchBuilderError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::WorkerChannelClosed
    }
}

impl PoolTransactionError for BatchBuilderError {
    fn is_bad_transaction(&self) -> bool {
        // no peer penalty
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
