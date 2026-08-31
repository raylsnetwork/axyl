//! Client implementation for local network messages between primary and worker.
use crate::{
    FetchBatchResponse, PrimaryToWorkerClient, WorkerOthersBatchMessage, WorkerOwnBatchMessage,
    WorkerSynchronizeMessage, WorkerToPrimaryClient,
};
use parking_lot::RwLock;
use rayls_infrastructure_types::{B256Set, BlsPublicKey};
use std::sync::Arc;

/// In-process request path between a primary and its worker(s).
///
/// Handlers are registered after construction because the primary and worker are built
/// independently; before a handler is set, a primary-to-worker request errors and a
/// worker-to-primary report is dropped with a warning.
#[derive(Debug, Clone)]
pub struct LocalNetwork {
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    /// The primary's BLS public key.
    primary_bls_key: BlsPublicKey,
    /// The type that holds logic for worker to primary requests.
    worker_to_primary_handler: Option<Arc<dyn WorkerToPrimaryClient>>,
    /// The type that holds logic for primary to worker requests.
    primary_to_worker_handler: Option<Arc<dyn PrimaryToWorkerClient>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalNetwork::Inner for {}", self.primary_bls_key)
    }
}

impl LocalNetwork {
    /// Creates a new instance of [Self].
    pub fn new(primary_bls_key: BlsPublicKey) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                primary_bls_key,
                worker_to_primary_handler: None,
                primary_to_worker_handler: None,
            })),
        }
    }

    /// Creates a new instance of [Self] with a default BLS key, for tests.
    pub fn new_with_empty_id() -> Self {
        Self::new(BlsPublicKey::default())
    }

    /// Sets the handler for worker to primary messages.
    pub fn set_worker_to_primary_local_handler(&self, handler: Arc<dyn WorkerToPrimaryClient>) {
        let mut inner = self.inner.write();
        inner.worker_to_primary_handler = Some(handler);
    }

    /// Sets the handler for primary to worker messages.
    pub fn set_primary_to_worker_local_handler(&self, handler: Arc<dyn PrimaryToWorkerClient>) {
        let mut inner = self.inner.write();
        inner.primary_to_worker_handler = Some(handler);
    }

    /// Returns the handler for primary to worker messages.
    async fn get_primary_to_worker_handler(&self) -> Option<Arc<dyn PrimaryToWorkerClient>> {
        let inner = self.inner.read();
        inner.primary_to_worker_handler.clone()
    }

    /// Returns the handler for worker to primary messages.
    async fn get_worker_to_primary_handler(&self) -> Option<Arc<dyn WorkerToPrimaryClient>> {
        let inner = self.inner.read();
        inner.worker_to_primary_handler.clone()
    }
}

#[async_trait::async_trait]
impl PrimaryToWorkerClient for LocalNetwork {
    async fn synchronize(&self, request: WorkerSynchronizeMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_primary_to_worker_handler().await {
            c.synchronize(request).await
        } else {
            tracing::warn!(target = "local_network", "primary to worker handler not set yet!");
            Err(eyre::eyre!("primary to worker not set yet"))
        }
    }

    async fn fetch_batches(&self, digests: B256Set) -> eyre::Result<FetchBatchResponse> {
        if let Some(c) = self.get_primary_to_worker_handler().await {
            c.fetch_batches(digests).await
        } else {
            tracing::warn!(target = "local_network", "primary to worker handler not set yet!");
            Err(eyre::eyre!("primary to worker not set yet"))
        }
    }
}

#[async_trait::async_trait]
impl WorkerToPrimaryClient for LocalNetwork {
    async fn report_own_batch(&self, request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_worker_to_primary_handler().await {
            c.report_own_batch(request).await?;
        } else {
            tracing::warn!(target = "local_network", "working to primary handler not set yet!");
        }
        Ok(())
    }

    async fn report_others_batch(&self, request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_worker_to_primary_handler().await {
            c.report_others_batch(request).await?;
        } else {
            tracing::warn!(target = "local_network", "working to primary handler not set yet!");
        }
        Ok(())
    }
}
