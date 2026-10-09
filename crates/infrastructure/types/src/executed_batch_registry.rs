//! Deduplication of executed batch digests across restarts and retries.

use alloy::primitives::{
    map::{B256Map, B256Set},
    B256,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{info, warn};

/// Maximum number of nonce_too_high retries before a digest is kept permanently.
const MAX_NONCE_TOO_HIGH_RETRIES: u8 = 3;

/// Deduplication registry for executed batch digests.
#[derive(Clone, Debug, Default)]
pub struct ExecutedBatchRegistry {
    /// Digests that have been executed, or whose retry budget is spent.
    digests: Arc<Mutex<B256Set>>,
    /// Nonce-too-high retries spent per digest; bounded by `MAX_NONCE_TOO_HIGH_RETRIES`.
    retry_counts: Arc<Mutex<B256Map<u8>>>,
}

impl ExecutedBatchRegistry {
    /// Creates a registry from a pre-populated set of digests.
    pub fn from_digests(digests: B256Set) -> Self {
        Self {
            digests: Arc::new(Mutex::new(digests)),
            retry_counts: Arc::new(Mutex::new(B256Map::default())),
        }
    }

    /// Registers a batch digest, returning false if it was already present.
    pub fn try_register(&self, batch_digest: B256, output_digest: B256) -> bool {
        let result = self.digests.lock().insert(batch_digest);
        if !result {
            info!(
                target: "executed_batch_registry",
                batch_digest = ?batch_digest,
                output_digest = ?output_digest,
                "skipping duplicate batch digest"
            );
        }
        result
    }

    /// Returns true if the digest is registered, without inserting it.
    pub fn contains(&self, batch_digest: &B256) -> bool {
        self.digests.lock().contains(batch_digest)
    }

    /// Removes a digest so the batch can be retried.
    ///
    /// Returns false once the retry cap is reached; the digest then stays registered so a batch
    /// that never becomes executable cannot be retried forever.
    pub fn drop_digest(&self, batch_digest: B256) -> bool {
        let mut retries = self.retry_counts.lock();
        let count = retries.entry(batch_digest).or_insert(0);
        if *count >= MAX_NONCE_TOO_HIGH_RETRIES {
            warn!(
                target: "executed_batch_registry",
                ?batch_digest,
                "max retries reached, keeping digest"
            );
            return false;
        }
        *count += 1;
        self.digests.lock().remove(&batch_digest);
        true
    }
}
