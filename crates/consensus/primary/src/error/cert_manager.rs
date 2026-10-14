//! Errors encountered during long-running certificate manager task.

use rayls_infrastructure_storage::StoreError;
use rayls_infrastructure_types::{error::CertificateError, CertificateDigest, Epoch, SendError};

use super::GarbageCollectorError;

/// Result alias for results that possibly return [`CertManagerError`].
pub(crate) type CertManagerResult<T> = Result<T, CertManagerError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CertManagerError {
    /// Error processing certificate.
    #[error(transparent)]
    Certificate(#[from] CertificateError),
    /// Error from garbage collection task.
    #[error(transparent)]
    GC(#[from] GarbageCollectorError),
    /// The certificate's signature verification state is unverified.
    #[error("Unverified signature verification state {0}")]
    UnverifiedSignature(CertificateDigest),
    /// Oneshot channel dropped for certificate manager.
    #[error("Failed to return certificate manager's result.")]
    CertificateManagerOneshot,
    /// The pending certificate is unexpectedly missing. This should not happen.
    #[error("Pending certificate not found by digest: {0}")]
    PendingCertificateNotFound(CertificateDigest),
    /// The certificate was verified, accepted, and stored in storage.
    /// However, an error occurred adding it to the collection of parents.
    /// This is the only way to advance the round and is fatal.
    #[error("Fatal error: failed to append accepted certs to parents.")]
    FatalAppendParent,
    /// The certificate was verified, accepted, and stored in storage.
    /// However, an error occured forwarding the certificate to bullshark consensus.
    /// This results in inconsistent state between consensus DAG and consensus store and is fatal.
    #[error("Fatal error: failed to forward accepted cert to consensus.")]
    FatalForwardAcceptedCertificate,
    /// JoinError for spawned blocking task when verifying many fetched certs.
    #[error("Failed to join blocking certificate verification task.")]
    JoinError,
    /// The certificate is pending acceptance due to missing parents.
    #[error("The certificate {0} is pending acceptance due to missing parents.")]
    Pending(CertificateDigest),
    /// Error retrieving value from storage.
    #[error("Storage failure: {0}")]
    Storage(#[from] StoreError),
    /// A duplicate certificate was received but it has different missing parents.
    #[error("The certificate {0} was already pending, but now it has different missing parents.")]
    PendingParentsMismatch(CertificateDigest),

    /// mpsc sender dropped while processig the certificate
    #[error("Failed to process certificate - Rayls sender error: {0}")]
    RAYLSSend(String),

    /// Fetch certificates failed.
    #[error("No peer can be reached for fetching certificates! Check if network is healthy.")]
    NoCertificateFetched,
    /// All fetched certificates belong to a future epoch.
    #[error("All {count} fetched certificates are from epoch {theirs} (local epoch {ours})")]
    FutureEpoch { ours: Epoch, theirs: Epoch, count: usize },
    /// Network error.
    #[error("Failed to set the bounds for MissingCertificatesRequest: {0}")]
    RequestBounds(String),
}

impl<T: std::fmt::Debug> From<SendError<T>> for CertManagerError {
    fn from(e: SendError<T>) -> Self {
        Self::RAYLSSend(e.to_string())
    }
}
