//! Response message types.
use rayls_infrastructure_types::{B256Map, Batch, Certificate};
use serde::{Deserialize, Serialize};

/// Reply to a certificate fetch request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchCertificatesResponse {
    /// Certificates sorted from lower to higher rounds.
    pub certificates: Vec<Certificate>,
}

/// Reply to a batch fetch request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBatchResponse {
    /// The missing batches fetched from peers.
    pub batches: B256Map<Batch>,
}
