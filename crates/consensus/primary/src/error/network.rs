//! Error types for primary's network task.

use super::CertManagerError;
use rayls_consensus_network::Penalty;
use rayls_infrastructure_storage::StoreError;
use rayls_infrastructure_types::{
    error::{CertificateError, HeaderError},
    BcsError, BlockHash, BlsPublicKey, Epoch,
};

/// Result alias for results that possibly return [`PrimaryNetworkError`].
pub(crate) type PrimaryNetworkResult<T> = Result<T, PrimaryNetworkError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PrimaryNetworkError {
    /// Error while processing a peer's request for vote.
    #[error("Error processing header vote request: {0}")]
    InvalidHeader(#[from] HeaderError),
    /// Error decoding with bcs.
    #[error("Failed to decode gossip message: {0}")]
    Decode(#[from] BcsError),
    /// Error processing certificate.
    #[error("Failed to process certificate: {0}")]
    Certificate(#[from] CertManagerError),
    /// Error conversion from [std::io::Error]
    #[error(transparent)]
    StdIo(#[from] std::io::Error),
    /// Error retrieving value from storage.
    #[error("Storage failure: {0}")]
    Storage(#[from] StoreError),
    /// The peer's request is invalid.
    #[error("{0}")]
    InvalidRequest(String),
    /// Internal error occurred.
    #[error("Internal error: {0}")]
    Internal(String),
    /// Unknown consensus header.
    #[error("Unknown consensus header: {0}")]
    UnknownConsensusHeaderNumber(u64),
    /// Unknown consensus header.
    #[error("Unknown consensus header: {0}")]
    UnknownConsensusHeaderDigest(BlockHash),
    /// Unknown consensus header certificate.
    #[error("Unknown consensus header certificate for: {0}")]
    UnknownConsensusHeaderCert(BlockHash),
    /// Peer that is not committee published invalid gosip.
    /// Temparily disabled, will be back soon.
    #[error("Peer {0} is not in the committee!")]
    PeerNotInCommittee(Box<BlsPublicKey>),
    /// Unavaliable epoch (either it is invalid or this node does not have it).
    #[error("Unknown epoch record: {0}")]
    UnavailableEpoch(Epoch),
    /// Unavaliable epoch hash (either it is invalid or this node does not have it).
    #[error("Unknown epoch record digest: {0}")]
    UnavailableEpochDigest(BlockHash),
    /// Invalid epoch request.
    #[error("Must suply an epoch or hash when requesting an epoch record")]
    InvalidEpochRequest,
    /// Invalid topic- something was published to the wrong topic.
    #[error("Gossip was published to the wrong topic")]
    InvalidTopic,
    /// A missing-certificates request names more authorities than any honest node sends.
    #[error("Request names {0} authorities, the limit is {1}")]
    TooManyAuthorities(usize, usize),
    /// Too many missing-certificates requests are already in progress.
    #[error("Too many missing certificate requests in progress, retry later")]
    Busy,
}

impl From<&PrimaryNetworkError> for Option<Penalty> {
    fn from(val: &PrimaryNetworkError) -> Self {
        //
        // explicitly match every error type to ensure penalties are updated with changes
        //
        match val {
            PrimaryNetworkError::InvalidHeader(header_error) => {
                penalty_from_header_error(header_error)
            }
            PrimaryNetworkError::Certificate(e) => match e {
                CertManagerError::Certificate(certificate_error) => match certificate_error {
                    CertificateError::Header(header_error) => {
                        penalty_from_header_error(header_error)
                    }
                    // no penalty - stale certs are from catching-up peers
                    CertificateError::TooOld(_, _, _) => None,
                    // fatal
                    CertificateError::RecoverBlsAggregateSignatureBytes
                    | CertificateError::Unsigned
                    | CertificateError::Inquorate { .. }
                    | CertificateError::InvalidSignature => Some(Penalty::Fatal),
                    // ignore
                    CertificateError::ResChannelClosed(_)
                    | CertificateError::TooNew(_, _, _)
                    | CertificateError::Storage(_) => None,
                },
                // fatal
                CertManagerError::UnverifiedSignature(_) => Some(Penalty::Fatal),
                // ignore
                CertManagerError::PendingCertificateNotFound(_)
                | CertManagerError::PendingParentsMismatch(_)
                | CertManagerError::CertificateManagerOneshot
                | CertManagerError::FatalForwardAcceptedCertificate
                | CertManagerError::NoCertificateFetched
                | CertManagerError::FutureEpoch { .. }
                | CertManagerError::FatalAppendParent
                | CertManagerError::GC(_)
                | CertManagerError::JoinError
                | CertManagerError::Pending(_)
                | CertManagerError::Storage(_)
                | CertManagerError::RequestBounds(_)
                | CertManagerError::RAYLSSend(_) => None,
            },
            PrimaryNetworkError::InvalidRequest(_) => None, // do not apply penalty on invalid request until we update certificate fetcher
            PrimaryNetworkError::UnknownConsensusHeaderNumber(_)
                | PrimaryNetworkError::UnknownConsensusHeaderDigest(_)
                | PrimaryNetworkError::UnknownConsensusHeaderCert(_) => Some(Penalty::Mild),
            PrimaryNetworkError::InvalidEpochRequest
                | PrimaryNetworkError::StdIo(_) => Some(Penalty::Medium),
            PrimaryNetworkError::InvalidTopic
                | PrimaryNetworkError::Decode(_) => Some(Penalty::Fatal),
            PrimaryNetworkError::TooManyAuthorities(..) => Some(Penalty::Mild),
            PrimaryNetworkError::Busy => None,
            PrimaryNetworkError::UnavailableEpoch(_)  // A node might not have this yet...
                | PrimaryNetworkError::UnavailableEpochDigest(_)  // A node might not have this yet....
                | PrimaryNetworkError::PeerNotInCommittee(_)
                | PrimaryNetworkError::Storage(_)
                | PrimaryNetworkError::Internal(_) => None,
        }
    }
}

/// Helper function to convert `HeaderError` to `Penalty`.
///
/// Header errors are responsible for more than one PrimaryNetworkHandle.
fn penalty_from_header_error(error: &HeaderError) -> Option<Penalty> {
    match error {
        // mild
        HeaderError::SyncBatches(_)
        | HeaderError::TooNew { .. }
        | HeaderError::Storage(_)
        | HeaderError::UnknownExecutionResult(_) => Some(Penalty::Mild),
        // medium
        HeaderError::InvalidParents
        | HeaderError::WrongNumberOfParents(_, _)
        | HeaderError::TooOld { .. } => Some(Penalty::Medium),
        // severe
        HeaderError::InvalidTimestamp { .. } | HeaderError::InvalidParentRound => {
            Some(Penalty::Severe)
        }
        // fatal
        HeaderError::AlreadyVotedForLaterRound { .. }
        | HeaderError::AlreadyVoted(_, _)
        | HeaderError::DuplicateParents
        | HeaderError::TooManyParents(_, _)
        | HeaderError::UnknownNetworkKey(_)
        | HeaderError::PeerNotAuthor
        | HeaderError::InvalidGenesisParent(_)
        | HeaderError::ParentMissingSignature
        | HeaderError::InvalidParentTimestamp { .. }
        | HeaderError::UnknownWorkerId
        | HeaderError::InvalidHeaderDigest
        | HeaderError::InvalidRound { .. }
        | HeaderError::UnknownAuthority(_) => Some(Penalty::Fatal),
        // ignore
        HeaderError::PendingCertificateOneshot
        | HeaderError::RAYLSSend(_)
        | HeaderError::InvalidEpoch { .. }
        | HeaderError::ClosedWatchChannel => None,
    }
}
