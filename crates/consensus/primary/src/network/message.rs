//! Messages exchanged between primaries.

use crate::error::{PrimaryNetworkError, PrimaryNetworkResult};
use rayls_consensus_network::{types::IntoRpcError, PeerExchangeMap, RLMessage};
use rayls_infrastructure_types::{
    error::HeaderError, AuthorityIdentifier, BlockHash, BlsPublicKey, BlsSignature, Certificate,
    CertificateDigest, ConsensusHeader, DefaultHashFunction, Epoch, EpochCertificate, EpochRecord,
    EpochVote, Header, Round, Votable, Vote, B256,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Info that is published (via gossip) by validators once they reach consensus.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct ConsensusResult {
    // epoch for this result (i.e. the current epoch)
    pub epoch: Epoch,
    // reound for epoch that consensus was reached on
    pub round: Round,
    /// the consensus header block number
    pub number: u64,
    /// hash of the consensus header that was reached
    pub hash: BlockHash,
    /// the validator that produced this result
    pub validator: BlsPublicKey,
    /// the signature of the validator publishing this record
    /// see digest() below, this is a signature over the has of the epoch, round, number and hash
    /// fields
    pub signature: BlsSignature,
}

impl Votable for ConsensusResult {
    fn voter_id(&self) -> AuthorityIdentifier {
        self.validator.into()
    }
}

impl ConsensusResult {
    /// Return the digest of the data fields (epoch, round, number and hash).
    /// This will be the same for all validadors and is what signature signs
    /// (verifying all the data fields not just the hash).
    pub fn digest(&self) -> BlockHash {
        Self::digest_data(self.epoch, self.round, self.number, self.hash)
    }

    /// Return the digest of the data fields (epoch, round, number and hash).
    /// Used for generating the signature of the raw data.
    /// This will be the same for all validadors and is what signature signs
    /// (verifying all the data fields not just the hash).
    pub fn digest_data(epoch: Epoch, round: Round, number: u64, hash: BlockHash) -> BlockHash {
        let mut hasher = DefaultHashFunction::new();
        hasher.update(&epoch.to_be_bytes());
        hasher.update(&round.to_be_bytes());
        hasher.update(&number.to_be_bytes());
        hasher.update(hash.as_ref());
        B256::from_slice(hasher.finalize().as_bytes())
    }
}

/// Primary messages on the gossip network.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum PrimaryGossip {
    /// A new certificate broadcast from peer.
    ///
    /// Certificates are small and okay to gossip uncompressed:
    /// - 3 signatures ~= 0.3kb
    /// - 99 signatures ~= 3.5kb
    ///
    /// NOTE: `snappy` is slightly larger than uncompressed.
    Certificate(Box<Certificate>),
    /// Consensus output reached- publish the consensus chain height and new block hash.
    Consensus(Box<ConsensusResult>),
    /// Signed hash sent out by committee memebers at epoch start.
    EpochVote(Box<EpochVote>),
}

// impl RLMessage trait for types
impl RLMessage for PrimaryRequest {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        match self {
            Self::PeerExchange { peers } => Some(peers.clone()),
            _ => None,
        }
    }
}
impl RLMessage for PrimaryResponse {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        None
    }
}

/// Requests from Primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryRequest {
    /// Primary request for vote on new header.
    Vote {
        /// This primary's header for the round.
        header: Arc<Header>,
        /// Parent certificates provided by the requesting peer in case the primary's peer is
        /// missing them. The peer requires parent certs in order to vote.
        parents: Vec<Certificate>,
    },
    /// Request for missing certificates.
    MissingCertificates {
        /// Inner type with specific helper methods for requesting missing certificates.
        inner: MissingCertificatesRequest,
    },
    /// Request a consensus chain header with consensus output.
    ///
    /// If both number and hash are set they should match (no need to set them both).
    /// If neither number or hash are set then will return the latest consensus chain header.
    ConsensusHeader {
        /// Block number requesting if not None.
        number: Option<u64>,
        /// Block hash requesting if not None.
        hash: Option<BlockHash>,
    },
    /// Exchange peer information.
    ///
    /// This "request" is sent to peers when this node disconnects
    /// due to excess peers. The peer exchange is intended to support
    /// discovery.
    PeerExchange { peers: PeerExchangeMap },
    /// Request an ['EpochRecord'] with ['EpochCertificate'].
    ///
    /// If both number and hash are set they should match (no need to set them both).
    /// If neither number or hash are set then will return the latest epoch record the node has
    /// available.
    EpochRecord {
        /// Block number requesting if not None.
        epoch: Option<Epoch>,
        /// Block hash requesting if not None.
        hash: Option<BlockHash>,
    },
}

// unit test for this struct in primary::src::tests::network_tests::test_missing_certs_request
/// Used by the primary to fetch certificates from other primaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingCertificatesRequest {
    /// The request is for certificates AFTER this round (non-inclusive). The boundary indicates
    /// the difference between the requestor's GC round and is the last round for which this peer
    /// has sufficient certificates.
    pub exclusive_lower_bound: Round,
    /// Rounds that should be skipped while processing this request (by authority). The rounds are
    /// serialized as [RoaringBitmap]s.
    pub skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
    /// The maximum size of the uncompressed response message (in bytes). The caller shares this so
    /// the response doesn't get rejected by the request_response codec.
    pub max_response_size: usize,
    /// Optional exclusive upper bound for the requested round range. When set, only certificates
    /// with round strictly less than this value will be returned. This allows fetching
    /// certificates in chunks. `None` means no upper bound (fetch from lower bound as far as
    /// possible).
    #[serde(default)]
    pub exclusive_upper_bound: Option<Round>,
}

impl MissingCertificatesRequest {
    /// Deserialize the [RoaringBitmap] representing the difference between the requesting peer's
    /// lower boundary and their GC round.
    pub(crate) fn get_bounds(
        &self,
    ) -> PrimaryNetworkResult<(Round, BTreeMap<AuthorityIdentifier, BTreeSet<Round>>)> {
        let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>> = self
            .skip_rounds
            .iter()
            .map(|(k, serialized)| {
                // Normalize on read: this bitmap comes from a peer request. It is
                // only iterated today (so the empty-container re-serialize panic
                // can't fire here), but routing every untrusted roaring
                // deserialization through the shared helper keeps that invariant
                // if this call site ever grows to re-encode the bitmap. See #55.
                let rounds =
                    rayls_infrastructure_types::serde::deserialize_normalized(&serialized[..])?
                        .into_iter()
                        .map(|r| self.exclusive_lower_bound + r as Round)
                        .collect::<BTreeSet<Round>>();
                Ok((k.clone(), rounds))
            })
            .collect::<PrimaryNetworkResult<BTreeMap<_, _>>>()?;
        Ok((self.exclusive_lower_bound, skip_rounds))
    }

    /// Set the bounds for requesting missing certificates based on the current GC round.
    ///
    /// This method specifies which rounds should be skipped because they are already in storage.
    pub(crate) fn set_bounds(
        mut self,
        gc_round: Round,
        skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    ) -> PrimaryNetworkResult<Self> {
        self.exclusive_lower_bound = gc_round;
        self.skip_rounds = skip_rounds
            .into_iter()
            .map(|(k, rounds)| {
                let mut serialized = Vec::new();
                rounds
                    .into_iter()
                    .map(|v| {
                        v.checked_sub(gc_round).unwrap_or_else(|| {
                            // A skip round below the exclusive lower bound means the chunk's
                            // lower bound was computed above one of its own skip rounds (see
                            // `chunk_skip_rounds`). Encoding the delta would underflow `Round`
                            // and panic; clamp to 0 (the server treats it as the inert lower
                            // bound, so the cert is simply re-fetched) and log loudly so this
                            // is greppable instead of a bare "subtract with overflow" panic.
                            tracing::error!(
                                target: "primary::network::message",
                                authority = %k,
                                skip_round = v,
                                exclusive_lower_bound = gc_round,
                                "set_bounds: skip round is below the exclusive lower bound; \
                                 clamping delta to 0 (chunk lower bound exceeds a skip round)"
                            );
                            0
                        })
                    })
                    .collect::<RoaringBitmap>()
                    .serialize_into(&mut serialized)?;

                Ok((k, serialized))
            })
            .collect::<PrimaryNetworkResult<Vec<_>>>()?;

        Ok(self)
    }

    /// Specify the maximum number of expected certificates in the peer's response.
    pub fn set_max_response_size(mut self, max_size: usize) -> Self {
        self.max_response_size = max_size;
        self
    }

    /// Set an exclusive upper bound for the requested round range.
    pub fn set_exclusive_upper_bound(mut self, exclusive_upper_bound: Round) -> Self {
        self.exclusive_upper_bound = Some(exclusive_upper_bound);
        self
    }
}

impl From<PeerExchangeMap> for PrimaryRequest {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}

//
//
//=== Response types
//
//

/// Response to primary requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryResponse {
    /// The peer's vote if the peer considered the proposed header valid.
    Vote(Vote),
    /// The requested certificates requested by a peer.
    RequestedCertificates(Vec<Certificate>),
    /// Missing certificates in order to vote.
    ///
    /// If the peer was unable to verify parents for a proposed header, they respond requesting
    /// the missing certificate by digest.
    MissingParents(Vec<CertificateDigest>),
    /// The requested consensus header.
    ConsensusHeader(Arc<ConsensusHeader>),
    /// The requested epoch record and certificate.
    EpochRecord { record: EpochRecord, certificate: EpochCertificate },
    /// Exchange peer information.
    PeerExchange { peers: PeerExchangeMap },
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    Error(PrimaryRPCError),
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    /// This error is likely to succeed in the future and can be retried.
    RecoverableError(PrimaryRPCError),
    /// The proposed header is too old for the responding peer.
    TooOld {
        /// The round of the header that was rejected.
        header_round: Round,
        /// The responding peer's limit round (below which headers are rejected).
        limit_round: Round,
    },
    /// The proposed header belongs to a different epoch than the responding peer.
    EpochMismatch {
        /// The epoch the responding peer expected.
        expected: Epoch,
        /// The epoch of the proposed header.
        received: Epoch,
    },
}

impl PrimaryResponse {
    /// Helper method if the response is an error.
    pub fn is_err(&self) -> bool {
        matches!(
            self,
            PrimaryResponse::Error(_)
                | PrimaryResponse::TooOld { .. }
                | PrimaryResponse::EpochMismatch { .. }
        )
    }

    pub(crate) fn into_error_ref(error: &PrimaryNetworkError) -> Self {
        match error {
            PrimaryNetworkError::InvalidHeader(HeaderError::TooOld {
                header_round,
                max_round,
                ..
            }) => Self::TooOld { header_round: *header_round, limit_round: *max_round },
            PrimaryNetworkError::InvalidHeader(HeaderError::InvalidEpoch { ours, theirs })
                if *theirs == ours + 1 =>
            {
                // This is a common race condition on epoch restart so report as recoverable.
                Self::RecoverableError(PrimaryRPCError(error.to_string()))
            }
            PrimaryNetworkError::InvalidHeader(HeaderError::InvalidEpoch { ours, theirs }) => {
                Self::EpochMismatch { expected: *ours, received: *theirs }
            }
            PrimaryNetworkError::InvalidHeader(_)
            | PrimaryNetworkError::Decode(_)
            | PrimaryNetworkError::Certificate(_)
            | PrimaryNetworkError::StdIo(_)
            | PrimaryNetworkError::Storage(_)
            | PrimaryNetworkError::InvalidRequest(_)
            | PrimaryNetworkError::Internal(_)
            | PrimaryNetworkError::PeerNotInCommittee(_)
            | PrimaryNetworkError::UnavailableEpoch(_)
            | PrimaryNetworkError::UnavailableEpochDigest(_)
            | PrimaryNetworkError::InvalidTopic
            | PrimaryNetworkError::UnknownConsensusHeaderNumber(_)
            | PrimaryNetworkError::UnknownConsensusHeaderDigest(_)
            | PrimaryNetworkError::UnknownConsensusHeaderCert(_)
            | PrimaryNetworkError::InvalidEpochRequest => {
                Self::Error(PrimaryRPCError(error.to_string()))
            }
        }
    }
}

impl IntoRpcError<PrimaryNetworkError> for PrimaryResponse {
    fn into_error(error: PrimaryNetworkError) -> Self {
        Self::into_error_ref(&error)
    }
}

impl From<PrimaryRPCError> for PrimaryResponse {
    fn from(value: PrimaryRPCError) -> Self {
        Self::Error(value)
    }
}

/// Application-specific error type while handling Primary request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PrimaryRPCError(pub String);

impl From<PeerExchangeMap> for PrimaryResponse {
    fn from(value: PeerExchangeMap) -> Self {
        Self::PeerExchange { peers: value }
    }
}
