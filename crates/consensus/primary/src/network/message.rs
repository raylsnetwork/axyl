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
    ///
    /// Decoding fails as soon as the list is longer than [MAX_SKIP_ROUND_AUTHORITIES].
    #[serde(deserialize_with = "deserialize_skip_rounds")]
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

/// Most authorities a missing-certificates request may list on the wire.
///
/// Honest requests list about one committee, so this bound is far above normal use.
/// This bound only stops a peer from making the node decode a huge list.
pub(crate) const MAX_SKIP_ROUND_AUTHORITIES: usize = 1024;

/// Decode the skip-round list, refusing a list longer than [MAX_SKIP_ROUND_AUTHORITIES].
///
/// The length is checked before any entry is decoded.
fn deserialize_skip_rounds<'de, D>(
    deserializer: D,
) -> Result<Vec<(AuthorityIdentifier, Vec<u8>)>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BoundedList;

    impl<'de> serde::de::Visitor<'de> for BoundedList {
        type Value = Vec<(AuthorityIdentifier, Vec<u8>)>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "at most {MAX_SKIP_ROUND_AUTHORITIES} skip-round entries")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            // bcs always gives the exact length, so a long list is refused before any entry.
            // The check in the loop covers formats that give no length.
            let len = seq.size_hint().unwrap_or(0);
            if len > MAX_SKIP_ROUND_AUTHORITIES {
                return Err(serde::de::Error::invalid_length(len, &self));
            }
            let mut list = Vec::with_capacity(len.min(MAX_SKIP_ROUND_AUTHORITIES));
            while let Some(entry) = seq.next_element()? {
                if list.len() == MAX_SKIP_ROUND_AUTHORITIES {
                    return Err(serde::de::Error::invalid_length(list.len() + 1, &self));
                }
                list.push(entry);
            }
            Ok(list)
        }
    }

    deserializer.deserialize_seq(BoundedList)
}

/// Most containers a skip-round bitmap may declare.
///
/// Rounds count up from the request's lower bound, so honest bitmaps use one container.
/// A container can stand for 65,536 rounds in 14 bytes, so the count is checked before decoding.
pub(crate) const MAX_SKIP_ROUND_CONTAINERS: usize = 16;

/// Most bytes a skip bitmap may take when it holds at most `max_rounds` rounds.
///
/// An honest bitmap stores each round in two bytes, after a header and an entry per container.
/// Capping the length bounds the decoding work, whatever containers the bitmap declares.
fn max_skip_bitmap_len(max_rounds: usize) -> usize {
    // the cookie and the container count
    const HEADER: usize = 2 * size_of::<u32>();
    // a container's key, its count and its offset
    const PER_CONTAINER: usize = 2 * size_of::<u32>();
    HEADER + PER_CONTAINER * MAX_SKIP_ROUND_CONTAINERS + size_of::<u16>() * max_rounds
}

/// Number of containers a serialized roaring bitmap declares, or `None` for an unknown header.
fn roaring_container_count(bytes: &[u8]) -> Option<usize> {
    // Header of a bitmap without run containers: this cookie, then the container count.
    const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
    // Header of a bitmap with run containers: this cookie, then the container count minus one.
    const SERIAL_COOKIE: u16 = 12347;
    const WORD: usize = size_of::<u32>();
    let word = |at: usize| -> Option<u32> {
        Some(u32::from_le_bytes(bytes.get(at..at + WORD)?.try_into().ok()?))
    };
    let cookie = word(0)?;
    if cookie as u16 == SERIAL_COOKIE {
        Some((cookie >> u16::BITS) as usize + 1)
    } else if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
        Some(word(WORD)? as usize)
    } else {
        None
    }
}

impl MissingCertificatesRequest {
    /// Deserialize the [RoaringBitmap] representing the difference between the requesting peer's
    /// lower boundary and their GC round.
    ///
    /// Each bitmap is checked for its container count and length before it is decoded, and for
    /// `max_rounds` before its rounds are collected, so a small bitmap cannot stand for billions
    /// of rounds.
    pub(crate) fn get_bounds(
        &self,
        max_rounds: usize,
    ) -> PrimaryNetworkResult<(Round, BTreeMap<AuthorityIdentifier, BTreeSet<Round>>)> {
        let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>> = self
            .skip_rounds
            .iter()
            .map(|(k, serialized)| {
                let containers = roaring_container_count(serialized).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "unknown skip bitmap")
                })?;
                if containers > MAX_SKIP_ROUND_CONTAINERS {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "skip bitmap declares too many containers",
                    )
                    .into());
                }
                if serialized.len() > max_skip_bitmap_len(max_rounds) {
                    return Err(PrimaryNetworkError::InvalidRequest(
                        "Skip bitmap is too long".into(),
                    ));
                }
                // Normalize on read: this bitmap comes from a peer request. It is
                // only iterated today (so the empty-container re-serialize panic
                // can't fire here), but routing every untrusted roaring
                // deserialization through the shared helper keeps that invariant
                // if this call site ever grows to re-encode the bitmap. See #55.
                // Both operands are peer-supplied. A plain `+` overflows `Round` on a crafted
                // request and, with `overflow-checks = true` and `panic = "abort"` in release,
                // takes the whole node down. Reject instead; the io error maps to a penalty.
                let bitmap =
                    rayls_infrastructure_types::serde::deserialize_normalized(&serialized[..])?;
                if bitmap.len() > max_rounds as u64 {
                    return Err(PrimaryNetworkError::InvalidRequest(
                        "Request for rounds out of bounds".into(),
                    ));
                }
                let rounds = bitmap
                    .into_iter()
                    .map(|r| {
                        self.exclusive_lower_bound.checked_add(r as Round).ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "skip round overflows the round range",
                            )
                        })
                    })
                    .collect::<std::io::Result<BTreeSet<Round>>>()?;
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
            | PrimaryNetworkError::InvalidEpochRequest
            | PrimaryNetworkError::TooManyAuthorities(..)
            | PrimaryNetworkError::Busy => Self::Error(PrimaryRPCError(error.to_string())),
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
