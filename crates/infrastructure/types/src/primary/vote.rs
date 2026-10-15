//! Vote implementation for consensus

use crate::{
    crypto::{self, to_intent_message, BlsSignature, IntentMessage, ProtocolSignature},
    encode, AuthorityIdentifier, BlsSigner, Digest, Epoch, Hash, Header, HeaderDigest, Round,
    Signer, Votable,
};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A Vote on a Header is a claim by the voting authority that all payloads and the full history
/// of Certificates included in the Header are available.
#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    /// HeaderDigest, round, epoch and origin for the header being voted on.
    pub header_digest: HeaderDigest,
    /// Round for this vote.
    pub round: Round,
    /// Epoch for this vote.
    pub epoch: Epoch,
    /// TODO - doc
    pub origin: AuthorityIdentifier,
    /// Author of this vote.
    pub author: AuthorityIdentifier,
    /// Signature of the HeaderDigest.
    pub signature: BlsSignature,
}

impl Vote {
    /// Create a new instance of [Vote]
    pub fn new<BLS: BlsSigner>(
        header: &Header,
        author: AuthorityIdentifier,
        signature_service: &BLS,
    ) -> Self {
        let header_digest = header.digest();
        let vote_digest: Digest<{ crypto::DIGEST_LENGTH }> = header_digest.into();
        let signature =
            signature_service.request_signature_direct(&encode(&to_intent_message(vote_digest)));
        Self {
            header_digest,
            round: header.round(),
            epoch: header.epoch(),
            origin: header.author().clone(),
            author,
            signature,
        }
    }

    /// Create a vote directly with a suplied signer: the node's threshold share in a threshold
    /// epoch, or a private key in tests. Otherwise use the BlsSigner version.
    pub fn new_with_signer<S>(header: &Header, author: AuthorityIdentifier, signer: &S) -> Self
    where
        S: Signer,
    {
        let header_digest = header.digest();
        let vote_digest: Digest<{ crypto::DIGEST_LENGTH }> = header_digest.into();
        let signature = BlsSignature::new_secure(&to_intent_message(vote_digest), signer);
        Self {
            header_digest,
            round: header.round(),
            epoch: header.epoch(),
            origin: header.author().clone(),
            author,
            signature,
        }
    }

    pub fn header_digest(&self) -> HeaderDigest {
        self.header_digest
    }
    pub fn round(&self) -> Round {
        self.round
    }
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    pub fn origin(&self) -> &AuthorityIdentifier {
        &self.origin
    }
    pub fn author(&self) -> &AuthorityIdentifier {
        &self.author
    }
    pub fn signature(&self) -> &BlsSignature {
        &self.signature
    }
}

impl Votable for Vote {
    fn voter_id(&self) -> AuthorityIdentifier {
        self.author.clone()
    }
}

/// Hash a Vote based on the crate's `DIGEST_LENGTH`
#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Serialize, Deserialize)]
pub struct VoteDigest(Digest<{ crypto::DIGEST_LENGTH }>);

impl VoteDigest {
    /// Create a VoteDigest
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        VoteDigest(Digest { digest })
    }
}

impl From<VoteDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: VoteDigest) -> Self {
        hd.0
    }
}

impl From<VoteDigest> for HeaderDigest {
    fn from(value: VoteDigest) -> Self {
        Self::new(value.0.into())
    }
}

impl From<VoteDigest> for Digest<{ crypto::INTENT_MESSAGE_LENGTH }> {
    fn from(digest: VoteDigest) -> Self {
        let intent_message: IntentMessage<HeaderDigest> = to_intent_message(digest.into());
        Digest {
            digest: encode(&intent_message).try_into().expect("INTENT_MESSAGE_LENGTH is correct"),
        }
    }
}

impl fmt::Debug for VoteDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for VoteDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_string().get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Vote {
    type TypedDigest = VoteDigest;

    fn digest(&self) -> VoteDigest {
        self.header_digest.into()
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V{}({}, {}, E{})",
            self.digest(),
            self.round(),
            self.author(),
            self.origin(),
            self.epoch()
        )
    }
}

impl PartialEq for Vote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct VoteInfo {
    /// The latest Epoch for which a vote was sent to given authority
    pub epoch: Epoch,
    /// The latest round for which a vote was sent to given authority
    pub round: Round,
    /// The hash of the vote used to ensure equality
    pub vote_digest: VoteDigest,
}

impl VoteInfo {
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn vote_digest(&self) -> VoteDigest {
        self.vote_digest
    }
}

impl From<&Vote> for VoteInfo {
    fn from(vote: &Vote) -> Self {
        VoteInfo { epoch: vote.epoch(), round: vote.round(), vote_digest: vote.digest() }
    }
}
