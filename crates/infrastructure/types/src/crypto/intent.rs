//! Intent message types to protect replay attacks on signatures.

use crate::try_decode;
use eyre::eyre;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::str::FromStr;

/// The prefix length for intent messages.
pub const INTENT_PREFIX_LENGTH: usize = 3;

/// The version here is to distinguish between signing different versions of the struct
/// or enum.
///
/// Serialized output between two different versions of the same struct/enum
/// might accidentally (or maliciously on purpose) match.
#[derive(Serialize_repr, Deserialize_repr, Copy, Clone, PartialEq, Eq, Debug, Hash)]
#[repr(u8)]
pub enum IntentVersion {
    V0 = 0,
}

impl TryFrom<u8> for IntentVersion {
    type Error = eyre::Report;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(try_decode(&[value])?)
    }
}

/// This enums specifies the application ID.
///
/// Two intents in two different applications
/// (ie. rayls, Ethereum, Polygon, etc) should never collide, so that even when a
/// signing key is reused, the signature designated for app_1 cannot be used as a
/// valid signature for any intent in app_2.
#[derive(Serialize_repr, Deserialize_repr, Copy, Clone, PartialEq, Eq, Debug, Hash, Default)]
#[repr(u8)]
pub enum AppId {
    #[default]
    Rayls = 0,
    Consensus = 1,
}

impl TryFrom<u8> for AppId {
    type Error = eyre::Report;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(try_decode(&[value])?)
    }
}
/// This enums specifies the intent scope.
///
/// Two intents for different scope should
/// never collide, so no signature provided for one intent scope can be used for
/// another, even when the serialized data itself may be the same.
#[derive(Serialize_repr, Deserialize_repr, Copy, Clone, PartialEq, Eq, Debug, Hash)]
#[repr(u8)]
pub enum IntentScope {
    ProofOfPossession = 0, // Used for authority's proof of possession for protocol keys.
    EpochBoundary = 1,     // Used for authority signature on a checkpoint at epochs boundaries.
    ConsensusDigest = 2,   // Used for authority signature on consensus digests.
    SystemMessage = 3,     // Used for signing system messages.
    DkgShare = 4,          // Used for DKG share decryption keys. Never published.
}

impl TryFrom<u8> for IntentScope {
    type Error = eyre::Report;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(try_decode(&[value])?)
    }
}

/// An intent is a compact struct serves as the domain separator for a message that a signature
/// commits to.
///
/// It consists of three parts: [enum IntentScope] (what the type of the message is),
/// [enum IntentVersion], [enum AppId] (what application that the signature refers to). It is used
/// to construct [struct IntentMessage] that what a signature commits to.
///
/// The serialization of an Intent is a 3-byte array where each field is represented by a byte.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct Intent {
    /// The scope of the intent within the system.
    pub scope: IntentScope,
    /// The version of intent.
    pub version: IntentVersion,
    /// The application id.
    pub app_id: AppId,
}

impl FromStr for Intent {
    type Err = eyre::Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.strip_prefix("0x").unwrap_or(s);
        let s = hex::decode(s)?;
        if s.len() != 3 {
            return Err(eyre!("Invalid Intent"));
        }
        Ok(Self { scope: s[0].try_into()?, version: s[1].try_into()?, app_id: s[2].try_into()? })
    }
}

impl Intent {
    pub fn rayls(scope: IntentScope) -> Self {
        Self { version: IntentVersion::V0, scope, app_id: AppId::Rayls }
    }

    pub fn consensus(scope: IntentScope) -> Self {
        Self { scope, version: IntentVersion::V0, app_id: AppId::Consensus }
    }
}

/// Intent Message is a wrapper around a message specifying its intent.
///
/// The message can be any type that implements [trait Serialize]. *ALL* signatures must
/// sign the intent message, not the data itself. This guarantees any intent
/// message signed in the system cannot collide with another since the domains
/// are separated by intent.
///
/// The serialization of an IntentMessage is compact: it only appends three bytes
/// to the message itself.
#[derive(Debug, PartialEq, Eq, Serialize, Clone, Hash, Deserialize)]
pub struct IntentMessage<T> {
    /// The data specifying the signature's intent.
    pub intent: Intent,
    /// The underlying data of the message to include.
    pub value: T,
}

impl<T> IntentMessage<T> {
    pub fn new(intent: Intent, value: T) -> Self {
        Self { intent, value }
    }
}
