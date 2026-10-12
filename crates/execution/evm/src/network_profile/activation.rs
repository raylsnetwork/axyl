use reth_chainspec::ForkCondition;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The activation condition of a single hardfork in a network config file.
///
/// Serialized as a plain block number (`Eip1559: 0`) or the string
/// `never` (`AdminTransfer: never`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkActivation {
    /// The fork activates at the given block number.
    Block(u64),
    /// The fork never activates.
    Never,
}

impl Serialize for ForkActivation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Block(block) => serializer.serialize_u64(*block),
            Self::Never => serializer.serialize_str("never"),
        }
    }
}

impl<'de> Deserialize<'de> for ForkActivation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Block(u64),
            Text(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Block(block) => Ok(Self::Block(block)),
            Raw::Text(text) => {
                if text.eq_ignore_ascii_case("never") {
                    Ok(Self::Never)
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid fork activation {text:?}; expected a block number or \"never\""
                    )))
                }
            }
        }
    }
}

impl ForkActivation {
    /// The activation block; `None` for a fork that never activates.
    pub fn block(&self) -> Option<u64> {
        match self {
            Self::Block(block) => Some(*block),
            Self::Never => None,
        }
    }
}

impl From<ForkActivation> for ForkCondition {
    fn from(activation: ForkActivation) -> Self {
        match activation {
            ForkActivation::Block(block) => ForkCondition::Block(block),
            ForkActivation::Never => ForkCondition::Never,
        }
    }
}
