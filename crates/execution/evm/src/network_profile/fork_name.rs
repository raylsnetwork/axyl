use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A hardfork name used as a config/record map key.
///
/// Always stored lower-cased, enforced at construction and deserialization,
/// so a fork is looked up with a plain `get` regardless of where the map was
/// populated (config file, schedule record, built-in schedule, or a test).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForkName(String);

impl ForkName {
    /// Create a key from any string, lower-casing it.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into().to_lowercase())
    }

    /// The stored (lower-cased) name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ForkName {
    fn from(name: &str) -> Self {
        Self(name.to_lowercase())
    }
}

impl From<String> for ForkName {
    fn from(name: String) -> Self {
        Self(name.to_lowercase())
    }
}

impl std::fmt::Display for ForkName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ForkName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ForkName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(Self(name.to_lowercase()))
    }
}
