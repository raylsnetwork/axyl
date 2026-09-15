// SPDX-License-Identifier: BUSL-1.1
//! Report types: one per subcommand, each a list of per-node views plus a network-wide verdict.

pub mod epoch;
pub mod header;
pub mod summary;

use serde::{ser::SerializeMap as _, Serialize, Serializer};

/// Verdict codes, shared by every command with the same meaning.
pub mod code {
    /// Every node has it and they agree (certified where that applies).
    pub const OK: &str = "OK";
    /// No node has reached it yet.
    pub const NOT_REACHED: &str = "NOT_REACHED";
    /// Some nodes lack it, are behind, or are uncertified.
    pub const PARTIAL: &str = "PARTIAL";
    /// No node has it although all should.
    pub const MISSING: &str = "MISSING";
    /// Nodes disagree on content.
    pub const DIVERGENT: &str = "DIVERGENT";
    /// A chain link check failed.
    pub const BROKEN: &str = "BROKEN";
    /// Nothing to check.
    pub const EMPTY: &str = "EMPTY";
}

/// A typed verdict field value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Value {
    Int(u64),
    /// Inclusive range, rendered `a..=b`.
    Range(u64, u64),
    Word(&'static str),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(n) => write!(f, "{n}"),
            Self::Range(a, b) => write!(f, "{a}..={b}"),
            Self::Word(w) => f.write_str(w),
        }
    }
}

/// The network-wide conclusion of a subcommand: one line in text mode (`CODE key=value ...`),
/// the `verdict` object in JSON, and the exit status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// One of [`code`].
    pub code: &'static str,
    /// Whether the process exits 0.
    pub healthy: bool,
    /// Ordered `key=value` fields; a fixed vocabulary per command, zero counts omitted.
    pub fields: Vec<(&'static str, Value)>,
}

impl Verdict {
    pub fn new(code: &'static str) -> Self {
        Self { code, healthy: code == code::OK, fields: Vec::new() }
    }

    /// Adds an integer field, always.
    pub fn num(mut self, key: &'static str, n: usize) -> Self {
        self.fields.push((key, Value::Int(n as u64)));
        self
    }

    /// Adds an integer field unless it is zero.
    pub fn count(self, key: &'static str, n: usize) -> Self {
        if n == 0 {
            self
        } else {
            self.num(key, n)
        }
    }

    /// Adds an optional integer field.
    pub fn opt<N: Into<u64>>(mut self, key: &'static str, n: Option<N>) -> Self {
        if let Some(n) = n {
            self.fields.push((key, Value::Int(n.into())));
        }
        self
    }

    pub fn range<N: Into<u64>>(mut self, key: &'static str, a: N, b: N) -> Self {
        self.fields.push((key, Value::Range(a.into(), b.into())));
        self
    }

    pub fn word(mut self, key: &'static str, w: &'static str) -> Self {
        self.fields.push((key, Value::Word(w)));
        self
    }

    /// The value of `key`, if present.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code)?;
        for (k, v) in &self.fields {
            write!(f, " {k}={v}")?;
        }
        Ok(())
    }
}

impl Serialize for Verdict {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Fields<'a>(#[serde(serialize_with = "ordered")] &'a [(&'static str, Value)]);
        fn ordered<S: Serializer>(
            fields: &[(&'static str, Value)],
            s: S,
        ) -> Result<S::Ok, S::Error> {
            let mut map = s.serialize_map(Some(fields.len()))?;
            for (k, v) in fields {
                map.serialize_entry(k, v)?;
            }
            map.end()
        }
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("code", self.code)?;
        map.serialize_entry("healthy", &self.healthy)?;
        map.serialize_entry("fields", &Fields(&self.fields))?;
        map.end()
    }
}

/// Output of one subcommand run.
#[derive(Debug, Serialize)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub enum Report {
    Epoch(epoch::EpochReport),
    Epochs(epoch::EpochsReport),
    ChainCheck(epoch::ChainCheckReport),
    Header(header::HeaderReport),
    Cert(header::CertReport),
    Walk(header::WalkReport),
    Summary(summary::SummaryReport),
}

impl Report {
    /// The verdict, if the subcommand produces one (`summary` does not).
    pub fn verdict(&self) -> Option<&Verdict> {
        match self {
            Self::Epoch(r) => Some(&r.verdict),
            Self::Epochs(r) => Some(&r.verdict),
            Self::ChainCheck(r) => Some(&r.verdict),
            Self::Header(r) => Some(&r.verdict),
            Self::Cert(r) => Some(&r.verdict),
            Self::Walk(r) => Some(&r.verdict),
            Self::Summary(_) => None,
        }
    }

    /// Whether the run should exit 0.
    pub fn healthy(&self) -> bool {
        self.verdict().is_none_or(|v| v.healthy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_code_and_fields() {
        let v = Verdict::new(code::PARTIAL)
            .num("nodes", 3)
            .count("missing", 0)
            .count("not_reached", 1)
            .range("epochs", 1u32, 3u32)
            .word("what", "signers");
        assert_eq!(v.to_string(), "PARTIAL nodes=3 not_reached=1 epochs=1..=3 what=signers");
        assert!(!v.healthy);
        assert!(Verdict::new(code::OK).healthy);
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(json["code"], "PARTIAL");
        assert_eq!(json["fields"]["nodes"], 3);
        assert_eq!(json["fields"]["epochs"], serde_json::json!([1, 3]));
        assert_eq!(json["fields"]["what"], "signers");
    }
}
