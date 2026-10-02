// SPDX-License-Identifier: BUSL-1.1
//! Report types: one per subcommand, each a list of per-node views plus a network-wide verdict.

pub mod batch;
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

// ---------------------------------------------------------------------------------------------
// Shared building blocks: point lookups and chain checks
// ---------------------------------------------------------------------------------------------

/// Outcome of looking one object up on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Lookup {
    /// The object was found (in some tier).
    Found,
    /// The node should have the object (its tip or epoch is past it) but does not: a real gap.
    Missing,
    /// The node has not reached the object's position yet.
    NotReached,
    /// No node holds the object and nothing says any of them should (`get-batch`, `get-tx`).
    NotFound,
}

/// Human description of a consensus-number lookup that found nothing.
pub fn describe_absent(lookup: Lookup, tip: Option<u64>) -> String {
    match (lookup, tip) {
        (Lookup::NotReached, Some(tip)) => format!("not reached (tip {tip})"),
        (Lookup::NotReached, None) => "not reached (no headers)".to_owned(),
        (Lookup::NotFound, _) => "not found".to_owned(),
        _ => "missing".to_owned(),
    }
}

/// Verdict for "one object looked up on every node", before any content comparison: covers not
/// reached / not found / missing everywhere / missing somewhere. `None` when every node found it.
pub(crate) fn absence_verdict(lookups: &[Lookup]) -> Option<Verdict> {
    let total = lookups.len();
    let tally = |wanted: Lookup| lookups.iter().filter(|l| **l == wanted).count();
    let found = tally(Lookup::Found);
    let not_reached = tally(Lookup::NotReached);
    let missing = tally(Lookup::Missing);
    let not_found = tally(Lookup::NotFound);
    if found == total {
        return None;
    }
    let code = if not_reached == total {
        code::NOT_REACHED
    } else if found == 0 && missing == 0 {
        // nobody has it and nothing says anybody should: not found, or not found plus behind
        code::EMPTY
    } else if found == 0 {
        code::MISSING
    } else {
        code::PARTIAL
    };
    Some(
        Verdict::new(code)
            .num("nodes", total)
            .count("found", found)
            .count("missing", missing)
            .count("not_reached", not_reached)
            .count("not_found", not_found),
    )
}

/// `verdict` with its code replaced by `code` and `healthy` recomputed; the fields stay.
pub(crate) fn recode(verdict: Verdict, code: &'static str) -> Verdict {
    Verdict { code, healthy: code == code::OK, fields: verdict.fields }
}

/// Verdict when every node holds content: agreement or divergence on `what`.
pub(crate) fn content_verdict(total: usize, variants: usize, what: &'static str) -> Verdict {
    if variants > 1 {
        Verdict::new(code::DIVERGENT)
            .num("nodes", total)
            .word("what", what)
            .num("variants", variants)
    } else {
        Verdict::new(code::OK).num("nodes", total)
    }
}

/// How a record's `parent_hash` resolved while checking a numbered chain back from a header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Link {
    /// The chain starts here: number 0, or a `parent_hash` that is zero or the genesis anchor
    /// (the digest of the default header, which header 1 points to and no node stores).
    Genesis,
    /// The parent is at number-1 with a matching digest, and the digest index agrees.
    Ok,
    /// The last record checked; its own parent link was not followed.
    End,
    /// No record at number-1 in any tier.
    ParentMissing,
    /// A record exists at number-1 but its digest is not `parent_hash`.
    ParentDigestMismatch { found: String },
    /// The parent record is right, but the digest index maps `parent_hash` elsewhere.
    IndexMismatch { indexed: Option<u64> },
}

impl Link {
    /// Whether the link is a checked, intact one (or has nothing to check).
    pub fn is_intact(&self) -> bool {
        matches!(self, Self::Genesis | Self::Ok | Self::End)
    }
}

impl std::fmt::Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Genesis => f.write_str("genesis"),
            Self::Ok => f.write_str("ok"),
            Self::End => f.write_str("end of range"),
            Self::ParentMissing => f.write_str("PARENT MISSING"),
            Self::ParentDigestMismatch { found } => {
                write!(f, "PARENT DIGEST MISMATCH (found {found})")
            }
            Self::IndexMismatch { indexed: Some(n) } => {
                write!(f, "INDEX MISMATCH (indexed at {n})")
            }
            Self::IndexMismatch { indexed: None } => f.write_str("INDEX MISMATCH (not indexed)"),
        }
    }
}

/// One node's outcome of a chain check, folded into the verdict by [`chain_verdict`].
pub(crate) struct ChainOutcome {
    /// Every followed link was intact.
    pub ok: bool,
    /// The start record lies beyond this node's position.
    pub start_not_reached: bool,
    /// The start record is within this node's position but absent: a missing record, not a
    /// broken link.
    pub start_missing: bool,
    /// Parent links followed.
    pub links: usize,
    /// Number of the first broken hop met checking back, if any.
    pub first_break: Option<u64>,
}

/// Verdict of a chain check back from a start record: `nodes hops divergent broken missing
/// not_reached first`. `divergent` lists the numbers at which nodes hold different digests,
/// ascending. Nodes that have not reached the start make the verdict `PARTIAL`, not `BROKEN`: being
/// behind is not a broken chain.
pub(crate) fn chain_verdict(outcomes: &[ChainOutcome], divergent: &[u64]) -> Verdict {
    let total = outcomes.len();
    let not_reached = outcomes.iter().filter(|o| o.start_not_reached).count();
    let missing = outcomes.iter().filter(|o| o.start_missing).count();
    let broken =
        outcomes.iter().filter(|o| !o.ok && !o.start_not_reached && !o.start_missing).count();
    let hops = outcomes.iter().map(|o| o.links).max().unwrap_or(0);
    // the first broken link met checking back is the one at the highest number
    let first_break = outcomes
        .iter()
        .filter(|o| !o.ok && !o.start_not_reached)
        .filter_map(|o| o.first_break)
        .max();
    let code = if !divergent.is_empty() {
        code::DIVERGENT
    } else if not_reached == total {
        code::NOT_REACHED
    } else if missing == total {
        code::MISSING
    } else if broken > 0 {
        code::BROKEN
    } else if missing + not_reached > 0 {
        code::PARTIAL
    } else {
        code::OK
    };
    let verdict = Verdict::new(code)
        .num("nodes", total)
        .num("hops", hops)
        .count("divergent", divergent.len())
        .count("broken", broken)
        .count("missing", missing)
        .count("not_reached", not_reached);
    match code {
        code::DIVERGENT => verdict.opt("first", divergent.last().copied()),
        code::BROKEN => verdict.opt("first", first_break),
        _ => verdict,
    }
}

/// Output of one subcommand run.
// One report is built per run; the size gap between the variants does not matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub enum Report {
    Epoch(epoch::EpochReport),
    Epochs(epoch::EpochsReport),
    EpochCheck(epoch::EpochCheckReport),
    Header(header::HeaderReport),
    Cert(header::CertReport),
    GetBatch(batch::BatchReport),
    GetTx(batch::TxReport),
    HeaderCheck(header::HeaderCheckReport),
    Summary(summary::SummaryReport),
}

impl Report {
    /// The verdict, if the subcommand produces one (`summary` does not).
    pub fn verdict(&self) -> Option<&Verdict> {
        match self {
            Self::Epoch(r) => Some(&r.verdict),
            Self::Epochs(r) => Some(&r.verdict),
            Self::EpochCheck(r) => Some(&r.verdict),
            Self::Header(r) => Some(&r.verdict),
            Self::Cert(r) => Some(&r.verdict),
            Self::GetBatch(r) => Some(&r.verdict),
            Self::GetTx(r) => Some(&r.verdict),
            Self::HeaderCheck(r) => Some(&r.verdict),
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

    #[test]
    fn chain_verdict_orders_codes_and_names_the_first_break() {
        let ok = ChainOutcome {
            ok: true,
            start_not_reached: false,
            start_missing: false,
            links: 3,
            first_break: None,
        };
        let broken = ChainOutcome {
            ok: false,
            start_not_reached: false,
            start_missing: false,
            links: 1,
            first_break: Some(7),
        };
        let behind = ChainOutcome {
            ok: false,
            start_not_reached: true,
            start_missing: false,
            links: 0,
            first_break: None,
        };
        assert_eq!(chain_verdict(&[ok], &[]).to_string(), "OK nodes=1 hops=3");
        assert_eq!(
            chain_verdict(&[behind], &[]).to_string(),
            "NOT_REACHED nodes=1 hops=0 not_reached=1"
        );
        let ok = ChainOutcome {
            ok: true,
            start_not_reached: false,
            start_missing: false,
            links: 3,
            first_break: None,
        };
        let broken2 = ChainOutcome {
            ok: false,
            start_not_reached: false,
            start_missing: false,
            links: 2,
            first_break: Some(5),
        };
        assert_eq!(
            chain_verdict(&[ok, broken, broken2], &[]).to_string(),
            "BROKEN nodes=3 hops=3 broken=2 first=7"
        );
        let ok = ChainOutcome {
            ok: true,
            start_not_reached: false,
            start_missing: false,
            links: 3,
            first_break: None,
        };
        assert_eq!(
            chain_verdict(&[ok], &[4, 6]).to_string(),
            "DIVERGENT nodes=1 hops=3 divergent=2 first=6"
        );
        // a node that has not reached the start is behind, not broken
        let ok = ChainOutcome {
            ok: true,
            start_not_reached: false,
            start_missing: false,
            links: 3,
            first_break: None,
        };
        let behind = ChainOutcome {
            ok: false,
            start_not_reached: true,
            start_missing: false,
            links: 0,
            first_break: None,
        };
        assert_eq!(
            chain_verdict(&[ok, behind], &[]).to_string(),
            "PARTIAL nodes=2 hops=3 not_reached=1"
        );
    }

    #[test]
    fn chain_verdict_reports_a_missing_start_as_missing_not_broken() {
        let gone = || ChainOutcome {
            ok: false,
            start_not_reached: false,
            start_missing: true,
            links: 0,
            first_break: None,
        };
        assert_eq!(chain_verdict(&[gone()], &[]).to_string(), "MISSING nodes=1 hops=0 missing=1");
        let ok = ChainOutcome {
            ok: true,
            start_not_reached: false,
            start_missing: false,
            links: 2,
            first_break: None,
        };
        assert_eq!(
            chain_verdict(&[ok, gone()], &[]).to_string(),
            "PARTIAL nodes=2 hops=2 missing=1"
        );
    }

    #[test]
    fn absence_verdict_tells_not_found_from_missing() {
        use Lookup::*;
        assert_eq!(absence_verdict(&[Found, Found]), None);
        assert_eq!(
            absence_verdict(&[NotFound, NotFound]).unwrap().to_string(),
            "EMPTY nodes=2 not_found=2"
        );
        assert_eq!(
            absence_verdict(&[Missing, Missing]).unwrap().to_string(),
            "MISSING nodes=2 missing=2"
        );
        assert_eq!(
            absence_verdict(&[Found, Missing, NotReached]).unwrap().to_string(),
            "PARTIAL nodes=3 found=1 missing=1 not_reached=1"
        );
        assert_eq!(
            absence_verdict(&[NotReached]).unwrap().to_string(),
            "NOT_REACHED nodes=1 not_reached=1"
        );
        // nobody has it and one node is merely behind: still nothing anyone should have
        assert_eq!(
            absence_verdict(&[NotFound, NotReached]).unwrap().to_string(),
            "EMPTY nodes=2 not_reached=1 not_found=1"
        );
    }
}
