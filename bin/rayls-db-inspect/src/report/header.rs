// SPDX-License-Identifier: BUSL-1.1
//! Consensus-header reports: `header`, `cert`, `walk header`.

use super::{code, Verdict};
use crate::{
    node_db::{LiveStatus, NodeDb, Position, Tier},
    view::{authority, b256, CertificateSummary, CertificateView},
};
use rayls_infrastructure_types::{ConsensusHeader, B256};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Outcome of looking up a consensus header number on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Lookup {
    /// A header was found (in some tier).
    Found,
    /// The node's tip is at or past this number, but no header is stored: a real gap.
    Missing,
    /// The node's tip has not reached this number yet.
    NotReached,
}

/// Human description of a lookup that found nothing.
pub fn describe_absent(lookup: Lookup, tip: Option<u64>) -> String {
    match (lookup, tip) {
        (Lookup::NotReached, Some(tip)) => format!("not reached (tip {tip})"),
        (Lookup::NotReached, None) => "not reached (no headers)".to_owned(),
        _ => "missing".to_owned(),
    }
}

fn classify(found: bool, position: &Position, number: u64) -> Lookup {
    if found {
        Lookup::Found
    } else if position.has_reached_header(number) {
        Lookup::Missing
    } else {
        Lookup::NotReached
    }
}

/// Verdict for "one object looked up by consensus number on every node", before any content
/// comparison: covers not reached / missing everywhere / missing somewhere. `None` when every
/// node found it.
fn absence_verdict(lookups: &[Lookup]) -> Option<Verdict> {
    let total = lookups.len();
    let found = lookups.iter().filter(|l| **l == Lookup::Found).count();
    let not_reached = lookups.iter().filter(|l| **l == Lookup::NotReached).count();
    let missing = lookups.iter().filter(|l| **l == Lookup::Missing).count();
    if found == total {
        return None;
    }
    let code = if not_reached == total {
        code::NOT_REACHED
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
            .count("not_reached", not_reached),
    )
}

/// Verdict when every node holds content: agreement or divergence on `what`.
fn content_verdict(total: usize, variants: usize, what: &'static str) -> Verdict {
    if variants > 1 {
        Verdict::new(code::DIVERGENT)
            .num("nodes", total)
            .word("what", what)
            .num("variants", variants)
    } else {
        Verdict::new(code::OK).num("nodes", total)
    }
}

// ---------------------------------------------------------------------------------------------
// header <N>
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct HeaderReport {
    pub number: u64,
    pub nodes: Vec<HeaderNodeView>,
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub struct HeaderNodeView {
    pub node: String,
    pub live: LiveStatus,
    pub lookup: Lookup,
    /// The node's latest canonical consensus number.
    pub tip: Option<u64>,
    pub tier: Option<Tier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<HeaderView>,
}

/// Presence of a batch referenced by the sub-dag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BatchPresence {
    pub digest: String,
    pub tier: Option<Tier>,
}

#[derive(Debug, Serialize)]
pub struct HeaderView {
    pub number: u64,
    pub digest: String,
    pub parent_hash: String,
    pub extra: String,
    pub leader: CertificateSummary,
    pub certificate_count: usize,
    pub batch_count: usize,
    pub commit_timestamp: u64,
    pub reputation_final_of_schedule: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificates: Option<Vec<CertificateView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batches: Option<Vec<BatchPresence>>,
    /// `(authority, score)` pairs, highest first.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reputation: Option<Vec<(String, u64)>>,
}

impl HeaderView {
    fn of(node: &NodeDb, header: &ConsensusHeader, verbose: bool) -> eyre::Result<Self> {
        let sub_dag = &header.sub_dag;
        let batch_digests: Vec<B256> = sub_dag
            .certificates
            .iter()
            .flat_map(|c| c.header().payload().keys().copied())
            .collect();
        let batches = if verbose {
            let mut out = Vec::with_capacity(batch_digests.len());
            for d in &batch_digests {
                out.push(BatchPresence { digest: b256(d), tier: node.batch_tier(*d)? });
            }
            Some(out)
        } else {
            None
        };
        Ok(Self {
            number: header.number,
            digest: b256(&header.digest()),
            parent_hash: b256(&header.parent_hash),
            extra: b256(&header.extra),
            leader: CertificateSummary::of(&sub_dag.leader),
            certificate_count: sub_dag.certificates.len(),
            batch_count: batch_digests.len(),
            commit_timestamp: sub_dag.commit_timestamp(),
            reputation_final_of_schedule: sub_dag.reputation_score.final_of_schedule,
            certificates: verbose.then(|| {
                sub_dag.certificates.iter().map(|c| CertificateView::of(c, false)).collect()
            }),
            batches,
            reputation: verbose.then(|| {
                sub_dag
                    .reputation_score
                    .authorities_by_score_desc()
                    .into_iter()
                    .map(|(id, score)| (authority(&id), score))
                    .collect()
            }),
        })
    }
}

pub fn header(nodes: &[NodeDb], number: u64, verbose: bool) -> eyre::Result<HeaderReport> {
    let mut views = Vec::with_capacity(nodes.len());
    let mut digests = BTreeSet::new();
    for node in nodes {
        let position = node.position()?;
        let found = node.header(number)?;
        let lookup = classify(found.is_some(), &position, number);
        let (tier, header) = match found {
            Some((header, tier)) => {
                digests.insert(header.digest());
                (Some(tier), Some(HeaderView::of(node, &header, verbose)?))
            }
            None => (None, None),
        };
        views.push(HeaderNodeView {
            node: node.label.clone(),
            live: node.live,
            lookup,
            tip: position.consensus_tip,
            tier,
            header,
        });
    }
    let lookups: Vec<Lookup> = views.iter().map(|v| v.lookup).collect();
    let verdict = absence_verdict(&lookups)
        .unwrap_or_else(|| content_verdict(views.len(), digests.len(), "header"));
    Ok(HeaderReport { number, nodes: views, verdict })
}

// ---------------------------------------------------------------------------------------------
// cert <N>
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct CertReport {
    pub number: u64,
    pub nodes: Vec<CertNodeView>,
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub struct CertNodeView {
    pub node: String,
    pub live: LiveStatus,
    pub lookup: Lookup,
    pub tip: Option<u64>,
    pub tier: Option<Tier>,
    /// Digest of the consensus header the leader certificate was taken from.
    pub header_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader: Option<CertificateView>,
}

pub fn cert(nodes: &[NodeDb], number: u64, verbose: bool) -> eyre::Result<CertReport> {
    let mut views = Vec::with_capacity(nodes.len());
    let mut header_digests = BTreeSet::new();
    let mut cert_digests = BTreeSet::new();
    let mut identities: BTreeSet<(Vec<u32>, Option<String>)> = BTreeSet::new();
    for node in nodes {
        let position = node.position()?;
        let found = node.header(number)?;
        let lookup = classify(found.is_some(), &position, number);
        let (tier, header_digest, leader) = match found {
            Some((header, tier)) => {
                header_digests.insert(header.digest());
                let leader = CertificateView::of(&header.sub_dag.leader, verbose);
                cert_digests.insert(leader.summary.digest.clone());
                let (signers, sig) = leader.signature_identity();
                identities.insert((signers.to_vec(), sig.map(str::to_owned)));
                (Some(tier), Some(b256(&header.digest())), Some(leader))
            }
            None => (None, None, None),
        };
        views.push(CertNodeView {
            node: node.label.clone(),
            live: node.live,
            lookup,
            tip: position.consensus_tip,
            tier,
            header_digest,
            leader,
        });
    }

    // no header means no leader certificate either
    let lookups: Vec<Lookup> = views.iter().map(|v| v.lookup).collect();
    let verdict = absence_verdict(&lookups).unwrap_or_else(|| {
        if header_digests.len() > 1 {
            content_verdict(views.len(), header_digests.len(), "header")
        } else if cert_digests.len() > 1 {
            content_verdict(views.len(), cert_digests.len(), "leader")
        } else {
            content_verdict(views.len(), identities.len(), "signers")
        }
    });
    Ok(CertReport { number, nodes: views, verdict })
}

// ---------------------------------------------------------------------------------------------
// walk header <N> --back K
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct WalkReport {
    pub start: u64,
    pub back: u64,
    pub nodes: Vec<WalkNodeView>,
    pub verdict: Verdict,
}

/// How a header's `parent_hash` resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Link {
    /// `parent_hash` is the zero hash (or number is 0): the chain starts here.
    Genesis,
    /// The parent is at number-1 with a matching digest, and the digest index agrees.
    Ok,
    /// No header at number-1 in any tier.
    ParentMissing,
    /// A header exists at number-1 but its digest is not `parent_hash`.
    ParentDigestMismatch { found: String },
    /// The parent header is right, but the digest index maps `parent_hash` elsewhere.
    IndexMismatch { indexed: Option<u64> },
}

impl std::fmt::Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Genesis => f.write_str("genesis"),
            Self::Ok => f.write_str("ok"),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Hop {
    pub number: u64,
    pub digest: String,
    pub parent_hash: String,
    pub tier: Tier,
    pub leader_round: u32,
    pub leader_epoch: u32,
    pub link: Link,
}

#[derive(Debug, Serialize)]
pub struct WalkNodeView {
    pub node: String,
    pub live: LiveStatus,
    pub tip: Option<u64>,
    pub hops: Vec<Hop>,
    pub ok: bool,
    /// The start header lies beyond this node's tip.
    pub start_not_reached: bool,
    /// Why the walk stopped.
    pub stopped: String,
}

pub fn walk(nodes: &[NodeDb], start: u64, back: u64) -> eyre::Result<WalkReport> {
    let mut views = Vec::with_capacity(nodes.len());
    let mut digests_by_number: BTreeMap<u64, BTreeSet<B256>> = BTreeMap::new();

    for node in nodes {
        let position = node.position()?;
        let mut view = WalkNodeView {
            node: node.label.clone(),
            live: node.live,
            tip: position.consensus_tip,
            hops: Vec::new(),
            ok: false,
            start_not_reached: false,
            stopped: String::new(),
        };
        let Some((mut current, mut tier)) = node.header(start)? else {
            if position.has_reached_header(start) {
                view.stopped = format!("start header {start} missing");
            } else {
                view.start_not_reached = true;
                view.stopped = format!(
                    "start header {start} {}",
                    describe_absent(Lookup::NotReached, position.consensus_tip)
                );
            }
            views.push(view);
            continue;
        };
        let mut ok = true;
        for _ in 0..=back {
            digests_by_number.entry(current.number).or_default().insert(current.digest());
            let mut hop = Hop {
                number: current.number,
                digest: b256(&current.digest()),
                parent_hash: b256(&current.parent_hash),
                tier,
                leader_round: current.sub_dag.leader_round(),
                leader_epoch: current.sub_dag.leader_epoch(),
                link: Link::Genesis,
            };
            if current.number == 0 || current.parent_hash == B256::ZERO {
                view.hops.push(hop);
                view.stopped = "reached genesis".to_owned();
                break;
            }
            if view.hops.len() as u64 == back {
                hop.link = Link::Ok;
                view.hops.push(hop);
                view.stopped = format!("followed {back} parent(s)");
                break;
            }
            let expected = current.number - 1;
            let parent = node.header(expected)?;
            let indexed = node.header_number_by_digest(current.parent_hash)?;
            let next = match parent {
                None => {
                    hop.link = Link::ParentMissing;
                    None
                }
                Some((p, _)) if p.digest() != current.parent_hash => {
                    hop.link = Link::ParentDigestMismatch { found: b256(&p.digest()) };
                    None
                }
                Some((p, t)) if indexed != Some(expected) => {
                    hop.link = Link::IndexMismatch { indexed };
                    Some((p, t))
                }
                Some((p, t)) => {
                    hop.link = Link::Ok;
                    Some((p, t))
                }
            };
            let broken = !matches!(hop.link, Link::Ok);
            view.hops.push(hop);
            match next {
                Some((p, t)) => {
                    current = p;
                    tier = t;
                    if broken {
                        ok = false;
                    }
                }
                None => {
                    ok = false;
                    view.stopped = format!("broken link below header {}", current.number);
                    break;
                }
            }
        }
        view.ok = ok;
        views.push(view);
    }

    let divergent: Vec<u64> =
        digests_by_number.iter().filter(|(_, d)| d.len() > 1).map(|(n, _)| *n).collect();
    let total = views.len();
    let not_reached = views.iter().filter(|v| v.start_not_reached).count();
    let broken = views.iter().filter(|v| !v.ok && !v.start_not_reached).count();
    let hops = views.iter().map(|v| v.hops.len().saturating_sub(1)).max().unwrap_or(0);
    // the first broken link met walking back is the one at the highest number
    let first_break = views
        .iter()
        .filter(|v| !v.ok && !v.start_not_reached)
        .filter_map(|v| v.hops.iter().find(|h| !matches!(h.link, Link::Ok | Link::Genesis)))
        .map(|h| h.number)
        .max();
    let code = if !divergent.is_empty() {
        code::DIVERGENT
    } else if not_reached == total {
        code::NOT_REACHED
    } else if broken + not_reached > 0 {
        code::BROKEN
    } else {
        code::OK
    };
    let mut verdict = Verdict::new(code)
        .num("nodes", total)
        .num("hops", hops)
        .count("divergent", divergent.len())
        .count("broken", broken)
        .count("not_reached", not_reached);
    verdict = match code {
        code::DIVERGENT => verdict.opt("first", divergent.last().copied()),
        code::BROKEN => verdict.opt("first", first_break),
        _ => verdict,
    };
    Ok(WalkReport { start, back, nodes: views, verdict })
}
