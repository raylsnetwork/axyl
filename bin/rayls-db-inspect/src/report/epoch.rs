// SPDX-License-Identifier: BUSL-1.1
//! Epoch-record reports: `epoch`, `epochs`, `chain-check`.

use super::{code, Verdict};
use crate::{
    node_db::{LiveStatus, NodeDb, Position, TableStatus, Tier},
    view::{b256, pubkey, signature, CheckpointView},
};
use rayls_infrastructure_storage::tables::EpochRecords;
use rayls_infrastructure_types::{BlsPublicKey, Epoch, EpochCertificate, EpochRecord, B256};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

// ---------------------------------------------------------------------------------------------
// epoch <N>
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct EpochReport {
    pub epoch: Epoch,
    pub nodes: Vec<EpochNodeView>,
    pub verdict: Verdict,
}

/// What one node holds for an epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EpochStatus {
    /// Record present, certificate present and valid.
    Certified,
    /// Record present; certificate missing or invalid.
    RecordOnly,
    /// No record although the node has closed this epoch: a real gap.
    Missing,
    /// The node has not closed this epoch yet, so no record is expected.
    NotReached,
    /// The epoch-record table was never created.
    TableAbsent,
}

impl std::fmt::Display for EpochStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Certified => "record+cert",
            Self::RecordOnly => "record-only",
            Self::Missing => "missing",
            Self::NotReached => "not-reached",
            Self::TableAbsent => "table-absent",
        })
    }
}

/// Result of checking a record's `parent_hash` against the previous record's digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum LinkCheck {
    /// Epoch 0 has no parent.
    Genesis,
    /// `parent_hash` equals the previous record's digest.
    Ok,
    /// The previous record is not on this node, so the link cannot be checked.
    PrevMissing,
    /// `parent_hash` differs from the previous record's digest.
    Broken { expected: String },
}

impl std::fmt::Display for LinkCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Genesis => f.write_str("genesis"),
            Self::Ok => f.write_str("ok"),
            Self::PrevMissing => f.write_str("prev-missing"),
            Self::Broken { expected } => write!(f, "BROKEN (expected {expected})"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct EpochNodeView {
    pub node: String,
    pub live: LiveStatus,
    pub status: EpochStatus,
    /// Where the node stands, to tell "not reached" from "missing".
    pub position: Position,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<RecordView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert: Option<EpochCertView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointView>,
}

#[derive(Debug, Serialize)]
pub struct RecordView {
    pub digest: String,
    /// What the digest index maps this record's digest to; should equal the epoch.
    pub index_epoch: Option<Epoch>,
    pub index_ok: bool,
    pub committee_size: usize,
    pub next_committee_size: usize,
    pub super_quorum: usize,
    pub parent_hash: String,
    pub parent_link: LinkCheck,
    /// Whether the previous record's `next_committee` equals this record's `committee`.
    pub committee_handoff_ok: Option<bool>,
    pub parent_consensus: String,
    /// Consensus number the boundary header resolves to on this node, and the tier it was in.
    pub parent_consensus_number: Option<u64>,
    pub parent_consensus_tier: Option<Tier>,
    pub parent_state_number: u64,
    pub parent_state_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committee: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_committee: Option<Vec<String>>,
}

/// The epoch certificate, checked one condition at a time (the node's `verify_with_cert`
/// collapses them all into one boolean).
#[derive(Debug, Serialize)]
pub struct EpochCertView {
    pub epoch_hash: String,
    /// `epoch_hash` equals the record digest.
    pub digest_match: bool,
    pub signers: Vec<u32>,
    pub signer_count: usize,
    /// `signer_count >= super_quorum`.
    pub quorum_ok: bool,
    /// The aggregate BLS signature verifies against the signers' committee keys.
    pub signature_ok: bool,
    pub valid: bool,
    pub signature: String,
}

impl EpochCertView {
    fn of(record: &EpochRecord, cert: &EpochCertificate) -> Self {
        let digest_match = record.digest() == cert.epoch_hash;
        let signers: Vec<u32> = cert.signed_authorities.iter().collect();
        let keys: Vec<BlsPublicKey> =
            signers.iter().filter_map(|i| record.committee.get(*i as usize).copied()).collect();
        let quorum_ok = keys.len() == signers.len() && signers.len() >= record.super_quorum();
        let signature_ok = !keys.is_empty() && cert.check_signatures(&keys);
        Self {
            epoch_hash: b256(&cert.epoch_hash),
            digest_match,
            signer_count: signers.len(),
            signers,
            quorum_ok,
            signature_ok,
            valid: digest_match && quorum_ok && signature_ok,
            signature: signature(&cert.signature),
        }
    }
}

fn record_view(
    node: &NodeDb,
    record: &EpochRecord,
    prev: Option<&EpochRecord>,
    verbose: bool,
) -> eyre::Result<RecordView> {
    let digest = record.digest();
    let index_epoch = node.epoch_by_digest(digest)?;
    let parent_link = if record.epoch == 0 {
        LinkCheck::Genesis
    } else {
        match prev {
            None => LinkCheck::PrevMissing,
            Some(p) if p.digest() == record.parent_hash => LinkCheck::Ok,
            Some(p) => LinkCheck::Broken { expected: b256(&p.digest()) },
        }
    };
    let committee_handoff_ok = prev.map(|p| p.next_committee == record.committee);
    let parent_consensus_number = node.header_number_by_digest(record.parent_consensus)?;
    let parent_consensus_tier = match parent_consensus_number {
        Some(n) => node.header(n)?.map(|(_, tier)| tier),
        None => None,
    };
    Ok(RecordView {
        digest: b256(&digest),
        index_epoch,
        index_ok: index_epoch == Some(record.epoch),
        committee_size: record.committee.len(),
        next_committee_size: record.next_committee.len(),
        super_quorum: record.super_quorum(),
        parent_hash: b256(&record.parent_hash),
        parent_link,
        committee_handoff_ok,
        parent_consensus: b256(&record.parent_consensus),
        parent_consensus_number,
        parent_consensus_tier,
        parent_state_number: record.parent_state.number,
        parent_state_hash: b256(&record.parent_state.hash),
        committee: verbose.then(|| record.committee.iter().map(pubkey).collect()),
        next_committee: verbose.then(|| record.next_committee.iter().map(pubkey).collect()),
    })
}

pub fn epoch(nodes: &[NodeDb], epoch: Epoch, verbose: bool) -> eyre::Result<EpochReport> {
    let mut views = Vec::with_capacity(nodes.len());
    let mut digests: BTreeSet<B256> = BTreeSet::new();

    for node in nodes {
        let position = node.position()?;
        let mut view = EpochNodeView {
            node: node.label.clone(),
            live: node.live,
            status: if position.has_closed_epoch(epoch) {
                EpochStatus::Missing
            } else {
                EpochStatus::NotReached
            },
            position,
            record: None,
            cert: None,
            checkpoint: node.checkpoint(epoch)?.as_ref().map(CheckpointView::of),
        };
        if node.table_status::<EpochRecords>()? == TableStatus::Absent {
            view.status = EpochStatus::TableAbsent;
            views.push(view);
            continue;
        }
        if let Some((record, cert)) = node.epoch(epoch)? {
            digests.insert(record.digest());
            let prev = if epoch == 0 { None } else { node.epoch(epoch - 1)?.map(|(r, _)| r) };
            view.record = Some(record_view(node, &record, prev.as_ref(), verbose)?);
            let cert = cert.map(|c| EpochCertView::of(&record, &c));
            view.status = match &cert {
                Some(c) if c.valid => EpochStatus::Certified,
                _ => EpochStatus::RecordOnly,
            };
            view.cert = cert;
        }
        views.push(view);
    }

    let verdict = epoch_verdict(epoch, &views, digests.len());
    Ok(EpochReport { epoch, nodes: views, verdict })
}

fn epoch_verdict(epoch: Epoch, views: &[EpochNodeView], distinct_digests: usize) -> Verdict {
    let total = views.len();
    let count = |st: EpochStatus| views.iter().filter(|v| v.status == st).count();
    let certified = count(EpochStatus::Certified);
    // epoch 0 is written unsigned, so record-only is its complete state
    let genesis = if epoch == 0 { count(EpochStatus::RecordOnly) } else { 0 };
    let record_only = count(EpochStatus::RecordOnly) - genesis;
    let not_reached = count(EpochStatus::NotReached);
    let missing = count(EpochStatus::Missing) + count(EpochStatus::TableAbsent);

    let code = if distinct_digests > 1 {
        code::DIVERGENT
    } else if not_reached == total {
        code::NOT_REACHED
    } else if missing == total {
        code::MISSING
    } else if certified + genesis == total {
        code::OK
    } else {
        code::PARTIAL
    };
    Verdict::new(code)
        .num("nodes", total)
        .count("variants", if distinct_digests > 1 { distinct_digests } else { 0 })
        .count("certified", certified)
        .count("genesis", genesis)
        .count("record_only", record_only)
        .count("missing", missing)
        .count("not_reached", not_reached)
}

/// `epoch 1, record 0`: the epoch a node is in and its latest epoch record.
pub fn describe_position(p: Position) -> String {
    let current =
        p.current_epoch.map(|e| format!("epoch {e}")).unwrap_or_else(|| "no headers".to_owned());
    let latest = p
        .latest_epoch_record
        .map(|e| format!("record {e}"))
        .unwrap_or_else(|| "no records".to_owned());
    format!("{current}, {latest}")
}

// ---------------------------------------------------------------------------------------------
// epochs <FROM> <TO>
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct EpochsReport {
    pub from: Epoch,
    pub to: Epoch,
    pub nodes: Vec<String>,
    pub rows: Vec<EpochsRow>,
    pub verdict: Verdict,
}

/// A cell of the epoch matrix: presence only, no signature verification (see `chain-check`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Cell {
    RecordAndCert,
    RecordOnly,
    /// No record although the node has closed the epoch.
    Missing,
    /// The node has not closed the epoch yet.
    NotReached,
    TableAbsent,
}

impl Cell {
    /// Two-character matrix glyph.
    pub fn glyph(self) -> &'static str {
        match self {
            Self::RecordAndCert => "RC",
            Self::RecordOnly => "R-",
            Self::Missing => "--",
            Self::NotReached => "..",
            Self::TableAbsent => "??",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct EpochsRow {
    pub epoch: Epoch,
    pub cells: Vec<Cell>,
    /// `ok`, `partial`, `missing`, `not-reached` or `divergent`.
    pub status: &'static str,
}

/// `range` is `Some((from, to))` or `None` for every epoch any node has a record for.
pub fn epochs(nodes: &[NodeDb], range: Option<(Epoch, Epoch)>) -> eyre::Result<EpochsReport> {
    let labels: Vec<String> = nodes.iter().map(|n| n.label.clone()).collect();
    let (from, to) = match range {
        Some((from, to)) => {
            if from > to {
                return Err(eyre::eyre!("epochs: FROM ({from}) is greater than TO ({to})"));
            }
            (from, to)
        }
        None => {
            let mut all = BTreeSet::new();
            for node in nodes {
                all.extend(node.epoch_numbers()?);
            }
            match (all.first(), all.last()) {
                (Some(&first), Some(&last)) => (first, last),
                _ => {
                    return Ok(EpochsReport {
                        from: 0,
                        to: 0,
                        nodes: labels,
                        rows: Vec::new(),
                        verdict: Verdict::new(code::EMPTY).num("epochs", 0),
                    })
                }
            }
        }
    };

    let table_absent: Vec<bool> = nodes
        .iter()
        .map(|n| n.table_status::<EpochRecords>().map(|s| s == TableStatus::Absent))
        .collect::<eyre::Result<_>>()?;
    let positions: Vec<Position> =
        nodes.iter().map(NodeDb::position).collect::<eyre::Result<_>>()?;

    let mut rows = Vec::new();
    for epoch in from..=to {
        let mut cells = Vec::with_capacity(nodes.len());
        let mut digests = BTreeSet::new();
        for ((node, absent), position) in nodes.iter().zip(&table_absent).zip(&positions) {
            if *absent {
                cells.push(Cell::TableAbsent);
                continue;
            }
            cells.push(match node.epoch(epoch)? {
                Some((record, cert)) => {
                    digests.insert(record.digest());
                    if cert.is_some() {
                        Cell::RecordAndCert
                    } else {
                        Cell::RecordOnly
                    }
                }
                None if position.has_closed_epoch(epoch) => Cell::Missing,
                None => Cell::NotReached,
            });
        }
        let status = row_status(epoch, &cells, digests.len());
        rows.push(EpochsRow { epoch, cells, status });
    }

    let tally = |st: &str| rows.iter().filter(|r| r.status == st).count();
    let (ok, partial, missing, divergent, not_reached) =
        (tally("ok"), tally("partial"), tally("missing"), tally("divergent"), tally("not-reached"));
    let first = rows.iter().find(|r| matches!(r.status, "partial" | "missing" | "divergent"));
    let nr_range = {
        let nr: Vec<Epoch> =
            rows.iter().filter(|r| r.status == "not-reached").map(|r| r.epoch).collect();
        nr.first().zip(nr.last()).map(|(a, b)| (*a, *b))
    };
    let code = if not_reached == rows.len() {
        code::NOT_REACHED
    } else if divergent > 0 {
        code::DIVERGENT
    } else if missing > 0 {
        code::MISSING
    } else if partial > 0 {
        code::PARTIAL
    } else {
        code::OK
    };
    let mut verdict = Verdict::new(code)
        .num("epochs", rows.len())
        .count("ok", ok)
        .count("partial", partial)
        .count("missing", missing)
        .count("divergent", divergent);
    if let Some((a, b)) = nr_range {
        verdict = verdict.range("not_reached", a, b);
    }
    verdict = verdict.opt("first", first.map(|r| r.epoch));
    Ok(EpochsReport { from, to, nodes: labels, rows, verdict })
}

/// Row word for the matrix: `ok`, `partial`, `missing`, `not-reached`, `divergent`.
fn row_status(epoch: Epoch, cells: &[Cell], distinct_digests: usize) -> &'static str {
    if distinct_digests > 1 {
        return "divergent";
    }
    let with_record =
        cells.iter().filter(|c| matches!(c, Cell::RecordAndCert | Cell::RecordOnly)).count();
    if with_record == 0 {
        if cells.iter().all(|c| matches!(c, Cell::NotReached | Cell::TableAbsent)) {
            return "not-reached";
        }
        return "missing";
    }
    if cells.iter().all(|c| *c == Cell::RecordAndCert) {
        return "ok";
    }
    // epoch 0 is written unsigned, so record-only is complete there
    if epoch == 0 && cells.iter().all(|c| matches!(c, Cell::RecordAndCert | Cell::RecordOnly)) {
        return "ok";
    }
    "partial"
}

// ---------------------------------------------------------------------------------------------
// chain-check
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct ChainCheckReport {
    pub nodes: Vec<ChainCheckNodeView>,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BrokenLink {
    pub epoch: Epoch,
    pub parent_hash: String,
    pub expected: String,
}

#[derive(Debug, Serialize)]
pub struct ChainCheckNodeView {
    pub node: String,
    pub live: LiveStatus,
    /// Range actually walked (defaults to the node's first and last record).
    pub from: Option<Epoch>,
    pub to: Option<Epoch>,
    pub checked: usize,
    pub certified: usize,
    /// Epochs in range with no record.
    pub gaps: Vec<Epoch>,
    /// Records whose `parent_hash` does not match the previous record's digest.
    pub broken_links: Vec<BrokenLink>,
    /// Records with no certificate (epoch 0 excluded: it is an unsigned dummy).
    pub uncertified: Vec<Epoch>,
    /// Records whose certificate is present but does not verify.
    pub invalid_certs: Vec<Epoch>,
    /// Records where the previous record's `next_committee` differs from this `committee`.
    pub committee_handoff_mismatch: Vec<Epoch>,
    pub ok: bool,
    /// Set when the requested range was adjusted, e.g. `--to` beyond the latest record.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

pub fn chain_check(
    nodes: &[NodeDb],
    from: Option<Epoch>,
    to: Option<Epoch>,
) -> eyre::Result<ChainCheckReport> {
    let mut views = Vec::with_capacity(nodes.len());
    // digest per epoch per node, to detect cross-node divergence
    let mut digests: BTreeMap<Epoch, BTreeSet<B256>> = BTreeMap::new();

    for node in nodes {
        let keys = node.epoch_numbers()?;
        let mut view = ChainCheckNodeView {
            node: node.label.clone(),
            live: node.live,
            from: None,
            to: None,
            checked: 0,
            certified: 0,
            gaps: Vec::new(),
            broken_links: Vec::new(),
            uncertified: Vec::new(),
            invalid_certs: Vec::new(),
            committee_handoff_mismatch: Vec::new(),
            ok: false,
            note: None,
        };
        let (Some(&first), Some(&last)) = (keys.first(), keys.last()) else {
            view.note = Some("no epoch records".to_owned());
            views.push(view);
            continue;
        };
        // an explicit inverted range is an argument error; everything else is reported
        if let (Some(f), Some(t)) = (from, to) {
            if f > t {
                return Err(eyre::eyre!("chain-check: --from ({f}) is greater than --to ({t})"));
            }
        }
        let from = from.unwrap_or(first);
        if from > last {
            view.note = Some(format!("--from {from} > latest record {last}, nothing to check"));
            views.push(view);
            continue;
        }
        let mut to = to.unwrap_or(last);
        if to > last {
            view.note = Some(format!("--to {to} > latest record {last}, checked to {last}"));
            to = last;
        }
        view.from = Some(from);
        view.to = Some(to);

        let mut prev: Option<EpochRecord> =
            if from == 0 { None } else { node.epoch(from - 1)?.map(|(r, _)| r) };
        for epoch in from..=to {
            let Some((record, cert)) = node.epoch(epoch)? else {
                view.gaps.push(epoch);
                prev = None;
                continue;
            };
            view.checked += 1;
            digests.entry(epoch).or_default().insert(record.digest());
            match cert {
                Some(cert) if record.verify_with_cert(&cert) => view.certified += 1,
                Some(_) => view.invalid_certs.push(epoch),
                None if epoch == 0 => {}
                None => view.uncertified.push(epoch),
            }
            if epoch > 0 {
                if let Some(p) = &prev {
                    if p.digest() != record.parent_hash {
                        view.broken_links.push(BrokenLink {
                            epoch,
                            parent_hash: b256(&record.parent_hash),
                            expected: b256(&p.digest()),
                        });
                    }
                    if p.next_committee != record.committee {
                        view.committee_handoff_mismatch.push(epoch);
                    }
                }
            }
            prev = Some(record);
        }
        view.ok = view.gaps.is_empty()
            && view.broken_links.is_empty()
            && view.uncertified.is_empty()
            && view.invalid_certs.is_empty()
            && view.committee_handoff_mismatch.is_empty()
            && view.checked > 0;
        views.push(view);
    }

    let divergent: Vec<Epoch> =
        digests.iter().filter(|(_, d)| d.len() > 1).map(|(e, _)| *e).collect();
    let checked = views.iter().map(|v| v.checked).max().unwrap_or(0);
    let sum = |f: fn(&ChainCheckNodeView) -> usize| views.iter().map(f).sum::<usize>();
    let gaps = sum(|v| v.gaps.len());
    let broken = sum(|v| v.broken_links.len());
    let uncertified = sum(|v| v.uncertified.len());
    let invalid = sum(|v| v.invalid_certs.len());
    let handoff = sum(|v| v.committee_handoff_mismatch.len());
    let first_issue = views
        .iter()
        .flat_map(|v| {
            v.gaps
                .iter()
                .chain(v.broken_links.iter().map(|b| &b.epoch))
                .chain(&v.uncertified)
                .chain(&v.invalid_certs)
                .chain(&v.committee_handoff_mismatch)
        })
        .min()
        .copied();

    let code = if !divergent.is_empty() {
        code::DIVERGENT
    } else if views.iter().all(|v| v.checked == 0) {
        code::EMPTY
    } else if gaps + broken + uncertified + invalid + handoff > 0 {
        code::BROKEN
    } else {
        code::OK
    };
    let mut verdict = Verdict::new(code)
        .num("checked", checked)
        .count("divergent", divergent.len())
        .count("gaps", gaps)
        .count("broken", broken)
        .count("uncertified", uncertified)
        .count("invalid", invalid)
        .count("handoff", handoff);
    verdict = match code {
        code::DIVERGENT => verdict.opt("first", divergent.first().copied()),
        code::BROKEN => verdict.opt("first", first_issue),
        _ => verdict,
    };
    Ok(ChainCheckReport { nodes: views, verdict })
}
