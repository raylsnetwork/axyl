// SPDX-License-Identifier: BUSL-1.1
//! Worker-batch reports: `get-batch`, `get-tx`.
//!
//! Batches are keyed by digest, so there is no tip to compare against directly. A node is expected
//! to hold a batch once it has executed the consensus header that committed it (execution fetches
//! every committed batch); before that, not holding it is normal: a worker stops distributing a
//! batch once a quorum has it, and observers fetch lazily. So absence is judged against the
//! committing header: a node whose tip is below it has "not reached" the batch, one at or past it
//! is "missing" it, and a batch no node has committed yet is not expected anywhere. A digest no
//! node holds at all is "not found", not missing.

use super::{absence_verdict, code, recode, Lookup, Verdict};
use crate::{
    node_db::{BatchLookup, LiveStatus, NodeDb, Position, ScanStats, Tier},
    view::{b256, TransactionView},
};
use rayls_infrastructure_storage::cold::ColdLocation;
use rayls_infrastructure_types::{keccak256, Batch, BlockHash, Epoch, WorkerId, B256};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Whether, and where, a consensus header committing a batch was found on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Commit {
    /// A canonical header of the batch's epoch (hot or cold) lists the batch in its sub-dag.
    Committed { number: u64, tier: Tier },
    /// A header in the verified-but-unprocessed cache lists it: consensus committed the batch,
    /// this node has not executed that header yet.
    Verified { number: u64 },
    /// No stored header of the batch's epoch lists it: sealed, not (yet) committed.
    NotCommitted,
}

impl Commit {
    fn of(found: Option<(u64, Tier)>) -> Self {
        match found {
            Some((number, Tier::Cache)) => Self::Verified { number },
            Some((number, tier)) => Self::Committed { number, tier },
            None => Self::NotCommitted,
        }
    }

    /// The header number consensus committed the batch at, if one was found.
    pub fn number(&self) -> Option<u64> {
        match self {
            Self::Committed { number, .. } | Self::Verified { number } => Some(*number),
            Self::NotCommitted => None,
        }
    }
}

impl std::fmt::Display for Commit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Committed { number, tier } => write!(f, "header {number} ({tier})"),
            Self::Verified { number } => write!(f, "header {number} (cache, not processed)"),
            Self::NotCommitted => f.write_str("not committed"),
        }
    }
}

/// Human description of a node that does not hold a batch. `committed_at` is the header the
/// absence was judged against, if any node saw the batch committed.
pub fn describe_absent_batch(
    lookup: Lookup,
    tip: Option<u64>,
    committed_at: Option<u64>,
) -> String {
    match (lookup, committed_at, tip) {
        (Lookup::NotReached, Some(n), Some(tip)) => {
            format!("not reached (tip {tip}, committed at header {n})")
        }
        (Lookup::NotReached, Some(n), None) => {
            format!("not reached (no headers, committed at header {n})")
        }
        (Lookup::NotReached, None, _) => "not reached (not committed on any node)".to_owned(),
        (Lookup::Missing, ..) => "missing".to_owned(),
        _ => "not found".to_owned(),
    }
}

/// Classifies a node that does not hold the batch. `committed_at` is the lowest header number any
/// node saw committing it; `held_elsewhere` says whether any node holds it at all.
fn classify_absent(position: &Position, committed_at: Option<u64>, held_elsewhere: bool) -> Lookup {
    match committed_at {
        Some(number) if position.has_reached_header(number) => Lookup::Missing,
        Some(_) => Lookup::NotReached,
        None if held_elsewhere => Lookup::NotReached,
        None => Lookup::NotFound,
    }
}

fn commit_of(node: &NodeDb, digest: BlockHash, epoch: Epoch) -> eyre::Result<Commit> {
    Ok(Commit::of(node.header_committing_batch(digest, epoch)?))
}

// ---------------------------------------------------------------------------------------------
// get-batch <DIGEST>
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct BatchReport {
    pub digest: String,
    /// Lowest header number any node saw committing the batch; what absences are judged against.
    pub committed_at: Option<u64>,
    pub nodes: Vec<BatchNodeView>,
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub struct BatchNodeView {
    pub node: String,
    pub live: LiveStatus,
    pub lookup: Lookup,
    /// The node's consensus tip, read against `committed_at`.
    pub tip: Option<u64>,
    pub tier: Option<Tier>,
    /// A cold index entry whose jar row is gone; `lookup` is `missing` and the verdict `BROKEN`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dangling: Option<ColdLocation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchView>,
}

#[derive(Debug, Serialize)]
pub struct BatchView {
    /// Digest recomputed from the stored bytes; must equal the key the batch is stored under.
    pub computed_digest: String,
    pub digest_ok: bool,
    pub epoch: Epoch,
    pub worker_id: WorkerId,
    pub seq: u64,
    pub beneficiary: String,
    pub base_fee_per_gas: u64,
    pub transaction_count: usize,
    /// Encoded size of all transactions together, in bytes.
    pub transaction_bytes: usize,
    /// The consensus header whose sub-dag commits this batch (`-v`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_in: Option<Commit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<TransactionView>>,
}

impl BatchView {
    fn of(digest: BlockHash, batch: &Batch, committed_in: Option<Commit>, verbose: bool) -> Self {
        let computed = batch.digest();
        Self {
            computed_digest: b256(&computed),
            digest_ok: computed == digest,
            epoch: batch.epoch,
            worker_id: batch.worker_id,
            seq: batch.seq,
            beneficiary: batch.beneficiary.to_string(),
            base_fee_per_gas: batch.base_fee_per_gas,
            transaction_count: batch.transactions.len(),
            transaction_bytes: batch.transactions.iter().map(|t| t.len()).sum(),
            committed_in,
            transactions: verbose.then(|| {
                batch
                    .transactions
                    .iter()
                    .enumerate()
                    .map(|(i, t)| TransactionView::of(i, t))
                    .collect()
            }),
        }
    }
}

pub fn get_batch(nodes: &[NodeDb], digest: B256, verbose: bool) -> eyre::Result<BatchReport> {
    let mut hits = Vec::with_capacity(nodes.len());
    for node in nodes {
        hits.push((node.position()?, node.batch(digest)?));
    }
    let held = hits.iter().any(|(_, hit)| matches!(hit, BatchLookup::Found(..)));
    let absent = hits.iter().any(|(_, hit)| !matches!(hit, BatchLookup::Found(..)));

    // The committing header is what absences are judged against: the lowest number any holding
    // node saw. It is looked up on every node that holds the batch whenever some node lacks it
    // or under -v, and not at all when every node holds it (the lookup scans the hot header
    // tables).
    let mut commits: Vec<Option<Commit>> = vec![None; nodes.len()];
    if verbose || absent {
        for (i, (node, (_, hit))) in nodes.iter().zip(&hits).enumerate() {
            let BatchLookup::Found(batch, _) = hit else { continue };
            commits[i] = Some(commit_of(node, digest, batch.epoch)?);
        }
    }
    let committed_at = commits.iter().flatten().filter_map(Commit::number).min();

    let mut views = Vec::with_capacity(nodes.len());
    for ((node, (position, hit)), commit) in nodes.iter().zip(hits).zip(commits) {
        let mut view = BatchNodeView {
            node: node.label.clone(),
            live: node.live,
            lookup: Lookup::Found,
            tip: position.consensus_tip,
            tier: None,
            dangling: None,
            batch: None,
        };
        match hit {
            BatchLookup::Found(batch, tier) => {
                view.tier = Some(tier);
                view.batch = Some(BatchView::of(
                    digest,
                    &batch,
                    if verbose { commit } else { None },
                    verbose,
                ));
            }
            BatchLookup::Dangling(location) => {
                view.lookup = Lookup::Missing;
                view.dangling = Some(location);
            }
            BatchLookup::Absent => view.lookup = classify_absent(&position, committed_at, held),
        }
        views.push(view);
    }

    let lookups: Vec<Lookup> = views.iter().map(|v| v.lookup).collect();
    let bad_digest =
        views.iter().filter(|v| v.batch.as_ref().is_some_and(|b| !b.digest_ok)).count();
    let dangling = views.iter().filter(|v| v.dangling.is_some()).count();
    let mut verdict = absence_verdict(&lookups)
        .unwrap_or_else(|| Verdict::new(code::OK).num("nodes", views.len()));
    if bad_digest + dangling > 0 {
        verdict = recode(verdict, code::BROKEN)
            .count("bad_digest", bad_digest)
            .count("dangling", dangling);
    }
    Ok(BatchReport { digest: b256(&digest), committed_at, nodes: views, verdict })
}

// ---------------------------------------------------------------------------------------------
// get-tx <TX_HASH> [--epoch E]
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct TxReport {
    pub hash: String,
    /// The `--epoch` restriction, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<Epoch>,
    /// Lowest header number any node saw committing a batch that holds the transaction.
    pub committed_at: Option<u64>,
    pub nodes: Vec<TxNodeView>,
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub struct TxNodeView {
    pub node: String,
    pub live: LiveStatus,
    pub lookup: Lookup,
    pub tip: Option<u64>,
    pub current_epoch: Option<Epoch>,
    /// Nothing was scanned: the node's latest header is in an epoch before `--epoch`, so it
    /// cannot hold batches of that epoch.
    pub skipped: bool,
    /// How much was read; everything in range when the transaction was not found.
    pub scanned: ScanStats,
    /// Every batch on this node that carries the transaction (a transaction can be sealed more
    /// than once).
    pub matches: Vec<TxMatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionView>,
}

/// One batch holding the transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TxMatch {
    pub digest: String,
    pub tier: Tier,
    /// Position of the transaction within the batch.
    pub index: usize,
    pub epoch: Epoch,
    pub worker_id: WorkerId,
    pub seq: u64,
    pub transaction_count: usize,
    /// The stored bytes hash to the digest they are stored under.
    pub digest_ok: bool,
    pub committed_in: Commit,
}

struct Hit {
    digest: BlockHash,
    tier: Tier,
    index: usize,
    batch: Batch,
}

struct Scan {
    position: Position,
    skipped: bool,
    scanned: ScanStats,
    hits: Vec<Hit>,
}

pub fn get_tx(nodes: &[NodeDb], hash: B256, epoch: Option<Epoch>) -> eyre::Result<TxReport> {
    let mut scans: Vec<Scan> = Vec::with_capacity(nodes.len());
    for node in nodes {
        let position = node.position()?;
        // a node still in an earlier epoch cannot hold batches of `epoch`: nothing to scan
        let behind = epoch.is_some_and(|e| position.current_epoch.is_some_and(|c| c < e));
        let mut scan =
            Scan { position, skipped: behind, scanned: ScanStats::default(), hits: Vec::new() };
        if !behind {
            scan.scanned = node.scan_batches(epoch, |digest, batch, tier| {
                for (index, tx) in batch.transactions.iter().enumerate() {
                    if keccak256(tx) == hash {
                        scan.hits.push(Hit { digest, tier, index, batch: batch.clone() });
                    }
                }
                Ok(true)
            })?;
        }
        scans.push(scan);
    }

    // one commit lookup per matching batch; the earliest committing header judges absences
    let mut matches: Vec<Vec<TxMatch>> = Vec::with_capacity(nodes.len());
    for (node, scan) in nodes.iter().zip(&scans) {
        let mut found = Vec::with_capacity(scan.hits.len());
        for hit in &scan.hits {
            found.push(TxMatch {
                digest: b256(&hit.digest),
                tier: hit.tier,
                index: hit.index,
                epoch: hit.batch.epoch,
                worker_id: hit.batch.worker_id,
                seq: hit.batch.seq,
                transaction_count: hit.batch.transactions.len(),
                digest_ok: hit.batch.digest() == hit.digest,
                committed_in: commit_of(node, hit.digest, hit.batch.epoch)?,
            });
        }
        matches.push(found);
    }
    let committed_at = matches.iter().flatten().filter_map(|m| m.committed_in.number()).min();
    let held = matches.iter().any(|m| !m.is_empty());

    // nodes disagree only when they name different batch sets for the same committing header;
    // one node holding two batches of the transaction under one header is a duplicate seal
    let mut by_header: BTreeMap<u64, BTreeSet<BTreeSet<String>>> = BTreeMap::new();
    for found in &matches {
        let mut per_header: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
        for m in found {
            if let Some(number) = m.committed_in.number() {
                per_header.entry(number).or_default().insert(m.digest.clone());
            }
        }
        for (number, digests) in per_header {
            by_header.entry(number).or_default().insert(digests);
        }
    }
    let variants = by_header.values().map(BTreeSet::len).max().unwrap_or(1);
    let copies = matches.iter().map(Vec::len).max().unwrap_or(0);
    let uncommitted = matches
        .iter()
        .filter(|m| !m.is_empty() && m.iter().all(|m| m.committed_in.number().is_none()))
        .count();
    let bad_digest = matches.iter().flatten().filter(|m| !m.digest_ok).count();

    let mut views = Vec::with_capacity(nodes.len());
    for ((node, scan), found) in nodes.iter().zip(scans).zip(matches) {
        let lookup = if !found.is_empty() {
            Lookup::Found
        } else if scan.skipped {
            // still in an earlier epoch than --epoch: behind, whatever the other nodes hold
            Lookup::NotReached
        } else {
            classify_absent(&scan.position, committed_at, held)
        };
        let transaction = scan
            .hits
            .first()
            .map(|hit| TransactionView::of(hit.index, &hit.batch.transactions[hit.index]));
        views.push(TxNodeView {
            node: node.label.clone(),
            live: node.live,
            lookup,
            tip: scan.position.consensus_tip,
            current_epoch: scan.position.current_epoch,
            skipped: scan.skipped,
            scanned: scan.scanned,
            matches: found,
            transaction,
        });
    }

    let lookups: Vec<Lookup> = views.iter().map(|v| v.lookup).collect();
    let mut verdict = absence_verdict(&lookups)
        .unwrap_or_else(|| Verdict::new(code::OK).num("nodes", views.len()));
    if variants > 1 {
        verdict = recode(verdict, code::DIVERGENT).word("what", "batch").num("variants", variants);
    }
    if bad_digest > 0 {
        verdict = recode(verdict, code::BROKEN).num("bad_digest", bad_digest);
    }
    verdict = verdict
        .count("copies", if copies > 1 { copies } else { 0 })
        .count("uncommitted", uncommitted)
        .count("short_scan", views.iter().filter(|v| v.scanned.short).count());
    Ok(TxReport { hash: b256(&hash), epoch, committed_at, nodes: views, verdict })
}
