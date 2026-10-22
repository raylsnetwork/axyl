// SPDX-License-Identifier: BUSL-1.1
//! Consensus-header reports: `header`, `cert`, `header-check`.

use super::{absence_verdict, chain_verdict, code, content_verdict, recode, ChainOutcome, Verdict};
pub use super::{describe_absent, Link, Lookup};
use crate::{
    node_db::{BatchLookup, NodeDb, Position, Tier},
    view::{b256, CertificateSummary, CertificateView},
};
use rayls_infrastructure_types::{
    dkg::DkgOutcome, BlsPublicKey, Certificate, ConsensusHeader, Epoch, EpochKeys,
    SignatureVerificationState, B256,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Digest of the header every chain starts from: header 1's `parent_hash`. It is never stored,
/// which is why `header 0` finds nothing on a healthy node.
pub fn genesis_anchor() -> B256 {
    ConsensusHeader::default().digest()
}

fn classify(found: bool, position: &Position, number: u64) -> Lookup {
    if found {
        Lookup::Found
    } else if number == 0 {
        // the genesis anchor is computed, not stored
        Lookup::NotFound
    } else if position.has_reached_header(number) {
        Lookup::Missing
    } else {
        Lookup::NotReached
    }
}

// ---------------------------------------------------------------------------------------------
// Re-checking certificate signatures (always on)
// ---------------------------------------------------------------------------------------------

/// Outcome of re-checking a certificate's aggregate BLS signature against the committee recorded
/// for its epoch, instead of trusting the verification state the node stored. Every report that
/// shows a certificate does this; the stored state is the node's own claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum SignatureCheck {
    /// Quorum reached and the aggregate signature verifies.
    Verified { epoch: Epoch, keys: usize, keys_from: &'static str },
    /// A genesis certificate: unsigned by design.
    Genesis,
    /// No quorum, or the signature does not verify against the epoch's committee.
    Failed { epoch: Epoch, keys: usize, error: String },
    /// The source holds no committee for that epoch (no record for it or the one before).
    NoKeys { epoch: Epoch },
    /// The certificate has no signer set, so the epoch may use threshold certificates, and the
    /// source holds the committee but no DKG outcome for the epoch and cannot compute one.
    SchemeUnknown { epoch: Epoch, keys: usize, cause: NoOutcome },
}

/// Why the source has no DKG outcome for an epoch that uses threshold certificates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum NoOutcome {
    /// The source stored no outcome for the epoch. The key comes from the dealings the chain's
    /// `DkgManager` accepted, which only a node reads; this tool reads the outcome the node saved
    /// when the epoch started.
    NotStored { epoch: Epoch },
}

impl std::fmt::Display for NoOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStored { epoch } => write!(
                f,
                "no stored DKG outcome for epoch {epoch}; the node saves it when the epoch starts"
            ),
        }
    }
}

impl SignatureCheck {
    pub fn failed(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }

    pub fn unverifiable(&self) -> bool {
        matches!(self, Self::NoKeys { .. } | Self::SchemeUnknown { .. })
    }

    /// One word for a table cell.
    pub fn word(&self) -> &'static str {
        match self {
            Self::Verified { .. } => "ok",
            Self::Genesis => "genesis",
            Self::Failed { .. } => "FAILED",
            Self::NoKeys { .. } => "no keys",
            Self::SchemeUnknown { .. } => "scheme?",
        }
    }
}

impl std::fmt::Display for SignatureCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Verified { epoch, keys, keys_from } => {
                write!(f, "verified (epoch {epoch}, {keys} keys from {keys_from})")
            }
            Self::Genesis => f.write_str("genesis (unsigned by design)"),
            Self::Failed { epoch, keys, error } => {
                write!(f, "FAILED against epoch {epoch}'s {keys} keys: {error}")
            }
            Self::NoKeys { epoch } => {
                write!(f, "no committee keys for epoch {epoch}: the source holds no record for epoch {epoch}")?;
                if *epoch > 0 {
                    write!(f, " or {}", epoch - 1)?;
                }
                Ok(())
            }
            Self::SchemeUnknown { epoch, keys, cause } => write!(
                f,
                "scheme unknown for epoch {epoch} ({keys} keys): no dkg outcome stored, and it \
                 cannot be computed: {cause}"
            ),
        }
    }
}

/// The committee that served `epoch`, from the source's own epoch records: the record of that
/// epoch, or the `next_committee` of the one before (the current epoch has no record yet).
pub(crate) fn committee_keys(
    node: &NodeDb,
    epoch: Epoch,
) -> eyre::Result<Option<(Vec<BlsPublicKey>, &'static str)>> {
    if let Some((record, _)) = node.epoch(epoch)? {
        return Ok(Some((record.committee, "its record")));
    }
    if epoch > 0 {
        if let Some((previous, _)) = node.epoch(epoch - 1)? {
            return Ok(Some((previous.next_committee, "the previous record")));
        }
    }
    Ok(None)
}

/// One epoch's keys as the source knows them.
// One per epoch looked up; the size gap between the variants does not matter.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
enum KeysLookup {
    /// The committee and the certificate scheme.
    Known(EpochKeys, &'static str),
    /// The committee (this many keys), but not whether the epoch uses threshold certificates.
    SchemeUnknown(usize, NoOutcome),
    /// No committee.
    Missing,
}

/// [`committee_keys`] with the epoch's certificate scheme: the source's own DKG outcome for the
/// epoch; else legacy for epoch 0, for a source no build with threshold certificates ever
/// opened (it has no outcome table), and for a certificate that names its signers (so a network
/// where the fork is not active never needs one); else unknown.
fn epoch_keys(node: &NodeDb, epoch: Epoch, threshold_shaped: bool) -> eyre::Result<KeysLookup> {
    let Some((keys, keys_from)) = committee_keys(node, epoch)? else {
        return Ok(KeysLookup::Missing);
    };
    let outcome = match node.dkg_outcome(epoch)? {
        Some(outcome) => outcome,
        None if epoch == 0 || !threshold_shaped || !node.has_dkg_outcomes()? => {
            DkgOutcome::Fallback
        }
        None => {
            return Ok(KeysLookup::SchemeUnknown(keys.len(), NoOutcome::NotStored { epoch }));
        }
    };
    let group_key = match outcome {
        DkgOutcome::Key(key) => Some(key.group_key()),
        DkgOutcome::Fallback => None,
    };
    Ok(KeysLookup::Known(EpochKeys { keys, group_key }, keys_from))
}

/// Re-checks one certificate's quorum and aggregate signature, or its threshold signature in a
/// threshold epoch.
pub fn check_certificate(node: &NodeDb, cert: &Certificate) -> eyre::Result<SignatureCheck> {
    if matches!(cert.signature_verification_state(), SignatureVerificationState::Genesis) {
        return Ok(SignatureCheck::Genesis);
    }
    let epoch = cert.epoch();
    let (keys, keys_from) = match epoch_keys(node, epoch, cert.signed_authorities().is_empty())? {
        KeysLookup::Known(keys, keys_from) => (keys, keys_from),
        KeysLookup::SchemeUnknown(keys, cause) => {
            return Ok(SignatureCheck::SchemeUnknown { epoch, keys, cause })
        }
        KeysLookup::Missing => return Ok(SignatureCheck::NoKeys { epoch }),
    };
    let count = keys.keys.len();
    Ok(match cert.clone().verify_cert_with(&keys.scheme()) {
        Ok(_) => SignatureCheck::Verified { epoch, keys: count, keys_from },
        Err(error) => SignatureCheck::Failed { epoch, keys: count, error: error.to_string() },
    })
}

/// The committee and scheme a source knows, looked up once per epoch and leader shape (whether
/// the leader certificate has no signer set, see [`epoch_keys`]).
type KeysByEpoch = BTreeMap<(Epoch, bool), KeysLookup>;

/// The [`KeysByEpoch`] key of a header: its leader's epoch and shape.
fn keys_slot(header: &ConsensusHeader) -> (Epoch, bool) {
    let leader = &header.sub_dag.leader;
    (leader.epoch(), leader.signed_authorities().is_empty())
}

/// Re-checks every certificate of a header's sub-dag the way the node does on receipt: the
/// leader directly, the rest directly unless the leader's parents vouch for them.
pub fn check_header(node: &NodeDb, header: &ConsensusHeader) -> eyre::Result<SignatureCheck> {
    let leader = &header.sub_dag.leader;
    if matches!(leader.signature_verification_state(), SignatureVerificationState::Genesis) {
        return Ok(SignatureCheck::Genesis);
    }
    let (epoch, threshold_shaped) = keys_slot(header);
    let keys =
        BTreeMap::from([((epoch, threshold_shaped), epoch_keys(node, epoch, threshold_shaped)?)]);
    Ok(verify_header(&keys, header))
}

/// [`check_header`] against committees already looked up: pure computation, safe on any thread.
fn verify_header(keys: &KeysByEpoch, header: &ConsensusHeader) -> SignatureCheck {
    let leader = &header.sub_dag.leader;
    if matches!(leader.signature_verification_state(), SignatureVerificationState::Genesis) {
        return SignatureCheck::Genesis;
    }
    let slot = keys_slot(header);
    let epoch = slot.0;
    let (keys, keys_from) = match keys.get(&slot) {
        Some(KeysLookup::Known(keys, keys_from)) => (keys, *keys_from),
        Some(KeysLookup::SchemeUnknown(keys, cause)) => {
            return SignatureCheck::SchemeUnknown { epoch, keys: *keys, cause: cause.clone() }
        }
        Some(KeysLookup::Missing) | None => return SignatureCheck::NoKeys { epoch },
    };
    let count = keys.keys.len();
    match header.clone().verify_header_with_scheme(&keys.scheme()) {
        Ok(_) => SignatureCheck::Verified { epoch, keys: count, keys_from },
        Err(error) => SignatureCheck::Failed { epoch, keys: count, error: error.to_string() },
    }
}

/// Verifies `headers` on every available core (the committees must already be in `keys`).
/// Results come back in input order.
fn verify_headers(keys: &KeysByEpoch, headers: &[&ConsensusHeader]) -> Vec<SignatureCheck> {
    if headers.is_empty() {
        return Vec::new();
    }
    let threads =
        std::thread::available_parallelism().map_or(1, |n| n.get()).clamp(1, headers.len());
    let chunk = headers.len().div_ceil(threads);
    std::thread::scope(|scope| {
        let handles: Vec<_> = headers
            .chunks(chunk)
            .map(|part| {
                scope.spawn(move || part.iter().map(|h| verify_header(keys, h)).collect::<Vec<_>>())
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("a certificate verification thread panicked"))
            .collect()
    })
}

/// Folds signature checks into a verdict: any failure is `BROKEN`; a source without the keys
/// to check a certificate turns an `OK` into `PARTIAL`, since that node cannot prove it.
fn with_signature_checks<'a>(
    verdict: Verdict,
    checks: impl Iterator<Item = Option<&'a SignatureCheck>>,
) -> Verdict {
    let (mut failed, mut unverifiable) = (0, 0);
    for check in checks.flatten() {
        failed += usize::from(check.failed());
        unverifiable += usize::from(check.unverifiable());
    }
    let verdict = if failed > 0 {
        recode(verdict, code::BROKEN)
    } else if unverifiable > 0 && verdict.code == code::OK {
        recode(verdict, code::PARTIAL)
    } else {
        verdict
    };
    verdict.count("sig_failed", failed).count("unverifiable", unverifiable)
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
    pub lookup: Lookup,
    /// The node's latest canonical consensus number.
    pub tip: Option<u64>,
    pub tier: Option<Tier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<HeaderView>,
    /// The sub-dag re-checked the way the node does on receipt: the leader directly, the other
    /// certificates unless the leader's parents vouch for them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature_check: Option<SignatureCheck>,
}

/// Presence of a batch referenced by the sub-dag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BatchPresence {
    pub digest: String,
    /// Where the batch can be read from; `None` when it cannot.
    pub tier: Option<Tier>,
    /// The cold index names a jar row that is gone.
    pub dangling: bool,
}

/// What the tool derives from a consensus header, plus (`-v`) the header itself.
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
    /// Where each batch the sub-dag commits is stored on this node (`-v`, databases only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batches: Option<Vec<BatchPresence>>,
    /// The header as stored, in the wire type's own JSON form: the object `rayls_latestHeader`
    /// returns (`-v`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<ConsensusHeader>,
}

impl HeaderView {
    fn of(node: &NodeDb, header: &ConsensusHeader, verbose: bool) -> eyre::Result<Self> {
        let sub_dag = &header.sub_dag;
        let batch_digests: Vec<B256> = sub_dag
            .certificates
            .iter()
            .flat_map(|c| c.header().payload().keys().copied())
            .collect();
        let batches = match (verbose, Some(node)) {
            (true, Some(db)) => {
                let mut out = Vec::with_capacity(batch_digests.len());
                for d in &batch_digests {
                    let (tier, dangling) = match db.batch(*d)? {
                        BatchLookup::Found(_, tier) => (Some(tier), false),
                        BatchLookup::Dangling(_) => (None, true),
                        BatchLookup::Absent => (None, false),
                    };
                    out.push(BatchPresence { digest: b256(d), tier, dangling });
                }
                Some(out)
            }
            _ => None,
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
            batches,
            raw: verbose.then(|| header.clone()),
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
        let (tier, header, signature_check) = match found {
            Some((header, tier)) => {
                digests.insert(header.digest());
                let check = Some(check_header(node, &header)?);
                (Some(tier), Some(HeaderView::of(node, &header, verbose)?), check)
            }
            None => (None, None, None),
        };
        views.push(HeaderNodeView {
            node: node.label.clone(),
            lookup,
            tip: position.consensus_tip,
            tier,
            header,
            signature_check,
        });
    }
    let lookups: Vec<Lookup> = views.iter().map(|v| v.lookup).collect();
    let verdict = absence_verdict(&lookups)
        .unwrap_or_else(|| content_verdict(views.len(), digests.len(), "header"));
    let verdict = with_signature_checks(verdict, views.iter().map(|v| v.signature_check.as_ref()));
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
    pub lookup: Lookup,
    pub tip: Option<u64>,
    pub tier: Option<Tier>,
    /// Digest of the consensus header the leader certificate was taken from.
    pub header_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader: Option<CertificateView>,
    /// The leader certificate re-checked against the epoch's committee.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature_check: Option<SignatureCheck>,
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
        let (tier, header_digest, leader, signature_check) = match found {
            Some((header, tier)) => {
                header_digests.insert(header.digest());
                let leader = CertificateView::of(&header.sub_dag.leader, verbose);
                cert_digests.insert(leader.summary.digest.clone());
                let (signers, sig) = leader.signature_identity();
                identities.insert((signers.to_vec(), sig.map(str::to_owned)));
                let check = Some(check_certificate(node, &header.sub_dag.leader)?);
                (Some(tier), Some(b256(&header.digest())), Some(leader), check)
            }
            None => (None, None, None, None),
        };
        views.push(CertNodeView {
            node: node.label.clone(),
            lookup,
            tip: position.consensus_tip,
            tier,
            header_digest,
            leader,
            signature_check,
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
    let verdict = with_signature_checks(verdict, views.iter().map(|v| v.signature_check.as_ref()));
    Ok(CertReport { number, nodes: views, verdict })
}

// ---------------------------------------------------------------------------------------------
// header-check <N> --back K
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct HeaderCheckReport {
    pub start: u64,
    pub back: u64,
    pub nodes: Vec<HeaderCheckNodeView>,
    pub verdict: Verdict,
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
    /// The hop's sub-dag re-checked like `header` does (leader directly, the rest unless vouched
    /// for by the leader's parents).
    pub verify: SignatureCheck,
}

#[derive(Debug, Serialize)]
pub struct HeaderCheckNodeView {
    pub node: String,
    pub tip: Option<u64>,
    pub hops: Vec<Hop>,
    pub ok: bool,
    /// The start header lies beyond this node's tip.
    pub start_not_reached: bool,
    /// The start header is at or below this node's tip but absent.
    pub start_missing: bool,
    /// Why the check stopped.
    pub stopped: String,
    /// Hops whose certificates could not be checked, per epoch: the node holds neither that
    /// epoch's record nor the one before, so it has no committee for them, or it cannot tell
    /// whether the epoch uses threshold certificates (see [`Unverifiable::cause`]).
    pub unverifiable: Vec<Unverifiable>,
    /// The read that failed on this node, when its database could not be checked (damaged rows
    /// or jars). The other nodes are still checked.
    pub error: Option<String>,
}

/// The hops of one epoch a node cannot verify, and the records whose absence causes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unverifiable {
    pub epoch: Epoch,
    pub hops: usize,
    /// The records that would supply the committee: the epoch's own and the one before. Empty
    /// when the committee is known but not the certificate scheme.
    pub missing_records: Vec<Epoch>,
    /// Why the certificate scheme is unknown, when the committee is known.
    pub cause: Option<NoOutcome>,
}

/// Hops walked before their certificates are verified together on every core; bounds the
/// headers held in memory per node.
const VERIFY_BATCH: usize = 256;
/// Hops between progress lines on stderr, for checks at least this long.
const PROGRESS_EVERY: u64 = 1000;

pub fn header_check(nodes: &[NodeDb], start: u64, back: u64) -> eyre::Result<HeaderCheckReport> {
    // one thread per node: the nodes are independent, so five cost the time of one
    let views: Vec<HeaderCheckNodeView> = std::thread::scope(|scope| {
        let handles: Vec<_> =
            nodes.iter().map(|node| scope.spawn(move || check_node(node, start, back))).collect();
        handles
            .into_iter()
            .zip(nodes)
            .map(|(h, node)| {
                // a node that cannot be read is reported as such; the others still count
                let result = h.join().unwrap_or_else(|_| Err(eyre::eyre!("the check panicked")));
                result.unwrap_or_else(|err| HeaderCheckNodeView {
                    node: node.label.clone(),
                    tip: None,
                    hops: Vec::new(),
                    ok: false,
                    start_not_reached: false,
                    start_missing: false,
                    stopped: format!("unreadable: {err:#}"),
                    unverifiable: Vec::new(),
                    error: Some(format!("{err:#}")),
                })
            })
            .collect()
    });
    let mut digests_by_number: BTreeMap<u64, BTreeSet<&str>> = BTreeMap::new();
    for hop in views.iter().flat_map(|v| &v.hops) {
        digests_by_number.entry(hop.number).or_default().insert(hop.digest.as_str());
    }
    let divergent: Vec<u64> =
        digests_by_number.iter().filter(|(_, d)| d.len() > 1).map(|(n, _)| *n).collect();
    let outcomes: Vec<ChainOutcome> = views
        .iter()
        .map(|v| ChainOutcome {
            ok: v.ok,
            start_not_reached: v.start_not_reached,
            start_missing: v.start_missing,
            links: v.hops.len().saturating_sub(1),
            first_break: v.hops.iter().find(|h| !h.link.is_intact()).map(|h| h.number),
        })
        .collect();
    let unreadable = views.iter().filter(|v| v.error.is_some()).count();
    let verdict = chain_verdict(&outcomes, &divergent).count("unreadable", unreadable);
    let verdict = if unreadable > 0 { recode(verdict, code::BROKEN) } else { verdict };
    let verdict =
        with_signature_checks(verdict, views.iter().flat_map(|v| &v.hops).map(|h| Some(&h.verify)));
    Ok(HeaderCheckReport { start, back, nodes: views, verdict })
}

/// Walks one node's chain back from `start`, verifying certificates a batch at a time.
fn check_node(node: &NodeDb, start: u64, back: u64) -> eyre::Result<HeaderCheckNodeView> {
    let position = node.position()?;
    let mut view = HeaderCheckNodeView {
        node: node.label.clone(),
        tip: position.consensus_tip,
        hops: Vec::new(),
        ok: false,
        start_not_reached: false,
        start_missing: false,
        stopped: String::new(),
        unverifiable: Vec::new(),
        error: None,
    };
    let Some((mut current, mut tier)) = node.header(start)? else {
        if position.has_reached_header(start) {
            view.start_missing = true;
            view.stopped = format!("start header {start} missing");
        } else {
            view.start_not_reached = true;
            view.stopped = format!(
                "start header {start} {}",
                describe_absent(Lookup::NotReached, position.consensus_tip)
            );
        }
        return Ok(view);
    };
    let mut ok = true;
    let mut keys = KeysByEpoch::new();
    // hops whose certificates are not checked yet, with their headers
    let mut pending: Vec<(Hop, ConsensusHeader)> = Vec::new();
    let mut walked = 0u64;
    for _ in 0..=back {
        let mut hop = Hop {
            number: current.number,
            digest: b256(&current.digest()),
            parent_hash: b256(&current.parent_hash),
            tier,
            leader_round: current.sub_dag.leader_round(),
            leader_epoch: current.sub_dag.leader_epoch(),
            link: Link::Genesis,
            // filled when the batch is verified
            verify: SignatureCheck::Genesis,
        };
        if current.number == 0
            || current.parent_hash == B256::ZERO
            || current.parent_hash == genesis_anchor()
        {
            pending.push((hop, current));
            view.stopped = "reached genesis".to_owned();
            break;
        }
        if (view.hops.len() + pending.len()) as u64 == back {
            hop.link = Link::End;
            pending.push((hop, current));
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
        let number = current.number;
        pending.push((hop, current));
        walked += 1;
        if back >= PROGRESS_EVERY && walked.is_multiple_of(PROGRESS_EVERY) {
            eprintln!("[{}] {walked} hops walked", view.node);
        }
        if pending.len() >= VERIFY_BATCH {
            verify_pending(node, &mut keys, &mut pending, &mut view.hops)?;
        }
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
                view.stopped = format!("broken link below header {number}");
                break;
            }
        }
    }
    verify_pending(node, &mut keys, &mut pending, &mut view.hops)?;
    view.ok = ok;
    // per epoch: hops with no committee, then hops with a committee but no certificate scheme,
    // by cause (one per epoch: the lookup is cached)
    let mut no_keys: BTreeMap<Epoch, usize> = BTreeMap::new();
    let mut no_scheme: BTreeMap<Epoch, (usize, NoOutcome)> = BTreeMap::new();
    for hop in &view.hops {
        match &hop.verify {
            SignatureCheck::NoKeys { epoch } => *no_keys.entry(*epoch).or_default() += 1,
            SignatureCheck::SchemeUnknown { epoch, cause, .. } => {
                no_scheme.entry(*epoch).or_insert_with(|| (0, cause.clone())).0 += 1
            }
            _ => {}
        }
    }
    view.unverifiable = no_keys
        .into_iter()
        .map(|(epoch, hops)| Unverifiable {
            epoch,
            hops,
            missing_records: (epoch.saturating_sub(1)..=epoch).collect(),
            cause: None,
        })
        .chain(no_scheme.into_iter().map(|(epoch, (hops, cause))| Unverifiable {
            epoch,
            hops,
            missing_records: Vec::new(),
            cause: Some(cause),
        }))
        .collect();
    Ok(view)
}

/// Looks up the committees the pending headers need (once per epoch, on this thread, which owns
/// the source), verifies the headers on every core, and moves the hops into `hops` in order.
fn verify_pending(
    node: &NodeDb,
    keys: &mut KeysByEpoch,
    pending: &mut Vec<(Hop, ConsensusHeader)>,
    hops: &mut Vec<Hop>,
) -> eyre::Result<()> {
    for (_, header) in pending.iter() {
        // a genesis leader is reported as such before any lookup
        if matches!(
            header.sub_dag.leader.signature_verification_state(),
            SignatureVerificationState::Genesis
        ) {
            continue;
        }
        // one lookup per epoch and leader shape, the one `check_header` would make for this header
        if let std::collections::btree_map::Entry::Vacant(slot) = keys.entry(keys_slot(header)) {
            let (epoch, threshold_shaped) = *slot.key();
            slot.insert(epoch_keys(node, epoch, threshold_shaped)?);
        }
    }
    let headers: Vec<&ConsensusHeader> = pending.iter().map(|(_, h)| h).collect();
    let checks = verify_headers(keys, &headers);
    for ((mut hop, _), check) in pending.drain(..).zip(checks) {
        hop.verify = check;
        hops.push(hop);
    }
    Ok(())
}
