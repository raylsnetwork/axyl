// SPDX-License-Identifier: BUSL-1.1
//! `participation <EPOCH>`: what each validator did in one epoch, counted from the committed
//! sub-dags on disk, in the shape the node hands to `applyIncentives` at the epoch's close.
//!
//! The node's own tally (`ConsensusRewardsCounter::tally_hybrid`) walks the same rows: for every
//! stored consensus header of the epoch whose leader round is not 0, the leader's author earns an
//! anchor round and every distinct author among the sub-dag's certificates earns a participation
//! round; the header count is the epoch's `totalRounds`. This report repeats that walk on each
//! node, over every tier it holds, and adds the counts the tally does not need (certificates,
//! batches, signatures) so a validator that earned nothing can be told apart from one whose
//! certificates never reached a commit.

use super::{code, header::committee_keys, Verdict};
use crate::{
    node_db::{NodeDb, Position, Tier},
    view::authority,
};
use rayls_infrastructure_types::{AuthorityIdentifier, Epoch};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Basis points: the contract's `MAX_BPS`, which `participationFloorBps` is measured against.
const MAX_BPS: u64 = 10_000;

#[derive(Debug, Serialize)]
pub struct ParticipationReport {
    pub epoch: Epoch,
    pub nodes: Vec<ParticipationNodeView>,
    pub verdict: Verdict,
}

/// Where the epoch stands on a node, which decides whether its tally is final.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EpochState {
    /// The node has closed the epoch: its tally is what the close-epoch call was built from.
    Closed,
    /// The node's consensus tip is still in the epoch: a running tally, printed but not
    /// compared.
    Open,
    /// The node has not reached the epoch.
    NotReached,
}

impl std::fmt::Display for EpochState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::NotReached => "not reached",
        })
    }
}

#[derive(Debug, Serialize)]
pub struct ParticipationNodeView {
    pub node: String,
    pub state: EpochState,
    pub position: Position,
    /// Headers tallied: the epoch's `totalRounds`.
    pub headers: u64,
    pub hot: u64,
    pub cold: u64,
    pub first_header: Option<u64>,
    pub last_header: Option<u64>,
    pub first_round: Option<u32>,
    pub last_round: Option<u32>,
    /// Canonical headers whose leader round is 0 (genesis). Stored, but the node's tally skips
    /// them, so they are listed and not counted.
    pub genesis_headers: Vec<u64>,
    /// Headers held only in the verified-but-unprocessed cache: not executed, so not tallied.
    pub cached_not_tallied: Vec<u64>,
    /// The epoch's last header per its record (`parent_consensus`), resolved on this node, and
    /// that header's leader round: the round the close block's tally stopped at (the round in
    /// the close block's nonce). `None` when the node holds no record for the epoch, or the
    /// record's boundary does not resolve to a header of this epoch.
    pub boundary_header: Option<u64>,
    pub boundary_round: Option<u32>,
    /// Canonical headers whose leader round is past the boundary: committed after the close
    /// block was built, so the node's tally never saw them. Listed, not counted.
    pub after_boundary: Vec<u64>,
    /// Size of the committee the counts are resolved against, when the node holds one.
    pub committee_size: Option<usize>,
    /// Which record supplied the committee: `its record` or `the previous record`.
    pub committee_from: Option<&'static str>,
    pub totals: Totals,
    /// Committee members in committee order, then authors seen outside the committee.
    pub authorities: Vec<AuthorityRow>,
    /// Signer bits beyond the committee's size, summed over every certificate.
    pub unknown_signers: u64,
}

/// Sums over every tallied header.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Totals {
    pub certs: u64,
    pub batches: u64,
    pub signatures: u64,
    /// Distinct authors seen, as a sub-dag's leader or as a certificate's author. A leader is
    /// normally inside its own sub-dag, so the two sets coincide on a healthy chain.
    pub authors: u64,
}

/// One authority's counts, all counts of stored rows or bits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Tally {
    /// Headers whose sub-dag holds at least one of its certificates: `participationRounds`.
    pub participation_rounds: u64,
    /// Headers it led: `anchorRounds`.
    pub anchor_rounds: u64,
    /// `participation_rounds` as a share of the tallied headers, in basis points: what the
    /// contract's participation floor is measured against. `None` with no tallied header.
    pub participation_bps: Option<u64>,
    /// Its certificates in the sub-dags.
    pub certs: u64,
    /// Batches its certificates carry.
    pub batches: u64,
    /// Certificates whose signer set includes it. `None` when the node holds no committee for
    /// the epoch, since signers are stored as committee indices.
    pub signed: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthorityRow {
    /// Hex of the authority identifier.
    pub authority: String,
    /// Position in the epoch's committee; `None` for an author outside it.
    pub committee_index: Option<usize>,
    #[serde(flatten)]
    pub tally: Tally,
}

/// Running counts while walking one node's headers.
#[derive(Default)]
struct Walk {
    headers: u64,
    hot: u64,
    cold: u64,
    first_header: Option<u64>,
    last_header: Option<u64>,
    first_round: Option<u32>,
    last_round: Option<u32>,
    genesis_headers: Vec<u64>,
    after_boundary: Vec<u64>,
    totals: Totals,
    tallies: BTreeMap<AuthorityIdentifier, Tally>,
    /// Signature counts by committee index.
    signed: Vec<u64>,
    unknown_signers: u64,
}

pub fn participation(nodes: &[NodeDb], epoch: Epoch) -> eyre::Result<ParticipationReport> {
    let mut views = Vec::with_capacity(nodes.len());
    for node in nodes {
        let position = node.position()?;
        let state = if position.has_closed_epoch(epoch) {
            EpochState::Closed
        } else if position.current_epoch == Some(epoch) {
            EpochState::Open
        } else {
            EpochState::NotReached
        };
        let mut view = ParticipationNodeView {
            node: node.label.clone(),
            state,
            position,
            headers: 0,
            hot: 0,
            cold: 0,
            first_header: None,
            last_header: None,
            first_round: None,
            last_round: None,
            genesis_headers: Vec::new(),
            cached_not_tallied: Vec::new(),
            boundary_header: None,
            boundary_round: None,
            after_boundary: Vec::new(),
            committee_size: None,
            committee_from: None,
            totals: Totals::default(),
            authorities: Vec::new(),
            unknown_signers: 0,
        };
        if state == EpochState::NotReached {
            views.push(view);
            continue;
        }

        // the committee the node's own tally would resolve authors against; ids derive from
        // the keys the way `Committee` derives them
        let committee: Option<(Vec<AuthorityIdentifier>, &'static str)> =
            committee_keys(node, epoch)?.map(|(keys, from)| {
                (keys.into_iter().map(AuthorityIdentifier::from).collect(), from)
            });
        let index_of: BTreeMap<&AuthorityIdentifier, usize> = committee
            .as_ref()
            .map(|(ids, _)| ids.iter().enumerate().map(|(i, id)| (id, i)).collect())
            .unwrap_or_default();

        // where the node's tally stopped: the close block carries the boundary output's leader
        // round in its nonce, and the epoch record names that output's header
        let boundary: Option<(u64, u32)> = match node.epoch(epoch)? {
            Some((record, _)) => match node.header_number_by_digest(record.parent_consensus)? {
                Some(number) => node.header(number)?.and_then(|(h, _)| {
                    let leader = &h.sub_dag.leader;
                    (leader.epoch() == epoch).then(|| (number, leader.round()))
                }),
                None => None,
            },
            None => None,
        };

        let numbers = node.epoch_header_numbers(epoch)?;
        let mut walk = Walk {
            signed: vec![0; committee.as_ref().map_or(0, |(ids, _)| ids.len())],
            ..Walk::default()
        };
        for number in &numbers.canonical {
            let Some((header, tier)) = node.header(*number)? else {
                eyre::bail!(
                    "{}: consensus header {number} was listed for epoch {epoch} but could not \
                     be read back",
                    node.label
                );
            };
            let leader = &header.sub_dag.leader;
            if leader.epoch() != epoch {
                eyre::bail!(
                    "{}: consensus header {number} was listed for epoch {epoch} but its leader \
                     is in epoch {}",
                    node.label,
                    leader.epoch()
                );
            }
            if leader.round() == 0 {
                walk.genesis_headers.push(*number);
                continue;
            }
            if boundary.is_some_and(|(_, round)| leader.round() > round) {
                walk.after_boundary.push(*number);
                continue;
            }
            walk.headers += 1;
            match tier {
                Tier::Hot => walk.hot += 1,
                Tier::Cold => walk.cold += 1,
                // canonical numbers come from the hot table and the cold index only
                Tier::Cache => {}
            }
            walk.first_header.get_or_insert(*number);
            walk.last_header = Some(*number);
            walk.first_round.get_or_insert(leader.round());
            walk.last_round = Some(leader.round());

            walk.tallies.entry(leader.origin().clone()).or_default().anchor_rounds += 1;
            let mut seen: BTreeSet<&AuthorityIdentifier> = BTreeSet::new();
            for cert in &header.sub_dag.certificates {
                let author = cert.origin();
                let tally = walk.tallies.entry(author.clone()).or_default();
                tally.certs += 1;
                tally.batches += cert.header().payload().len() as u64;
                walk.totals.certs += 1;
                walk.totals.batches += cert.header().payload().len() as u64;
                if seen.insert(author) {
                    tally.participation_rounds += 1;
                }
                for bit in cert.signed_authorities().iter() {
                    walk.totals.signatures += 1;
                    // without a committee no bit resolves, so none is "beyond" it either
                    if committee.is_none() {
                        continue;
                    }
                    match walk.signed.get_mut(bit as usize) {
                        Some(count) => *count += 1,
                        None => walk.unknown_signers += 1,
                    }
                }
            }
        }

        // rows: the committee in its order, then authors outside it
        let mut rows = Vec::new();
        if let Some((ids, _)) = &committee {
            for (i, id) in ids.iter().enumerate() {
                let mut tally = walk.tallies.get(id).copied().unwrap_or_default();
                tally.signed = Some(walk.signed[i]);
                rows.push(AuthorityRow {
                    authority: authority(id),
                    committee_index: Some(i),
                    tally,
                });
            }
        }
        for (id, tally) in &walk.tallies {
            if index_of.contains_key(id) {
                continue;
            }
            rows.push(AuthorityRow {
                authority: authority(id),
                committee_index: None,
                tally: *tally,
            });
        }
        for row in &mut rows {
            row.tally.participation_bps =
                (walk.headers > 0).then(|| row.tally.participation_rounds * MAX_BPS / walk.headers);
        }

        view.headers = walk.headers;
        view.hot = walk.hot;
        view.cold = walk.cold;
        view.first_header = walk.first_header;
        view.last_header = walk.last_header;
        view.first_round = walk.first_round;
        view.last_round = walk.last_round;
        view.genesis_headers = walk.genesis_headers;
        view.cached_not_tallied = numbers.cache_only.into_iter().collect();
        view.boundary_header = boundary.map(|(number, _)| number);
        view.boundary_round = boundary.map(|(_, round)| round);
        view.after_boundary = walk.after_boundary;
        view.committee_size = committee.as_ref().map(|(ids, _)| ids.len());
        view.committee_from = committee.as_ref().map(|(_, from)| *from);
        view.totals = Totals { authors: walk.tallies.len() as u64, ..walk.totals };
        view.authorities = rows;
        view.unknown_signers = walk.unknown_signers;
        views.push(view);
    }

    let verdict = verdict(&views);
    Ok(ParticipationReport { epoch, nodes: views, verdict })
}

/// Closed nodes are compared; an open node's running tally is printed, not compared.
///
/// Two fingerprints: the counts every node can produce (`tally`), and the signer counts, which
/// need a committee (`signers`). Fields: `nodes closed open not_reached what variants headers
/// certs cached after_boundary outsiders no_committee unknown_signers`.
fn verdict(views: &[ParticipationNodeView]) -> Verdict {
    let closed: Vec<&ParticipationNodeView> =
        views.iter().filter(|v| v.state == EpochState::Closed).collect();
    let open = views.iter().filter(|v| v.state == EpochState::Open).count();
    let not_reached = views.iter().filter(|v| v.state == EpochState::NotReached).count();

    /// What two closed nodes must agree on for the tally to match: the header count (the
    /// epoch's `totalRounds`) and, per authority, participation, anchor, certs and batches.
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    struct TallyFingerprint {
        headers: u64,
        rows: Vec<(String, [u64; 4])>,
    }
    let tally_variants: BTreeSet<TallyFingerprint> = closed
        .iter()
        .map(|v| {
            let mut rows: Vec<_> = v
                .authorities
                .iter()
                .map(|r| {
                    (
                        r.authority.clone(),
                        [
                            r.tally.participation_rounds,
                            r.tally.anchor_rounds,
                            r.tally.certs,
                            r.tally.batches,
                        ],
                    )
                })
                .collect();
            rows.sort();
            TallyFingerprint { headers: v.headers, rows }
        })
        .collect();
    let signer_variants: BTreeSet<Vec<(String, u64)>> = closed
        .iter()
        .filter(|v| v.committee_size.is_some())
        .map(|v| {
            let mut rows: Vec<_> = v
                .authorities
                .iter()
                .filter_map(|r| r.tally.signed.map(|s| (r.authority.clone(), s)))
                .collect();
            rows.sort();
            rows
        })
        .collect();

    // the fullest closed node (or open, if none) supplies the sizes
    let fullest =
        closed.iter().copied().max_by_key(|v| v.headers).or_else(|| {
            views.iter().filter(|v| v.state == EpochState::Open).max_by_key(|v| v.headers)
        });

    let (code, what, variants) = if tally_variants.len() > 1 {
        (code::DIVERGENT, Some("tally"), tally_variants.len())
    } else if signer_variants.len() > 1 {
        (code::DIVERGENT, Some("signers"), signer_variants.len())
    } else if not_reached == views.len() {
        (code::NOT_REACHED, None, 0)
    } else if !closed.is_empty()
        && closed.len() == views.len()
        && closed.iter().all(|v| v.headers == 0)
    {
        (code::EMPTY, None, 0)
    } else if open + not_reached > 0 {
        (code::PARTIAL, None, 0)
    } else {
        (code::OK, None, 0)
    };

    let mut verdict = Verdict::new(code)
        .num("nodes", views.len())
        .count("closed", closed.len())
        .count("open", open)
        .count("not_reached", not_reached);
    if let Some(what) = what {
        verdict = verdict.word("what", what).num("variants", variants);
    }
    let (headers, certs, cached, after_boundary, outsiders, unknown) =
        fullest.map_or((0, 0, 0, 0, 0, 0), |v| {
            (
                v.headers as usize,
                v.totals.certs as usize,
                v.cached_not_tallied.len(),
                v.after_boundary.len(),
                if v.committee_size.is_some() {
                    v.authorities.iter().filter(|r| r.committee_index.is_none()).count()
                } else {
                    0
                },
                v.unknown_signers as usize,
            )
        });
    let no_committee = views
        .iter()
        .filter(|v| v.state != EpochState::NotReached && v.committee_size.is_none())
        .count();
    verdict
        .count("headers", headers)
        .count("certs", certs)
        .count("cached", cached)
        .count("after_boundary", after_boundary)
        .count("outsiders", outsiders)
        .count("no_committee", no_committee)
        .count("unknown_signers", unknown)
}
