//! Test-only Byzantine override for by-number consensus-header gap fetches.
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. It lets a
//! node answer a follower's by-number consensus-header request with a crafted header, which is the
//! reproduction lever for the gap-fetch-stall issue. It is armed per process by environment, so a
//! single node in a test network lies while the rest stay honest:
//!
//!   RL_BYZ_GAP_TARGET=<N> or <A-B>  block number, or inclusive range, to lie about (arms the hook)
//!   RL_BYZ_GAP_MODE=fork            serve the real header for the number with its commit timestamp
//!                                   raised, keeping the real quorum-signed leader and certificates
//!                                   so the follower's signature re-verify still passes while the
//!                                   header digest and the executed block diverge (default)
//!   RL_BYZ_GAP_MODE=stall           serve a fabricated header with the real parent link and an
//!                                   unsigned sub-dag, to stall the follower's streamer anchor
//!   RL_BYZ_GAP_MODE=crash           serve the real header with one extra certificate added: one
//!                                   of this node's own certificates, saved by the certifier (see
//!                                   below). Only headers of an older epoch that carry batches
//!                                   get it, since only those make a follower look up authors
//!   RL_BYZ_GAP_MODE=crash-probe     the same poisoned header, but served on the follower's
//!                                   "latest" probe (number and hash both absent) and on a by-hash
//!                                   request for the poisoned header's own digest, instead of on a
//!                                   by-number request. This is the second delivery route for the
//!                                   crash: a committee member answers a catching-up follower's
//!                                   startup probe with the poison, the follower's backwards walk
//!                                   caches it and makes it the forward streamer's target, and the
//!                                   streamer hands it to execution. It needs RL_BYZ_PROBE_NUMBER.
//!   RL_BYZ_PROBE_NUMBER=<N>         the block number to wrap for crash-probe mode: this node's
//!                                   real block N (which must be from an older epoch and carry
//!                                   batches) is served with the extra certificate added
//!   RL_BYZ_GAP_BUMP=<secs>          commit-timestamp increase for fork mode (default 1000000)
//!
//! The certificate for crash mode comes from this node's certifier, armed by:
//!
//!   RL_BYZ_EXTRA_CERT=selfless      for a validator that joined the committee: build a second
//!                                   certificate for one of its own headers from the votes of the
//!                                   previous epoch's members only, leaving out its own signature,
//!                                   with the signer list laid out over the previous epoch's keys
//!   RL_BYZ_EXTRA_CERT=honest        save the real certificate instead, which carries this node's
//!                                   own signature, as every real certificate does

use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::ConsensusStore;
use rayls_infrastructure_types::{
    Address, AuthorityIdentifier, BlockHash, BlsPublicKey, BlsSignature, Certificate,
    CommitteeBuilder, CommittedSubDag, ConsensusHeader, Database, Epoch, Header, ReputationScores,
};
use std::sync::OnceLock;
use tracing::warn;

/// The certificate added to replies in crash mode. Set once, by the certifier.
static EXTRA_CERT: OnceLock<Certificate> = OnceLock::new();

/// Called by the certifier after it certifies one of this node's headers. Saves the certificate
/// that crash mode adds to replies, once, if `RL_BYZ_EXTRA_CERT` asks for one.
pub(crate) fn record_extra_certificate<DB: Database>(
    config: &ConsensusConfig<DB>,
    header: &Header,
    certificate: &Certificate,
    others_votes: &[(AuthorityIdentifier, BlsSignature)],
) {
    let Ok(kind) = std::env::var("RL_BYZ_EXTRA_CERT") else { return };
    // An empty payload keeps the follower from having to fetch this header's batches.
    if EXTRA_CERT.get().is_some() || !header.payload().is_empty() || header.epoch() == 0 {
        return;
    }
    let previous_epoch = header.epoch() - 1;
    let Some(previous_keys) = config.get_committee_keys_for_epoch(previous_epoch) else { return };
    // Only a certificate whose author was not in the previous committee can trip a follower.
    if previous_keys.iter().any(|key| AuthorityIdentifier::from(*key) == *header.author()) {
        return;
    }
    let extra = match kind.as_str() {
        "selfless" => match selfless_certificate(previous_epoch, &previous_keys, header, others_votes) {
            Some(extra) => extra,
            None => return,
        },
        "honest" => certificate.clone(),
        other => {
            warn!(target: "primary::byzantine", kind = other, "unknown RL_BYZ_EXTRA_CERT, ignoring");
            return;
        }
    };
    let passes = extra.clone().verify_cert(&previous_keys).is_ok();
    // Hand the certificate to other processes: it is plain data, so any peer can serve it.
    if let Ok(path) = std::env::var("RL_BYZ_EXTRA_CERT_OUT") {
        if let Err(e) = std::fs::write(&path, rayls_infrastructure_types::encode(&extra)) {
            warn!(target: "primary::byzantine", path, ?e, "could not write the extra certificate");
        }
    }
    if EXTRA_CERT.set(extra).is_ok() {
        warn!(
            target: "primary::byzantine",
            kind,
            epoch = header.epoch(),
            round = header.round(),
            author = %header.author(),
            passes_previous_epoch_keys = passes,
            "BYZANTINE EXTRA CERT: saved a certificate of this node for crash mode"
        );
    }
}

/// A certificate for `header` signed only by members of the previous committee, with the signer
/// list laid out over the previous committee's keys. None without a quorum of that committee.
fn selfless_certificate(
    previous_epoch: Epoch,
    previous_keys: &[BlsPublicKey],
    header: &Header,
    others_votes: &[(AuthorityIdentifier, BlsSignature)],
) -> Option<Certificate> {
    let mut builder = CommitteeBuilder::new(previous_epoch);
    for key in previous_keys {
        builder.add_authority(*key, 1, Address::ZERO);
    }
    let previous = builder.build();
    let votes: Vec<_> = others_votes
        .iter()
        .filter(|(author, _)| previous.authority(author).is_some())
        .cloned()
        .collect();
    Certificate::new_unverified(&previous, header.clone(), votes).ok()
}

/// Returns a crafted header to serve back for a by-number gap fetch of `number`, or None to serve
/// the honest stored header. `honest` is the real header for `number` read from local storage.
pub(super) fn override_gap_fetch(number: u64, honest: &ConsensusHeader) -> Option<ConsensusHeader> {
    let spec = std::env::var("RL_BYZ_GAP_TARGET").ok()?;
    let (lo, hi) = match spec.split_once('-') {
        Some((a, b)) => (a.trim().parse().ok()?, b.trim().parse().ok()?),
        None => {
            let n: u64 = spec.trim().parse().ok()?;
            (n, n)
        }
    };
    if number < lo || number > hi {
        return None;
    }
    let mode = std::env::var("RL_BYZ_GAP_MODE").unwrap_or_else(|_| "fork".to_string());
    match mode.as_str() {
        "fork" => Some(fork_header(number, honest)),
        "stall" => Some(stall_header(number, honest)),
        "crash" => crash_header(number, honest),
        other => {
            warn!(target: "primary::byzantine", mode = other, number, "unknown RL_BYZ_GAP_MODE, serving honest header");
            None
        }
    }
}

/// Keep the real quorum-signed leader and certificates, only raise the commit timestamp. The
/// commit timestamp is hashed into the header digest but is not covered by any committee signature,
/// so the result still passes the follower's signature re-verify yet executes to a different block.
fn fork_header(number: u64, honest: &ConsensusHeader) -> ConsensusHeader {
    let bump: u64 =
        std::env::var("RL_BYZ_GAP_BUMP").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    // The commit timestamp is max(previous sub-dag timestamp, leader created_at). Raise it by
    // building the sub-dag with a throwaway previous sub-dag whose timestamp is higher.
    let mut higher_leader = Certificate::default();
    higher_leader.header.created_at = honest.sub_dag.commit_timestamp() + bump;
    let higher = CommittedSubDag::new(vec![], higher_leader, 0, ReputationScores::default(), None);
    // sub_dag_index is not stored on CommittedSubDag and is not part of its digest, so pass zero.
    let tampered_sub_dag = CommittedSubDag::new(
        honest.sub_dag.certificates.clone(),
        honest.sub_dag.leader.clone(),
        0,
        honest.sub_dag.reputation_score.clone(),
        Some(&higher),
    );
    let tampered = ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag: tampered_sub_dag,
        number: honest.number,
        extra: honest.extra,
    };
    warn!(
        target: "primary::byzantine",
        number,
        honest_digest = ?honest.digest(),
        tampered_digest = ?tampered.digest(),
        "BYZANTINE GAP-FETCH: serving fork header (real leader, bumped commit timestamp)"
    );
    tampered
}

/// Fabricate a header with the real parent link for the previous block and an unsigned sub-dag.
fn stall_header(number: u64, honest: &ConsensusHeader) -> ConsensusHeader {
    let mut fake_leader = Certificate::default();
    fake_leader.header.created_at = honest.sub_dag.commit_timestamp();
    let fake_sub_dag =
        CommittedSubDag::new(vec![], fake_leader, 0, ReputationScores::default(), None);
    let fake = ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag: fake_sub_dag,
        number: honest.number,
        extra: honest.extra,
    };
    warn!(
        target: "primary::byzantine",
        number,
        honest_digest = ?honest.digest(),
        fake_digest = ?fake.digest(),
        "BYZANTINE GAP-FETCH: serving stall header (fabricated sub-dag, real parent link)"
    );
    fake
}

/// The real header with the saved certificate added, for an older-epoch header that has batches.
/// The saved certificate, or one loaded from `RL_BYZ_EXTRA_CERT_IN` once that file exists.
fn extra_certificate() -> Option<&'static Certificate> {
    if let Some(extra) = EXTRA_CERT.get() {
        return Some(extra);
    }
    let bytes = std::fs::read(std::env::var("RL_BYZ_EXTRA_CERT_IN").ok()?).ok()?;
    let extra = rayls_infrastructure_types::try_decode::<Certificate>(&bytes).ok()?;
    if EXTRA_CERT.set(extra).is_ok() {
        let extra = EXTRA_CERT.get()?;
        warn!(
            target: "primary::byzantine",
            epoch = extra.epoch(),
            author = %extra.origin(),
            "BYZANTINE EXTRA CERT: loaded a certificate made by another node for crash mode"
        );
    }
    EXTRA_CERT.get()
}

fn crash_header(number: u64, honest: &ConsensusHeader) -> Option<ConsensusHeader> {
    let extra = extra_certificate()?;
    if honest.sub_dag.leader_epoch() >= extra.epoch() || honest.sub_dag.num_primary_blocks() == 0 {
        return None;
    }
    let mut sub_dag = honest.sub_dag.clone();
    sub_dag.certificates.push(extra.clone());
    let crafted = ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag,
        number: honest.number,
        extra: honest.extra,
    };
    warn!(
        target: "primary::byzantine",
        number,
        header_epoch = honest.sub_dag.leader_epoch(),
        cert_epoch = extra.epoch(),
        cert_author = %extra.origin(),
        honest_digest = ?honest.digest(),
        crafted_digest = ?crafted.digest(),
        "BYZANTINE GAP-FETCH: serving crash header (real header plus a certificate from a later epoch)"
    );
    Some(crafted)
}

/// True when this node is armed for the "crash-probe" delivery route (the backwards-walk route).
fn crash_probe_armed() -> bool {
    std::env::var("RL_BYZ_GAP_MODE").ok().as_deref() == Some("crash-probe")
}

/// Build the crash-probe poison once and keep it: this node's real block `RL_BYZ_PROBE_NUMBER`,
/// which must be from an older epoch and carry batches, with the extra certificate added. It is
/// served on a follower's "latest" probe and on a by-hash request for its own digest, so the
/// poison reaches the follower through the backwards walk rather than a by-number request.
fn probe_poison<DB: Database>(db: &DB) -> Option<&'static ConsensusHeader> {
    static POISON: OnceLock<ConsensusHeader> = OnceLock::new();
    if let Some(poison) = POISON.get() {
        return Some(poison);
    }
    let number: u64 = std::env::var("RL_BYZ_PROBE_NUMBER").ok()?.parse().ok()?;
    let extra = extra_certificate()?;
    let honest = db.get_consensus_by_number(number)?;
    if honest.sub_dag.leader_epoch() >= extra.epoch() || honest.sub_dag.num_primary_blocks() == 0 {
        warn!(
            target: "primary::byzantine",
            number,
            header_epoch = honest.sub_dag.leader_epoch(),
            cert_epoch = extra.epoch(),
            batches = honest.sub_dag.num_primary_blocks(),
            "BYZANTINE PROBE: block is not a usable crash target (needs an older epoch and batches)"
        );
        return None;
    }
    let mut sub_dag = honest.sub_dag.clone();
    sub_dag.certificates.push(extra.clone());
    let crafted = ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag,
        number: honest.number,
        extra: honest.extra,
    };
    let _ = POISON.set(crafted);
    let poison = POISON.get()?;
    warn!(
        target: "primary::byzantine",
        number,
        cert_epoch = extra.epoch(),
        cert_author = %extra.origin(),
        poison_digest = ?poison.digest(),
        "BYZANTINE PROBE: built crash-probe poison (served on latest-probe and by-hash)"
    );
    Some(poison)
}

/// Returns the poison to serve for a follower's "latest" probe (number and hash both absent), or
/// None to serve the honest latest. Armed only in crash-probe mode.
pub(super) fn override_latest<DB: Database>(db: &DB) -> Option<ConsensusHeader> {
    if !crash_probe_armed() {
        return None;
    }
    let poison = probe_poison(db)?;
    warn!(
        target: "primary::byzantine",
        number = poison.number,
        "BYZANTINE PROBE: serving crash-probe poison on the latest probe"
    );
    Some(poison.clone())
}

/// Returns the poison to serve for a by-hash request whose hash is the poison's own digest, or None
/// to serve the honest header for that hash. Armed only in crash-probe mode.
pub(super) fn override_by_hash<DB: Database>(db: &DB, hash: BlockHash) -> Option<ConsensusHeader> {
    if !crash_probe_armed() {
        return None;
    }
    let poison = probe_poison(db)?;
    if poison.digest() != hash {
        return None;
    }
    warn!(
        target: "primary::byzantine",
        number = poison.number,
        "BYZANTINE PROBE: serving crash-probe poison on a by-hash request"
    );
    Some(poison.clone())
}
