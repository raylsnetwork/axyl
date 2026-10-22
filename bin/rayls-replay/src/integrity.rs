//! Cross-reference the replay against the consensus DB's committed epoch anchors.
//!
//! Confirms the snapshot's consensus and execution DBs agree before trusting it
//! as the replay oracle, and confirms each replayed boundary block matches its
//! BFT-signed `EpochRecord.parent_state` anchor.

use rayls_execution_evm::reth_env::RethEnv;
use rayls_infrastructure_storage::{
    tables::{EpochCerts, EpochRecords},
    EpochStore as _,
};
use rayls_infrastructure_types::{Database, Epoch, EpochRecord, RecordKeys, SealedHeader, B256};
use std::collections::BTreeMap;
use tracing::{error, info, warn};

/// A BFT-committed epoch boundary read from the consensus DB.
#[derive(Debug, Clone, Copy)]
pub struct EpochAnchor {
    /// Epoch whose record carries this checkpoint.
    pub epoch: Epoch,
    /// Committed number of the epoch's last execution block (`parent_state.number`).
    pub number: u64,
    /// Committed hash of that block (`parent_state.hash`).
    pub hash: B256,
    /// Boundary certificate present and BLS super-quorum verified.
    pub cert_verified: bool,
}

/// Aggregate outcome of a consensus<->execution cross-reference pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct AgreementSummary {
    /// Anchors with a corresponding execution header that was compared.
    pub checked: u64,
    /// Anchors whose execution header hash matched the committed hash.
    pub agreed: u64,
    /// Anchors whose execution header hash differed from the committed hash.
    pub disagreed: u64,
    /// Anchors with no execution header (or a read error) to compare against.
    pub missing: u64,
}

/// Load committed execution anchors from the consensus DB for the `[from, to]`
/// replay window, keyed by EVM block number for O(1) lookup during replay.
///
/// Walks the full EpochRecord hash chain (each record's `parent_hash` equals the
/// prior record's digest) to surface tamper or gaps, but only fetches the
/// boundary certificate and runs the BLS super-quorum verification for anchors
/// inside the window; out-of-window boundaries are never looked up during replay
/// and the BLS check is the dominant cost.
pub fn load_epoch_anchors<DB: Database>(
    consensus_store: &DB,
    from: u64,
    to: u64,
) -> BTreeMap<u64, EpochAnchor> {
    // single ordered pass: lightweight linkage tuple per epoch, whole records kept
    // only for in-window anchors (avoids materializing every EpochRecord at once)
    let mut chain: BTreeMap<Epoch, (B256, B256)> = BTreeMap::new();
    let mut windowed: Vec<(Epoch, EpochRecord, B256)> = Vec::new();
    let mut epoch_count = 0u64;
    for (epoch, record) in consensus_store.iter::<EpochRecords>() {
        epoch_count += 1;
        let digest = record.digest();
        // (digest, parent_hash) is everything the linkage walk needs
        chain.insert(epoch, (digest, record.parent_hash));

        // genesis / dummy epoch-0 record carries no real execution anchor
        let number = record.parent_state.number;
        if number != 0 && number >= from && number <= to {
            windowed.push((epoch, record, digest));
        }
    }

    // walk the hash chain in epoch order (BTreeMap iterates sorted) to surface
    // tamper or gaps; warning-only, never gates the anchors built below
    let mut prev: Option<(Epoch, B256)> = None;
    for (epoch, (digest, parent_hash)) in &chain {
        if let Some((prev_epoch, prev_digest)) = prev {
            if *epoch == prev_epoch + 1 && *parent_hash != prev_digest {
                warn!(
                    target: "rayls_replay::integrity",
                    epoch = *epoch,
                    parent_hash = ?parent_hash,
                    expected = ?prev_digest,
                    "epoch record chain linkage broken (parent_hash != prior record digest)"
                );
            }
        }
        prev = Some((*epoch, *digest));
    }

    // fetch the cert by the already-computed digest and BLS super-quorum verify,
    // for in-window anchors only (the BLS check is the dominant cost)
    let mut anchors = BTreeMap::new();
    let mut cert_verified_count = 0u64;
    for (epoch, record, digest) in windowed {
        let cert_verified = consensus_store
            .get::<EpochCerts>(&digest)
            .ok()
            .flatten()
            .map(|cert| {
                // what the certificate signs besides the record, and the epoch's key when it
                // uses threshold certificates, as the consensus DB holds them
                let keys = RecordKeys {
                    binding: consensus_store.get_record_binding(digest),
                    group_key: consensus_store.epoch_group_key(epoch).flatten(),
                };
                record.verify_with_cert(&cert, &keys)
            })
            .unwrap_or(false);
        if cert_verified {
            cert_verified_count += 1;
        }
        let number = record.parent_state.number;
        anchors.insert(
            number,
            EpochAnchor { epoch, number, hash: record.parent_state.hash, cert_verified },
        );
    }

    info!(
        target: "rayls_replay::integrity",
        epochs = epoch_count,
        anchors = anchors.len(),
        cert_verified = cert_verified_count,
        "loaded consensus-DB epoch anchors"
    );
    anchors
}

/// Cross-reference the snapshot's execution DB against the consensus DB's
/// committed epoch anchors.
///
/// Confirms the snapshot is internally consistent (its two databases agree) before
/// the replay trusts it as the state-root oracle. A disagreement here means the
/// SNAPSHOT itself committed a fork, independent of any replay computation.
pub fn cross_check_snapshot(
    snapshot_evm: &RethEnv,
    anchors: &BTreeMap<u64, EpochAnchor>,
) -> AgreementSummary {
    let mut summary = AgreementSummary::default();
    for anchor in anchors.values() {
        match snapshot_evm.sealed_header_by_number(anchor.number) {
            Ok(Some(header)) => {
                summary.checked += 1;
                if header.hash() == anchor.hash {
                    summary.agreed += 1;
                } else {
                    summary.disagreed += 1;
                    error!(
                        target: "rayls_replay::integrity",
                        epoch = anchor.epoch,
                        block_number = anchor.number,
                        consensus_hash = ?anchor.hash,
                        execution_hash = ?header.hash(),
                        cert_verified = anchor.cert_verified,
                        "consensus<->execution MISMATCH in snapshot: committed boundary != snapshot EVM header"
                    );
                }
            }
            Ok(None) => {
                summary.missing += 1;
                warn!(
                    target: "rayls_replay::integrity",
                    epoch = anchor.epoch,
                    block_number = anchor.number,
                    "snapshot EVM header missing for committed epoch anchor"
                );
            }
            Err(e) => {
                summary.missing += 1;
                warn!(
                    target: "rayls_replay::integrity",
                    block_number = anchor.number,
                    %e,
                    "failed to read snapshot header for epoch anchor"
                );
            }
        }
    }
    info!(
        target: "rayls_replay::integrity",
        checked = summary.checked,
        agreed = summary.agreed,
        disagreed = summary.disagreed,
        missing = summary.missing,
        "snapshot consensus<->execution cross-reference complete"
    );
    summary
}

/// Verify a freshly replayed boundary block against its consensus commitment.
///
/// Returns true on agreement. Logs at error on mismatch (the replay diverged from
/// the BFT-committed boundary) and at info on agreement.
pub fn check_replay_anchor(anchor: &EpochAnchor, replayed: &SealedHeader) -> bool {
    if replayed.hash() == anchor.hash {
        info!(
            target: "rayls_replay::integrity",
            epoch = anchor.epoch,
            block_number = anchor.number,
            cert_verified = anchor.cert_verified,
            hash = ?anchor.hash,
            "replay matches consensus-committed epoch anchor"
        );
        true
    } else {
        error!(
            target: "rayls_replay::integrity",
            epoch = anchor.epoch,
            block_number = anchor.number,
            consensus_hash = ?anchor.hash,
            replay_hash = ?replayed.hash(),
            cert_verified = anchor.cert_verified,
            "replay DIVERGES from consensus-committed epoch anchor (BFT boundary)"
        );
        false
    }
}
