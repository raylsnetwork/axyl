// An example target links its parent crate's full dependency set, so the crate-level
// `unused_crate_dependencies` lint fires for every dep this file happens not to use. Nothing to
// fix in the example itself.
#![allow(unused_crate_dependencies)]

//! Offline validation of the signing-based participation projection (axyl#246) against real
//! committed history from a devnet cold-archive copy.
//!
//! This is a *differential* check, not a descriptive one. For every committed sub-dag it derives
//! the signer set twice and requires the two to be identical:
//!
//! * **new** — `ConsensusHeaderParticipation::from_bytes`, the cheap BCS projection that
//!   `tally_hybrid` uses in production. Never decodes a signature or a payload.
//! * **oracle** — a full `ConsensusHeader` decode followed by `Certificate::signed_by`, the
//!   long-standing path whose correctness is not in question.
//!
//! The projection feeds `state_root` and `withdrawals_root`, so a disagreement is a chain split
//! rather than a wrong reward number. Unit tests pin the layout against synthetic fixtures; only
//! real archive bytes exercise the committee mapping, the bitmap widths and the sub-dag shapes
//! that production actually produces.
//!
//! Also reports the two invariants axyl#246 calls for — every certificate's author appears in its
//! own signer set, and every committed certificate carries at least quorum signers — plus the
//! old (authorship) vs new (signing) per-validator tally the reward change turns on.
//!
//! Usage:
//!   cargo run --release --example validate_signer_projection -p rayls-infrastructure-storage \
//!     -- <cold_dir> [num_epochs]

use rayls_infrastructure_storage::{ColdConfig, ColdStore};
use rayls_infrastructure_types::{
    Address, AuthorityIdentifier, BlsPublicKey, Committee, CommitteeBuilder, ConsensusHeader,
    ConsensusHeaderParticipation,
};
use std::collections::{BTreeMap, BTreeSet};

// (label, base58 protocol_key, execution_address) from devnet committee.yaml
const VALIDATORS: &[(&str, &str, &str)] = &[
    ("Ohio-1", "mzPoKe2RJXCmyUGW9jz9v6ap19sHR93q3BPHhsEptkWyquaVHV3YHvL4WHYQNs8dMJqivScH8nLma8u4oXiy8WJbqKuMEoYUu4VdwZNKhoWrte3W9RSzKJXxjP7xsQmtiGU", "0xf841014f76b85d2c95f48f713b991038f554fffa"),
    ("Ohio-2", "rhwSR1buF7QRXZnAbsTKhuCuH4A6J7n6vknsqRFsksHvKo1gfc7ReXhsSr9bKrL6XFuJaKUTFVs1uR6uB9jJgS6SJ1Z2ZEyiohXYdafeFddye5XiknaaiPKCD3VLByZvAtj", "0x0d77342c5bdf86c28a15a84831dbf1ab33703e2f"),
    ("Oregon-1", "uc2vU7sYb8u1K4q7y3eABdYoTJZK3CAFok8sgiRFJ2Sc3zxyPzrpa3qKPLyJmS1Luahw88ZryNmfqy1u4dQ1X8W7bXf9CGkTmY2kHHf9DNCVfCrTB375JbXrkN7Y4dLtTib", "0xa334a19e859cd78d73adcd4ca51c5778aa445ec6"),
    ("Oregon-2", "uip3Ga9DMQUVqyAXfiPYc44A2u2Ujb47eQ3QS3vdVgHuweEeYpqCz7pL5auAQ1LhioLag7DTXKPvhJ35AG8ANpqDgUNh6h6R7fBZ7RAn3ZU187fUEZc1LTC6SBD5nCdkR4b", "0x3e7f5e0c5b01e1401f64a421a2fd9761957e947d"),
    ("London-1", "zvW92zHtagiEJyW8LM3rXFLfDHvy4XXZwHqGFk81WFUFC5ahTyva5LKJn1uRFudNvbuPFg9Jw8g3Ng8V2ibukRNQkiu6RdQGafwCwWBU1YqyqBuBWmFC2v8AzBm5QJ2CC9m", "0xeb72baa91144bbab1a651a980469c58901a22441"),
    ("Tokyo-1", "22975pr49DnSi43wG9m66EX3mbafMvuAL9UhuvfiaYiKwJV4Rs82wp3HcszpBrdk32vkAtwySgeigcTg2tMzYTzngrcrese99uYrG3YaNNXfvZg2LjmwVQRq4KtNgsMkUEiU", "0xc392ac0247b59f1b925dace1cc90b441deeae6c0"),
    ("London-2", "22iW9Wgng3Eqf5AEhaCkSPZZ8DTvsd9C3Xw3EWUC35M3Zg1VpuzKpY2MHzAhufZRgPGhYWdr7GeoPdsjcyHXXTbHErc7s7MoVVUUESHZu9jgWQ8id2PmqwgSxs6EhS3wDtGJ", "0xe1a31d1c4c490595bae4b0bbde891bfc7efa90c5"),
];

fn build_committee() -> (Committee, BTreeMap<AuthorityIdentifier, &'static str>) {
    let mut builder = CommitteeBuilder::new(0);
    let mut names = BTreeMap::new();
    for (label, b58, addr) in VALIDATORS {
        let bytes = bs58::decode(b58).into_vec().expect("valid base58");
        let pubkey = BlsPublicKey::from_literal_bytes(&bytes).expect("valid bls pubkey");
        let address: Address = addr.parse().expect("valid address");
        builder.add_authority(pubkey, 1, address);
        names.insert(AuthorityIdentifier::from(pubkey), *label);
    }
    (builder.build(), names)
}

#[derive(Default)]
struct EpochStats {
    /// Rows handed to us by the archive, before any filtering. Separates "found no data" from
    /// "found data the projection could not read" — two very different failures that look
    /// identical if you only count successful rounds.
    rows_visited: u64,
    rounds: u32,
    certs: u64,
    /// Rows where the cheap projection and the full-decode oracle disagreed. Must stay 0.
    signer_mismatches: u64,
    /// Rows where the projection's ordered author list disagreed with the oracle's. One entry
    /// per certificate, so unlike the signer union this is sharp at per-certificate granularity:
    /// a walk that mis-measures any certificate's `signed_authorities` span desyncs every
    /// following author. Must stay 0.
    author_sequence_mismatches: u64,
    /// Rounds whose signer union was the entire committee. Reported because a saturated union
    /// cannot detect a per-certificate error — it is already full — so this number is how much
    /// weight the union comparison can actually bear on this data set.
    saturated_rounds: u64,
    /// First mismatch, kept for the report so a failure is diagnosable without a re-run.
    first_mismatch: Option<String>,
    /// Certificates whose own author was absent from their decoded signer set.
    author_not_in_own_signers: u64,
    /// Certificates carrying fewer than quorum signers.
    below_quorum: u64,
    min_signers: usize,
    /// Rounds where the leader certificate was NOT inside `sub_dag.certificates`. The projection
    /// only walks that vector, so a non-zero count means it is missing the leader's signers.
    leader_outside_certificates: u64,
    /// Signer indices that did not resolve to a committee member.
    unmapped_signers: u64,
    /// Run lengths of consecutive rounds in which a validator signed nothing.
    ///
    /// Vote collection is cancelled the moment quorum is reached (`certifier.rs`), so a
    /// certificate carries the first `quorum` responders and a latency-distant validator loses
    /// most of those races. That is only unfair if it costs whole *rounds*, so the shape of the
    /// misses is what matters: one long run is an outage the metric should penalise, while
    /// scattered singletons would be luck-of-the-draw the metric should not.
    miss_streak_open: BTreeMap<AuthorityIdentifier, u64>,
    /// Closed run lengths: validator -> (run length -> how many such runs).
    miss_streak_hist: BTreeMap<AuthorityIdentifier, BTreeMap<u64, u64>>,
    /// How many distinct DAG rounds each committed sub-dag spans.
    ///
    /// The reward credits one unit per committed sub-dag (one `ConsensusBlocks` row), and a
    /// sub-dag carries the leader's uncommitted causal history, so it spans several DAG rounds
    /// rather than one. With N validators a single DAG round holds at most N certificates, so
    /// this is what explains a certificate count above the committee size.
    subdag_round_span: BTreeMap<u64, u64>,
    authored_rounds: BTreeMap<AuthorityIdentifier, u32>,
    signed_rounds: BTreeMap<AuthorityIdentifier, u32>,
    signed_certs: BTreeMap<AuthorityIdentifier, u64>,
    leader_rounds: BTreeMap<AuthorityIdentifier, u32>,
}

fn main() -> eyre::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let cold_dir = args.get(1).cloned().unwrap_or_else(|| ".".to_string());
    let num_epochs: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);

    let (committee, names) = build_committee();
    let bls_keys = committee.bls_keys();
    let quorum = committee.quorum_threshold();

    // The index space the `signed_authorities` bitmap addresses: authorities sorted by BLS
    // public key. Resolving against `authorities()` order instead yields a silent permutation,
    // which is precisely what the author-in-own-signer-set invariant below detects.
    let id_by_index: Vec<AuthorityIdentifier> =
        bls_keys.iter().map(|k| AuthorityIdentifier::from(*k)).collect();
    let id_by_key: BTreeMap<String, AuthorityIdentifier> =
        bls_keys.iter().map(|k| (k.to_string(), AuthorityIdentifier::from(*k))).collect();

    let store = ColdStore::open(&ColdConfig { dir: cold_dir.clone().into() })?;
    let segment = store.consensus_blocks();
    let mut sealed: Vec<u32> = segment.sealed_epochs().into_iter().collect();
    sealed.sort_unstable();
    let targets: Vec<u32> = sealed.iter().rev().take(num_epochs).rev().copied().collect();

    println!("archive: {cold_dir}");
    println!(
        "sealed epochs: {} (first={:?} last={:?}); analyzing {}",
        sealed.len(),
        sealed.first(),
        sealed.last(),
        targets.len(),
    );
    println!("committee: {} validators, quorum_threshold={quorum}\n", committee.size());

    let mut totals = EpochStats { min_signers: usize::MAX, ..Default::default() };

    for epoch in &targets {
        let mut st = EpochStats { min_signers: usize::MAX, ..Default::default() };

        segment.for_each_row_in_epoch_pub(*epoch, |row, bytes| {
            st.rows_visited += 1;
            // ---- new path: the cheap BCS projection tally_hybrid runs in production ----
            let meta = match ConsensusHeaderParticipation::from_bytes(bytes) {
                Ok(m) => m,
                // A row that will not project is itself a finding, not something to skip
                // silently: count it as a mismatch so the run cannot come back clean.
                Err(e) => {
                    st.signer_mismatches += 1;
                    st.first_mismatch
                        .get_or_insert_with(|| format!("epoch {epoch} row {row}: projection failed: {e}"));
                    return Ok(());
                }
            };
            if meta.leader_round == 0 {
                return Ok(()); // genesis carries no leader
            }
            st.rounds += 1;
            *st.leader_rounds.entry(meta.leader_author.clone()).or_default() += 1;

            let new_signers: BTreeSet<AuthorityIdentifier> = meta
                .signers
                .iter()
                .filter_map(|i| {
                    let resolved = id_by_index.get(*i as usize).cloned();
                    if resolved.is_none() {
                        st.unmapped_signers += 1;
                    }
                    resolved
                })
                .collect();

            // ---- oracle: full decode, production signature-resolution path ----
            let header: ConsensusHeader = rayls_infrastructure_types::decode(bytes);
            let leader_key =
                (header.sub_dag.leader.origin().clone(), header.sub_dag.leader.round());
            if !header
                .sub_dag
                .certificates
                .iter()
                .any(|c| (c.origin().clone(), c.round()) == leader_key)
            {
                st.leader_outside_certificates += 1;
            }

            // Ordered, one per certificate: the sharp check. The projection derives these from
            // the same dual-span walk that produces the signer spans, so an author sequence that
            // matches over ~900k certificates is direct evidence the walk measured every
            // `signed_authorities` span exactly right.
            let oracle_authors: Vec<AuthorityIdentifier> =
                header.sub_dag.certificates.iter().map(|c| c.origin().clone()).collect();
            if meta.participants != oracle_authors {
                st.author_sequence_mismatches += 1;
                st.first_mismatch.get_or_insert_with(|| {
                    format!(
                        "epoch {epoch} row {row} round {}: author sequence desync (projection {} entries, oracle {} entries)",
                        meta.leader_round,
                        meta.participants.len(),
                        oracle_authors.len(),
                    )
                });
            }

            let distinct_rounds: BTreeSet<u32> =
                header.sub_dag.certificates.iter().map(|c| c.round()).collect();
            *st.subdag_round_span.entry(distinct_rounds.len() as u64).or_default() += 1;

            let mut oracle_signers: BTreeSet<AuthorityIdentifier> = BTreeSet::new();
            // Union over exactly `sub_dag.certificates` — the same vector the projection walks.
            // Chaining the leader in here would compare two different things and mask a real gap.
            for cert in header.sub_dag.certificates.iter() {
                st.certs += 1;
                let (_weight, signer_keys) = cert.signed_by(&bls_keys);
                st.min_signers = st.min_signers.min(signer_keys.len());
                if (signer_keys.len() as u64) < quorum {
                    st.below_quorum += 1;
                }

                let author = cert.origin();
                let mut author_found = false;
                for key in signer_keys {
                    if let Some(id) = id_by_key.get(&key.to_string()) {
                        if id == author {
                            author_found = true;
                        }
                        *st.signed_certs.entry(id.clone()).or_default() += 1;
                        oracle_signers.insert(id.clone());
                    }
                }
                if !author_found {
                    st.author_not_in_own_signers += 1;
                }
            }

            // ---- the differential assertion ----
            if new_signers != oracle_signers {
                st.signer_mismatches += 1;
                st.first_mismatch.get_or_insert_with(|| {
                    let fmt = |s: &BTreeSet<AuthorityIdentifier>| {
                        s.iter()
                            .map(|id| names.get(id).copied().unwrap_or("?").to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    format!(
                        "epoch {epoch} row {row} round {}: new=[{}] oracle=[{}]",
                        meta.leader_round,
                        fmt(&new_signers),
                        fmt(&oracle_signers),
                    )
                });
            }

            if oracle_signers.len() == id_by_index.len() {
                st.saturated_rounds += 1;
            }

            // Rows arrive in ascending row order within an epoch, so consecutive misses are
            // genuinely consecutive rounds. Streaks are closed at the epoch boundary below.
            for id in &id_by_index {
                if oracle_signers.contains(id) {
                    if let Some(run) = st.miss_streak_open.remove(id) {
                        *st.miss_streak_hist
                            .entry(id.clone())
                            .or_default()
                            .entry(run)
                            .or_default() += 1;
                    }
                } else {
                    *st.miss_streak_open.entry(id.clone()).or_default() += 1;
                }
            }
            for id in &oracle_signers {
                *st.signed_rounds.entry(id.clone()).or_default() += 1;
            }
            let mut authored = BTreeSet::new();
            for author in &meta.participants {
                if authored.insert(author.clone()) {
                    *st.authored_rounds.entry(author.clone()).or_default() += 1;
                }
            }
            Ok(())
        })?;

        // Close any run still open when the epoch ends.
        let open: Vec<(AuthorityIdentifier, u64)> =
            st.miss_streak_open.iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (id, run) in open {
            *st.miss_streak_hist.entry(id).or_default().entry(run).or_default() += 1;
        }
        st.miss_streak_open.clear();

        print_epoch(*epoch, &st, &names, quorum);
        merge(&mut totals, st);
    }

    println!("================ SUMMARY over {} epochs ================", targets.len());
    print_epoch_header(&totals, quorum);
    println!();
    println!(
        "{:<10} {:>14} {:>14} {:>14}",
        "validator", "authored%", "signed-round%", "signed-cert%"
    );
    for (id, name) in &names {
        let a =
            pct(totals.authored_rounds.get(id).copied().unwrap_or(0) as u64, totals.rounds as u64);
        let sr =
            pct(totals.signed_rounds.get(id).copied().unwrap_or(0) as u64, totals.rounds as u64);
        let sc = pct(totals.signed_certs.get(id).copied().unwrap_or(0), totals.certs);
        println!("{name:<10} {a:>13.2}% {sr:>13.2}% {sc:>13.2}%");
    }
    println!();

    // What a credited unit actually spans. A committed sub-dag carries the leader's uncommitted
    // causal history, so "certificates per credited round" is not bounded by the committee size.
    let total_spans: u64 = totals.subdag_round_span.values().sum();
    let weighted: u64 = totals.subdag_round_span.iter().map(|(s, c)| s * c).sum();
    println!("---- what one credited round spans ----");
    println!(
        "committee={}  certs/sub-dag={:.2}  DAG rounds/sub-dag={:.2}  certs per DAG round={:.2}",
        id_by_index.len(),
        totals.certs as f64 / totals.rounds as f64,
        weighted as f64 / total_spans.max(1) as f64,
        (totals.certs as f64 / totals.rounds as f64)
            / (weighted as f64 / total_spans.max(1) as f64),
    );
    print!("distribution of DAG rounds per sub-dag:");
    for (span, count) in &totals.subdag_round_span {
        print!(" {span}:{count}");
    }
    println!("\n");

    println!("---- shape of the signing gaps (axyl#279 review question) ----");
    print_miss_shape(&totals, &names);
    println!();

    // An empty archive must never report PASS: a validation that proves nothing is worse than no
    // validation, because it gets quoted as evidence.
    if totals.rows_visited == 0 {
        eyre::bail!(
            "no rows found in the archive — check the path (expects a dir containing `consensus_blocks/`)",
        );
    }
    if totals.rounds == 0 || totals.certs == 0 {
        println!("RESULT: FAIL");
        if let Some(m) = &totals.first_mismatch {
            println!("  first failure: {m}");
        }
        eyre::bail!(
            "archive returned {} rows but produced {} usable rounds / {} certificates: \
             the projection failed to read real stored headers ({} projection failures)",
            totals.rows_visited,
            totals.rounds,
            totals.certs,
            totals.signer_mismatches,
        );
    }

    let ok = totals.signer_mismatches == 0
        && totals.author_sequence_mismatches == 0
        && totals.unmapped_signers == 0
        && totals.author_not_in_own_signers == 0
        && totals.below_quorum == 0
        && totals.leader_outside_certificates == 0;
    if ok {
        println!(
            "RESULT: PASS — projection agrees with full decode on all {} rounds / {} certificates",
            totals.rounds, totals.certs,
        );
        Ok(())
    } else {
        println!("RESULT: FAIL");
        if let Some(m) = &totals.first_mismatch {
            println!("  first mismatch: {m}");
        }
        eyre::bail!(
            "validation failed: {} author-sequence, {} signer-union, {} unmapped, {} author-not-in-signers, {} below-quorum, {} leader-outside-certs",
            totals.author_sequence_mismatches,
            totals.signer_mismatches,
            totals.unmapped_signers,
            totals.author_not_in_own_signers,
            totals.below_quorum,
            totals.leader_outside_certificates,
        )
    }
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        100.0 * n as f64 / d as f64
    }
}

fn print_epoch_header(st: &EpochStats, quorum: u64) {
    println!(
        "rounds={} certs={} min_signers_per_cert={} (quorum={quorum})",
        st.rounds,
        st.certs,
        if st.min_signers == usize::MAX { 0 } else { st.min_signers },
    );
    println!(
        "  author_sequence_mismatches={}  signer_union_mismatches={}  unmapped_signers={}",
        st.author_sequence_mismatches, st.signer_mismatches, st.unmapped_signers,
    );
    println!(
        "  signer_union_saturated_rounds={}/{} ({:.1}% — union check is blind above this)",
        st.saturated_rounds,
        st.rounds,
        pct(st.saturated_rounds, st.rounds as u64),
    );
    println!(
        "  author_not_in_own_signer_set={}  certs_below_quorum={}  leader_outside_certificates={}",
        st.author_not_in_own_signers, st.below_quorum, st.leader_outside_certificates,
    );
}

fn print_epoch(
    epoch: u32,
    st: &EpochStats,
    names: &BTreeMap<AuthorityIdentifier, &'static str>,
    quorum: u64,
) {
    print!("=== epoch {epoch} === ");
    print_epoch_header(st, quorum);
    for (id, name) in names {
        let a = pct(st.authored_rounds.get(id).copied().unwrap_or(0) as u64, st.rounds as u64);
        let sr = pct(st.signed_rounds.get(id).copied().unwrap_or(0) as u64, st.rounds as u64);
        let sc = pct(st.signed_certs.get(id).copied().unwrap_or(0), st.certs);
        let l = st.leader_rounds.get(id).copied().unwrap_or(0);
        println!(
            "  {name:<10} authored={a:6.2}%  signed_rounds={sr:6.2}%  signed_certs={sc:6.2}%  leader_rounds={l}"
        );
    }
    if let Some(m) = &st.first_mismatch {
        println!("  !! {m}");
    }
    println!();
}

fn merge(dst: &mut EpochStats, src: EpochStats) {
    dst.rows_visited += src.rows_visited;
    dst.rounds += src.rounds;
    dst.certs += src.certs;
    dst.signer_mismatches += src.signer_mismatches;
    dst.author_sequence_mismatches += src.author_sequence_mismatches;
    dst.saturated_rounds += src.saturated_rounds;
    dst.author_not_in_own_signers += src.author_not_in_own_signers;
    dst.below_quorum += src.below_quorum;
    dst.min_signers = dst.min_signers.min(src.min_signers);
    dst.leader_outside_certificates += src.leader_outside_certificates;
    dst.unmapped_signers += src.unmapped_signers;
    if dst.first_mismatch.is_none() {
        dst.first_mismatch = src.first_mismatch;
    }
    for (k, v) in src.authored_rounds {
        *dst.authored_rounds.entry(k).or_default() += v;
    }
    for (k, v) in src.signed_rounds {
        *dst.signed_rounds.entry(k).or_default() += v;
    }
    for (k, v) in src.signed_certs {
        *dst.signed_certs.entry(k).or_default() += v;
    }
    for (k, v) in src.leader_rounds {
        *dst.leader_rounds.entry(k).or_default() += v;
    }
    for (span, count) in src.subdag_round_span {
        *dst.subdag_round_span.entry(span).or_default() += count;
    }
    for (id, hist) in src.miss_streak_hist {
        let into = dst.miss_streak_hist.entry(id).or_default();
        for (run, count) in hist {
            *into.entry(run).or_default() += count;
        }
    }
}

/// Reports whether validators' signing gaps look like outages or like lost races.
///
/// Answers the review question on axyl#279: vote collection is cancelled at quorum, so a
/// latency-distant validator loses most per-certificate races. This shows whether that ever
/// costs it a whole round, and if so whether those rounds cluster (an outage, correctly
/// penalised) or scatter (luck, which would be unfair).
fn print_miss_shape(totals: &EpochStats, names: &BTreeMap<AuthorityIdentifier, &'static str>) {
    println!(
        "{:<10} {:>8} {:>10} {:>11} {:>10} {:>9}",
        "validator", "missed", "of rounds", "singletons", "longest", "runs"
    );
    for (id, name) in names {
        let hist = match totals.miss_streak_hist.get(id) {
            Some(h) if !h.is_empty() => h,
            _ => {
                println!("{name:<10} {:>8} {:>10} {:>11} {:>10} {:>9}", 0, totals.rounds, 0, 0, 0);
                continue;
            }
        };
        let runs: u64 = hist.values().sum();
        let missed: u64 = hist.iter().map(|(len, count)| len * count).sum();
        let singletons = hist.get(&1).copied().unwrap_or(0);
        let longest = hist.keys().next_back().copied().unwrap_or(0);
        println!(
            "{name:<10} {missed:>8} {:>10} {singletons:>11} {longest:>10} {runs:>9}",
            totals.rounds
        );
    }
}
