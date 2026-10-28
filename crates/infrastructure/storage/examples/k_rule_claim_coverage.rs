// An example target links its parent crate's full dependency set, so the crate-level
// `unused_crate_dependencies` lint fires for every dep this file happens not to use.
#![allow(unused_crate_dependencies)]

//! Measures the header-claim participation design (axyl#310, specified by @kstoykov on axyl#279)
//! against real committed history.
//!
//! The rule being modelled: a proposer carries claims for its own unclaimed certified headers
//! from the last unclaimed sequence through `current - k`, oldest-first, at most `M` per header;
//! epoch `E` rewards are computed at `E+1` close, so a claim landing in `E+1` still counts.
//!
//! **This model is deliberately built so it can fail.** An earlier version could not: it indexed
//! each author's headers by dense ordinal position, so a backlog could never form, `M` was
//! trivially satisfied, and the unclaimed tail was forced to exactly `k` per author per epoch
//! regardless of the data. Three things changed:
//!
//! * `k` counts **global rounds**, not an author's own header count. An author that skips rounds
//!   now accumulates a real backlog, which is what makes `M` a live constraint.
//! * Claims may cross one epoch boundary, matching the `E+1` cutoff the spec defines.
//! * Backlog depth is reported uncapped, and the tail is split into "never became eligible"
//!   versus "was eligible but `M` starved it" — two different failures with different fixes.
//!
//! A self-test with hand-computed answers runs first. If the instrument is wrong the run aborts
//! rather than printing numbers.
//!
//! Usage:
//!   cargo run --release --example k_rule_claim_coverage -p rayls-infrastructure-storage \
//!     -- <cold_dir> [num_epochs]

use rayls_infrastructure_storage::{ColdConfig, ColdStore};
use rayls_infrastructure_types::{
    Address, AuthorityIdentifier, BlsPublicKey, Committee, CommitteeBuilder, ConsensusHeader,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// (label, base58 protocol_key, execution_address) from devnet committee.yaml.
// Devnet-specific: against another archive no signer resolves.
const VALIDATORS: &[(&str, &str, &str)] = &[
    ("Ohio-1", "mzPoKe2RJXCmyUGW9jz9v6ap19sHR93q3BPHhsEptkWyquaVHV3YHvL4WHYQNs8dMJqivScH8nLma8u4oXiy8WJbqKuMEoYUu4VdwZNKhoWrte3W9RSzKJXxjP7xsQmtiGU", "0xf841014f76b85d2c95f48f713b991038f554fffa"),
    ("Ohio-2", "rhwSR1buF7QRXZnAbsTKhuCuH4A6J7n6vknsqRFsksHvKo1gfc7ReXhsSr9bKrL6XFuJaKUTFVs1uR6uB9jJgS6SJ1Z2ZEyiohXYdafeFddye5XiknaaiPKCD3VLByZvAtj", "0x0d77342c5bdf86c28a15a84831dbf1ab33703e2f"),
    ("Oregon-1", "uc2vU7sYb8u1K4q7y3eABdYoTJZK3CAFok8sgiRFJ2Sc3zxyPzrpa3qKPLyJmS1Luahw88ZryNmfqy1u4dQ1X8W7bXf9CGkTmY2kHHf9DNCVfCrTB375JbXrkN7Y4dLtTib", "0xa334a19e859cd78d73adcd4ca51c5778aa445ec6"),
    ("Oregon-2", "uip3Ga9DMQUVqyAXfiPYc44A2u2Ujb47eQ3QS3vdVgHuweEeYpqCz7pL5auAQ1LhioLag7DTXKPvhJ35AG8ANpqDgUNh6h6R7fBZ7RAn3ZU187fUEZc1LTC6SBD5nCdkR4b", "0x3e7f5e0c5b01e1401f64a421a2fd9761957e947d"),
    ("London-1", "zvW92zHtagiEJyW8LM3rXFLfDHvy4XXZwHqGFk81WFUFC5ahTyva5LKJn1uRFudNvbuPFg9Jw8g3Ng8V2ibukRNQkiu6RdQGafwCwWBU1YqyqBuBWmFC2v8AzBm5QJ2CC9m", "0xeb72baa91144bbab1a651a980469c58901a22441"),
    ("Tokyo-1", "22975pr49DnSi43wG9m66EX3mbafMvuAL9UhuvfiaYiKwJV4Rs82wp3HcszpBrdk32vkAtwySgeigcTg2tMzYTzngrcrese99uYrG3YaNNXfvZg2LjmwVQRq4KtNgsMkUEiU", "0xc392ac0247b59f1b925dace1cc90b441deeae6c0"),
    ("London-2", "22iW9Wgng3Eqf5AEhaCkSPZZ8DTvsd9C3Xw3EWUC35M3Zg1VpuzKpY2MHzAhufZRgPGhYWdr7GeoPdsjcyHXXTbHErc7s7MoVVUUESHZu9jgWQ8id2PmqwgSxs6EhS3wDtGJ", "0xe1a31d1c4c490595bae4b0bbde891bfc7efa90c5"),
];

/// One committed certificate. `signers` is a bitmask over committee index (BLS-key order).
#[derive(Clone)]
struct Cert {
    epoch: u32,
    /// Monotonic round across the whole archive. Rounds reset per epoch, so a raw round cannot
    /// express "k rounds later" across a boundary.
    global_round: u64,
    author: usize,
    signers: u64,
    subdag: usize,
}

#[derive(Default, Debug, PartialEq)]
struct Stats {
    headers: u64,
    claimed: u64,
    /// Target never reached `k` rounds of age before its cutoff.
    tail_never_eligible: u64,
    /// Target was eligible but every eligible header had already spent its `M` budget.
    tail_m_starved: u64,
    /// Eligible-targets-at-hand per header, recorded before the cap is applied, so `M` pressure
    /// is visible rather than hidden by the cap.
    backlog_hist: BTreeMap<usize, u64>,
    max_pending: usize,
}

/// Runs the claim rule.
///
/// `k` is the age requirement in global rounds, `m` the per-header claim cap, `carry_epochs` how
/// many epoch boundaries a claim may cross (0 = same epoch only, 1 = the `E+1` cutoff).
/// `credit` receives (subdag, signer-mask) for every claim that lands.
fn simulate(
    certs: &[Cert],
    k: u64,
    m: usize,
    carry_epochs: u32,
    mut credit: impl FnMut(usize, u64),
) -> Stats {
    let mut by_author: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (i, c) in certs.iter().enumerate() {
        by_author.entry(c.author).or_default().push(i);
    }
    let mut st = Stats::default();

    for idxs in by_author.values_mut() {
        idxs.sort_by_key(|i| (certs[*i].global_round, certs[*i].subdag));
        st.headers += idxs.len() as u64;

        // Targets awaiting a claim, oldest first.
        let mut pending: VecDeque<usize> = VecDeque::new();
        // Targets that became eligible at some point, so an eventual loss is attributable to `M`
        // rather than to age.
        let mut eligible_seen: BTreeSet<usize> = BTreeSet::new();

        for &h in idxs.iter() {
            let here_epoch = certs[h].epoch;
            let here_round = certs[h].global_round;
            let here_subdag = certs[h].subdag;

            // Expire targets past their cutoff: a target in epoch E must be claimed by a header
            // in epoch <= E + carry_epochs.
            while let Some(&front) = pending.front() {
                if certs[front].epoch + carry_epochs < here_epoch {
                    pending.pop_front();
                    if eligible_seen.contains(&front) {
                        st.tail_m_starved += 1;
                    } else {
                        st.tail_never_eligible += 1;
                    }
                } else {
                    break;
                }
            }

            // Eligible right now (age >= k), counted before the cap.
            let eligible_now =
                pending.iter().take_while(|&&t| certs[t].global_round + k <= here_round).count();
            for &t in pending.iter().take(eligible_now) {
                eligible_seen.insert(t);
            }
            *st.backlog_hist.entry(eligible_now).or_default() += 1;
            st.max_pending = st.max_pending.max(pending.len());

            let take = eligible_now.min(m);
            for _ in 0..take {
                let t = pending.pop_front().expect("eligible implies present");
                credit(here_subdag, certs[t].signers);
                st.claimed += 1;
            }

            pending.push_back(h);
        }

        // Anything still pending when the author stops proposing is lost.
        for t in pending {
            if eligible_seen.contains(&t) {
                st.tail_m_starved += 1;
            } else {
                st.tail_never_eligible += 1;
            }
        }
    }
    st
}

/// Hand-computed cases. If these do not hold, the instrument is wrong and the numbers are noise.
fn self_test() -> Result<(), String> {
    let mk = |rounds: &[u64]| -> Vec<Cert> {
        rounds
            .iter()
            .enumerate()
            .map(|(i, r)| Cert { epoch: 0, global_round: *r, author: 0, signers: 1, subdag: i })
            .collect()
    };

    // Dense rounds 0..=4, k=2, M=1.
    // g=2 drains g=0; g=3 drains g=1; g=4 drains g=2. Claimed 3; g=3 and g=4 never reach age 2.
    let s = simulate(&mk(&[0, 1, 2, 3, 4]), 2, 1, 0, |_, _| {});
    if s.claimed != 3 || s.tail_never_eligible != 2 || s.tail_m_starved != 0 {
        return Err(format!("dense case: {s:?}"));
    }

    // Gap: rounds 0,1,2,100 with k=2, M=1.
    // g=2 drains g=0. At g=100 both g=1 and g=2 are eligible but M=1 takes only g=1, so g=2 is
    // starved; g=100 itself never reaches age.
    let s = simulate(&mk(&[0, 1, 2, 100]), 2, 1, 0, |_, _| {});
    if s.claimed != 2 || s.tail_m_starved != 1 || s.tail_never_eligible != 1 {
        return Err(format!("gap case M=1: {s:?}"));
    }
    if s.backlog_hist.get(&2).copied().unwrap_or(0) != 1 {
        return Err(format!("gap case should see a backlog of 2: {:?}", s.backlog_hist));
    }

    // Same gap, M unbounded: g=100 drains both, so nothing is starved.
    let s = simulate(&mk(&[0, 1, 2, 100]), 2, usize::MAX, 0, |_, _| {});
    if s.claimed != 3 || s.tail_m_starved != 0 || s.tail_never_eligible != 1 {
        return Err(format!("gap case M=inf: {s:?}"));
    }

    // Epoch cutoff: a target in epoch 0 cannot be claimed by a header two epochs later.
    let certs = vec![
        Cert { epoch: 0, global_round: 0, author: 0, signers: 1, subdag: 0 },
        Cert { epoch: 2, global_round: 50, author: 0, signers: 1, subdag: 1 },
    ];
    let s = simulate(&certs, 1, 1, 1, |_, _| {});
    if s.claimed != 0 || s.tail_never_eligible + s.tail_m_starved != 2 {
        return Err(format!("cutoff case: {s:?}"));
    }
    Ok(())
}

fn build_committee() -> (Committee, Vec<&'static str>) {
    let mut builder = CommitteeBuilder::new(0);
    for (_l, b58, addr) in VALIDATORS {
        let bytes = bs58::decode(b58).into_vec().expect("valid base58");
        let pubkey = BlsPublicKey::from_literal_bytes(&bytes).expect("valid bls pubkey");
        let address: Address = addr.parse().expect("valid address");
        builder.add_authority(pubkey, 1, address);
    }
    let committee = builder.build();
    // Label by committee index (BLS-key order) — the index space the bitmask uses.
    let labels = committee
        .bls_keys()
        .iter()
        .map(|k| {
            let id = AuthorityIdentifier::from(*k);
            VALIDATORS
                .iter()
                .find(|(_, b58, _)| {
                    let b = bs58::decode(b58).into_vec().unwrap();
                    AuthorityIdentifier::from(BlsPublicKey::from_literal_bytes(&b).unwrap()) == id
                })
                .map(|(l, _, _)| *l)
                .unwrap_or("?")
        })
        .collect();
    (committee, labels)
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        100.0 * n as f64 / d as f64
    }
}

fn main() -> eyre::Result<()> {
    if let Err(e) = self_test() {
        eyre::bail!("SELF-TEST FAILED, refusing to report numbers: {e}");
    }
    println!("self-test: PASS (dense, gap/M=1, gap/M=inf, epoch cutoff)\n");

    let args: Vec<String> = std::env::args().collect();
    let cold_dir = args.get(1).cloned().unwrap_or_else(|| ".".to_string());
    let num_epochs: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);

    let (committee, labels) = build_committee();
    let bls_keys = committee.bls_keys();
    let idx_of: BTreeMap<String, usize> =
        bls_keys.iter().enumerate().map(|(i, k)| (k.to_string(), i)).collect();
    let author_slot: BTreeMap<AuthorityIdentifier, usize> =
        bls_keys.iter().enumerate().map(|(i, k)| (AuthorityIdentifier::from(*k), i)).collect();

    let store = ColdStore::open(&ColdConfig { dir: cold_dir.clone().into() })?;
    let segment = store.consensus_blocks();
    let mut sealed: Vec<u32> = segment.sealed_epochs().into_iter().collect();
    sealed.sort_unstable();
    let targets: Vec<u32> = sealed.iter().rev().take(num_epochs).rev().copied().collect();

    // Pass 1: collect every committed certificate.
    let mut raw: Vec<(u32, u32, usize, u64, usize)> = Vec::new();
    let mut subdag = 0usize;
    let mut unresolved_authors: u64 = 0;
    for epoch in &targets {
        segment.for_each_row_in_epoch_pub(*epoch, |_row, bytes| {
            let header: ConsensusHeader = rayls_infrastructure_types::decode(bytes);
            if header.sub_dag.leader.round() == 0 {
                return Ok(());
            }
            for cert in header.sub_dag.certificates.iter() {
                let Some(a) = author_slot.get(cert.origin()).copied() else {
                    unresolved_authors += 1;
                    continue;
                };
                let mut mask = 0u64;
                for key in cert.signed_by(&bls_keys).1 {
                    if let Some(i) = idx_of.get(&key.to_string()) {
                        mask |= 1u64 << i;
                    }
                }
                raw.push((*epoch, cert.round(), a, mask, subdag));
            }
            subdag += 1;
            Ok(())
        })?;
    }
    let total_subdags = subdag;

    // Global monotonic round: dense rank over distinct (epoch, round) pairs.
    let mut pairs: Vec<(u32, u32)> = raw.iter().map(|r| (r.0, r.1)).collect();
    pairs.sort_unstable();
    pairs.dedup();
    let grank: BTreeMap<(u32, u32), u64> =
        pairs.iter().enumerate().map(|(i, p)| (*p, i as u64)).collect();

    let certs: Vec<Cert> = raw
        .iter()
        .map(|(e, r, a, mask, sd)| Cert {
            epoch: *e,
            global_round: grank[&(*e, *r)],
            author: *a,
            signers: *mask,
            subdag: *sd,
        })
        .collect();

    println!("archive: {cold_dir}");
    println!(
        "epochs {}  sub-dags {}  certificates {}  distinct global rounds {}  unresolved authors {}\n",
        targets.len(),
        total_subdags,
        certs.len(),
        pairs.len(),
        unresolved_authors,
    );

    // How sparse is each author in round space? Sparsity is what creates backlog.
    println!("Author density in global-round space (gap = rounds between own consecutive headers)");
    println!(
        "{:<10} {:>9} {:>9} {:>9} {:>9}",
        "validator", "headers", "gap p50", "gap p99", "gap max"
    );
    let mut by_author: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for c in &certs {
        by_author.entry(c.author).or_default().push(c.global_round);
    }
    for (a, gs) in by_author.iter_mut() {
        gs.sort_unstable();
        let mut gaps: Vec<u64> = gs.windows(2).map(|w| w[1] - w[0]).collect();
        gaps.sort_unstable();
        let p = |q: f64| -> u64 {
            if gaps.is_empty() {
                0
            } else {
                gaps[((gaps.len() as f64 - 1.0) * q) as usize]
            }
        };
        println!(
            "{:<10} {:>9} {:>9} {:>9} {:>9}",
            labels.get(*a).copied().unwrap_or("?"),
            gs.len(),
            p(0.50),
            p(0.99),
            gaps.last().copied().unwrap_or(0)
        );
    }

    println!("\nCoverage under the k rule (k in global rounds, E+1 carry-over allowed)");
    println!(
        "{:<4} {:>4} {:>13} {:>9} {:>12} {:>11} {:>12}",
        "k", "M", "claimed", "claimed%", "never-elig", "M-starved", "max pending"
    );
    for (k, m) in
        [(1u64, 1usize), (3, 1), (10, 1), (3, 4), (10, 4), (3, usize::MAX), (10, usize::MAX)]
    {
        let st = simulate(&certs, k, m, 1, |_, _| {});
        let mlabel = if m == usize::MAX { "inf".to_string() } else { m.to_string() };
        println!(
            "{k:<4} {mlabel:>4} {:>13} {:>8.2}% {:>12} {:>11} {:>12}",
            st.claimed,
            pct(st.claimed, st.headers),
            st.tail_never_eligible,
            st.tail_m_starved,
            st.max_pending,
        );
    }

    // Sensitivity: the same sweep with the E+1 carry-over removed, so a claim dies at the close
    // of its own epoch. The backlog that carry-over silently absorbs has to reappear here as
    // M-starvation; if it does not, the simulator is not actually modelling the cap.
    println!("
Sensitivity: carry-over disabled (claims expire at their own epoch close)");
    println!(
        "{:<4} {:>4} {:>13} {:>9} {:>12} {:>11} {:>12}",
        "k", "M", "claimed", "claimed%", "never-elig", "M-starved", "max pending"
    );
    for (k, m) in
        [(1u64, 1usize), (3, 1), (10, 1), (3, 4), (10, 4), (3, usize::MAX), (10, usize::MAX)]
    {
        let st = simulate(&certs, k, m, 0, |_, _| {});
        let mlabel = if m == usize::MAX { "inf".to_string() } else { m.to_string() };
        println!(
            "{k:<4} {mlabel:>4} {:>13} {:>8.2}% {:>12} {:>11} {:>12}",
            st.claimed,
            pct(st.claimed, st.headers),
            st.tail_never_eligible,
            st.tail_m_starved,
            st.max_pending,
        );
    }

    for (k, m) in [(1u64, 1usize), (3, 1), (10, 4)] {
        let mut credited: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); total_subdags];
        let st = simulate(&certs, k, m, 1, |sd, mask| {
            for i in 0..bls_keys.len() {
                if mask & (1u64 << i) != 0 {
                    credited[sd].insert(i);
                }
            }
        });
        let mut counts = vec![0u64; bls_keys.len()];
        for s in &credited {
            for i in s {
                counts[*i] += 1;
            }
        }
        let mlabel = if m == usize::MAX { "inf".to_string() } else { m.to_string() };
        println!("\nPer-validator credit, k={k} M={mlabel} (share of {total_subdags} sub-dags)");
        for (i, c) in counts.iter().enumerate() {
            println!(
                "  {:<10} {:>7.2}%",
                labels.get(i).copied().unwrap_or("?"),
                pct(*c, total_subdags as u64)
            );
        }
        println!("  (claimed {} of {} headers)", st.claimed, st.headers);
    }

    Ok(())
}
