//! Single-pass sweep of `ConsensusBlocks` across an epoch range, bucketing per-epoch
//! leader/participation tallies as it goes.
//!
//! `tally_replay` calls `tally_hybrid(epoch, ..)` once per epoch, and each call independently
//! reverse-walks from the table's tip down to that epoch — cheap for one epoch near the tip, but
//! O(epochs^2) if called in a loop across the whole table's history. This does the walk exactly
//! once, splitting rows into per-epoch buckets as they're visited, so sweeping the entire history
//! costs the same single pass as replaying just the most recent epoch.
//!
//! Read-only: only ever opens a read transaction, never writes.
//!
//! Usage:
//!   cargo run --example tally_sweep -- <consensus_db_path> <committee_yaml_path> <start_epoch> <end_epoch>

use rayls_infrastructure_storage::{open_db, tables::ConsensusBlocks};
use rayls_infrastructure_types::{
    Address, Committee, ConsensusHeaderParticipation, Database, DbTx, Epoch,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default, Clone, Copy)]
struct Tally {
    leader_rounds: u32,
    participation_rounds: u32,
}

struct EpochStats {
    total_rounds: u32,
    per_address: BTreeMap<Address, Tally>,
}

fn main() -> eyre::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!(
            "usage: tally_sweep <consensus_db_path> <committee_yaml_path> <start_epoch> <end_epoch>"
        );
        std::process::exit(1);
    }

    let db_path = &args[1];
    let committee_path = &args[2];
    let start_epoch: Epoch = args[3].parse().map_err(|e| eyre::eyre!("bad start_epoch: {e}"))?;
    let end_epoch: Epoch = args[4].parse().map_err(|e| eyre::eyre!("bad end_epoch: {e}"))?;

    let committee_yaml = std::fs::read_to_string(committee_path)
        .map_err(|e| eyre::eyre!("reading {committee_path}: {e}"))?;
    let committee: Committee =
        serde_yaml::from_str(&committee_yaml).map_err(|e| eyre::eyre!("parsing committee yaml: {e}"))?;
    committee.load();

    eprintln!("opening consensus db at {db_path} (read-only walk)...");
    let db = open_db(db_path);

    let mut per_epoch: BTreeMap<Epoch, EpochStats> = BTreeMap::new();

    db.with_read_txn(|txn| {
        txn.disable_long_read_safety();
        eprintln!("sweep: read txn opened, walking tip -> epoch {start_epoch}");

        let mut walked: u64 = 0;
        let mut seen: BTreeSet<Address> = BTreeSet::new();
        for (_key_bytes, value_bytes) in txn.reverse_raw_iter::<ConsensusBlocks>() {
            walked += 1;
            if walked.is_multiple_of(500_000) {
                eprintln!("sweep: walked {walked} rows...");
            }

            let meta = ConsensusHeaderParticipation::from_bytes(&value_bytes)
                .map_err(|e| eyre::eyre!("decode failed at row {walked}: {e}"))?;
            let header_epoch = meta.leader_epoch;
            let header_round = meta.leader_round;

            // Above the requested window - keep walking down toward it without accumulating.
            if header_epoch > end_epoch {
                continue;
            }
            // Below the requested window - reverse-iter is done.
            if header_epoch < start_epoch {
                break;
            }
            // genesis has no leader / participants.
            if header_round == 0 {
                continue;
            }

            let stats = per_epoch
                .entry(header_epoch)
                .or_insert_with(|| EpochStats { total_rounds: 0, per_address: BTreeMap::new() });
            stats.total_rounds = stats.total_rounds.saturating_add(1);

            if let Some(authority) = committee.authority(&meta.leader_author) {
                let addr = authority.execution_address();
                stats.per_address.entry(addr).or_default().leader_rounds += 1;
            }

            seen.clear();
            for author in &meta.participants {
                if let Some(authority) = committee.authority(author) {
                    let addr = authority.execution_address();
                    if seen.insert(addr) {
                        stats.per_address.entry(addr).or_default().participation_rounds += 1;
                    }
                }
            }
        }
        eprintln!("sweep: walk done, {walked} rows visited, {} epochs bucketed", per_epoch.len());
        Ok(())
    })
    .map_err(|e: eyre::Report| eyre::eyre!("sweep walk failed: {e}"))?;

    // ── Per-epoch compact line ──────────────────────────────────────────
    println!();
    println!(
        "{:>8} {:>12} {:>6} {:>10} {:>10} {:>18}",
        "epoch", "total_rnds", "n_val", "min_part%", "max_part%", "leader_only_count"
    );
    let mut anomalous_epochs: Vec<Epoch> = Vec::new();
    let mut ratio_sum = 0f64;
    let mut ratio_count = 0u64;
    let mut min_ratio_seen = 100f64;
    let mut min_ratio_epoch = 0u32;

    for (epoch, stats) in &per_epoch {
        if stats.total_rounds == 0 || stats.per_address.is_empty() {
            continue;
        }
        let mut min_ratio = 100f64;
        let mut max_ratio = 0f64;
        let mut leader_only_count = 0u32;
        for tally in stats.per_address.values() {
            let ratio = 100.0 * tally.participation_rounds as f64 / stats.total_rounds as f64;
            min_ratio = min_ratio.min(ratio);
            max_ratio = max_ratio.max(ratio);
            ratio_sum += ratio;
            ratio_count += 1;
            if tally.participation_rounds <= tally.leader_rounds {
                leader_only_count += 1;
            }
        }
        if min_ratio < min_ratio_seen {
            min_ratio_seen = min_ratio;
            min_ratio_epoch = *epoch;
        }
        // Flag epochs worth a second look: a validator with <50% participation, or one
        // that's never credited beyond its own leader rounds (possible non-hybrid data).
        if min_ratio < 50.0 || leader_only_count > 0 {
            anomalous_epochs.push(*epoch);
        }

        println!(
            "{epoch:>8} {:>12} {:>6} {:>10.1} {:>10.1} {:>18}",
            stats.total_rounds,
            stats.per_address.len(),
            min_ratio,
            max_ratio,
            leader_only_count
        );
    }

    // ── Whole-history per-validator rollup ──────────────────────────────
    let mut totals: BTreeMap<Address, Tally> = BTreeMap::new();
    let mut total_rounds_grand: u64 = 0;
    for stats in per_epoch.values() {
        total_rounds_grand += stats.total_rounds as u64;
        for (addr, t) in &stats.per_address {
            let e = totals.entry(*addr).or_default();
            e.leader_rounds += t.leader_rounds;
            e.participation_rounds += t.participation_rounds;
        }
    }

    println!();
    println!("=== whole-history per-validator rollup (all {} epochs, {total_rounds_grand} total rounds) ===", per_epoch.len());
    println!("{:>44} {:>14} {:>18} {:>12} {:>12}", "address", "leader_rounds", "participation_rounds", "leadership%", "participation%");
    let mut rows: Vec<_> = totals.iter().collect();
    rows.sort_by(|a, b| b.1.participation_rounds.cmp(&a.1.participation_rounds));
    for (addr, t) in rows {
        let lead_pct = 100.0 * t.leader_rounds as f64 / total_rounds_grand as f64;
        let part_pct = 100.0 * t.participation_rounds as f64 / total_rounds_grand as f64;
        println!("{addr:>44?} {:>14} {:>18} {:>12.2} {:>12.2}", t.leader_rounds, t.participation_rounds, lead_pct, part_pct);
    }

    // ── Aggregate summary ────────────────────────────────────────────────
    println!();
    println!("=== sweep summary: epochs [{start_epoch}, {end_epoch}] ===");
    println!("epochs with data: {}", per_epoch.len());
    if ratio_count > 0 {
        println!("mean per-validator participation ratio across all epochs: {:.2}%", ratio_sum / ratio_count as f64);
        println!("single lowest participation ratio observed: {min_ratio_seen:.1}% (epoch {min_ratio_epoch})");
    }
    println!(
        "epochs flagged for a second look (a validator <50% participation, or leader-only-looking data): {}",
        anomalous_epochs.len()
    );
    if !anomalous_epochs.is_empty() {
        let preview: Vec<String> = anomalous_epochs.iter().take(30).map(|e| e.to_string()).collect();
        println!("first {} flagged epochs: {}", preview.len(), preview.join(", "));
    }

    Ok(())
}
