//! Offline replay of `ConsensusRewardsCounter::tally_hybrid` against an on-disk consensus DB.
//!
//! Verifies empirically (not just by code reading) whether the hybrid reward tally credits
//! participation (any validator whose certificate lands in a committed sub-dag) separately
//! from leadership (round anchor only) for a given closed epoch — see axyl-private#633.
//!
//! Read-only: opens the consensus DB and only ever calls `with_read_txn` / `reverse_raw_iter`
//! under the hood (via `tally_hybrid`), never a write transaction.
//!
//! Usage:
//!   cargo run --example tally_replay -- <consensus_db_path> <committee_yaml_path> <epoch> [last_executed_round]
//!
//! `committee_yaml_path` is the node's `genesis/committee.yaml` — this approximates the
//! per-epoch committee as the genesis committee, which is only exact if validator membership
//! hasn't changed since genesis. Mismatched output (e.g. addresses the tally never sees, or a
//! distinct-address count that doesn't match committee size) is a sign membership has churned
//! and the true on-chain committee for that epoch is needed instead.

use rayls_infrastructure_storage::open_db;
use rayls_infrastructure_types::{rewards::RewardsBackend, Committee, Epoch};
use rayls_middleware_rewards::ConsensusRewardsCounter;

fn main() -> eyre::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: tally_replay <consensus_db_path> <committee_yaml_path> <epoch> [last_executed_round]"
        );
        std::process::exit(1);
    }

    let db_path = &args[1];
    let committee_path = &args[2];
    let epoch: Epoch = args[3].parse().map_err(|e| eyre::eyre!("bad epoch: {e}"))?;
    let last_executed_round: u32 = args
        .get(4)
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| eyre::eyre!("bad last_executed_round: {e}"))?
        .unwrap_or(u32::MAX);

    let committee_yaml = std::fs::read_to_string(committee_path)
        .map_err(|e| eyre::eyre!("reading {committee_path}: {e}"))?;
    let committee: Committee =
        serde_yaml::from_str(&committee_yaml).map_err(|e| eyre::eyre!("parsing committee yaml: {e}"))?;
    committee.load();
    let committee_size = committee.size();

    eprintln!("opening consensus db at {db_path} (read-only walk)...");
    let db = open_db(db_path);
    let counter = ConsensusRewardsCounter::new(db);
    counter.set_committee(committee);

    let tally = counter
        .tally_hybrid(epoch, last_executed_round)
        .map_err(|e| eyre::eyre!("tally_hybrid failed: {e}"))?;

    println!();
    println!(
        "epoch={epoch} last_executed_round={last_executed_round} total_rounds={} committee_size={committee_size} distinct_addresses={}",
        tally.total_rounds,
        tally.per_address.len()
    );
    if tally.per_address.len() != committee_size {
        println!(
            "NOTE: distinct addresses in tally ({}) != genesis committee size ({}) — \
             validator membership may have changed since genesis; treat this replay's \
             committee mapping as approximate.",
            tally.per_address.len(),
            committee_size
        );
    }
    println!();
    println!("{:<44} {:>14} {:>21}", "address", "leader_rounds", "participation_rounds");
    let mut rows: Vec<_> = tally.per_address.iter().collect();
    rows.sort_by(|a, b| b.1.participation_rounds.cmp(&a.1.participation_rounds));
    for (addr, t) in rows {
        println!("{addr:<44?} {:>14} {:>21}", t.leader_rounds, t.participation_rounds);
    }

    let only_leaders_ever_credited =
        tally.per_address.values().all(|t| t.participation_rounds == t.leader_rounds);
    println!();
    if only_leaders_ever_credited && tally.per_address.len() > 1 {
        println!(
            "VERDICT: participation_rounds == leader_rounds for every validator — no evidence \
             of vote-based crediting in this epoch's data (could mean the hybrid path isn't \
             active yet, or every non-leader happened to have zero certificates this epoch)."
        );
    } else {
        println!(
            "VERDICT: at least one validator is credited with participation_rounds > \
             leader_rounds — confirms non-leader participation is being counted, not just \
             block leadership."
        );
    }

    Ok(())
}
