// SPDX-License-Identifier: BUSL-1.1
//! Command-line surface. Kept free of I/O so it can be parsed in tests.

use clap::{Args, Parser, Subcommand};

/// Parses a number, naming the likely cause when a path was passed instead.
///
/// `--db` takes one path per value, so a stray path lands on a numeric positional; say so rather
/// than reporting "invalid digit".
fn number<T: std::str::FromStr>(s: &str) -> Result<T, String> {
    s.parse::<T>().map_err(|_| {
        if s.contains(['/', '\\', '=']) || std::path::Path::new(s).is_dir() {
            format!(
                "`{s}` is a path, not a number: pass one path per --db \
                 (--db a --db b), or comma-separated (--db a,b)"
            )
        } else {
            format!("`{s}` is not a number")
        }
    })
}

/// Read-only inspection of Rayls consensus databases.
///
/// Opens each --db read-only and compares what the nodes hold on disk. Safe to run against a
/// running node: nothing is written and no lock is taken. Results show what the node has flushed
/// to disk, which can lag its memory by a few seconds.
///
/// Exit status: 0 all nodes agree, 1 a problem was found, 2 a database could not be opened or an
/// argument was invalid.
#[derive(Debug, Parser)]
#[command(name = "rayls-db-inspect", version, about, long_about)]
pub struct Cli {
    /// Print the report as one JSON object instead of text.
    #[arg(long, global = true)]
    pub json: bool,

    /// Print every field: committee keys, sub-dag certificates, batch presence, reputation.
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Open the database exclusively. Fails if any other process has it open. Use on copies to
    /// make sure you are not reading a live node.
    #[arg(long, global = true)]
    pub exclusive: bool,

    /// Refuse to inspect a database that a running process holds open.
    #[arg(long, global = true)]
    pub require_stopped: bool,

    /// Repair a copy taken from a running node so it can be opened read-only. Opens it read-write
    /// once and closes it. Fails if any other process has it open. Never use on a live node.
    #[arg(long, global = true)]
    pub recover: bool,

    #[command(subcommand)]
    pub command: Command,
}

/// The databases to inspect. Shared by every subcommand.
#[derive(Debug, Clone, Args)]
pub struct NodeArgs {
    /// Node to inspect: its datadir or its consensus-db directory. Repeat the flag or separate
    /// paths with commas to compare nodes. Prefix a path with `label=` to name the node in the
    /// output, for example `v1=/data/node1`.
    #[arg(
        short = 'd',
        long = "db",
        required = true,
        num_args = 1,
        value_delimiter = ',',
        action = clap::ArgAction::Append,
        value_name = "[LABEL=]DATADIR"
    )]
    pub dbs: Vec<String>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Show one epoch's record and certificate on each node.
    ///
    /// Reports whether the record and its certificate exist, whether the certificate is valid
    /// (signer count, super-quorum, BLS signature), whether the record links to the previous
    /// one, and whether a transition checkpoint was left behind.
    Epoch {
        /// The epoch to inspect.
        #[arg(value_name = "EPOCH", value_parser = number::<u32>)]
        epoch: u32,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Show which epochs each node has a record and certificate for, as a table.
    Epochs {
        /// First epoch of the range, inclusive. Omit both bounds when using --all.
        #[arg(
            value_name = "FROM_EPOCH",
            required_unless_present = "all",
            value_parser = number::<u32>
        )]
        from: Option<u32>,
        /// Last epoch of the range, inclusive.
        #[arg(
            value_name = "TO_EPOCH",
            required_unless_present = "all",
            value_parser = number::<u32>
        )]
        to: Option<u32>,
        /// Cover every epoch that any node has a record for.
        #[arg(long, conflicts_with_all = ["from", "to"])]
        all: bool,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Check the whole epoch-record chain on each node: links, certificates, gaps.
    ChainCheck {
        /// First epoch to check. Default: the first record on disk.
        #[arg(long, value_name = "EPOCH", value_parser = number::<u32>)]
        from: Option<u32>,
        /// Last epoch to check. Default: the last record on disk.
        #[arg(long, value_name = "EPOCH", value_parser = number::<u32>)]
        to: Option<u32>,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Show one consensus header on each node and compare them.
    ///
    /// A consensus header records one committed sub-dag (the certificates consensus committed
    /// together and the leader that triggered the commit). Headers form a chain by parent_hash and
    /// are numbered in order.
    Header {
        /// Position in the consensus chain. `summary` shows each node's latest under `consensus
        /// #`.
        #[arg(value_name = "HEADER_NUMBER", value_parser = number::<u64>)]
        number: u64,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Show the leader certificate of one consensus header on each node and compare them.
    ///
    /// Compares the signer set too: the same certificate with different signers on different
    /// nodes is a fork signal.
    Cert {
        /// Position in the consensus chain of the header whose leader certificate to show.
        #[arg(value_name = "HEADER_NUMBER", value_parser = number::<u64>)]
        number: u64,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Follow a chain of linked records back and report the first broken link.
    Walk {
        #[command(subcommand)]
        target: WalkTarget,
    },

    /// Overview of each node's database: live status, epochs, consensus tip, table sizes.
    Summary {
        #[command(flatten)]
        nodes: NodeArgs,
    },
}

#[derive(Debug, Subcommand)]
pub enum WalkTarget {
    /// Follow consensus headers back through parent_hash, checking that each parent is the
    /// header one number below with the matching digest.
    Header {
        /// Position in the consensus chain to start from.
        #[arg(value_name = "HEADER_NUMBER", value_parser = number::<u64>)]
        number: u64,
        /// How many headers to walk back.
        #[arg(long, default_value_t = 10, value_name = "COUNT", value_parser = number::<u64>)]
        back: u64,
        #[command(flatten)]
        nodes: NodeArgs,
    },
}
