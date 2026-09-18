// SPDX-License-Identifier: BUSL-1.1
//! Command-line surface. Kept free of I/O so it can be parsed in tests.

use clap::{Args, Parser, Subcommand};
use rayls_infrastructure_types::B256;

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

/// Parses a 32-byte hash written as hex, with or without `0x`.
fn hash(s: &str) -> Result<B256, String> {
    let bytes = const_hex::decode(s).map_err(|_| {
        if s.contains(['/', '\\', '=']) || std::path::Path::new(s).is_dir() {
            format!(
                "`{s}` is a path, not a hash: pass one path per --db \
                 (--db a --db b), or comma-separated (--db a,b)"
            )
        } else {
            format!("`{s}` is not a hex hash")
        }
    })?;
    B256::try_from(bytes.as_slice())
        .map_err(|_| format!("`{s}` is {} bytes, a hash is 32", bytes.len()))
}

/// Read-only inspection of Rayls consensus databases, on disk (--db) or through a running
/// node's RPC (--rpc), comparing what the nodes hold.
///
/// Each --db is opened read-only: nothing is written and no lock is taken, so it is safe against
/// a running node. Results show what the node has flushed to disk, which can lag its memory by a
/// few seconds. An --rpc node answers the epoch and header commands from its live state.
///
/// Exit status: 0 the verdict is OK, 1 any other verdict (including "not reached" and "not
/// found"), 2 a node could not be opened or an argument was invalid.
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

    /// Refuse to inspect a database that a running process holds open (or whose liveness cannot
    /// be determined).
    #[arg(long, global = true)]
    pub require_stopped: bool,

    /// Make a raw file copy taken from a running node openable: one read-write, exclusive open
    /// that settles its unsynced head. Refuses a directory a running process holds. The copy's
    /// meta pages are rewritten, and on another host or after a reboot MDBX rolls the copy back
    /// to its last steady commit. Prefer `snapshot`, which needs no recovery.
    #[arg(long, global = true)]
    pub recover: bool,
    /// Maximum requests per second sent to each RPC node; 0 lifts the limit. Nodes are queried
    /// in parallel but each one sequentially, so this bounds the load one node sees.
    #[arg(long, global = true, default_value_t = 10, value_name = "PER_SECOND")]
    pub rpc_rate: u32,

    /// Node to inspect: its datadir or its consensus-db directory. Repeat the flag or separate
    /// paths with commas to compare nodes. Prefix a path with `label=` to name the node in the
    /// output, for example `v1=/data/node1`. Accepted before or after the command.
    #[arg(
        short = 'd',
        long = "db",
        num_args = 1,
        value_delimiter = ',',
        action = clap::ArgAction::Append,
        value_name = "[LABEL=]DATADIR"
    )]
    pub dbs: Vec<String>,

    /// Node to inspect through its JSON-RPC endpoint, for example `v2=http://10.0.0.2:8545`.
    /// Repeat or comma-separate to add more; mix freely with --db. A running node answers the
    /// epoch and header commands (only certified epoch records are served); batches, `summary`
    /// and node-local state need a database.
    #[arg(
        long = "rpc",
        num_args = 1,
        value_delimiter = ',',
        action = clap::ArgAction::Append,
        value_name = "[LABEL=]URL"
    )]
    pub rpcs: Vec<String>,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    /// Every `--db` given, before and after the command, in command-line order.
    pub fn dbs(&self) -> Vec<String> {
        self.dbs.iter().chain(&self.command.nodes().dbs).cloned().collect()
    }

    /// Every `--rpc` given, before and after the command, in command-line order.
    pub fn rpcs(&self) -> Vec<String> {
        self.rpcs.iter().chain(&self.command.nodes().rpcs).cloned().collect()
    }
}

/// The `--db` values given after the command name. A plain global option cannot serve here:
/// clap propagates a global from the command level upward by replacing the parent's values, so
/// paths given before and after the command would not add up.
#[derive(Debug, Clone, Default, Args)]
pub struct NodeArgs {
    /// Node to inspect, same as the top-level `--db`; may be repeated or comma-separated.
    #[arg(
        short = 'd',
        long = "db",
        num_args = 1,
        value_delimiter = ',',
        action = clap::ArgAction::Append,
        value_name = "[LABEL=]DATADIR"
    )]
    pub dbs: Vec<String>,
    /// Node to inspect over RPC, same as the top-level `--rpc`; may be repeated or
    /// comma-separated.
    #[arg(
        long = "rpc",
        num_args = 1,
        value_delimiter = ',',
        action = clap::ArgAction::Append,
        value_name = "[LABEL=]URL"
    )]
    pub rpcs: Vec<String>,
}

impl Command {
    fn nodes(&self) -> &NodeArgs {
        match self {
            Self::Epoch { nodes, .. }
            | Self::Epochs { nodes, .. }
            | Self::EpochCheck { nodes, .. }
            | Self::Header { nodes, .. }
            | Self::Cert { nodes, .. }
            | Self::GetBatch { nodes, .. }
            | Self::GetTx { nodes, .. }
            | Self::Summary { nodes }
            | Self::Snapshot { nodes, .. }
            | Self::HeaderCheck { nodes, .. } => nodes,
        }
    }
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

    /// Check the whole epoch-record chain on each node: links, certificates, gaps, digest index.
    ///
    /// `-v` lists every record in range. Narrow with --from/--to for a focused view around one
    /// epoch.
    EpochCheck {
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

    /// Show one worker batch on each node: where it is stored and what it contains.
    ///
    /// A batch is what a worker seals from its transaction pool; DAG headers reference batches by
    /// digest and `header -v` lists the digests a consensus header commits. This command follows
    /// one of them to the batch itself. `-v` lists the transactions and finds the consensus header
    /// that committed the batch.
    GetBatch {
        /// Batch digest: 32 bytes of hex, `0x` optional.
        #[arg(value_name = "DIGEST", value_parser = hash)]
        digest: B256,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Find the batch holding a transaction on each node, and the consensus header that
    /// committed that batch.
    ///
    /// The consensus database has no transaction index, so this scans the batch tables: the hot
    /// table, then every sealed epoch of the cold archive. `--epoch` skips hot batches sealed in
    /// other epochs (the hot table is still read end to end) and opens only that epoch's cold jar.
    /// The hot pass holds one read transaction for the whole table.
    GetTx {
        /// Transaction hash: 32 bytes of hex, `0x` optional.
        #[arg(value_name = "TX_HASH", value_parser = hash)]
        hash: B256,
        /// Only look at batches sealed in this epoch.
        #[arg(long, value_name = "EPOCH", value_parser = number::<u32>)]
        epoch: Option<u32>,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Check the consensus-header chain back from one header: each parent must be the header one
    /// number below with the matching digest, the digest index must agree, and every certificate
    /// is re-verified.
    ///
    /// `epoch-check` covers the whole epoch-record chain; the header chain is too long for that,
    /// so this starts at a header (the tip, or a suspicious one) and goes back COUNT hops.
    HeaderCheck {
        /// Position in the consensus chain to start from.
        #[arg(value_name = "HEADER_NUMBER", value_parser = number::<u64>)]
        number: u64,
        /// How many headers to check back.
        #[arg(long, default_value_t = 10, value_name = "COUNT", value_parser = number::<u64>)]
        back: u64,
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Overview of each node's database: live status, epochs, consensus tip, table sizes.
    Summary {
        #[command(flatten)]
        nodes: NodeArgs,
    },

    /// Copy one node's consensus database into DIR as one committed state, with its sealed cold
    /// jars. MDBX copies inside a read transaction, so the copy is consistent even from a running
    /// node and opens without --recover; a SNAPSHOT.json in DIR records what it holds. DIR must
    /// not exist, or be an empty directory, outside the source. Exactly one --db.
    Snapshot {
        /// Destination directory.
        #[arg(long, value_name = "DIR")]
        to: std::path::PathBuf,
        #[command(flatten)]
        nodes: NodeArgs,
    },
}
