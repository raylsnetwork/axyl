//! This should allow for easier upgrades.
//! It still re-exports some stuff and a few places use Reth directly but eventually
//! it all should go through this crate.

#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub mod accessors;
pub mod chain;
pub mod config;
pub mod env;
pub mod execution;
pub mod genesis;
pub mod init;
pub mod persistence;
pub mod registry;
pub(crate) mod reth_runtime;
pub mod rpc;
#[cfg(feature = "archive-replay")]
pub mod snapshot_reader;
#[cfg(feature = "archive-replay")]
pub mod solver;
pub mod types;

#[cfg(test)]
mod tests;

pub use chain::ChainSpec;
pub use config::{RethCommand, RethConfig};
pub use env::RethEnv;
#[cfg(feature = "archive-replay")]
pub use snapshot_reader::SnapshotReader;
pub use types::{
    ExecutedBatchDigestReceiver, FailedTxNotification, NonceTooHighDetail, RethDb, RpcServer,
    SparseRootFn, ToTree, TxValidationCounts,
};
