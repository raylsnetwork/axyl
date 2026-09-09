//! This should allow for easier upgrades.
//! It still re-exports some stuff and a few places use Reth directly but eventually
//! it all should go through this crate.

#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

// Reth stuff we are just re-exporting.  Need to reduce this over time.
pub use alloy::primitives::FixedBytes;
pub use reth::{
    chainspec::chain_value_parser, dirs::MaybePlatformPath, payload::BlobSidecars,
    rpc::builder::RpcServerHandle,
};
pub use reth_chain_state::{CanonicalInMemoryState, ExecutedBlock, NewCanonicalChain};
pub use reth_chainspec::{BaseFeeParams, ChainSpec as RethChainSpec};
pub use reth_cli_util::{parse_duration_from_secs, parse_socket_address};
pub use reth_errors::{ProviderError, RethError};
pub use reth_node_core::{
    args::{ColorMode, LogArgs},
    node_config::DEFAULT_PERSISTENCE_THRESHOLD,
};
pub use reth_primitives_traits::crypto::secp256k1::sign_message;
pub use reth_provider::{AccountReader, CanonStateNotificationStream, ExecutionOutcome};
pub use reth_rpc_eth_types::EthApiError;
pub use reth_tracing::FileWorkerGuard;
pub use reth_transaction_pool::{
    error::{InvalidPoolTransactionError, PoolError, PoolTransactionError},
    identifier::SenderIdentifiers,
    BestTransactions, EthPooledTransaction, PoolTransaction, TransactionPool as TransactionPoolT,
};

pub mod bypass_validator;
pub mod chainspec;
pub mod dirs;
pub mod payload;
pub mod traits;
pub mod txn_pool;
pub use txn_pool::*;
pub mod error;
mod evm;
pub mod native_erc20;
pub(crate) mod persistence;
pub mod reth_env;
pub mod rpc_server_args;
pub mod system_calls;
pub mod worker;

pub use chainspec::RaylsChainSpec;

#[cfg(any(feature = "test-utils", test))]
pub mod test_utils;
