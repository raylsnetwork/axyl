// SPDX-License-Identifier: BUSL-1.1

mod aggregator;
pub mod batch_tracker;
pub mod bcs_layout;
mod build_metadata;
mod codec;
mod committee;
mod crypto;
pub mod database_traits;
pub mod executed_batch_registry;
pub mod gas_accumulator;
mod genesis;
mod helpers;
pub mod nonce;
mod notifier;
mod primary;
mod processor;
pub mod serde;
mod sync;
mod task_manager;
mod worker;
#[macro_use]
pub mod error;
pub mod payload;
pub mod rewards;
pub use aggregator::*;
pub use build_metadata::*;
pub use codec::*;
pub use committee::*;
pub use crypto::*;
pub use database_traits::*;
pub use genesis::*;
pub use helpers::*;
pub use notifier::*;
pub use primary::*;
pub use processor::*;
pub use sync::*;
pub use task_manager::*;
pub use worker::*;
#[cfg(feature = "test-utils")]
pub mod test_utils;

// re-exports for easier maintainability
pub use alloy::{
    consensus::{
        constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        proofs::calculate_transaction_root,
        BlockHeader, Header as ExecHeader, SignableTransaction, Transaction as TransactionTrait,
        TxEip1559,
    },
    eips::{
        eip1559::MIN_PROTOCOL_BASE_FEE,
        eip2718::Encodable2718,
        eip4844::{env_settings::EnvKzgSettings, BlobAndProofV1, BlobTransactionSidecar},
        BlockHashOrNumber, BlockNumHash,
    },
    genesis::{Genesis, GenesisAccount},
    hex::{self, FromHex},
    primitives::{
        address, hex_literal, keccak256, Address, BlockHash, BlockNumber, Bloom, Bytes, Sealable,
        TxHash, TxKind, B256, U160, U256,
    },
    rpc::types::{AccessList, Withdrawals},
    signers::Signature as EthSignature,
    sol,
    sol_types::{SolType, SolValue},
};
pub use libp2p::{multiaddr::Protocol, Multiaddr};
pub use reth_primitives::{
    Block, BlockBody, EthPrimitives, NodePrimitives, PooledTransaction, Receipt, Recovered,
    RecoveredBlock, SealedBlock, SealedHeader, Transaction, TransactionSigned,
};

mod network;
pub use network::{RaylsNetwork, MIN_RAYLS_PROTOCOL_BASE_FEE};
