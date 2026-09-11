//! Transaction spam fault injector.
//!
//! Generates and submits malformed, oversized, and high-volume transactions
//! to stress the mempool and batch validator.

use crate::rpc;
use eyre::Report;
use jsonrpsee::rpc_params;
use rand::Rng;
use tracing::{info, warn};

/// Maximum batch size in bytes (2 MB).
const MAX_BATCH_SIZE: usize = 2_000_000;

/// Maximum batch gas (30M).
const MAX_BATCH_GAS: u64 = 30_000_000;

/// Axyl chain ID — re-exported from crate root.
use crate::CHAIN_ID;

/// Result of a spam campaign.
#[derive(Debug)]
pub struct SpamResult {
    /// Number of transactions submitted.
    pub submitted: usize,
    /// Number of transactions that were accepted by the RPC.
    pub accepted: usize,
    /// Number of transactions that were rejected by the RPC.
    pub rejected: usize,
}

/// Generate and submit transactions with invalid signatures.
///
/// These should be rejected by the transaction pool or batch validator.
pub fn spam_invalid_signatures(rpc_url: &str, count: usize) -> eyre::Result<SpamResult> {
    info!(target: "chaos", count, "spamming invalid signature transactions");
    let mut result = SpamResult { submitted: 0, accepted: 0, rejected: 0 };

    for i in 0..count {
        // Create a transaction with garbage signature bytes.
        let mut tx_bytes = Vec::with_capacity(128);
        // Legacy transaction type prefix (no EIP-2718 type byte → legacy)
        // RLP-encode a minimal legacy transaction with garbage signature.
        tx_bytes.extend_from_slice(&rlp_encode_garbage_legacy_tx(i as u64));

        result.submitted += 1;
        match submit_raw_tx(rpc_url, &tx_bytes) {
            Ok(_) => result.accepted += 1,
            Err(_) => result.rejected += 1,
        }
    }

    info!(target: "chaos", ?result, "invalid signature spam complete");
    Ok(result)
}

/// Generate and submit transactions with wrong chain ID.
///
/// These should be rejected because chain ID doesn't match the cluster's CHAIN_ID.
pub fn spam_wrong_chain_id(rpc_url: &str, count: usize) -> eyre::Result<SpamResult> {
    info!(target: "chaos", count, "spamming wrong chain ID transactions");
    let mut result = SpamResult { submitted: 0, accepted: 0, rejected: 0 };

    let key = rpc::get_key("test-source");
    let to_account = rpc::address_from_word("spam-wrong-chain");

    for i in 0..count {
        // Sign with wrong chain ID (0x1 = mainnet Ethereum instead of CHAIN_ID).
        result.submitted += 1;
        match send_tx_with_chain_id(rpc_url, &key, to_account, 1, i as u128) {
            Ok(_) => result.accepted += 1,
            Err(_) => result.rejected += 1,
        }
    }

    info!(target: "chaos", ?result, "wrong chain ID spam complete");
    Ok(result)
}

/// Generate and submit transactions with oversized calldata.
///
/// Targets the batch size limit (2 MB). Each transaction carries a large
/// data payload that should be rejected when batch building encounters
/// the size limit.
pub fn spam_oversized_calldata(
    rpc_url: &str,
    count: usize,
    data_size: usize,
) -> eyre::Result<SpamResult> {
    info!(target: "chaos", count, data_size, "spamming oversized calldata transactions");
    let mut result = SpamResult { submitted: 0, accepted: 0, rejected: 0 };

    let key = rpc::get_key("test-source");
    let to_account = rpc::address_from_word("spam-oversized");

    for i in 0..count {
        result.submitted += 1;
        match send_tx_with_data(rpc_url, &key, to_account, data_size, i as u128) {
            Ok(_) => result.accepted += 1,
            Err(_) => result.rejected += 1,
        }
    }

    info!(target: "chaos", ?result, "oversized calldata spam complete");
    Ok(result)
}

/// Generate and submit transactions with excessively high gas limits.
///
/// Each transaction requests more gas than the batch gas limit (30M).
pub fn spam_excessive_gas(rpc_url: &str, count: usize) -> eyre::Result<SpamResult> {
    info!(target: "chaos", count, "spamming excessive gas transactions");
    let mut result = SpamResult { submitted: 0, accepted: 0, rejected: 0 };

    let key = rpc::get_key("test-source");
    let to_account = rpc::address_from_word("spam-gas");

    for i in 0..count {
        // Request gas well above the 30M batch gas limit.
        let gas = MAX_BATCH_GAS as u128 + 1_000_000;
        result.submitted += 1;
        match send_tx_with_gas(rpc_url, &key, to_account, gas, i as u128) {
            Ok(_) => result.accepted += 1,
            Err(_) => result.rejected += 1,
        }
    }

    info!(target: "chaos", ?result, "excessive gas spam complete");
    Ok(result)
}

/// Generate and submit a burst of valid transactions to stress throughput.
///
/// These are well-formed transfers that the chain should process without
/// error. The goal is to fill batches to capacity and test backpressure.
pub fn spam_valid_transfers(rpc_url: &str, count: usize) -> eyre::Result<SpamResult> {
    info!(target: "chaos", count, "spamming valid transfer transactions");
    let mut result = SpamResult { submitted: 0, accepted: 0, rejected: 0 };

    let key = rpc::get_key("test-source");
    let to_account = rpc::address_from_word("spam-valid");

    // Start from the sender's on-chain nonce, not 0: `test-source` is the dev-funded
    // ceremony account and begins at a genesis nonce of 5+N, so a 0-based burst would
    // be rejected as "nonce too low".
    let sender = rpc::address_from_key(&key)?;
    let start_nonce = rpc::get_transaction_count(rpc_url, &sender.to_string())?;

    for i in 0..count {
        result.submitted += 1;
        match rpc::send_rls(
            rpc_url,
            &key,
            to_account,
            rpc::WEI_PER_RLS,
            rpc::GAS_PRICE,
            21000,
            start_nonce + i as u128,
        ) {
            Ok(_) => result.accepted += 1,
            Err(_) => result.rejected += 1,
        }
    }

    info!(target: "chaos", ?result, "valid transfer spam complete");
    Ok(result)
}

/// Submit a mix of malformed transaction types.
///
/// Combines invalid signatures, wrong chain IDs, oversized calldata,
/// and excessive gas in a randomized pattern.
pub fn spam_mixed(rpc_url: &str, count: usize) -> eyre::Result<SpamResult> {
    info!(target: "chaos", count, "spamming mixed malformed transactions");
    let mut result = SpamResult { submitted: 0, accepted: 0, rejected: 0 };
    let mut rng = rand::rng();

    for i in 0..count {
        let sub_result = match rng.random_range(0u32..4) {
            0 => spam_invalid_signatures(rpc_url, 1),
            1 => spam_wrong_chain_id(rpc_url, 1),
            2 => spam_oversized_calldata(rpc_url, 1, MAX_BATCH_SIZE + 1024),
            _ => spam_excessive_gas(rpc_url, 1),
        };

        match sub_result {
            Ok(sr) => {
                result.submitted += sr.submitted;
                result.accepted += sr.accepted;
                result.rejected += sr.rejected;
            }
            Err(e) => {
                warn!(target: "chaos", i, ?e, "mixed spam iteration failed");
                result.submitted += 1;
                result.rejected += 1;
            }
        }
    }

    info!(target: "chaos", ?result, "mixed spam complete");
    Ok(result)
}

// ---- Internal helpers ----

/// Submit a raw transaction hex string via eth_sendRawTransaction.
fn submit_raw_tx(rpc_url: &str, tx_bytes: &[u8]) -> eyre::Result<String> {
    let hex = format!("0x{}", const_hex::encode(tx_bytes));
    rpc::call_rpc(rpc_url, "eth_sendRawTransaction", rpc_params!(hex), 0)
}

/// Sign and submit a transaction with a specific chain ID.
fn send_tx_with_chain_id(
    rpc_url: &str,
    key: &str,
    to_account: rayls_infrastructure_types::Address,
    chain_id: u64,
    nonce: u128,
) -> eyre::Result<()> {
    use ethereum_tx_sign::{LegacyTransaction, Transaction};
    use secp256k1::SecretKey;

    let mut to_addr = [0_u8; 20];
    to_addr.copy_from_slice(to_account.as_slice());
    let new_transaction = LegacyTransaction {
        chain: chain_id,
        nonce,
        to: Some(to_addr),
        value: rpc::WEI_PER_RLS,
        gas_price: 250,
        gas: 21000,
        data: vec![],
    };
    let key_bytes: [u8; 32] =
        const_hex::decode(key)?.try_into().map_err(|_| Report::msg("Invalid secret key length"))?;
    let secret_key = SecretKey::from_byte_array(key_bytes)?;
    let ecdsa = new_transaction
        .ecdsa(&secret_key.secret_bytes())
        .map_err(|_| Report::msg("Failed to get ecdsa"))?;
    let tx_bytes = new_transaction.sign(&ecdsa);
    submit_raw_tx(rpc_url, &tx_bytes)?;
    Ok(())
}

/// Sign and submit a transaction with a large data payload.
fn send_tx_with_data(
    rpc_url: &str,
    key: &str,
    to_account: rayls_infrastructure_types::Address,
    data_size: usize,
    nonce: u128,
) -> eyre::Result<()> {
    use ethereum_tx_sign::{LegacyTransaction, Transaction};
    use secp256k1::SecretKey;

    let mut to_addr = [0_u8; 20];
    to_addr.copy_from_slice(to_account.as_slice());
    // Large calldata: random-ish bytes.
    let data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();
    // Gas must cover calldata: 16 gas per non-zero byte, 4 per zero byte.
    let gas = 21000u128 + (data_size as u128 * 16);
    let new_transaction = LegacyTransaction {
        chain: CHAIN_ID,
        nonce,
        to: Some(to_addr),
        value: 0,
        gas_price: 250,
        gas,
        data,
    };
    let key_bytes: [u8; 32] =
        const_hex::decode(key)?.try_into().map_err(|_| Report::msg("Invalid secret key length"))?;
    let secret_key = SecretKey::from_byte_array(key_bytes)?;
    let ecdsa = new_transaction
        .ecdsa(&secret_key.secret_bytes())
        .map_err(|_| Report::msg("Failed to get ecdsa"))?;
    let tx_bytes = new_transaction.sign(&ecdsa);
    submit_raw_tx(rpc_url, &tx_bytes)?;
    Ok(())
}

/// Sign and submit a transaction with a specific gas limit.
fn send_tx_with_gas(
    rpc_url: &str,
    key: &str,
    to_account: rayls_infrastructure_types::Address,
    gas: u128,
    nonce: u128,
) -> eyre::Result<()> {
    use ethereum_tx_sign::{LegacyTransaction, Transaction};
    use secp256k1::SecretKey;

    let mut to_addr = [0_u8; 20];
    to_addr.copy_from_slice(to_account.as_slice());
    let new_transaction = LegacyTransaction {
        chain: CHAIN_ID,
        nonce,
        to: Some(to_addr),
        value: rpc::WEI_PER_RLS,
        gas_price: 250,
        gas,
        data: vec![],
    };
    let key_bytes: [u8; 32] =
        const_hex::decode(key)?.try_into().map_err(|_| Report::msg("Invalid secret key length"))?;
    let secret_key = SecretKey::from_byte_array(key_bytes)?;
    let ecdsa = new_transaction
        .ecdsa(&secret_key.secret_bytes())
        .map_err(|_| Report::msg("Failed to get ecdsa"))?;
    let tx_bytes = new_transaction.sign(&ecdsa);
    submit_raw_tx(rpc_url, &tx_bytes)?;
    Ok(())
}

/// RLP-encode a minimal garbage legacy transaction.
///
/// This creates bytes that look transaction-like but have invalid
/// signature components (v, r, s are all zeros).
fn rlp_encode_garbage_legacy_tx(nonce: u64) -> Vec<u8> {
    // Minimal RLP-encoded legacy transaction with invalid signature.
    // Fields: [nonce, gasPrice, gasLimit, to, value, data, v, r, s]
    // We use very simple RLP encoding here — just enough to be parseable
    // but with garbage signature values.
    let mut rng = rand::rng();
    let mut tx_data = Vec::with_capacity(128);

    // RLP list header placeholder.
    tx_data.push(0xf8); // RLP prefix for list > 55 bytes
    let len_pos = tx_data.len();
    tx_data.push(0x00); // placeholder for length

    // nonce (small integer)
    rlp_encode_u64(&mut tx_data, nonce);
    // gasPrice
    rlp_encode_u64(&mut tx_data, 250);
    // gasLimit
    rlp_encode_u64(&mut tx_data, 21000);
    // to (20 bytes)
    tx_data.push(0x94); // RLP prefix for 20-byte string
    tx_data.extend_from_slice(&[0x11u8; 20]);
    // value (1 RLS)
    rlp_encode_u64(&mut tx_data, 1_000_000_000);
    // data (empty)
    tx_data.push(0x80);
    // v (garbage)
    tx_data.push(0x1c); // 28
                        // r (32 random bytes)
    tx_data.push(0xa0); // RLP prefix for 32-byte string
    let r_bytes: [u8; 32] = rng.random();
    tx_data.extend_from_slice(&r_bytes);
    // s (32 random bytes)
    tx_data.push(0xa0);
    let s_bytes: [u8; 32] = rng.random();
    tx_data.extend_from_slice(&s_bytes);

    // Fix up the list length. The 0xf8 prefix declares a single length byte, so
    // the payload must stay <= 255 bytes; the fixed fields above produce ~88 bytes.
    // Guard it so a future change that grows the payload fails loudly in debug
    // builds instead of silently truncating the length via `as u8`.
    let payload_len = tx_data.len() - len_pos - 1;
    debug_assert!(
        payload_len <= 0xff,
        "garbage-tx payload {payload_len} exceeds 1-byte RLP length"
    );
    tx_data[len_pos] = payload_len as u8;

    tx_data
}

/// Simple RLP encoding for a u64 value.
fn rlp_encode_u64(buf: &mut Vec<u8>, val: u64) {
    if val == 0 {
        buf.push(0x80);
    } else if val < 128 {
        buf.push(val as u8);
    } else {
        let bytes = val.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
        let len = 8 - start;
        buf.push(0x80 + len as u8);
        buf.extend_from_slice(&bytes[start..]);
    }
}
