//! Full-node dev-mode e2e test (issue #590).
//!
//! Boots `rayls-network dev` against a fresh datadir (auto-bootstrap: validator
//! key + single-validator genesis + committee, RPC on, pre-funded dev accounts),
//! submits a transaction over RPC from a well-known dev account, and asserts the
//! chain produced a block that includes the transaction.

use alloy::providers::{Provider, ProviderBuilder};
use e2e_tests::{get_rayls_network_binary, IT_TEST_MUTEX};
use ethereum_tx_sign::{LegacyTransaction, Transaction};
use rayls_infrastructure_types::RaylsNetwork;
use rayls_network_cli::dev::DEV_ACCOUNTS;
use secp256k1::SecretKey;
use std::{process::Child, time::Duration};
use tokio::time::timeout;

const DEV_CHAIN_ID: u64 = RaylsNetwork::Local.chain_id();

/// Kills the node process on drop (including panic unwind) so a failed assertion
/// never leaves an orphaned node holding port 8545.
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[ignore = "boots a full dev node; run independently from other it tests"]
#[tokio::test]
async fn dev_bootstraps_produces_blocks_and_includes_tx() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();

    let temp = tempfile::TempDir::with_prefix("dev_e2e")?;
    let datadir = temp.path();

    // 1. Boot `rayls-network dev` on an EMPTY datadir. This must auto-bootstrap the
    //    single-validator genesis, default the BLS passphrase, and enable HTTP RPC on 8545 — with
    //    no manual keytool/genesis steps and no env vars.
    let bin = get_rayls_network_binary();
    let mut command = bin.command();
    command.arg("dev").arg("--datadir").arg(&*datadir.to_string_lossy());
    let _child = ChildGuard(command.spawn().expect("failed to spawn `rayls-network dev`"));

    let rpc_url = "http://127.0.0.1:8545";
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // 2. Wait for RPC to come up and report the dev chain-id.
    timeout(Duration::from_secs(90), async {
        loop {
            match provider.get_chain_id().await {
                Ok(id) => {
                    assert_eq!(id, DEV_CHAIN_ID, "dev RPC reported unexpected chain-id");
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    })
    .await?;

    // 3. The chain must actually be producing blocks (single-validator commit loop).
    timeout(Duration::from_secs(60), async {
        loop {
            if provider.get_block_number().await.unwrap_or(0) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .map_err(|_| eyre::eyre!("dev chain did not produce any blocks"))?;

    // 4. Submit a transfer from a pre-funded well-known dev account (account #0 -> account #1),
    //    signed locally with the public Anvil key.
    let (from_addr, from_key) = DEV_ACCOUNTS[0];
    let to_addr = DEV_ACCOUNTS[1].0;
    let from_nonce =
        provider.get_transaction_count(from_addr.parse()?).await.expect("nonce for dev account");

    let key_bytes: [u8; 32] = const_hex::decode(from_key.trim_start_matches("0x"))?
        .try_into()
        .map_err(|_| eyre::eyre!("dev key not 32 bytes"))?;
    let secret = SecretKey::from_byte_array(key_bytes)?;

    let mut to = [0u8; 20];
    to.copy_from_slice(&const_hex::decode(to_addr.trim_start_matches("0x"))?);
    let tx = LegacyTransaction {
        chain: DEV_CHAIN_ID,
        nonce: from_nonce as u128,
        to: Some(to),
        value: 1_000_000_000_000_000_000u128, // 1 USDr
        gas_price: 1_000_000_000,             // 1 gwei; the dev account is well funded
        gas: 21_000,
        data: vec![],
    };
    let ecdsa = tx.ecdsa(&secret.secret_bytes()).map_err(|_| eyre::eyre!("ecdsa sign failed"))?;
    let raw = tx.sign(&ecdsa);

    let pending = provider.send_raw_transaction(&raw).await?;
    let receipt = timeout(Duration::from_secs(30), pending.get_receipt())
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for the dev tx receipt"))??;

    // 5. The tx must be included in a real block and have succeeded.
    assert!(
        receipt.block_number.is_some_and(|b| b > 0),
        "dev tx was not included in a block: {receipt:?}"
    );
    assert!(receipt.status(), "dev tx reverted: {receipt:?}");

    Ok(())
}
