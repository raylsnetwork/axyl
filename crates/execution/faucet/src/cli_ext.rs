//! Extension for cli.
//!
//! CLI supports adding extensions to the main components for the node.
//! The only extension supported right now is the `faucet` for testnet.

use crate::{FaucetConfig, FaucetRpcExt, FaucetWallet};
use clap::Args;
use ecdsa::elliptic_curve::{pkcs8::DecodePublicKey as _, sec1::ToEncodedPoint};
use eyre::ContextCompat;
use k256::PublicKey as PubKey;
use rayls_execution_evm::{parse_duration_from_secs, reth_env::RethEnv, WorkerTxPool};
use rayls_infrastructure_types::{public_key_to_address, Address, U256};
use secp256k1::PublicKey;
use std::{str::FromStr, time::Duration};
use tracing::{info, warn};

/// Args for running the faucet.
/// Used to build the faucet config.
#[derive(Args, Clone, Debug)]
pub struct FaucetArgs {
    /// The amount of time a recipient must wait before
    /// the faucet will transfer again.
    ///
    /// Specified in seconds.
    #[clap(long, default_value = "86400", value_parser = parse_duration_from_secs, value_name = "WAIT_PERIOD")]
    pub(crate) wait_period: Duration,

    /// The address for the Rayls-Network Faucet which handles RLS and XYZ transfers to each
    /// recipient.
    #[clap(long, default_value_t = Address::ZERO, value_parser = Address::from_str, value_name = "FAUCET_CONTRACT_ADDRESS")]
    pub(crate) faucet_contract: Address,

    /// The public key for the wallet.
    ///
    /// Currently supports pem format or hex (without leading 0x)
    ///
    /// Google KMS strategy:
    /// Use the startup script to retrieve this value and set the env variable in pem format.
    #[clap(
        long,
        value_parser = parse_pubkey,
        env = "FAUCET_PUBLIC_KEY",
        help_heading = "The public key for the faucet wallet.",
        default_value = "0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99",
    )]
    pub(crate) public_key: PublicKey,

    /// Bool indicating Google KMS is in use for faucet signatures.
    ///
    /// When true, the following keys must be set:
    /// - project_id
    /// - key_locations
    /// - key_rings
    /// - crypto_keys
    /// - crypto_keys_versions
    ///
    /// If set to false, the faucet endpoint isn't merged with the configured RPC modules.
    #[clap(long)]
    pub(crate) google_kms: bool,

    /// Google KMS Project ID.
    ///
    /// Used by `name` to make API call.
    #[clap(long, env = "PROJECT_ID")]
    pub(crate) project_id: Option<String>,

    /// Google KMS key locations.
    ///
    /// Used by `name` to make API call.
    #[clap(long, env = "KMS_KEY_LOCATIONS")]
    pub(crate) key_locations: Option<String>,

    /// Google KMS key rings.
    ///
    /// Used by `name` to make API call.
    #[clap(long, env = "KMS_KEY_RINGS")]
    pub(crate) key_rings: Option<String>,

    /// Google KMS crypto keys.
    ///
    /// Used by `name` to make API call.
    #[clap(long, env = "KMS_CRYPTO_KEYS")]
    pub(crate) crypto_keys: Option<String>,

    /// Google KMS crypto key versions.
    ///
    /// Used by `name` to make API call.
    #[clap(long, env = "KMS_CRYPTO_KEY_VERSIONS")]
    pub(crate) crypto_key_versions: Option<String>,
}

impl FaucetArgs {
    /// Create RPC Extension
    ///
    /// reth currently requires an auth server to run this properly.
    /// This is a workaround to manually call `extend_rpc_modules` for
    /// the engine.
    pub fn create_rpc_extension(
        &self,
        reth_env: RethEnv,
        pool: WorkerTxPool,
    ) -> eyre::Result<FaucetRpcExt> {
        // only support google kms for now
        if self.google_kms {
            // calculate address from uncompressed public key
            let public_key = self.public_key;
            let address = public_key_to_address(public_key);
            // compressed public key bytes
            let public_key_bytes = public_key.serialize();

            // set in arg
            let google_project_id = self.project_id.as_ref()
                    .expect("No Google Project ID detected. Please specify it explicitly using env variable: PROJECT_ID");
            // retrieve api information from env
            let locations = self
                .key_locations
                .as_ref()
                .expect("KMS_KEY_LOCATIONS must be set in the environment");
            let key_rings =
                self.key_rings.as_ref().expect("KMS_KEY_RINGS must be set in the environment");
            let crypto_keys =
                self.crypto_keys.as_ref().expect("KMS_CRYPTO_KEYS must be set in the environment");
            let crypto_key_versions = self
                .crypto_key_versions
                .as_ref()
                .expect("KMS_CRYPTO_KEY_VERSIONS must be set in the environment");

            // construct api endpoint for Google KMS requests
            let name = format!(
                "projects/{google_project_id}/locations/{locations}/keyRings/{key_rings}/cryptoKeys/{crypto_keys}/cryptoKeyVersions/{crypto_key_versions}"
            );

            let wallet = FaucetWallet { address, public_key_bytes, name };
            // Derive the chain-id from the node's chain spec so the faucet always signs
            // for the chain it is actually serving (no separate --chain-id to drift).
            let config = FaucetConfig {
                wait_period: self.wait_period,
                chain_id: reth_env.chainspec().chain_id(),
                wallet,
                contract_address: self.faucet_contract,
            };

            let ext = FaucetRpcExt::new(reth_env, pool, config);

            info!(target: "faucet", "Google KMS active - merging faucet extension.");
            return Ok(ext);
        }

        // TODO: support local/hardcoded hot wallet signatures
        warn!(target: "faucet", "Google KMS inactive - skipping faucet extension.");
        Err(eyre::Report::msg("Google KMS inactive - skipping faucet extension."))
        //todo!("Only Google KMS supported right now.")
    }
}

// begin helper/utility functions for parsing faucet values

/// Parse decimal representation for value of RLS.
///
/// RLS has 18 decimal places and requires U256 for
pub fn parse_u256_from_decimal_value(value: &str) -> eyre::Result<U256> {
    let decimal_amount = value.parse::<u16>()?;
    let token_amount = U256::from(10 * decimal_amount)
        .checked_pow(U256::from(18))
        .with_context(|| "Unable to parse decimal representation for faucet amount")?;
    Ok(token_amount)
}

/// Parse public key from pem or hex slice.
fn parse_pubkey(value: &str) -> eyre::Result<PublicKey> {
    // google kms uses pem key formatting
    let public_key = if value.contains("-----BEGIN PUBLIC KEY-----") {
        // k256 public key to convert from pem
        let pubkey_from_pem = PubKey::from_public_key_pem(value)?;
        // secp256k1 public key from uncompressed k256 variation
        PublicKey::from_slice(pubkey_from_pem.to_encoded_point(false).as_bytes())?
    } else {
        // note: default value set if missing from env
        PublicKey::from_str(value)?
    };

    Ok(public_key)
}

#[cfg(test)]
mod tests {
    use crate::FaucetArgs;
    use clap::Parser;
    use rayls_infrastructure_types::test_utils::CommandParser;
    use secp256k1::PublicKey;
    use std::str::FromStr;

    #[test]
    fn test_pem_pubkey_parses() {
        // test pubkey passed to cli
        let parsed = CommandParser::<FaucetArgs>::try_parse_from([
            "rayls",
            "--public-key",
            "029bef8d556d80e43ae7e0becb3a7e6838b95defe45896ed6075bb9035d06c9964",
        ])
        .expect("parsed default args");

        let expected = PublicKey::from_str(
            "029bef8d556d80e43ae7e0becb3a7e6838b95defe45896ed6075bb9035d06c9964",
        )
        .unwrap();
        assert_eq!(parsed.args.public_key, expected);

        // test google kms active without setting required API info in the env
        let missing_env_parsed =
            CommandParser::<FaucetArgs>::try_parse_from(["rayls", "--google_kms"]);
        assert!(missing_env_parsed.is_err());

        // Google KMS example
        let pem_public_key = "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEqzv8pSIJXo3PJZsGv+feaCZJFQoG3ed5\ngl0o/dpBKtwT+yajMYTCravDiqW/g62W+PNVzLoCbaot1WdlwXcp4Q==\n-----END PUBLIC KEY-----\n";
        std::env::set_var("FAUCET_PUBLIC_KEY", pem_public_key);
        // std::env::set_var("PROJECT_ID", "test-project");
        // std::env::set_var("KMS_KEY_LOCATIONS", "global");
        // std::env::set_var("KMS_KEY_RINGS", "test-key-ring");
        // std::env::set_var("KMS_CRYPTO_KEYS", "test-crypto-keys");
        // std::env::set_var("KMS_CRYPTO_KEY_VERSIONS", "1");

        let pem_parsed = CommandParser::<FaucetArgs>::try_parse_from(["rayls", "--google-kms"])
            .expect("parse google kms active");

        let expected = PublicKey::from_str(
            "03ab3bfca522095e8dcf259b06bfe7de682649150a06dde779825d28fdda412adc",
        )
        .unwrap();
        assert_eq!(pem_parsed.args.public_key, expected);
    }
}
