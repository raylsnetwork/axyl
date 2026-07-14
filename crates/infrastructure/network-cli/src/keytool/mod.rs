//! Key command to generate all keys for running a node.

mod generate;
mod rotate_passphrase;
mod stake_calldata;
use self::generate::NodeType;
use clap::{Args, Subcommand};
use eyre::{eyre, Context};

use generate::GenerateKeys;
use rayls_infrastructure_config::RaylsDirs as _;
// dev-only: used by the feature-gated `generate_validator_keys` below
#[cfg(feature = "dev-single-node-setup")]
use rayls_infrastructure_types::Address;
use rotate_passphrase::RotatePassphrase;
use stake_calldata::GenerateStakeCalldata;
use std::path::{Path, PathBuf};
use tracing::warn;

/// Programmatically generate a single validator's keys and `node-info.yaml` into
/// `datadir`, equivalent to `keytool generate validator --address <execution_address>`.
///
/// Used by dev-mode auto-bootstrap so a developer doesn't have to run the keytool
/// step by hand. Network addresses default to `127.0.0.1` with OS-assigned UDP ports.
#[cfg(feature = "dev-single-node-setup")]
pub fn generate_validator_keys(
    datadir: &Path,
    execution_address: Address,
    passphrase: String,
) -> eyre::Result<()> {
    // Owned PathBuf so the `RaylsDirs` blanket impl (which requires `'static`)
    // resolves to `PathBuf` rather than a borrowed `&Path`.
    let datadir = datadir.to_path_buf();
    let args = generate::KeygenArgs {
        workers: 1,
        force: false,
        address: execution_address,
        external_primary_addr: None,
        external_worker_addrs: None,
        relay: None,
        advertise_dnsaddr: None,
    };
    let key_path = datadir.node_keys_path();
    if !key_path.exists() {
        std::fs::create_dir_all(&key_path)?;
    }
    args.execute(&datadir, passphrase)
}

/// Generate keypairs and node info to go with them and save them to a file.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct KeyArgs {
    /// Generate command that creates keypairs and writes to file.
    ///
    /// Intentionally leaving this here to help others identify
    /// patterns in clap.
    #[command(subcommand)]
    pub command: KeySubcommand,
}

///Subcommand to either generate keys or read public keys.
#[derive(Debug, Clone, Subcommand)]
pub enum KeySubcommand {
    /// Generate keys and write to file.
    #[command(name = "generate")]
    Generate(GenerateKeys),
    /// Generate stake calldata for staking transaction.
    #[command(name = "stake-calldata")]
    StakeCalldata(GenerateStakeCalldata),
    /// Re-encrypt the BLS keystore under a new passphrase (node identity unchanged).
    #[command(name = "rotate-passphrase")]
    RotatePassphrase(RotatePassphrase),
}

impl KeyArgs {
    /// Execute command
    pub fn execute(&self, datadir: PathBuf, passphrase: String) -> eyre::Result<()> {
        match &self.command {
            // generate keys
            KeySubcommand::Generate(args) => {
                let args = match &args.node_type {
                    NodeType::ValidatorKeys(args) => args,
                    NodeType::ObserverKeys(args) => args,
                };
                let authority_key_path = datadir.node_keys_path();
                // initialize path and warn users if overwriting keys
                self.init_path(&authority_key_path, args.force)?;
                // execute and store keypath
                args.execute(&datadir, passphrase)?;
            }
            KeySubcommand::StakeCalldata(args) => {
                args.execute(&datadir)?;
            }
            // `passphrase` is the current one; the new passphrase is sourced in execute.
            KeySubcommand::RotatePassphrase(args) => {
                args.execute(&datadir, passphrase)?;
            }
        }

        Ok(())
    }

    /// Ensure the path exists, and if not, create it.
    fn init_path<P: AsRef<Path>>(&self, path: P, force: bool) -> eyre::Result<()> {
        let rpath = path.as_ref();

        // create the dir if it doesn't exist or is empty
        if self.is_key_dir_empty(rpath) {
            // authority dir
            std::fs::create_dir_all(rpath).wrap_err_with(|| {
                format!("Could not create authority key directory {}", rpath.display())
            })?;
        } else if !force {
            warn!("pass `force` to overwrite keys for node");
            return Err(eyre!("cannot overwrite node keys without passing --force"));
        }

        Ok(())
    }

    /// Check if key file directory is empty.
    fn is_key_dir_empty<P: AsRef<Path>>(&self, path: P) -> bool {
        let rpath = path.as_ref();

        if !rpath.exists() {
            true
        } else if let Ok(dir) = rpath.read_dir() {
            dir.count() == 0
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{cli::Cli, NoArgs};
    use clap::Parser;
    use rayls_infrastructure_config::{Config, ConfigFmt, ConfigTrait, NodeInfo};

    /// Test that generate keys command works.
    /// This test also ensures that confy is able to
    /// load the default config.toml, update the file,
    /// and save it.
    #[tokio::test]
    async fn test_generate_keypairs() {
        // use tempdir
        let tempdir = tempfile::TempDir::new().expect("tempdir created");
        let temp_path = tempdir.path();
        let _ = Cli::<NoArgs>::try_parse_from([
            "rayls-network",
            "keytool",
            "generate",
            "validator",
            "--workers",
            "1",
            "--datadir",
            temp_path.to_str().expect("tempdir path clean"),
            "--address",
            "0",
        ])
        .expect("cli parsed");

        Config::load_from_path_or_default::<NodeInfo>(
            temp_path.join("node-info.yaml").as_path(),
            ConfigFmt::YAML,
        )
        .expect("config loaded yaml okay");
    }
}
