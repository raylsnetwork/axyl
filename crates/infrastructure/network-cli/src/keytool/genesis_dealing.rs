//! Deal epoch 0's threshold key as a genesis validator.

use clap::Args;
use rayls_infrastructure_config::{
    Config, ConfigFmt, ConfigTrait as _, GenesisDealing, KeyConfig, NodeInfo, RaylsDirs,
};
use rayls_infrastructure_types::{
    encode,
    nidkg::{dealing_context, dealing_message, Dealing, EncryptionKey, Recipient},
    quorum_threshold,
    threshold::SecretPolynomial,
    BlsSigner as _,
};
use std::path::PathBuf;
use tracing::info;

/// Second round of a genesis ceremony that gives epoch 0 a threshold key: once every genesis
/// validator's node-info is collected, each validator deals a share of the key to all of them and
/// hands in the output file; the genesis builder takes the directory of those files
/// (`genesis --dkg-dealings`). At least a third of the validators must deal.
#[derive(Debug, Clone, Args)]
pub struct GenesisDealingArgs {
    /// Directory with every genesis validator's node-info file, as the genesis builder collects
    /// them (`genesis/validators`).
    #[arg(long, value_name = "DIR")]
    pub validators: PathBuf,
    /// Chain id the genesis is for.
    #[arg(long = "chain-id")]
    pub chain_id: u64,
    /// File to write this validator's dealing to.
    #[arg(long, value_name = "FILE")]
    pub output: PathBuf,
}

impl GenesisDealingArgs {
    pub fn execute<RLD: RaylsDirs>(
        &self,
        rl_datadir: &RLD,
        passphrase: String,
    ) -> eyre::Result<()> {
        let key_config = KeyConfig::read_config(rl_datadir, passphrase)?;
        let own: NodeInfo = Config::load_from_path(&rl_datadir.node_info_path(), ConfigFmt::YAML)?;
        let mut validators = Vec::new();
        for entry in std::fs::read_dir(&self.validators)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "yaml" || ext == "yml") {
                validators.push(Config::load_from_path::<NodeInfo>(&path, ConfigFmt::YAML)?);
            }
        }
        eyre::ensure!(
            validators.iter().any(|v| v.bls_public_key == key_config.primary_public_key()),
            "this validator is not among the genesis validators in {}",
            self.validators.display()
        );
        validators.sort_by(|a, b| a.bls_public_key.cmp(&b.bls_public_key));
        let recipients = validators
            .iter()
            .enumerate()
            .map(|(position, validator)| {
                let key =
                    EncryptionKey::from_bytes(&validator.dkg_encryption_key).ok_or_else(|| {
                        eyre::eyre!(
                            "validator {} has no dkg encryption key",
                            validator.execution_address
                        )
                    })?;
                Ok(Recipient { position: position as u32, key })
            })
            .collect::<eyre::Result<Vec<_>>>()?;
        let threshold = quorum_threshold(validators.len() as u64) as usize;
        let mut rng = rand::rng();
        let polynomial = SecretPolynomial::random(threshold, &mut rng)?;
        let dealer = own.execution_address;
        let dealing = Dealing::new(
            &dealing_context(self.chain_id, 0, dealer),
            &polynomial,
            &recipients,
            &mut rng,
        )?;
        let bytes = encode(&dealing);
        let signature =
            key_config.request_signature_direct(&dealing_message(self.chain_id, 0, dealer, &bytes));
        let entry = GenesisDealing { dealer, dealing: bytes.into(), signature };
        Config::write_to_path(&self.output, &entry, ConfigFmt::YAML)?;
        info!(target: "rl::genesis_dealing", %dealer, recipients = recipients.len(), threshold, output = %self.output.display(), "genesis dealing written");
        Ok(())
    }
}
