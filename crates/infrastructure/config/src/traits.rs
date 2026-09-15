//! Trait for configurations to read and write to paths.

use eyre::{Context, ContextCompat};
use rayls_infrastructure_types::Epoch;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind::NotFound, Read, Write},
    path::{Path, PathBuf},
};
use tracing::info;

/// The serialization format for the config.
#[derive(PartialEq, Debug)]
pub enum ConfigFmt {
    /// Serialize using YAML.
    YAML,
    /// Serialize using JSON.
    JSON,
}

impl ConfigFmt {
    /// Helper method to identify type.
    pub fn is_json(&self) -> bool {
        *self == Self::JSON
    }
}

/// A trait to read/write types to the filesystem in the specified [ConfigFmt].
///
/// Based on `confy` crate.
pub trait ConfigTrait {
    /// Load an application configuration from a specified path.
    ///
    /// A new configuration file is created with default values if none
    /// exists.
    fn load_from_path<T: Serialize + DeserializeOwned + Default>(
        path: impl AsRef<Path>,
        fmt: ConfigFmt,
    ) -> eyre::Result<T> {
        info!(target: "rayls::config", path = ?path.as_ref(), "Loading configuration");
        let mut file = File::open(path.as_ref())?;
        let mut cfg_string = String::new();
        file.read_to_string(&mut cfg_string)?;

        // return deserialized data in specified format
        match fmt {
            ConfigFmt::YAML => serde_yaml::from_str(&cfg_string).with_context(|| "bad yaml data"),
            ConfigFmt::JSON => serde_json::from_str(&cfg_string).with_context(|| "bad json data"),
        }
    }

    /// Load an application configuration from a specified path.
    ///
    /// A new configuration file is created with default values if none
    /// exists.
    fn load_from_path_or_default<T: Serialize + DeserializeOwned + Default>(
        path: impl AsRef<Path>,
        fmt: ConfigFmt,
    ) -> eyre::Result<T> {
        info!(target: "rayls::config", path = ?path.as_ref(), "Loading configuration");
        match File::open(path.as_ref()) {
            Ok(mut file) => {
                let mut cfg_string = String::new();
                file.read_to_string(&mut cfg_string)?;

                // return deserialized data in specified format
                match fmt {
                    ConfigFmt::YAML => {
                        serde_yaml::from_str(&cfg_string).with_context(|| "bad yaml data")
                    }
                    ConfigFmt::JSON => {
                        serde_json::from_str(&cfg_string).with_context(|| "bad json data")
                    }
                }
            }
            Err(ref e) if e.kind() == NotFound => {
                if let Some(parent) = path.as_ref().parent() {
                    fs::create_dir_all(parent).with_context(|| "Directory creation failed")?;
                }
                let cfg = T::default();
                Self::write_to_path(path, &cfg, fmt)?;
                Ok(cfg)
            }
            Err(e) => eyre::bail!("Failed to open file: {e}"),
        }
    }

    /// Save changes made to a configuration object at a specified path
    ///
    /// This is an alternate version of [`store`] that allows the specification of
    /// an arbitrary path instead of a system one.  For more information on errors
    /// and behavior, see [`store`]'s documentation.
    ///
    /// [`store`]: fn.store.html
    fn write_to_path<T: Serialize>(
        path: impl AsRef<Path>,
        cfg: T,
        fmt: ConfigFmt,
    ) -> eyre::Result<()> {
        let path = path.as_ref();
        let config_dir = path.parent().with_context(|| format!("{path:?} is a root or prefix"))?;
        fs::create_dir_all(config_dir)
            .with_context(|| "directory creation failed while storing")?;

        // serialize in specified fmt
        let s = if fmt.is_json() {
            serde_json::to_string(&cfg).with_context(|| "Failed to serialize config to json")?
        } else {
            serde_yaml::to_string(&cfg).with_context(|| "Failed to serialize config to yaml")?
        };

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| "Failed to open configuration file using OpenOptions")?;

        f.write_all(s.as_bytes()).with_context(|| "Failed to write configuration file")?;
        Ok(())
    }
}

/// Rayls Network specific directories.
pub trait RaylsDirs: std::fmt::Debug + Send + Sync + 'static {
    /// Return the path to parameters yaml file.
    fn node_config_parameters_path(&self) -> PathBuf;
    /// Return the path to the directory that holds
    /// private keys for this node.
    fn node_keys_path(&self) -> PathBuf;
    /// Return the path to `genesis` dir.
    fn genesis_path(&self) -> PathBuf;
    /// Return the path to the directory where individual and public node information stored.
    fn node_info_path(&self) -> PathBuf;
    /// Return the path to the committee file.
    fn committee_path(&self) -> PathBuf;
    /// Return the path to the chain spec file.
    fn genesis_file_path(&self) -> PathBuf;
    /// Return the path to consensus's node storage.
    fn consensus_db_path(&self) -> PathBuf;
    /// Return the path to reth's node storage.
    fn reth_db_path(&self) -> PathBuf;

    /// Return the path to `network_config` file.
    fn network_config_path(&self) -> PathBuf;

    /// Return the path to the hardfork schedule record file.
    fn schedule_record_path(&self) -> PathBuf;

    /// Return the path to consensus's epoch storage for a specific epoch.
    fn epoch_db_path(&self, epoch: Epoch) -> PathBuf {
        let extension = format!("epoch_{epoch}");
        self.consensus_db_path().join(extension)
    }
}

impl<P> RaylsDirs for P
where
    P: AsRef<Path> + std::fmt::Debug + Send + Sync + 'static,
{
    fn node_config_parameters_path(&self) -> PathBuf {
        self.as_ref().join("parameters.yaml")
    }

    fn node_keys_path(&self) -> PathBuf {
        self.as_ref().join("node-keys")
    }

    fn node_info_path(&self) -> PathBuf {
        self.as_ref().join("node-info.yaml")
    }

    fn genesis_path(&self) -> PathBuf {
        self.as_ref().join("genesis")
    }

    fn committee_path(&self) -> PathBuf {
        self.genesis_path().join("committee.yaml")
    }

    fn genesis_file_path(&self) -> PathBuf {
        self.genesis_path().join("genesis.yaml")
    }

    fn consensus_db_path(&self) -> PathBuf {
        self.as_ref().join("consensus-db")
    }

    fn reth_db_path(&self) -> PathBuf {
        self.as_ref().join("db")
    }

    fn network_config_path(&self) -> PathBuf {
        self.as_ref().join("network-config")
    }

    fn schedule_record_path(&self) -> PathBuf {
        self.as_ref().join("schedule-record.yaml")
    }
}
