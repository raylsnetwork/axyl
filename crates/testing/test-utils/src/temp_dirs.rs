use rayls_infrastructure_config::RaylsDirs;
use std::path::PathBuf;
use tempfile::{tempdir, TempDir};

#[derive(Debug)]
pub struct RaylsTempDirs(TempDir);

impl Default for RaylsTempDirs {
    fn default() -> Self {
        Self(tempdir().expect("tempdir created"))
    }
}

impl RaylsDirs for RaylsTempDirs {
    fn node_config_parameters_path(&self) -> PathBuf {
        self.0.as_ref().join("parameters.yaml")
    }

    fn node_keys_path(&self) -> PathBuf {
        self.0.path().join("node-keys")
    }

    fn node_info_path(&self) -> PathBuf {
        self.0.path().join("node-info.yaml")
    }

    fn genesis_path(&self) -> PathBuf {
        self.0.path().join("genesis")
    }

    fn committee_path(&self) -> PathBuf {
        self.genesis_path().join("committee.yaml")
    }

    fn genesis_file_path(&self) -> PathBuf {
        self.genesis_path().join("genesis.yaml")
    }

    fn consensus_db_path(&self) -> PathBuf {
        self.0.path().join("consensus-db")
    }

    fn reth_db_path(&self) -> PathBuf {
        self.0.path().join("db")
    }

    fn network_config_path(&self) -> PathBuf {
        self.0.path().join("network-config")
    }

    fn schedule_record_path(&self) -> PathBuf {
        self.0.path().join("schedule-record.yaml")
    }
}
