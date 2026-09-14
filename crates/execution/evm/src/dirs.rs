//! Rayls Network data directories.
use rayls_infrastructure_config::RaylsDirs;
use reth::{
    args::DatadirArgs,
    dirs::{ChainPath, MaybePlatformPath, PlatformPath, XdgPath},
};
use reth_chainspec::Chain;
use std::{
    fmt::Debug,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr as _,
};

/// The path to join for the directory that stores node keys.
pub const NODE_KEYS_DIR: &str = "node-keys";
/// The constant for default root directory.
/// This is a workaround for using RL default dir instead of "reth".
pub const DEFAULT_ROOT_DIR: &str = "rayls-network";

/// Workaround for getting default DatadirArgs for reth node config.
pub fn default_datadir_args() -> DatadirArgs {
    // The only way to use "rayls-network" as datadir instead of "reth"
    DatadirArgs {
        datadir: MaybePlatformPath::from_str(DEFAULT_ROOT_DIR)
            .expect("default datadir args always work"),
        // default static path should resolve to: `DEFAULT_ROOT_DIR/<CHAIN_ID>/static_files`
        static_files_path: None,
        pprof_dumps_path: None,
        rocksdb_path: None,
    }
}

/// Returns the path to the rayls network data directory.
///
/// Refer to [dirs_next::data_dir] for cross-platform behavior.
fn data_dir() -> Option<PathBuf> {
    dirs_next::data_dir().map(|root| root.join(DEFAULT_ROOT_DIR))
}

/// Returns the path to the rayls network cache directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
fn cache_dir() -> Option<PathBuf> {
    dirs_next::cache_dir().map(|root| root.join("rayls-network"))
}

/// Returns the path to the rayls network logs directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
fn logs_dir() -> Option<PathBuf> {
    cache_dir().map(|root| root.join("logs"))
}

/// Turn a path (for instance a testing temp directory) into ['DatadirArgs'].
pub fn path_to_datadir<P: AsRef<Path>>(path: P) -> DatadirArgs {
    let path = path.as_ref();
    DatadirArgs {
        datadir: MaybePlatformPath::from(path.to_path_buf()),
        static_files_path: None,
        pprof_dumps_path: None,
        rocksdb_path: None,
    }
}

/// Wrapper around a Reth [ChainPath].
#[derive(Clone, Debug)]
pub struct DataDirChainPath(ChainPath<DataDirPath>);

impl DataDirChainPath {
    /// Create a new DataDirChainPath for testing.  This uses a path for it's base
    /// and defaults for other params.  Going from a simple path to a DataDirChainPath
    /// is a real PITA so capturing this here for lower friction testing and as
    /// documentation for some of this insanity...
    pub fn new_for_test<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        // The None static path may be all that is used here but set the datadir just in case...
        let datadir = path_to_datadir(path);
        // Just use a dummy test chain name.
        let chain = Chain::from_str("rayls-test").expect("valid named chain");
        // Seem to need to use a string for this despite already having a Path...
        let platform_path = PlatformPath::from_str(&path.to_string_lossy())
            .expect("path to string back to path...");
        let chain_path = ChainPath::new(platform_path, chain, datadir);
        Self(chain_path)
    }
}

impl Deref for DataDirChainPath {
    type Target = ChainPath<DataDirPath>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ChainPath<DataDirPath>> for DataDirChainPath {
    fn from(value: ChainPath<DataDirPath>) -> Self {
        Self(value)
    }
}

impl From<DataDirChainPath> for PathBuf {
    fn from(value: DataDirChainPath) -> Self {
        value.0.into()
    }
}

//impl RaylsDirs for ['DataDirChainPath'] (wrapper around ['ChainPath<DataDirPath>']) {
impl RaylsDirs for DataDirChainPath {
    fn node_config_parameters_path(&self) -> PathBuf {
        self.0.as_ref().join("parameters.yaml")
    }

    fn node_keys_path(&self) -> PathBuf {
        self.0.as_ref().join(NODE_KEYS_DIR)
    }

    fn node_info_path(&self) -> PathBuf {
        self.0.as_ref().join("node-info.yaml")
    }

    fn genesis_path(&self) -> PathBuf {
        self.0.as_ref().join("genesis")
    }

    fn committee_path(&self) -> PathBuf {
        self.genesis_path().join("committee.yaml")
    }

    fn genesis_file_path(&self) -> PathBuf {
        self.genesis_path().join("genesis.yaml")
    }

    fn consensus_db_path(&self) -> PathBuf {
        self.0.as_ref().join("consensus-db")
    }

    fn reth_db_path(&self) -> PathBuf {
        self.0.as_ref().join("db")
    }

    fn network_config_path(&self) -> PathBuf {
        self.0.as_ref().join("network-config")
    }
}

/// Returns the path to the rayls network data dir.
///
/// The data dir should contain a subdirectory for each chain, and those chain directories will
/// include all information for that chain, such as the p2p secret.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct DataDirPath;

impl XdgPath for DataDirPath {
    fn resolve() -> Option<PathBuf> {
        data_dir()
    }
}

/// Returns the path to the rayls network logs directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct LogsDir;

impl XdgPath for LogsDir {
    fn resolve() -> Option<PathBuf> {
        logs_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::RaylsNetwork;
    use reth::dirs::MaybePlatformPath;
    use std::str::FromStr;

    #[test]
    fn test_maybe_data_dir_path() {
        let chain_id = RaylsNetwork::Local.chain_id();
        let chain_dir = format!("rayls-network/{chain_id}");
        let path = MaybePlatformPath::<DataDirPath>::default();
        let path = path.unwrap_or_chain_default(Chain::from_id(chain_id), default_datadir_args());
        assert!(path.as_ref().ends_with(&chain_dir), "actual default path is: {path:?}");

        let db_path = path.db();
        assert!(db_path.ends_with(format!("{chain_dir}/db")), "actual db path is: {db_path:?}");

        let static_files_path = path.static_files();
        assert!(
            static_files_path.ends_with(format!("{chain_dir}/static_files")),
            "actual static_files path is: {static_files_path:?}"
        );

        let path = MaybePlatformPath::<DataDirPath>::from_str("my/path/to/datadir").unwrap();
        let path = path.unwrap_or_chain_default(Chain::from_id(chain_id), default_datadir_args());
        assert!(path.as_ref().ends_with("my/path/to/datadir"), "{path:?}");
    }
}
