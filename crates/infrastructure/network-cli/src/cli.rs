//! CLI definition and entrypoint to executable
#[cfg(feature = "dev-single-node-setup")]
use crate::dev;
use crate::{
    genesis, keytool, node,
    version::{LONG_VERSION, SHORT_VERSION},
    NoArgs,
};
use clap::{Parser, Subcommand};
use rayls_execution_evm::{dirs::DEFAULT_ROOT_DIR, FileWorkerGuard, LogArgs};
use rayls_middleware_orchestrator::engine::RaylsBuilder;
use std::{ffi::OsString, fmt, path::PathBuf, str::FromStr};

/// How do we want to get the BLS key passphrase?
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum PassSource {
    /// Get the passphrase from then environment variable RL_BLS_PASSPHRASE.
    Env,
    /// Read the passphrase from stdin.  Will read the first line of stdin or until EOF.
    Stdin,
    /// Ask the user on startup, only works if running in foreground on a TTY.
    Ask,
}

/// The main RL cli interface.
///
/// This is the entrypoint to the executable.
#[derive(Debug, Parser)]
#[command(author, version = SHORT_VERSION, long_version = LONG_VERSION, about = "Rayls Network", long_about = None)]
pub struct Cli<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// The command to run
    #[clap(subcommand)]
    pub command: Commands<Ext>,

    /// How to get the BLS key passphrase.
    ///
    /// The default is to use the env variable RL_BLS_PASSPHRASE
    /// Note, this variable should be securily managed if used on a validator.
    #[arg(
        long,
        value_name = "RL_PASSPHRASE_SOURCE",
        verbatim_doc_comment,
        default_value = "env",
        global = true
    )]
    pub bls_passphrase_source: PassSource,

    /// The path to the data dir for all rayls-network files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/rayls-network/` or `$HOME/.local/share/rayls-network/`
    /// - Windows: `{FOLDERID_RoamingAppData}/rayls-network/`
    /// - macOS: `$HOME/Library/Application Support/rayls-network/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, global = true)]
    pub datadir: Option<PathBuf>,

    /// The log configuration.
    #[clap(flatten)]
    pub logs: LogArgs,
}

impl Cli {
    /// Parsers only the default CLI arguments
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Parsers only the default CLI arguments from the given iterator
    pub fn try_parse_args_from<I, T>(itr: I) -> Result<Self, clap::error::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        Cli::try_parse_from(itr)
    }
}

impl<Ext: clap::Args + fmt::Debug> Cli<Ext> {
    /// Execute the configured cli command.
    ///
    /// This accepts a closure that is used to launch the node via the
    /// [NodeCommand](node::NodeCommand).
    ///
    ///
    /// # Example
    ///
    /// Parse additional CLI arguments for the node command and use it to configure the node.
    ///
    /// ```no_run
    /// use clap::Parser;
    /// use rayls_middleware_orchestrator::launch_node;
    /// use rayls_network_cli::cli::Cli;
    ///
    /// #[derive(Debug, Parser)]
    /// pub struct MyArgs {
    ///     pub enable: bool,
    /// }
    ///
    /// if let Err(err) = rayls_network_cli::cli::Cli::<MyArgs>::parse()
    ///     .run("password".to_owned(), |builder, _, rl_datadir, passphrase| {
    ///         launch_node(builder, rl_datadir, passphrase)
    ///     })
    /// {
    ///     eprintln!("Error: {err:?}");
    ///     std::process::exit(1);
    /// }
    /// ```
    pub fn run<L>(mut self, passphrase: String, launcher: L) -> eyre::Result<()>
    where
        L: FnOnce(RaylsBuilder, Ext, PathBuf, String) -> eyre::Result<()>,
    {
        let datadir: PathBuf = self.datadir.take().unwrap_or_else(|| {
            dirs_next::data_dir().map(|root| root.join(DEFAULT_ROOT_DIR)).unwrap_or_else(|| {
                PathBuf::from_str(&format!("./{DEFAULT_ROOT_DIR}")).expect("data dir")
            })
        });
        // add network name to logs dir
        self.logs.log_file_directory = self.logs.log_file_directory.join("rayls-network-logs");

        // The migration's progress lines are info-level and it can run for many minutes; floor
        // its targets at info through the stdout filter (target directives override the global
        // verbosity default) so a quieter `-v`/`-vv` does not run it silently. An explicit
        // `--quiet` still wins.
        #[cfg(feature = "cold-storage")]
        if matches!(self.command, Commands::ColdMigrate(_))
            && self.logs.verbosity.directive().to_string() != "off"
        {
            let floor = "cold-archive=info,epoch-manager=info,storage=info";
            self.logs.log_stdout_filter = if self.logs.log_stdout_filter.is_empty() {
                floor.to_owned()
            } else {
                format!("{},{floor}", self.logs.log_stdout_filter)
            };
        }
        let _guard = self.init_tracing()?;

        match self.command {
            Commands::Genesis(command) => command.execute(datadir),
            Commands::Node(command) => command.execute(datadir, passphrase, launcher),
            // Reuses the node command's full argument surface but swaps the launcher: the
            // orchestrator runs only the boot-time cold work and returns without starting the
            // node. The maintenance entry point emits no node start banner and never
            // dev-bootstraps the datadir; the BLS passphrase is never used.
            #[cfg(feature = "cold-storage")]
            Commands::ColdMigrate(command) => {
                let ColdMigrateCommand { node, compact } = *command;
                node.execute_maintenance(datadir, passphrase, move |builder, _, rl_datadir, _| {
                    rayls_middleware_orchestrator::run_cold_migration(builder, rl_datadir, compact)
                })
            }
            Commands::Keytool(command) => command.execute(datadir, passphrase),
            #[cfg(feature = "dev-single-node-setup")]
            Commands::Dev(command) => command.execute(datadir, passphrase, launcher),
        }
    }

    /// Initializes tracing with the configured options.
    ///
    /// If file logging is enabled, this function returns a guard that must be kept alive to ensure
    /// that all logs are flushed to disk.
    pub fn init_tracing(&self) -> eyre::Result<Option<FileWorkerGuard>> {
        let guard = self.logs.init_tracing()?;
        Ok(guard)
    }
}

/// Commands to be executed
#[derive(Debug, Subcommand)]
pub enum Commands<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Genesis ceremony for starting the network.
    #[command(name = "genesis")]
    Genesis(Box<genesis::GenesisArgs>),

    /// Key management.
    /// Generate or read keys for node management.
    #[command(name = "keytool")]
    Keytool(keytool::KeyArgs),

    /// Start the node
    #[command(name = "node")]
    Node(Box<node::NodeCommand<Ext>>),

    /// Run the boot-time cold-storage migration and exit without starting the node.
    ///
    /// Accepts the same arguments as `node`, needs no BLS passphrase, and is idempotent: it
    /// drains the cold backlog (and heals a crash-interrupted archive) ahead of a real start.
    #[cfg(feature = "cold-storage")]
    #[command(name = "cold-migrate")]
    ColdMigrate(Box<ColdMigrateCommand<Ext>>),

    /// Run a one-command local dev chain.
    ///
    /// Bootstraps an empty datadir (validator key + single-validator genesis +
    /// committee), starts the node with RPC enabled, a gasless local chain-id, and
    /// pre-funded well-known dev accounts. For local development only.
    #[cfg(feature = "dev-single-node-setup")]
    #[command(name = "dev")]
    Dev(Box<dev::DevCommand<Ext>>),
}

impl<Ext: clap::Args + fmt::Debug> Commands<Ext> {
    /// Returns true when the command needs the BLS key passphrase to run.
    ///
    /// The per-command policy lives here, next to the variant declarations, so the binary's
    /// passphrase gate stays free of feature-gated variant knowledge.
    pub fn needs_bls_passphrase(&self) -> bool {
        // The migration never opens the BLS key; every other command keeps the requirement so a
        // missing passphrase fails closed before any real work starts.
        #[cfg(feature = "cold-storage")]
        if matches!(self, Commands::ColdMigrate(_)) {
            return false;
        }
        true
    }
}

/// Arguments for the `cold-migrate` command: the node surface plus migration-only options.
#[cfg(feature = "cold-storage")]
#[derive(Debug, Parser)]
pub struct ColdMigrateCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// The node arguments; `cold-migrate` accepts the same surface as `node`.
    #[clap(flatten)]
    pub node: node::NodeCommand<Ext>,

    /// Copy-compact the consensus DB in place after the migration.
    ///
    /// Archival prunes most hot rows into the cold store, but MDBX only recycles the freed
    /// pages; compaction rewrites the datafile to its live size, reclaiming the disk.
    #[arg(long)]
    pub compact: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use rayls_execution_evm::ColorMode;
    use rayls_infrastructure_config::Config;

    #[test]
    fn parse_color_mode() {
        let rl = Cli::try_parse_args_from(["rl", "node", "--color", "always"]).unwrap();
        assert_eq!(rl.logs.color, ColorMode::Always);
    }

    /// Tests that the help message is parsed correctly. This ensures that clap args are configured
    /// correctly and no conflicts are introduced via attributes that would result in a panic at
    /// runtime
    #[test]
    fn test_parse_help_all_subcommands() {
        let rl = Cli::<NoArgs>::command();
        for sub_command in rl.get_subcommands() {
            let err = Cli::try_parse_args_from(["rl", sub_command.get_name(), "--help"])
                .err()
                .unwrap_or_else(|| {
                    panic!("Failed to parse help message {}", sub_command.get_name())
                });

            // --help is treated as error, but
            // > Not a true "error" as it means --help or similar was used. The help message will be sent to stdout.
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
        }
    }

    /// Tests that the log directory is parsed correctly.
    #[test]
    fn parse_logs_path() {
        let rl = Cli::try_parse_args_from(["rl", "node"]).unwrap();
        let log_dir = rl.logs.log_file_directory;

        // let end = format!("{}/logs", DEFAULT_ROOT_DIR);

        let end = "reth/logs".to_string();
        assert!(log_dir.as_ref().ends_with(end), "{log_dir:?}");
    }

    #[tokio::test]
    async fn parse_env_filter_directives() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create config files or the run() below will fail.
        Config::load_or_default(&temp_dir.path().to_path_buf(), true, "test").unwrap();
        std::env::set_var("RUST_LOG", "info,evm=debug");
        let _rl: Cli = Cli::try_parse_args_from([
            "rl",
            "node",
            "--datadir",
            temp_dir.path().to_str().unwrap(),
            "--log.file.filter",
            "debug,net=trace",
        ])
        .unwrap();
    }

    /// `cold-migrate` accepts the node command's argument surface plus `--compact`.
    #[cfg(feature = "cold-storage")]
    #[test]
    fn parse_cold_migrate() {
        let rl = Cli::try_parse_args_from(["rl", "cold-migrate", "--observer"]).unwrap();
        match rl.command {
            Commands::ColdMigrate(cmd) => assert!(!cmd.compact, "--compact must default off"),
            _ => panic!("expected ColdMigrate command"),
        }

        let rl = Cli::try_parse_args_from(["rl", "cold-migrate", "--compact"]).unwrap();
        match rl.command {
            Commands::ColdMigrate(cmd) => assert!(cmd.compact),
            _ => panic!("expected ColdMigrate command"),
        }
    }

    #[cfg(feature = "dev-single-node-setup")]
    #[test]
    fn parse_dev_flag() {
        let rl = Cli::try_parse_args_from(["rl", "node", "--dev"]).unwrap();
        match rl.command {
            Commands::Node(cmd) => assert!(cmd.dev, "--dev should set NodeCommand::dev=true"),
            _ => panic!("expected Node command"),
        }
    }
}
