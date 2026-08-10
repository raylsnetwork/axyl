//! clap [Args](clap::Args) for database configuration

use std::{fmt, str::FromStr};

// use crate::version::default_client_version;
use clap::Args;
use rayls_infrastructure_storage::mdbx::MdbxConfig;

/// Parameters for database configuration
#[derive(Debug, Args, PartialEq, Eq, Default, Clone, Copy)]
#[command(next_help_heading = "Consensus Database")]
pub struct ConsensusDatabaseArgs {
    /// Maximum database size (e.g., 4TB, 8MB)
    #[arg(long = "consensus-db.max-size", value_parser = parse_byte_size)]
    pub consensus_db_max_size: Option<usize>,
    /// Database growth step (e.g., 4GB, 4KB)
    #[arg(long = "consensus-db.growth-step", value_parser = parse_byte_size)]
    pub consensus_db_growth_step: Option<usize>,
    /// Maximum number of readers allowed to access the database concurrently.
    #[arg(long = "consensus-db.max-readers")]
    pub consensus_db_max_readers: Option<u32>,
}

impl ConsensusDatabaseArgs {
    pub fn database_args(&self) -> MdbxConfig {
        let config = MdbxConfig::new();

        let consensus_db_max_size = match self.consensus_db_max_size {
            None => config.max_db_size,
            Some(size) => size,
        };

        let consensus_db_growth_step = match self.consensus_db_growth_step {
            None => config.growth_step,
            Some(step) => step,
        };

        let consensus_db_max_readers = match self.consensus_db_max_readers {
            None => config.max_readers,
            Some(readers) => readers,
        };

        MdbxConfig::new()
            .with_max_db_size(consensus_db_max_size)
            .with_growth_step(consensus_db_growth_step)
            .with_max_readers(consensus_db_max_readers)
    }
}

/// Size in bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub usize);

impl From<ByteSize> for usize {
    fn from(s: ByteSize) -> Self {
        s.0
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_uppercase();
        let parts: Vec<&str> = s.split_whitespace().collect();

        let (num_str, unit) = match parts.len() {
            1 => {
                let (num, unit) =
                    s.split_at(s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len()));
                (num, unit)
            }
            2 => (parts[0], parts[1]),
            _ => {
                return Err("Invalid format. Use '<number><unit>' or '<number> <unit>'.".to_string())
            }
        };

        let num: usize = num_str.parse().map_err(|_| "Invalid number".to_string())?;

        let multiplier = match unit {
            "B" | "" => 1, // Assume bytes if no unit is specified
            "KB" => 1024,
            "MB" => 1024 * 1024,
            "GB" => 1024 * 1024 * 1024,
            "TB" => 1024 * 1024 * 1024 * 1024,
            _ => return Err(format!("Invalid unit: {unit}. Use B, KB, MB, GB, or TB.")),
        };

        Ok(Self(num * multiplier))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const KB: usize = 1024;
        const MB: usize = KB * 1024;
        const GB: usize = MB * 1024;
        const TB: usize = GB * 1024;

        let (size, unit) = if self.0 >= TB {
            (self.0 as f64 / TB as f64, "TB")
        } else if self.0 >= GB {
            (self.0 as f64 / GB as f64, "GB")
        } else if self.0 >= MB {
            (self.0 as f64 / MB as f64, "MB")
        } else if self.0 >= KB {
            (self.0 as f64 / KB as f64, "KB")
        } else {
            (self.0 as f64, "B")
        };

        write!(f, "{size:.2}{unit}")
    }
}

/// Value parser function that supports various formats.
fn parse_byte_size(s: &str) -> Result<usize, String> {
    s.parse::<ByteSize>().map(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use rayls_infrastructure_storage::mdbx::{GIGABYTE, KILOBYTE, MEGABYTE, TERABYTE};

    /// A helper type to parse Args more easily
    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn test_default_database_args() {
        let default_args = ConsensusDatabaseArgs::default();
        let args = CommandParser::<ConsensusDatabaseArgs>::parse_from(["reth"]).args;
        assert_eq!(args, default_args);
    }

    #[test]
    fn test_command_parser_with_valid_max_size() {
        let cmd = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.max-size",
            "4398046511104",
        ])
        .unwrap();
        assert_eq!(cmd.args.consensus_db_max_size, Some(TERABYTE * 4));
    }

    #[test]
    fn test_command_parser_with_invalid_max_size() {
        let result = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.max-size",
            "invalid",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_parser_with_valid_growth_step() {
        let cmd = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.growth-step",
            "4294967296",
        ])
        .unwrap();
        assert_eq!(cmd.args.consensus_db_growth_step, Some(GIGABYTE * 4));
    }

    #[test]
    fn test_command_parser_with_invalid_growth_step() {
        let result = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.growth-step",
            "invalid",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_parser_with_valid_max_size_and_growth_step_from_str() {
        let cmd = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.max-size",
            "2TB",
            "--consensus-db.growth-step",
            "1GB",
        ])
        .unwrap();
        assert_eq!(cmd.args.consensus_db_max_size, Some(TERABYTE * 2));
        assert_eq!(cmd.args.consensus_db_growth_step, Some(GIGABYTE));

        let cmd = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.max-size",
            "12MB",
            "--consensus-db.growth-step",
            "2KB",
        ])
        .unwrap();
        assert_eq!(cmd.args.consensus_db_max_size, Some(MEGABYTE * 12));
        assert_eq!(cmd.args.consensus_db_growth_step, Some(KILOBYTE * 2));

        // with spaces
        let cmd = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.max-size",
            "12 MB",
            "--consensus-db.growth-step",
            "2 KB",
        ])
        .unwrap();
        assert_eq!(cmd.args.consensus_db_max_size, Some(MEGABYTE * 12));
        assert_eq!(cmd.args.consensus_db_growth_step, Some(KILOBYTE * 2));

        let cmd = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.max-size",
            "1073741824",
            "--consensus-db.growth-step",
            "1048576",
        ])
        .unwrap();
        assert_eq!(cmd.args.consensus_db_max_size, Some(GIGABYTE));
        assert_eq!(cmd.args.consensus_db_growth_step, Some(MEGABYTE));
    }

    #[test]
    fn test_command_parser_max_size_and_growth_step_from_str_invalid_unit() {
        let result = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.growth-step",
            "1 PB",
        ]);
        assert!(result.is_err());

        let result = CommandParser::<ConsensusDatabaseArgs>::try_parse_from([
            "reth",
            "--consensus-db.max-size",
            "2PB",
        ]);
        assert!(result.is_err());
    }
}
