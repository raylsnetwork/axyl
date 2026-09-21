//! External hardfork configuration: one file per client, holding any number
//! of named subnets.
//!
//! A node started with `--config-file` / `--subnet` loads the selected
//! subnet's hardfork schedule from such a file instead of the schedule baked
//! into the binary; everything else (genesis, parameters, committee, node
//! identity) still comes from the node's datadir.
//!
//! The selected subnet is stored in a process-wide
//! [`OnceLock`](std::sync::OnceLock) so the execution layer can reach it
//! without threading it through every constructor.

mod activation;
mod active;
mod config_file;
mod fork_name;
mod profile;
mod record;
mod verify;

#[cfg(test)]
mod tests;

pub use activation::ForkActivation;
pub use active::{active_profile, set_active_profile};
pub use config_file::NetworkConfigFile;
pub use fork_name::ForkName;
pub use profile::NetworkProfile;
pub use record::ScheduleRecord;
pub use verify::{verify_schedule, FutureForkMove};
