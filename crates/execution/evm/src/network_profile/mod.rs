//! External hardfork configuration: one file per client, holding any number
//! of named subnets.
//!
//! A node started with `--config-file` / `--subnet` loads the selected
//! subnet's hardfork schedule from such a file instead of the schedule baked
//! into the binary; everything else (genesis, parameters, committee, node
//! identity) still comes from the node's datadir.
//!
//! The selected subnet is carried explicitly: the CLI boot gate hands the
//! [`NetworkProfile`] to the node builder, which passes it to every execution
//! layer constructor. Nothing is process-global, so several nodes (or tests)
//! can run in one process with different schedules.

mod activation;
mod config_file;
mod fork_name;
mod genesis_check;
mod profile;
mod record;
mod verify;

#[cfg(test)]
mod tests;

pub use activation::ForkActivation;
pub use config_file::NetworkConfigFile;
pub use fork_name::ForkName;
pub use genesis_check::{verify_schedule_against_genesis, SimAlloc};
pub use profile::NetworkProfile;
pub use record::ScheduleRecord;
pub use verify::{
    verify_datadir_schedule_record, verify_schedule, FutureForkMove, ScheduleRecordVerification,
};
