use std::sync::OnceLock;

use super::profile::NetworkProfile;

/// The hardfork schedule selected at node start: a `--config-file` subnet
/// profile or the `--network` built-in. `None` only in processes that never
/// ran the CLI's boot gate (in-process test engines), which run all-`Never`.
static ACTIVE_PROFILE: OnceLock<NetworkProfile> = OnceLock::new();

/// Install the active hardfork schedule. Called exactly once, at node start,
/// after the boot gates pass, before the execution layer is built.
pub fn set_active_profile(profile: NetworkProfile) -> eyre::Result<()> {
    ACTIVE_PROFILE.set(profile).map_err(|_| eyre::eyre!("active network profile is already set"))
}

/// The active hardfork schedule, if the CLI boot gate installed one.
pub fn active_profile() -> Option<&'static NetworkProfile> {
    ACTIVE_PROFILE.get()
}
