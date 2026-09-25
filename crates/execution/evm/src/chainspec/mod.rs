//! Rayls ChainSpec wrapper with dynamic base fee and custom hardforks.
//!
//! The submodules are crate-internal; the public API is the re-exports below.

pub(crate) mod fork;
pub(crate) mod hardforks;
pub(crate) mod schedule;
pub(crate) mod spec;

pub use fork::RaylsHardFork;
pub use hardforks::{RaylsChainHardforks, RaylsHardforks};
pub use schedule::ScheduledFork;
pub use spec::{RaylsChainSpec, RaylsChainSpecBuilder};

#[cfg(test)]
mod tests;
