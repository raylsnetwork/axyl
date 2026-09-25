//! Rayls ChainSpec wrapper with dynamic base fee and custom hardforks.

pub mod fork;
pub mod hardforks;
pub mod schedule;
pub mod spec;

pub use fork::RaylsHardFork;
pub use hardforks::{RaylsChainHardforks, RaylsHardforks};
pub use schedule::ScheduledFork;
pub use spec::{RaylsChainSpec, RaylsChainSpecBuilder};

#[cfg(test)]
mod tests;
