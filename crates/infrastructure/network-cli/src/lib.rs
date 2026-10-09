// SPDX-License-Identifier: BUSL-1.1
//! Rayls Network (RL) binary executable.
//!
//! ## Feature Flags
//!
//! - `min-error-logs`: Disables all logs below `error` level.
//! - `min-warn-logs`: Disables all logs below `warn` level.
//! - `min-info-logs`: Disables all logs below `info` level. This can speed up the node, since fewer
//!   calls to the logging component is made.
//! - `min-debug-logs`: Disables all logs below `debug` level.
//! - `min-trace-logs`: Disables all logs below `trace` level.
//! - `dev`: Enables local single-node development mode — the `dev` subcommand and `node --dev`
//!   (auto-bootstrap, gasless chain, embedded dashboard). Off by default; NOT FOR PRODUCTION (never
//!   enable in release builds).

#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub mod args;
pub mod cli;
#[cfg(feature = "dev-single-node-setup")]
pub mod dev;
pub mod genesis;
pub mod keytool;
pub mod node;
pub mod schedule;
pub mod version;

/// No Additional arguments
#[derive(Debug, Clone, Copy, Default, clap::Args)]
pub struct NoArgs;
