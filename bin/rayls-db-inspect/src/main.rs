// SPDX-License-Identifier: BUSL-1.1
//! `rayls-db-inspect`: read-only inspection of Rayls consensus databases.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use clap::Parser;
use rayls_db_inspect::{cli::Cli, render, run};
use tracing_subscriber::EnvFilter;

// Used by the library target only.
use const_hex as _;
use eyre as _;
use rayls_infrastructure_storage as _;
use rayls_infrastructure_types as _;
use serde as _;

fn main() {
    let cli = Cli::parse();

    // Storage-layer logs go to stderr so stdout stays a clean report (or JSON).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let report = match run(&cli) {
        Ok(report) => report,
        Err(err) => {
            eprintln!("error: {err:#}");
            std::process::exit(2);
        }
    };

    if cli.json {
        match serde_json::to_string_pretty(&report) {
            Ok(json) => println!("{json}"),
            Err(err) => {
                eprintln!("error: serialize report: {err}");
                std::process::exit(2);
            }
        }
    } else {
        print!("{}", render::render(&report));
    }

    std::process::exit(if report.healthy() { 0 } else { 1 });
}
