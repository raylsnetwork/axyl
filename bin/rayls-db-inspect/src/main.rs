// SPDX-License-Identifier: BUSL-1.1
//! `rayls-db-inspect`: read-only inspection of Rayls consensus databases.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use clap::Parser;
use rayls_db_inspect::{cli::Cli, render, run};
use tracing_subscriber::EnvFilter;

// Used by the library target only.
use alloy as _;
use comfy_table as _;
use const_hex as _;
use eyre as _;
use rayls_infrastructure_storage as _;
use rayls_infrastructure_types as _;
use rayls_infrastructure_utils as _;
use reth_db as _;
use reth_ethereum_primitives as _;
use reth_libmdbx as _;
use reth_provider as _;
use reth_trie as _;
use reth_trie_db as _;
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

    // This tool has no chainspec, so nothing here sets the SignerClaims activation handle: it stays
    // at its never default and every header is read with the pre-activation layout. That is not
    // safe over a database written after the activation, where the raw leader projections can
    // return plausible garbage rather than only an error, so `NodeDb::open` probes the newest
    // header and refuses such a database instead of reporting projections built on the wrong
    // era.
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
