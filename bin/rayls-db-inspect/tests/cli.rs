// SPDX-License-Identifier: BUSL-1.1
//! Argument parsing and `--help` for every subcommand.

#![allow(unused_crate_dependencies)]

use clap::{CommandFactory, Parser};
use rayls_db_inspect::{
    cli::{Cli, Command, WalkTarget},
    run,
};

#[test]
fn parses_every_subcommand() {
    let cli = Cli::try_parse_from(["x", "epoch", "7", "--db", "a", "--db", "b=/p"]).unwrap();
    assert!(matches!(cli.command, Command::Epoch { epoch: 7, .. }));
    assert_eq!(cli.dbs(), vec!["a", "b=/p"]);
    assert!(!cli.json && !cli.verbose && !cli.exclusive && !cli.require_stopped);

    let cli = Cli::try_parse_from(["x", "--json", "-v", "epochs", "1", "5", "-d", "a"]).unwrap();
    assert!(cli.json && cli.verbose);
    assert!(matches!(cli.command, Command::Epochs { from: Some(1), to: Some(5), all: false, .. }));

    let cli = Cli::try_parse_from(["x", "epochs", "--all", "-d", "a", "--exclusive"]).unwrap();
    assert!(cli.exclusive);
    assert!(matches!(cli.command, Command::Epochs { from: None, to: None, all: true, .. }));

    let cli = Cli::try_parse_from(["x", "chain-check", "--from", "2", "-d", "a"]).unwrap();
    assert!(matches!(cli.command, Command::ChainCheck { from: Some(2), to: None, .. }));

    assert!(matches!(
        Cli::try_parse_from(["x", "header", "3", "-d", "a"]).unwrap().command,
        Command::Header { number: 3, .. }
    ));
    assert!(matches!(
        Cli::try_parse_from(["x", "cert", "3", "-d", "a"]).unwrap().command,
        Command::Cert { number: 3, .. }
    ));
    assert!(matches!(
        Cli::try_parse_from(["x", "walk", "header", "9", "--back", "3", "-d", "a"])
            .unwrap()
            .command,
        Command::Walk { target: WalkTarget::Header { number: 9, back: 3, .. } }
    ));
    let cli =
        Cli::try_parse_from(["x", "summary", "--require-stopped", "--recover", "-d", "a"]).unwrap();
    assert!(cli.require_stopped && cli.recover);
    assert!(matches!(cli.command, Command::Summary { .. }));
}

#[test]
fn db_is_accepted_anywhere_and_never_swallows_positionals() {
    for args in [
        ["x", "epochs", "--db", "a", "--db", "b", "0", "5"].as_slice(),
        ["x", "epochs", "0", "5", "--db", "a", "--db", "b"].as_slice(),
        ["x", "epochs", "--db", "a,b", "0", "5"].as_slice(),
        ["x", "--db", "a", "--db", "b", "epochs", "0", "5"].as_slice(),
        ["x", "--db", "a", "epochs", "0", "5", "--db", "b"].as_slice(),
    ] {
        let cli = Cli::try_parse_from(args).unwrap_or_else(|e| panic!("{args:?}: {e}"));
        assert!(
            matches!(cli.command, Command::Epochs { from: Some(0), to: Some(5), .. }),
            "{args:?}"
        );
        assert_eq!(cli.dbs(), vec!["a", "b"], "{args:?}");
    }
}

#[test]
fn missing_db_is_reported_by_run() {
    let cli = Cli::try_parse_from(["x", "epoch", "7"]).expect("clap accepts, run checks");
    assert!(cli.dbs().is_empty());
    let err = run(&cli).expect_err("no database");
    assert!(err.to_string().contains("pass --db"), "{err}");
}

#[test]
fn a_path_where_a_number_belongs_explains_itself() {
    let err = Cli::try_parse_from(["x", "epoch", "/data/node1", "--db", "a"]).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("is a path, not a number"), "{msg}");
    assert!(msg.contains("--db a --db b"), "{msg}");
    let err = Cli::try_parse_from(["x", "epoch", "abc", "--db", "a"]).unwrap_err();
    assert!(err.to_string().contains("`abc` is not a number"), "{err}");
}

#[test]
fn rejects_bad_invocations() {
    assert!(Cli::try_parse_from(["x", "epochs", "-d", "a"]).is_err(), "FROM TO or --all");
    assert!(Cli::try_parse_from(["x", "epochs", "1", "2", "--all", "-d", "a"]).is_err());
    assert!(Cli::try_parse_from(["x", "-d", "a"]).is_err(), "a subcommand is required");
}

#[test]
fn help_renders_for_every_subcommand() {
    let mut cmd = Cli::command();
    cmd.build();
    let top = cmd.render_long_help().to_string();
    assert!(top.contains("Exit status"), "{top}");
    let mut seen = 0;
    for sub in cmd.get_subcommands_mut().filter(|s| s.get_name() != "help") {
        let help = sub.render_long_help().to_string();
        assert!(!help.trim().is_empty(), "{} has no help", sub.get_name());
        seen += 1;
        for nested in sub.get_subcommands_mut() {
            assert!(!nested.render_long_help().to_string().trim().is_empty());
        }
    }
    assert_eq!(seen, 7);
}
