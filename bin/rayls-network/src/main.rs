// SPDX-License-Identifier: BUSL-1.1
//! Main binary for Rayls CLI

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser as _;
#[cfg(feature = "faucet")]
use rayls_execution_faucet::FaucetArgs;
use rayls_middleware_orchestrator::launch_node;
use rayls_network_cli::cli::{Commands, PassSource};

const BLS_PASSPHRASE_ENVVAR: &str = "RL_BLS_PASSPHRASE";

/// Read the bls key passphrase from then incoming environment if set.
/// This also will remove the key once read to avoid leaks in future.
/// This is meant to be called once at the very beginning of program
/// start before any threads exists.  It will only return the passphrase
/// on the first call (it clears the env if it is set).
fn get_bls_passphrase_from_env() -> Option<String> {
    if let Ok(passphrase) = std::env::var(BLS_PASSPHRASE_ENVVAR) {
        if !passphrase.is_empty() {
            // Clear then remove the passphrase from the env.
            // NOTE: This is probably not doing much but is an attempt to make the var "more
            // deleted". This will depend on the underlying platform/libc but should
            // worst case does nothing. Note on safety, these need calls need to happen
            // to avoid any leaks of the passphrase if set and they are unsafe.  They
            // are unsafe because they are not thread safe and we only call this
            // function once at the beginning of startup so no threads should exist yet.
            unsafe {
                std::env::set_var(BLS_PASSPHRASE_ENVVAR, "");
                std::env::remove_var(BLS_PASSPHRASE_ENVVAR);
            }
            Some(passphrase)
        } else {
            None
        }
    } else {
        None
    }
}

fn read_passphrase() -> Option<String> {
    while let Ok(pw) = rpassword::prompt_password("Enter a passphrase to ecrypt BLS key: ") {
        if let Ok(pw2) = rpassword::prompt_password("Re-enter BLS key passphrase to confirm: ") {
            if pw == pw2 {
                return if pw.is_empty() {
                    println!("No passphrase set for BLS key, this is not recommended.");
                    None
                } else {
                    Some(pw)
                };
            }
        }
        println!("Passphrases do not match, retry.");
    }
    None
}

fn main() {
    // Access the environment befor we do anything else, even use CLAP.
    let mut passphrase = get_bls_passphrase_from_env();
    #[cfg(not(feature = "faucet"))]
    let cli = rayls_network_cli::cli::Cli::<rayls_network_cli::NoArgs>::parse();
    #[cfg(feature = "faucet")]
    let cli = rayls_network_cli::cli::Cli::<FaucetArgs>::parse();

    // Sort out the BLS key passphrase depending on the command run.
    match cli.bls_passphrase_source {
        PassSource::Env => {} // Already have the env var if provided.
        // Only read stdin for commands that use the passphrase: a passphrase-free command
        // (cold-migrate) must not block on, or consume, piped input it will discard.
        PassSource::Stdin if cli.command.needs_bls_passphrase() => {
            let mut buffer = String::new();
            if let Err(err) = std::io::stdin().read_line(&mut buffer) {
                eprintln!("Error reading BLS passphrase from stdin: {err:?}");
                std::process::exit(1);
            }
            passphrase = Some(buffer.trim_end().to_string());
        }
        PassSource::Stdin => {}
        PassSource::Ask => match cli.command {
            Commands::Keytool(_) => {
                // Need to ask and confirm before it used to encrypt.
                passphrase = read_passphrase();
            }
            Commands::Genesis(_) => {} // Don't need the passphrase..
            Commands::Node(_) => {
                // Simple ask once and app will error out later if this is wrong.
                passphrase =
                    rpassword::prompt_password("Enter the BLS key passphrase to decrypt: ").ok();
            }
            // The migration never opens the BLS key, so there is nothing to ask for.
            #[cfg(feature = "cold-storage")]
            Commands::ColdMigrate(_) => {}
            // `dev` is one-command and supplies its own default passphrase below;
            // never prompt for it.
            #[cfg(feature = "dev-single-node-setup")]
            Commands::Dev(_) => {}
        },
    }

    // `rayls-network dev` is meant to run with zero setup, so default the BLS-key
    // passphrase when none was supplied. The key stays in the local dev datadir and
    // protects nothing of value on a throwaway dev chain.
    #[cfg(feature = "dev-single-node-setup")]
    if passphrase.is_none() && matches!(cli.command, Commands::Dev(_)) {
        eprintln!("dev mode: using the default dev BLS passphrase (local development only)");
        passphrase = Some(rayls_network_cli::dev::DEV_PASSPHRASE.to_string());
    }

    if cli.command.needs_bls_passphrase() && passphrase.is_none() {
        eprintln!(
            "Error passphrase is required, see the option --bls-passphrase-source for options"
        );
        std::process::exit(1);
    }
    let passphrase = passphrase.unwrap_or_default();

    #[cfg(not(feature = "faucet"))]
    if let Err(err) = cli.run(passphrase, |builder, _, rl_datadir, passphrase| {
        launch_node(builder, rl_datadir, passphrase)
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }

    #[cfg(feature = "faucet")]
    if let Err(err) = cli.run(passphrase, |mut builder, faucet, rl_datadir, passphrase| {
        builder.opt_faucet_args = Some(faucet);
        launch_node(builder, rl_datadir, passphrase)
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
