//! Tests for the boot-time schedule shape gate wiring: the peer-head probe,
//! the genesis / no-other-validator cases, and the refusal to guess when no
//! peer answers.

use crate::epoch_manager::{verify_boot_schedule_shape, BootShapeGateOutcome};
use rayls_consensus_primary::network::{
    NetworkCommand, PrimaryNetworkHandle, PrimaryRPCError, PrimaryRequest, PrimaryResponse,
    NO_CONSENSUS_OUTPUT,
};
use rayls_execution_evm::network_profile::{ForkActivation, ForkName, NetworkProfile};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{ConsensusHeader, Notifier, RaylsNetwork};
use rayls_testing_test_utils::CommitteeFixture;
use std::{num::NonZeroUsize, sync::Arc};
use tokio::sync::mpsc;

type PrimaryCommand = NetworkCommand<PrimaryRequest, PrimaryResponse>;

fn config_with_committee(
    size: usize,
) -> (CommitteeFixture<MemDatabase>, ConsensusConfig<MemDatabase>) {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(size).unwrap())
        .build();
    let config = fixture.authorities().next().unwrap().consensus_config();
    (fixture, config)
}

fn config() -> (CommitteeFixture<MemDatabase>, ConsensusConfig<MemDatabase>) {
    config_with_committee(4)
}

/// A profile that is invalid on a new chain but valid once the chain exists:
/// `AdminTransfer` is a Set A fork pinned to `Block(0)` for a greenfield chain,
/// while an existing chain accepts `Never`.
fn tampered_profile() -> NetworkProfile {
    let mut profile = NetworkProfile::from_builtin(RaylsNetwork::Local);
    profile.hardforks.insert(ForkName::from("AdminTransfer"), ForkActivation::Never);
    profile
}

/// A network whose receiver is dropped: every request fails immediately, as
/// with no reachable peers.
fn no_peer_network() -> PrimaryNetworkHandle {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    PrimaryNetworkHandle::new_for_test(tx)
}

/// A network that answers every `SendRequest` with `response`.
fn answering_network(response: PrimaryResponse) -> PrimaryNetworkHandle {
    let (tx, mut rx) = mpsc::channel::<PrimaryCommand>(64);
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            if let NetworkCommand::SendRequest { reply, .. } = cmd {
                let _ = reply.send(Ok(response.clone()));
            }
        }
    });
    PrimaryNetworkHandle::new_for_test(tx)
}

/// A network that answers every `SendRequest` with a consensus header at
/// `number`.
fn serving_network(number: u64) -> PrimaryNetworkHandle {
    answering_network(PrimaryResponse::ConsensusHeader(Arc::new(ConsensusHeader {
        number,
        ..Default::default()
    })))
}

/// A network that answers every `SendRequest` with the given application RPC
/// error.
fn rpc_error_network(message: impl Into<String>) -> PrimaryNetworkHandle {
    answering_network(PrimaryResponse::Error(PrimaryRPCError(message.into())))
}

/// A network whose peers answer that they have no consensus header to report
/// yet (the production "genesis" answer): a chain still at genesis. This is a
/// *response*, not a missing one.
fn genesis_network() -> PrimaryNetworkHandle {
    rpc_error_network(format!("Internal error: {NO_CONSENSUS_OUTPUT}"))
}

/// A network that accepts requests but never answers them. Keeping the returned
/// receiver alive keeps the reply senders alive, so the probe stays pending.
fn hanging_network() -> (PrimaryNetworkHandle, mpsc::Receiver<PrimaryCommand>) {
    let (tx, rx) = mpsc::channel(8);
    (PrimaryNetworkHandle::new_for_test(tx), rx)
}

#[tokio::test]
async fn refuses_invalid_new_chain_when_peers_report_genesis() {
    let (_fixture, config) = config();
    let shutdown_notifier = Notifier::new();
    let shutdown = shutdown_notifier.subscribe();

    let err =
        verify_boot_schedule_shape(&tampered_profile(), &config, &genesis_network(), &shutdown)
            .await
            .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("canonical new-chain shape"), "{msg}");
    assert!(msg.contains("AdminTransfer"), "{msg}");
}

#[tokio::test(start_paused = true)]
async fn fails_boot_when_no_peer_answers() {
    let (_fixture, config) = config();
    let shutdown_notifier = Notifier::new();
    let shutdown = shutdown_notifier.subscribe();

    let err =
        verify_boot_schedule_shape(&tampered_profile(), &config, &no_peer_network(), &shutdown)
            .await
            .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("no peer"), "{msg}");
    assert!(msg.contains("refusing to start"), "{msg}");
}

/// A peer that answers with anything other than the explicit "no consensus
/// output yet" error has not reported its chain state: the gate must refuse
/// rather than read the answer as genesis. Here it is the startup race where
/// the peer cannot map us yet.
#[tokio::test(start_paused = true)]
async fn non_genesis_rpc_errors_count_as_no_response() {
    let (_fixture, config) = config();
    let shutdown_notifier = Notifier::new();

    let err = verify_boot_schedule_shape(
        &tampered_profile(),
        &config,
        &rpc_error_network("requesting peer unknown: 12D3KooWTest"),
        &shutdown_notifier.subscribe(),
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("no peer"), "{msg}");
    assert!(msg.contains("refusing to start"), "{msg}");
    assert!(!msg.contains("canonical new-chain shape"), "{msg}");
}

/// A response of the wrong type is a protocol violation, not a genesis report:
/// it must also count as no answer.
#[tokio::test(start_paused = true)]
async fn wrong_response_variant_counts_as_no_response() {
    let (_fixture, config) = config();
    let shutdown_notifier = Notifier::new();

    let err = verify_boot_schedule_shape(
        &tampered_profile(),
        &config,
        &answering_network(PrimaryResponse::EpochMismatch { expected: 0, received: 1 }),
        &shutdown_notifier.subscribe(),
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("no peer"), "{msg}");
    assert!(msg.contains("refusing to start"), "{msg}");
    assert!(!msg.contains("canonical new-chain shape"), "{msg}");
}

#[tokio::test]
async fn uses_peer_reported_height_for_a_fresh_datadir() {
    let (_fixture, config) = config();
    let shutdown_notifier = Notifier::new();
    let shutdown = shutdown_notifier.subscribe();

    let outcome =
        verify_boot_schedule_shape(&tampered_profile(), &config, &serving_network(42), &shutdown)
            .await
            .unwrap();

    assert_eq!(outcome, BootShapeGateOutcome::Verified);
}

/// A peer set that never answers must not hold the boot open: the timeout
/// refuses the boot instead of guessing the chain's state.
#[tokio::test(start_paused = true)]
async fn times_out_when_no_peer_answers() {
    let (_fixture, config) = config();
    let (network, _rx) = hanging_network();
    let shutdown_notifier = Notifier::new();
    let shutdown = shutdown_notifier.subscribe();

    let err = verify_boot_schedule_shape(&tampered_profile(), &config, &network, &shutdown)
        .await
        .unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("timed out"), "{msg}");
    assert!(msg.contains("refusing to start"), "{msg}");
}

/// A single-validator committee has nobody to ask, so a fresh datadir is a new
/// chain by definition: the strict rules apply without a probe.
///
/// A committee of one can only be built under `dev-single-node-setup` (the
/// constructor's floor is `> 1` otherwise), which is also the only build where
/// single-validator chains run.
#[cfg(feature = "dev-single-node-setup")]
#[tokio::test]
async fn no_peers_on_a_single_validator_chain_use_new_chain_rules() {
    let (_fixture, config) = config_with_committee(1);

    let shutdown_notifier = Notifier::new();
    let err = verify_boot_schedule_shape(
        &tampered_profile(),
        &config,
        &no_peer_network(),
        &shutdown_notifier.subscribe(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("canonical new-chain shape"), "{err}");

    let shutdown_notifier = Notifier::new();
    let outcome = verify_boot_schedule_shape(
        &NetworkProfile::from_builtin(RaylsNetwork::Local),
        &config,
        &no_peer_network(),
        &shutdown_notifier.subscribe(),
    )
    .await
    .unwrap();
    assert_eq!(outcome, BootShapeGateOutcome::Verified);
}

#[tokio::test]
async fn shutdown_skips_the_gate_and_sends_no_probe() {
    let (_fixture, config) = config();
    let (tx, mut rx) = mpsc::channel::<PrimaryCommand>(8);
    let network = PrimaryNetworkHandle::new_for_test(tx);
    let shutdown_notifier = Notifier::new();
    let shutdown = shutdown_notifier.subscribe();
    shutdown_notifier.notify();

    let outcome = verify_boot_schedule_shape(&tampered_profile(), &config, &network, &shutdown)
        .await
        .unwrap();

    assert_eq!(outcome, BootShapeGateOutcome::Shutdown);
    assert!(rx.try_recv().is_err(), "shutdown must skip the peer probe");
}
