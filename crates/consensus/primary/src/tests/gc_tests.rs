//! Garbage collector tests.

use super::{AtomicRound, GarbageCollector};
use crate::{
    certificate_fetcher::CertificateFetcherCommand, error::GarbageCollectorError, ConsensusBus,
};
use assert_matches::assert_matches;
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{RaylsReceiver as _, RaylsSender as _};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::time::Duration;
use tokio::time::timeout;

/// When no consensus round update arrives within `max_consensus_round_timeout`, the
/// garbage collector must kick the certificate fetcher so the node can recover by
/// requesting missing certificates from peers.
///
/// `start_paused` lets the 30s fallback interval fire on virtual time.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn gc_timeout_kicks_certificate_fetcher() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().last().unwrap();
    let config = primary.consensus_config();

    let cb = ConsensusBus::new();
    let mut cert_fetcher_rx = cb.certificate_fetcher().subscribe();

    let mut gc = GarbageCollector::new(config, cb, AtomicRound::new(0));

    // guard bound must exceed max_consensus_round_timeout (30s) so the fallback
    // interval wins the race on the paused clock
    let result = timeout(Duration::from_secs(60), gc.ready())
        .await
        .expect("ready() must resolve before the guard bound");

    assert_matches!(result, Err(GarbageCollectorError::Timeout));

    let command = timeout(Duration::from_secs(60), cert_fetcher_rx.recv())
        .await
        .expect("kick must be sent before the guard bound")
        .expect("certificate fetcher channel closed");
    assert_matches!(command, CertificateFetcherCommand::Kick);

    Ok(())
}
