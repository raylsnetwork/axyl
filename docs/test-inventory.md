# Axyl Test Inventory

A catalog of the automated tests in the Axyl repository, by category. **Solidity / contract tests are excluded.**

There is **no separate "regression" suite** — regression protection comes from the unit tests (run on every PR) plus the nightly e2e restart/epoch suite.

## At a glance

| Category | Count | Location | In CI? |
|---|---|---|---|
| E2E integration tests | 11 | `crates/testing/e2e-tests/tests/it/` | Partial (PR: 3, nightly: 10) |
| Chaos scenarios | 25 | `crates/testing/chaos-framework/tests/chaos_scenarios.rs` | ❌ manual-only |
| Fuzz targets | 7 | `crates/testing/fuzz-targets/fuzz/fuzz_targets/` | ❌ manual (`cargo fuzz`) |
| Property-based tests | 1 | `crates/consensus/primary/` | ✅ (with unit tests) |
| Criterion benchmarks | 2 | `crates/execution/evm/benches/` | ❌ `cargo bench` |
| Unit tests | ~540 | across 20 crates (`#[cfg(test)]` in `src/`) | ✅ PR (`pr.yaml`) |
| Integration test files | 12 | various crates' `tests/` dirs | ✅ PR |
| Docker chaos harness | 9 scenarios / 10 scripts | `etc/chaos-network/` | ❌ manual bash |

---

## 1. E2E integration tests (`e2e-tests`) — 11 total

`crates/testing/e2e-tests/tests/it/`. Each spins up a real local 4-validator testnet.

| Test | File | Run | Covers |
|---|---|---|---|
| `test_genesis_with_precompiles` | genesis_tests.rs | **PR + nightly** | Precompile accounts deployed at genesis with bytecode |
| `test_precompile_genesis_accounts` | genesis_tests.rs | **PR + nightly** | Genesis precompile accounts populated; key addresses from deployments.json present |
| `test_genesis_with_consensus_registry` | genesis_tests.rs | **PR + nightly** | ConsensusRegistry + BLS G1 contracts on-chain; RPC epoch/validator queries |
| `test_restartstt` | restarts.rs | nightly (`#[ignore]`) | Kill 1 validator (short delay) → rejoins consensus; balance + nonce monotonicity hold |
| `test_restarts_delayed` | restarts.rs | nightly | Kill 1 validator (70s delay) → doesn't rejoin consensus but follows chain |
| `test_restarts_lagged_delayed` | restarts.rs | nightly | Kill (70s, network lagged) → restart must sync txs it missed |
| `test_restarts_observer` | restarts.rs | nightly | Observer node submits txs, receives transfers, participates |
| `test_observer_late_start_catchup` | restarts.rs | nightly | Observer started 30s late catches up genesis→tip; balance + nonce verified |
| `test_epoch_boundary` | epochs.rs | nightly | New validator stakes/activates, gets shuffled into committee within 25 epochs; certified epoch records |
| `test_epoch_sync` | epochs.rs | nightly | Kill validator for 5 epochs → restart syncs all missed epoch records |
| `test_faucet_transfers_rls_and_xyz_with_google_kms_e2e` | faucet.rs | **manual** (needs Google KMS creds) | Faucet w/ KMS signing: deploy Stablecoin proxies, RLS transfers, XYZ drips, dup rejection, 50-concurrent stress |

**CI wiring:** the 3 genesis tests run by default (`pr.yaml` excludes `e2e-tests` from the workspace run, but the nightly default suite runs them; the isolated `#[ignore]` tests run one-per-job in the nightly matrix). The faucet test is internal/manual only.

---

## 2. Chaos scenarios (`chaos-framework`) — 25 total

`crates/testing/chaos-framework/tests/chaos_scenarios.rs`. All `#[ignore]`, **manual-only** (not in any CI workflow). Run with:

```
cargo test -p chaos-framework --test chaos_scenarios -- --ignored --test-threads 1
```

Network-fault scenarios require root / `CAP_NET_ADMIN` (they use `tc`/`netem` and `iptables`). All scenarios verify against **block consistency**, **chain-advancing/liveness**, and **nonce monotonicity** (fork detection).

### Crash / restart

| Test | Fault → Verifies |
|---|---|
| `test_single_validator_crash_and_recovery` | Kill 1 + restart → consistency, nonce monotonicity |
| `test_rolling_validator_failure` | Kill A then B → BFT f=1 tolerance, state preserved |
| `test_sigterm_vs_sigkill_recovery` | Graceful vs hard kill → both recovery paths |
| `test_multi_validator_kill_within_bft_tolerance` | Kill f=1 simultaneously → liveness at threshold |
| `test_delayed_restart_state_sync` | Kill + fall behind + restart → state-sync catch-up |
| `test_rapid_kill_restart_cycling` | Kill/restart same node 3× rapidly → responsiveness, consistency |
| `test_full_network_restart` | Kill all 4 → reconverge, state preserved |
| `test_kill_during_state_sync` | Kill mid-sync → interrupted-sync recovery |

### Transaction spam (mempool)

| Test | Fault → Verifies |
|---|---|
| `test_spam_invalid_signatures` | 100 bad-sig txns → all rejected, chain healthy |
| `test_spam_wrong_chain_id` | 100 wrong-chainID → pool filtering |
| `test_spam_oversized_calldata` | 5× 2.1MB calldata → batch-size limit enforced |
| `test_spam_excessive_gas` | 50 max-gas txns → gas-limit handling |
| `test_spam_valid_burst` | 200 valid txns → throughput, batch packing |
| `test_spam_mixed_malformed` | 200 mixed malformed → mempool robustness |

### Network faults (need root / NET_ADMIN)

| Test | Fault → Verifies |
|---|---|
| `test_network_latency_uniform` | 350±150ms all links → consensus under latency |
| `test_latency_under_tx_load` | 200±50ms + tx load → mempool on degraded net |
| `test_network_packet_loss` | 10% loss → resilience to lossy links |
| `test_network_partition_single_node` | iptables isolate 1 node → partition tolerance + heal |

### Combined / load / boundary

| Test | Fault → Verifies |
|---|---|
| `test_validator_crash_under_tx_load` | Kill during tx load → state sync, balance recovery |
| `test_kill_plus_latency` | Kill + latency → combined fault |
| `test_latency_plus_tx_spam` | Latency + spam + transfers → multi-fault stress |
| `test_kill_during_epoch_transition` | Kill at epoch boundary → epoch-checkpoint recovery |
| `test_combined_chaos` | Kill + latency + spam together → multi-layer |
| `test_tx_flood_during_crash` | Hard kill during flood → restarted node balance correct |
| `test_progressive_degradation` | Escalating latency + kill + reverse recovery → degradation sequencing |

---

## 3. Fuzz targets (`fuzz-targets`) — 7

`crates/testing/fuzz-targets/fuzz/fuzz_targets/`. Run with `cargo +nightly-2026-06-24 fuzz run <name>` (the date-pinned nightly shared with the fmt check, see the Makefile `NIGHTLY` variable).

| Target | Fuzzes |
|---|---|
| `batch_digest` | Batch digest determinism, collision resistance, `seal_slow()` consistency |
| `bcs_roundtrip` | BCS serialize/deserialize stability for Certificate, Header, SealedBatch |
| `certificate_signed_by` | `Certificate::signed_by()` panic-safety with arbitrary RoaringBitmaps, weight bounds |
| `difficulty_field_encoding` | EVM difficulty pack/unpack invariant `(batch_index<<16)\|worker_id` |
| `header_digest_integrity` | Header digest determinism + avalanche on single-byte mutation |
| `header_nonce_roundtrip` | Header nonce encoding `(epoch<<32)\|round` round-trip |
| `quorum_threshold` | Quorum formula `((2*members)/3)+1` arithmetic — no panic, bounds |

---

## 4. Property-based tests & 5. Benchmarks

| Type | Name | Covers |
|---|---|---|
| proptest | `test_certificate_verification` (`consensus/primary`) | Certificate validation across committee sizes 4–34 |
| criterion | `reth_recover_raw_transactions_parallel` (`execution/evm`) | Parallel sig recovery, 200 txns |
| criterion | `reth_recover_raw_transactions_sequential` (`execution/evm`) | Sequential baseline for comparison |

Run benchmarks with `cargo bench -p rayls-execution-evm`.

---

## 6. Unit + integration tests by subsystem

~540 unit tests across 20 crates plus 12 integration test files; all run on every PR (`pr.yaml`).

| Subsystem · Crate | ~Unit | Integ. files | Coverage themes |
|---|---|---|---|
| **Consensus** · network | ~100 | – | Peer mgmt, state transitions, banning, Kademlia, gossip mesh, reputation, codec |
| Consensus · primary | ~81 | 4 | Certificate verification, equivocation, vote validation, batch sync, header validation |
| Consensus · worker | ~64 | 4 | Batch fetching, sequencing, quorum waiting, rejection |
| Consensus · batch-validator | ~23 | – | Batch/gas validation, base fee, EIP-4844, epoch checks |
| Consensus · consensus-metrics | ~10 | – | Histograms, metered channels, backpressure |
| Consensus · state-sync | ~5 | – | Round tracking, GC depth, epoch recovery, block-number fallback |
| Consensus · batch-builder | ~3 | 2 | Batch building, block acks, pool updates, EL→CL transition |
| **Execution** · evm | ~82 | – | Hardforks (UUPS, tokenomics), native ERC20, storage slots, genesis, chainspec, precompiles |
| Execution · faucet | ~3 | 1 | Credential handling (KMS, gcloud), PEM pubkey parsing |
| **Infrastructure** · storage | ~66 | – | Multi-DB (MDBX/MemDB/Reth/Layered), CRUD, iteration, tombstones, gaps |
| Infra · types | ~41 | – | Genesis, chain spec, broadcast lag, task manager, drainable ops, notifications |
| Infra · network-cli | ~14 | – | CLI parsing (db args, color, env filters), keytool, help |
| Infra · config | ~4 | – | BLS passphrase, genesis validation |
| Infra · utils | ~1 | – | Notify read ops |
| **Middleware** · orchestrator | ~92 | 1 | Epoch transitions, checkpoints, drain protocol, mode transitions, shutdown, recovery |
| Middleware · bridge | ~6 | 1 | Sequential headers, dup rejection, out-of-order, recovery, batch-fetch errors |
| Middleware · processor | ~4 | 3 | Batch ordering, hardfork transitions, dup handling, empty outputs, max round |
| **Binary** · rayls-replay | ~5 | – | Committee building, sub-quorum registry, rewards tally, epoch storage |

### ⚠️ Zero test coverage

`primary-metrics`, `execution-metrics`, `execution-rpc`, `network-types`, `middleware-rewards`, and the `rayls-network` node binary.

Metrics/types crates are low-risk; **`execution-rpc`, `middleware-rewards`, and the node binary are the notable gaps.**

---

## 7. Docker chaos harness (`etc/chaos-network/`)

Manual, bash + docker — parallel to the Rust chaos framework but driven via `docker exec` (works on macOS too). `run-chaos.sh` orchestrates 9 scenarios — node-crash, latency, partition, packet-loss, combined, rapid-cycling, full-restart, kill-during-sync, asymmetric-latency — using `tc`/`netem`, `iptables`, and `docker stop/kill`. Useful for hand-reproducing a scenario; not a CI gate.

| Script | Fault | Method |
|---|---|---|
| `run-chaos.sh` | Orchestrates all scenarios | Bash harness (9 scenarios) |
| `kill-validator.sh` | Graceful / hard kill | `docker stop -t 5` / `docker kill` |
| `restart-validator.sh` | Process restart | `docker start` |
| `inject-latency.sh` / `remove-latency.sh` | Network latency | `tc qdisc … netem delay` |
| `inject-packet-loss.sh` | Packet loss | `tc qdisc … netem loss` |
| `inject-partition.sh` / `remove-partition.sh` | Network partition | `iptables` DROP rules |
| `genesis.sh`, `setup_validator.sh` | Network bring-up | Genesis config + container setup |

---

## CI execution summary

| Workflow | Trigger | Runs |
|---|---|---|
| `pr.yaml` | every PR to `main` | rustfmt + full workspace unit/integration tests (`--exclude e2e-tests --exclude rayls-execution-faucet`) + 3 genesis e2e tests |
| `nightly-e2e.yml` | 02:00 UTC daily + manual | default suite (3 genesis) + isolated matrix (5 restart + 2 epoch tests, one job each) |
| `nightly-chaos.yml` | 03:00 UTC daily + manual | all 25 chaos scenarios (`--ignored`, single-threaded, run as root for `tc`/`iptables`); manual dispatch can target one test |
| `nightly-fuzz.yml` | 04:00 UTC daily + manual | 7 fuzz targets (matrix), time-boxed per target; manual dispatch can pick one target + duration |
| `nightly-bench.yml` | Mondays 05:00 UTC + manual | EVM Criterion benchmarks (no regression gate) |

**Still manual-only / not in CI:**

- **KMS faucet test** (`test_faucet_transfers_rls_and_xyz_with_google_kms_e2e`) — requires real Google KMS credentials/infra, so it can't run in generic CI. Run locally with the appropriate GCP auth.
- **Docker chaos harness** (`etc/chaos-network/`) — bash/docker reproduction tool, superseded for CI by `nightly-chaos.yml`.

> Chaos / fuzz / bench workflows are intentionally **not** PR gates: fault-injection is noisier and fuzzing/benching are time-boxed exploration, so they must never block a merge. All three have `workflow_dispatch` for on-demand testing.
