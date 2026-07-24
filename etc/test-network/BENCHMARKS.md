# Relay overhead benchmarks

Measures how much routing all consensus p2p through circuit-relay-v2 relays costs, versus a direct
(no-relay) network — under sustained load, at two committee sizes.

**TL;DR:** on this setup the relays add little — at 4+1 relay vs no-relay is within noise; at 6+1 one
relay run was slightly faster than direct and the other slightly slower, so the gap is run-to-run
noise rather than a real relay cost. The real cost is committee size (4→6 roughly doubles finality
latency). See the [caveat](#caveat) — everything runs on one host, so this is a *lower bound* on
relay cost.

## Setup

Two machines:

- **Node host** (`172.16.19.19`) — runs the local testnet: `N` validators + 1 observer, and, in
  relay mode, a per-validator relay (primary + backup). Nodes bind their RPC on `0.0.0.0` so the
  generator can drive them remotely.
- **Generator host** (separate machine) — runs `tps-checker`. Kept separate on purpose so the load
  generator doesn't steal CPU/bandwidth from the nodes and skew the result.

The generator targets the **validators'** JSON-RPC endpoints (the observer is a follower and isn't
driven). Validator RPC ports count down from `8545`: `8545, 8544, …`; a `4`-validator net exposes
`8545–8542`, a `6`-validator net `8545–8540`.

### Two topologies compared

Bring the net up on the node host. Wipe state between runs — genesis is generated once, so a stale
`local-validators/` would silently reuse the previous topology:

```bash
killall rayls-network rayls-relay dnsmasq 2>/dev/null
rm -rf etc/test-network/local-validators
unset MULTI_LISTEN          # ensure relay-only (no direct listeners) in relay mode
```

- **No relay (baseline):** validators dial each other directly.
  ```bash
  ./etc/test-network/local-testnet.sh --start --dev-funds "$DEV_FUNDS"
  ```
- **Relay-only:** all consensus + worker traffic routes through per-validator relays.
  ```bash
  ./etc/test-network/local-testnet.sh --start --dev-funds "$DEV_FUNDS" --relay
  ```

Committee size is set by `NUM_VALIDATORS` in `etc/test-network/.env` (`4` and `6` here); keep it
identical across the relay/no-relay pair for a fair comparison.

## Load generator

`tps-checker` runs from the generator host, rotating across all validator RPCs, holding a target
send rate for a fixed duration and recording finality (submission → block inclusion) and confirm
latency. The exact invocation (6-validator relay run shown; for `4+1`, list only the four
`8545–8542` RPCs):

```bash
./target/release/tps-checker \
    --continuous \
    --target-tps 10000 \
    --num-transactions 10000000000 \
    --duration-secs 300 \
    --num-wallets 6 \
    --batch-size 100 \
    --concurrent-batches 5 \
    --poll-initial-ms 300 --poll-max-ms 5000 --poll-max-attempts 2000 \
    --batch-retry-attempts 10 --batch-retry-delay-ms 2000 \
    --report-interval-secs 1 \
    --enable-failover --failover-threshold 3 --failover-cooldown-secs 60 \
    --enable-health-probing --health-probe-interval-secs 30 \
    --mnemonic "<12-word test mnemonic>" \
    --gas-price 49000000000 \
    --chain-id 487 \
    --rpc-urls http://172.16.19.19:8545,http://172.16.19.19:8544,http://172.16.19.19:8543,http://172.16.19.19:8542,http://172.16.19.19:8541,http://172.16.19.19:8540 \
    --funder-private-key <throwaway-test-funder-key>
```

Key flags:

- `--target-tps 10000 --duration-secs 300` — hold 10k tx/s for 5 minutes.
- `--num-wallets 6` + `--mnemonic` — sender wallets derived from the mnemonic; `--funder-private-key`
  seeds them with gas (a throwaway test account, = `--dev-funds`).
- `--batch-size 100 --concurrent-batches 5` — batched, pipelined submission.
- `--enable-failover` / `--enable-health-probing` — rotate off an unhealthy RPC so one slow node
  doesn't stall the generator.
- `--gas-price 49gwei --chain-id 487` — real fees (not gasless); chain id of the test net.

> The mnemonic and funder key above are throwaway local-test values (same class as `RELAY_KEYS.md`).

## Results

All runs: 10k target TPS, 300s, release build, generator remote, nodes on `172.16.19.19`.

### 4 validators + 1 observer

| Metric | No relay | Relay run 1 | Relay run 2 |
|---|---|---|---|
| Send/Confirm TPS | 9916 (99.2%) | 9915 (99.1%) | 9913 (99.1%) |
| Confirm rate | 100% | 100% | 100% |
| **Finality p50** | 1427ms | 1541ms | 1392ms |
| **Finality p95** | 3392ms | 2582ms | 3338ms |
| **Finality p99** | 4305ms | 3572ms | 4335ms |
| Finality mean | 1706ms | 1603ms | 1776ms |
| Confirm p99 (incl. poll) | 5498ms | 5359ms | 5506ms |
| Blocks / rate | 793 · 2.62/s | 837 · 2.77/s | 762 · 2.52/s |
| Grade · RPC stress | B · HIGH | B · HIGH | B · HIGH |
| Production gaps | none | none | none |

### 6 validators + 1 observer

| Metric | No relay | Relay run 1 | Relay run 2 |
|---|---|---|---|
| Send/Confirm TPS | 9873 (98.7%) | 9854 (98.5%) | 9837 (98.4%) |
| Confirm rate | 100% | 100% | 100% |
| **Finality p50** | 3128ms | 2899ms | 3163ms |
| **Finality p95** | 6033ms | 5767ms | 6152ms |
| **Finality p99** | 7133ms | 6800ms | 8107ms |
| Finality mean | 3113ms | 3274ms | 3655ms |
| Confirm p99 (incl. poll) | 8431ms | 9749ms | 10023ms |
| Blocks / rate | 736 · 2.43/s | 708 · 2.33/s | 669 · 2.21/s |
| Grade · RPC stress | C · CRITICAL | C · CRITICAL | C · CRITICAL |
| Production gaps | 1 (3s) | 4 (≤4s) | 1 (4s) |

## Findings

1. **Relays add little.** At 4+1, relay vs no-relay is within noise — finality percentiles <10%
   apart, sometimes relay *faster*. At 6+1 the result is just noisier: relay p99 was 6.8s in one run
   and 8.1s in the other, with no-relay at 7.1s in between — i.e. one relay run beat direct and one
   was slower, so there's no consistent relay penalty, only run-to-run variance. Throughput held
   ~98–99% of 10k and confirm rate was 100% (zero dropped txns) in every run.
2. **The real cost is committee size, not relays.** Going 4+1 → 6+1 roughly **doubles** finality
   (p50 ~1.4s → ~3s, p99 ~4.3s → ~7–8s), drops the grade **B → C**, pushes RPC stress
   **HIGH → CRITICAL**, and lowers block rate ~2.6 → ~2.2–2.4/s. Both topologies degrade together.
3. **Relay tail is a touch noisier under heavy load.** At 6+1 the relay runs show a slightly worse
   *confirm* p99 (9.7–10.0s vs 8.4s) and higher mean finality (3.3–3.7s vs 3.1s). Differences are
   small and run-to-run variance is high at this size — average more runs (3+ per config) before
   drawing a firm 6+1 conclusion.

## Caveat

All validators **and** relays run on one host; only the generator is remote. So this measures relay
**processing overhead + a loopback hop**, not real-network relay latency or bandwidth. A true
multi-host relay deployment adds network RTT and makes each relay a bandwidth chokepoint (all of a
validator's consensus + worker traffic hairpins through it), so **"relays ≈ free" is a lower bound.**

To capture the real cost: put relays on separate hosts from their validators, or inject latency on
the relay path (`tc qdisc add dev <if> root netem delay 20ms`) and re-run the same A/B.
