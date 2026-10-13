# Local testnet replay with a real snapshot

Run a 4-validator rayls testnet locally seeded with real testnet chain state.
Useful for debugging consensus / EVM / RPC behaviour against realistic history
without touching prod.

## Prerequisites

- `zstd` — `brew install zstd` / `apt install zstd`
- Free disk: ~30 GB on macOS APFS or Linux with reflink-capable FS (Btrfs,
  XFS-reflink, ZFS); ~120 GB on plain ext4.
- The repo's normal Rust toolchain.
- `etc/test-network/.env` exists — if not, `cp etc/test-network/.env.example etc/test-network/.env`.
  Then set in `.env`:
  - `RL_BLS_PASSPHRASE="local"` (the testnet `bls.kw` files were wrapped with that — default already).
  - `RAYLS_NETWORK="testnet"` — the hardfork schedule must match the chain the
    snapshot was captured from; the `.env.example` default `devnet` won't
    (different chain-id and fork schedule — see `RaylsHardFork::testnet()` vs
    `devnet()`/`local()` in `crates/execution/evm/src/chainspec.rs`).

  The `accounts.yaml` / `rls-accounts.yaml` files are **not** needed for replay — they
  apply only when generating a fresh genesis (see `etc/test-network/README.md`).

## Steps

### 1. Get the two archives
```sh
mkdir -p ~/Downloads/rayls-snapshot && cd ~/Downloads/rayls-snapshot
curl -O https://testnet-snapshots.rayls.com/us-east-2/validator-01/latest.tar.zst
```
`testnet-setup.tar.gz` is not hosted — ask a dev to share it and place it
alongside `latest.tar.zst` in this directory.

The `latest.tar.zst` snapshot carries one validator's `NodeIdentity` stamp;
the binary sanitises it for the other three on first boot (see § Identity note).

### 2. Run the replay script
```sh
cd <repo>
./etc/test-network/replay-snapshot.sh \
  ~/Downloads/rayls-snapshot/testnet-setup.tar.gz \
  ~/Downloads/rayls-snapshot/latest.tar.zst
```
The script wipes `local-validators/` (with a confirm prompt — pass `--yes` to
skip), extracts both archives, fixes the flat MDBX layout in the snapshot,
clones chain state to validators 2–4 (CoW on APFS / reflink-capable FS), and
patches `.env` to `NUM_VALIDATORS=4 / NUM_OBSERVERS=0`. Launch is deliberately
left to the dev.

### 3. Launch
```sh
./etc/test-network/local-testnet.sh --start
```
The script does an incremental `cargo build`, sees the populated
`local-validators/`, prints `skipping configuration`, then launches the four
validators in the background.

## Verify
```sh
pgrep -f "target/.+/rayls-network node" | wc -l   # → 4 (BSD pgrep on macOS has no -c)

for p in 8542 8543 8544 8545; do
  cast block-number --rpc-url http://localhost:$p
done                                              # heights match, advance ~1/sec

curl -s http://localhost:8545 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"rayls_nodeStatus","params":[]}' | jq .result
# → {"role":"active_cvv","is_caught_up":true,...}
```

## Identity sanitization note

On first boot, three of the four validators log
`"foreign consensus DB detected, clearing identity-bound tables"` (followed by
`"foreign consensus DB sanitized"`). The snapshot was captured from one
specific validator — its BLS pubkey is stamped in `NodeIdentity`, and whichever
local validator happens to inherit that pubkey skips sanitisation; the other
three detect a mismatch and clear identity-bound state. The code path at
`crates/middleware/orchestrator/src/epoch_manager/state.rs:134` clears
identity-bound tables (`LastProposed`, `Votes`, `Payload`, `NodeBatchesCache`,
`EpochTransitionCheckpoints`, `BatchSeqCounter`, KAD records) and re-stamps
`NodeIdentity` with the local validator's pubkey. Chain history
(`ConsensusBlocks`, `EpochRecords`, `EpochCerts`) is preserved, so all four
agree on every block up to the snapshot tip and consensus resumes from there.

## Chaos / debugging

Once the network is up, you usually want to (a) push transactions through it,
(b) stop and restart validators to simulate the prod backup procedure, and
(c) watch logs for the specific failure modes the fix branches target. This
section walks each in turn.

### 1. Operate one validator at a time

`local-testnet.sh` exposes per-validator control:

```sh
./etc/test-network/local-testnet.sh --stop-validator <SEQ>
./etc/test-network/local-testnet.sh --start-validator <SEQ>
```

`SEQ` is **0-based**. RPC port is `8545 - SEQ`. PID file lives at
`etc/test-network/local-validators/validator-<N>.pid` where `N = SEQ + 1`.

| SEQ | Validator dir | RPC port |
|---|---|---|
| 0 | `validator-1` | 8545 |
| 1 | `validator-2` | 8544 |
| 2 | `validator-3` | 8543 |
| 3 | `validator-4` | 8542 |

`--start-validator` is also what *restarts* a stopped one — it reads the
existing data dir and resumes from the last persisted block. No special
"restart" mode.

Stop is a SIGTERM with a 10 s drain budget before SIGKILL. Under load
(replay + tps-checker burst) graceful drain can legitimately take 60–150 s
as the explicit consensus-db flush and `Drainable` task wait complete — this
is the fix branch behaviour and is correct, not a hang.

### 2. Send a single transaction

```sh
cast send \
  --rpc-url http://localhost:8545 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --legacy \
  --gas-price 50gwei \
  0x70997970C51812dc3A010C7d01b50e0d17dc79C8 \
  --value 0.001ether
```

That's Anvil#0 → Anvil#1 (the well-known Foundry dev keypair). Your local
replay inherits whatever balance the snapshot had on this address — usually
enough for routine local testing. `--legacy` + explicit `--gas-price` is
required (rayls expects type-0 transactions). The gas price must be at least
the protocol base-fee floor of **48 gwei** (`MIN_RAYLS_PROTOCOL_BASE_FEE`) —
an underpriced tx (e.g. 1 gwei) is accepted into the pool but never mines,
and blocks the account's nonce until replaced with a higher-priced tx.

Note: don't reuse this key against the live testnet RPC — it's a published
constant; treat it as local-only.

Verify the tx landed identically on all four RPCs:
```sh
for p in 8542 8543 8544 8545; do
  echo "$p: $(cast balance 0x70997970C51812dc3A010C7d01b50e0d17dc79C8 \
    --rpc-url http://localhost:$p)"
done
```

### 3. Simulate the backup procedure

The prod-reported failure mode (PR #481) is: stop one validator for ~5 min
while the chain advances, then restart and watch it catch up. The
single-shot version:

```sh
SEQ=2   # validator-3
./etc/test-network/local-testnet.sh --stop-validator $SEQ
sleep 300   # 5 minutes
./etc/test-network/local-testnet.sh --start-validator $SEQ

# poll until caught up
while true; do
  s=$(curl -s --max-time 2 http://localhost:$((8545 - SEQ)) \
    -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"rayls_nodeStatus","params":[]}')
  echo "$s" | jq -c .result
  echo "$s" | jq -e '.result | select(.role == "active_cvv" and .is_caught_up == true)' >/dev/null && break
  sleep 5
done
```

Success looks like `role=active_cvv, is_caught_up=true` within seconds of
RPC coming up. On the fix branch the catch-up is via state-sync streaming
pre-processed blocks — it bypasses `gc_depth` entirely and works even after
20+ minutes of downtime (~600 rounds, well past `gc_depth=500`).

### 4. Chaos loop

The repo ships a chaos loop at `fork_test_configs/kill.sh`:

```sh
./fork_test_configs/kill.sh <SEQ>
```

It polls `rayls_nodeStatus` until `is_caught_up=true`, sleeps 30 s, sends
SIGTERM via the PID file, waits for exit, restarts via `--start-validator`,
and repeats forever. Ctrl-C to stop the loop terminal.

`kill.sh` only ever touches one validator at a time. **Quorum is 3 of 4 —
stopping two validators simultaneously halts block production until at
least one returns** (correct BFT behaviour, but worth knowing if you write
your own multi-validator chaos).

### 5. Load the network with tps-checker

`tps-checker` lives at https://github.com/bronxyz/tps-checker.

```sh
git clone https://github.com/bronxyz/tps-checker.git
cd tps-checker
cargo build --release
```

Canonical invocation against this snapshot:

```sh
./target/release/tps-checker \
  --rpc-url http://localhost:8545 \
  --chain-id 7295799 \
  --mnemonic "test test test test test test test test test test test junk" \
  --funder-private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --num-wallets 3 \
  --num-transactions 10000 \
  --batch-size 1000
```

Three things must be right:

- **`--chain-id 7295799`** — required. The tool defaults to `487`, which
  is wrong for the testnet snapshot; txs will be rejected.
- **`--funder-private-key`** — Anvil#0 (Foundry dev key). Pre-funds the
  per-wallet accounts before the run; works against the local replay
  because the snapshot's state on this address is sufficient. Same caveat
  as above: local-only, don't point at the live testnet.
- Default tx type is legacy — matches the rayls expectation. But the default
  1 gwei gas price does **not**: the base-fee floor is 48 gwei (see § 2), so
  txs would pool up without ever mining. Set the tool's gas-price option to
  ≥ 48 gwei (e.g. 50 gwei).

For continuous load instead of a burst, swap `--num-transactions / --batch-size`
for `--continuous --target-tps 500 --duration-secs 60`.

### 6. What to watch while breaking things

Heights across all 4 RPCs — should advance in lockstep:
```sh
for p in 8542 8543 8544 8545; do
  echo "$p: $(cast block-number --rpc-url http://localhost:$p 2>/dev/null)"
done
```

Per-validator status:
```sh
curl -s http://localhost:<port> -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"rayls_nodeStatus","params":[]}' | jq .result
```

Grep recipe for genuine failure indicators across logs:
```sh
grep -iE "HASH MISMATCH|panicked at|Could not get the epoch record|potential fork" \
  etc/test-network/local-validators/validator-*.log
```

On `fix/epoch-boundary-forks` (not yet on `main`) additionally grep for the
integrity-gate failure markers:
```sh
grep -iE "MissingBoundary|integrity: gaps remain|integrity: shutdown signalled|\
integrity: peer heal failed|integrity: FORK RISK" \
  etc/test-network/local-validators/validator-*.log
```

Two things that *look* alarming but are normal:

- On the fix branch, `integrity: ConsensusBlocks contiguous` — success
  message, fires on every restart. Don't grep for just `integrity:` —
  match the full strings above. (On `main` the only integrity message is
  `MDBX database integrity check passed`.)
- Long shutdown (60–150 s under load) — the explicit consensus-db flush
  and `Drainable` task wait. Wait for it; don't `kill -9` (on the fix
  branch that bypasses the flush and the integrity gate will refuse to
  boot on the next start).

### 7. Scenarios run on the fix branch

These were validated against `fix/epoch-boundary-forks` against the
real-testnet snapshot at block ~5,020,000. All passed cleanly. Useful as a
starting catalogue when picking what to reproduce or extend.

| ID | What | Result |
|---|---|---|
| A | Stop v3, hold 5 min, restart (literal prod scenario) | caught up in seconds |
| B | Same on v4 | clean |
| D | 5 rapid kill+restart cycles on v3 in ~50 s (no backup hold) | clean, lockstep |
| E | 3 cycles with 60 s holds, kill targeted while in catch-up | clean |
| F | Restart v3, immediately stop v4 while v3 still catching up | clean, recovered in ~30 s |
| G | v3 down 20 min — past `gc_depth=500` | clean, caught up in 40 s via state-sync streaming |
| H | Log-tail kill at epoch boundary | skipped — no marker log line during the session |
| I | Concurrent stop of v3+v4 (chain halts), then dual restart | clean, recovered in 10 s |

The status emitter / fork detector scripts used during these runs lived
under `/tmp/` and aren't committed yet; the grep recipe in § 6 covers the
same failure signal.

## Reset
```sh
killall rayls-network 2>/dev/null
./etc/test-network/replay-snapshot.sh <setup> <snapshot> --yes
```
