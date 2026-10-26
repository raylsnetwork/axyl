# Testbed

`testbed.sh` runs a local Rayls network as native processes and runs test plans against it. The
network has 4 validators, and you can add a 5th validator and an RPC node. The same commands work
on a laptop (macOS or Linux) and on the Proxmox test VM. All parameters are fixed, so every run
gets the same chain id, accounts and ports.

## Requirements

- A Rust toolchain. `up` builds `target/release/rayls-network`; set `RAYLS_BIN` to use a prebuilt
  binary instead.
- Foundry `cast`, `curl`.
- Optional: `jq` for `extended/04-erc20-precompile`, Node.js 22+ for `load`, `soak` and
  `extended/05-tps`.

## Quick start

```sh
./etc/testbed/testbed.sh up              # build, genesis (first run only), start 4 validators
./etc/testbed/testbed.sh test            # basic plan, ~30s
./etc/testbed/testbed.sh test chaos      # stop / kill / pause / restart scenarios, ~6 min
./etc/testbed/testbed.sh test extended   # add validator-5 and an rpc node, then fault them
./etc/testbed/testbed.sh status
./etc/testbed/testbed.sh down            # stop; `up` resumes the same chain
./etc/testbed/testbed.sh clean           # stop and delete all data
```

### Load and overnight soak

`etc/tps` can keep a steady stream of transfers going. It sends back-to-back batches of `LOAD_TXS`
transfers (default 500) through validator-1 and logs to `data/load.log`.

```sh
./etc/testbed/testbed.sh load                 # load only, until Ctrl-C
LOAD=1 ./etc/testbed/testbed.sh test chaos    # any plan, with load running underneath

# Overnight: load plus a fault every round on a random node other than validator-1
SOAK_MINUTES=720 nohup ./etc/testbed/testbed.sh test soak > soak.log 2>&1 &
```

Each soak round does the following:

1. Stops, kills or freezes a random node.
2. Keeps the node down for 0-30s; the chain must keep growing meanwhile.
3. Brings the node back and waits for it to catch up.
4. Checks that all nodes have the same block hash.
5. Checks that load transactions landed during the round.

The soak stops at the first fork, stall or load gap. It prints the round and timestamp so you can
find the moment in the node logs. Rounds are `SOAK_INTERVAL` seconds apart (default 60).

Only one test run can use a testbed at a time. While one is running, a second `test`, `down` or
`clean` refuses to start. The lock is `data/test.lock`, which holds the runner's PID, so a stale
lock is ignored.

## Commands

| Command | What it does |
|---|---|
| `up` | Build, run genesis if there is none, start every node, wait for the first block |
| `down` / `clean` | Stop all nodes / stop and delete `TESTBED_DATA` |
| `status` | PID, RPC URL, block, epoch and role of every node |
| `start\|stop\|kill\|pause\|resume <node>` | Control one node. `<node>` is `1`..`5` or `rpc`. `stop` is SIGTERM, `kill` is SIGKILL, `pause`/`resume` are SIGSTOP/SIGCONT |
| `add-validator` | Add `validator-5`: fund, mint RLS, allowlist, stake and activate it on-chain. It joins the committee within a few epochs |
| `add-rpc` | Add an RPC node (observer) |
| `logs <node>` | Follow a node's log |
| `load` | Steady `etc/tps` load through validator-1 until Ctrl-C |
| `test [plan\|plan/test ...]` | Run plans or single tests, e.g. `test chaos/03-lose-quorum`. Defaults to `basic`. `all` runs `basic chaos extended basic chaos`, so basic and chaos cover both the 4-validator network and the network with validator-5 and the rpc node |

## Network

| Node | RPC | Consensus metrics | Operator (anvil account) |
|---|---|---|---|
| validator-1 | 8545 | 9100 | #5 `0x9965…A4dc` |
| validator-2 | 8544 | 9101 | #6 `0x976E…0aa9` |
| validator-3 | 8543 | 9102 | #7 `0x14dC…9955` |
| validator-4 | 8542 | 9103 | #8 `0x2361…1E8f` |
| validator-5 | 8541 | 9104 | #9 `0xa0Ee…9720` |
| rpc | 8540 | 9105 | none (observer) |

- Chain id 487, run with `--network local` (all hardforks active).
- Epochs last `EPOCH_SECS` seconds (default 30).
- The dev account is anvil #0, `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` with key
  `0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80`. It is funded at genesis,
  owns the ConsensusRegistry, and is network admin and RLS minter.
- Data and logs (`<node>.log`) go to `etc/testbed/data`.

## Settings

| Variable | Default | Purpose |
|---|---|---|
| `RAYLS_BIN` | build `target/release/rayls-network` | Use this binary and skip the build |
| `TESTBED_DATA` | `etc/testbed/data` | Data directory |
| `EPOCH_SECS` | `30` | Epoch length; applied at genesis (`clean` to change it) |
| `HTTP_ADDR` | `127.0.0.1` | RPC bind address; `0.0.0.0` exposes the RPCs on the VM. A specific interface IP also works; the scripts then connect to that IP |
| `LOG_LEVEL` | `vvv` | Node verbosity (`v`..`vvvvv`) |
| `NODE_ARGS` / `GENESIS_ARGS` | empty | Extra flags for `rayls-network node` / `genesis`, e.g. `GENESIS_ARGS="--base-fee 0 --min-base-fee 0"` |
| `CYCLES` | `3` | Iterations of `chaos/11-restart-loop` |
| `LOAD` | unset | `1` runs the background load during `test` |
| `LOAD_TXS` / `TPS_TXS` | `500` / `2000` | Transfers per load batch / in `extended/05-tps` |
| `SOAK_MINUTES` / `SOAK_INTERVAL` | `480` / `60` | Soak duration / seconds between rounds |

On the Proxmox VM, start with `HTTP_ADDR=0.0.0.0 ./etc/testbed/testbed.sh up`. The RPCs are then
reachable at `http://<vm-ip>:8545` and the ports below it.

## Test plans

A plan is a directory under `tests/`, and a test is one file in it. Tests run in file order.

| Test | Checks |
|---|---|
| **basic** (default) | |
| `01-liveness` | Every node is up and its chain grows |
| `02-consistency` | All nodes have the same block hash at the same height |
| `03-node-status` | `rayls_nodeStatus`: validators are `active_cvv`, the rpc node is `observer`, all are caught up in the same epoch |
| `04-transfer` | A transfer sent through each node lands, and all nodes see the balance |
| `05-restart` | Graceful restart of a validator: the chain continues, the node catches up |
| `06-epoch` | An epoch closes and its certified record is served |
| **chaos** | |
| `01-kill` | SIGKILL one validator, then restart it |
| `02-pause` | Freeze one validator (SIGSTOP, in place of a partition), then resume it |
| `03-lose-quorum` | Stop enough validators to break quorum: the chain halts, then resumes without a fork |
| `04-flicker` | 5 quick freeze/resume cycles on one validator |
| `05-rolling-restart` | Restart every validator in turn; the chain never stops |
| `06-full-restart` | Stop and start all nodes; the chain resumes from the same height |
| `07-rapid-cycling` | Kill and restart one validator 3 times in a row |
| `08-kill-during-sync` | Crash a validator while it is catching up |
| `09-long-downtime` | Validator down for 2 epochs syncs the missed epochs and rejoins the committee |
| `10-kill-at-epoch-boundary` | Crash a validator right after an epoch change |
| `11-restart-loop` | Stop/restart once caught up, `CYCLES` times (soak) |
| **extended** | |
| `01-add-validator` | `add-validator`, then validator-5 becomes `active_cvv` and the next epoch's committee has 5 members |
| `02-rpc-node` | `add-rpc`: syncs from genesis, relays a transfer, survives a restart |
| `03-faults-with-new-nodes` | `chaos/01-kill` and `chaos/03-lose-quorum` with 5 validators and the rpc node |
| `04-erc20-precompile` | Runs `etc/test-network/erc20_precompile_test.sh` against validator-1 |
| `05-tps` | Load test with `etc/tps` through validators 1-3; every transfer lands |
| **soak** (opt-in) | |
| `01-faults-under-load` | Steady load plus a random stop, kill or freeze each round, for `SOAK_MINUTES` |
| **rust** (opt-in) | |
| `e2e` | `e2e-tests` crate, including ignored tests. It starts its own nodes on 8545-8542, so it stops the testbed while it runs |
| `chaos` | `chaos-framework` crate, 25 scenarios. Needs Linux and root for `tc`/`iptables`; skipped otherwise. Run it with `sudo -E env "PATH=$PATH" ./etc/testbed/testbed.sh test rust/chaos`; afterwards `target/` is handed back to your user, so later builds don't fail on root-owned files. Stop the testbed first (`down`); otherwise a failure there restarts its nodes as root |

After `extended` adds validator-5 and the rpc node, every other plan includes them too, so
`test basic chaos` exercises the 5-validator network.

## Writing a test

Add a file to `tests/<plan>/`. Its first line is a comment that describes the test; the runner
prints it. Each test runs in its own `bash` with `set -e` and `lib.sh` sourced, so any failing
command fails the test. Call `fail "<reason>"` to fail it explicitly, or `skip "<reason>"` to skip
it. If a test fails, the runner resumes and restarts every node that was running before it.

```sh
# Graceful restart of one validator: the rest keep producing blocks, it rejoins and catches up.
v=$(validators | tail -1)
stop_node "$v"
wait_blocks validator-1 5
start_node "$v"
wait_synced "$v"
check_consistency
```

Helpers in `lib.sh`:

- **Nodes:** `nodes`, `validators`, `quorum`, `url`, `{start,stop,kill,pause,resume}_node`, `start_all`, `stop_all`.
- **Queries:** `height`, `block_hash`, `epoch`, `role`, `status_field`, `committee_size`, `rpc`.
- **Waits and asserts:** `wait_blocks`, `wait_synced`, `wait_for`, `assert_stalled`, `check_consistency`.

## Where the scenarios come from

This testbed brings scenarios from several existing scripts under one harness. The original
scripts are unchanged.

| Existing script | Testbed equivalent |
|---|---|
| `etc/test-network/local-testnet.sh` (plain mode), `start-local-validator.sh`, `start-local-observer.sh`, `clear-*.sh` | `up`, `start`/`stop`, `add-rpc`, `clean` |
| `etc/chaos-network/scripts/run-chaos.sh` scenarios `node-crash`, `rapid-cycling`, `full-restart`, `kill-during-sync` | `chaos/01`, `07`, `06`, `08` |
| `etc/test/edge-cases/net-01.sh` (one node partitioned) | `chaos/02-pause` |
| `etc/test/edge-cases/net-02.sh`, `net-03.sh` (quorum loss / split brain) | `chaos/03-lose-quorum` |
| `etc/test/edge-cases/net-04.sh` (flickering connectivity) | `chaos/04-flicker` |
| `fork_test_configs/kill.sh`, `bounce-node.sh` | `chaos/11-restart-loop` |
| `etc/test-network/stake-relay-node.sh`, `etc/validator/create-validator.sh` (staking flow) | `add-validator` |
| `etc/test-network/erc20_precompile_test.sh`, `etc/tps` | `extended/04`, `extended/05` |
| `e2e-tests`, `chaos-framework` crates | `rust/e2e`, `rust/chaos` |

These are not covered:

- **Per-node latency and packet loss.** They need `tc` on each node's traffic. Use `rust/chaos`
  (Linux, root) or the docker harness in `etc/chaos-network`.
- **Clock skew** (`etc/test/edge-cases/clk-01.sh`).
- **Relay and `/dnsaddr` topologies.** Use `etc/test-network/local-testnet.sh --relay` / `--relay-dns`.
