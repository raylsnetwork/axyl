# Testbed

`testbed.sh` starts a local Rayls network (4 validators + 2 RPC nodes as native processes) and runs test plans
against it. It works the same on a laptop and on the Proxmox VM. Chain id, accounts and ports are
fixed.

**Requirements**

- A Rust toolchain, or `RAYLS_BIN` pointing to a prebuilt binary. `BUILD_CONFIG=debug` builds and
  runs a debug binary. It is slower, but you get more detail when something breaks.
- Foundry `cast` and `curl`.
- `jq`, for the ERC-20 test.
- [tps-checker](https://github.com/raylsnetwork/tps-checker) next to this repo (`../tps-checker`),
  for load, soak and the TPS test. If it isn't there, the testbed clones and builds it.

## Overnight run (the main use case)

```sh
tmux new -s overnight                        # keeps running after you log out; detach with Ctrl-b d
./etc/testbed/testbed.sh clean && ./etc/testbed/testbed.sh up
LOAD=1 SOAK_MINUTES=600 ./etc/testbed/testbed.sh test all soak 2>&1 | tee overnight.log
```

1. The whole suite runs first (`all`: 37 tests, ~20 min).
2. Then the soak runs for `SOAK_MINUTES` on the extended network, 5 validators and 3 RPC nodes.
   Every round it stops or freezes a random node and checks that it comes back, nothing forks, and
   the load keeps landing.
3. Steady tps-checker load with occasional nonce collisions runs underneath the whole time.

The run doesn't stop at a failing test. The soak itself stops at its first problem and prints the
round, the time and the reason.

Optional variations:

```sh
BUILD_CONFIG=debug ./etc/testbed/testbed.sh up        # debug build (on a cleaned testbed)
LOAD=1 SOAK_MINUTES=600 SOAK_VICTIMS=validator-5 ./etc/testbed/testbed.sh test all soak   # bounce only the added validator
LOAD=1 LOAD_TPS=1000 SOAK_MINUTES=600 ./etc/testbed/testbed.sh test all soak             # heavier load
```

In the morning:

- `tail overnight.log` shows the last line, `passed: N skipped: N failed: N ...`.
- `./etc/testbed/testbed.sh status` shows the state of each node.
- `tail etc/testbed/data/load.log` shows the TPS reports.
- `etc/testbed/data/<node>.log` holds each node's log, which you can read around a failure time.

Don't loop `test all` instead: `extended` adds a new RPC node on every run. The soak is the part
meant to run for hours.

## Usage

```sh
./etc/testbed/testbed.sh up                  # build, genesis on first run, start 4 validators + rpc-1, rpc-2
./etc/testbed/testbed.sh test                # basic plan, ~30s
./etc/testbed/testbed.sh test chaos          # stop / pause / restart scenarios, ~5 min
./etc/testbed/testbed.sh test all            # basic, chaos, extended, then basic and chaos again, ~15 min
./etc/testbed/testbed.sh test chaos/02-lose-quorum   # a single test
./etc/testbed/testbed.sh down                # stop with SIGTERM; `up` resumes the same chain
./etc/testbed/testbed.sh clean               # stop and delete all data

LOAD=1 ./etc/testbed/testbed.sh test chaos   # any plan with steady tps-checker load underneath
SOAK_MINUTES=60 ./etc/testbed/testbed.sh test soak   # soak only, without the rest of the suite
HTTP_ADDR=0.0.0.0 ./etc/testbed/testbed.sh up   # on the VM: RPCs reachable at <vm-ip>:8545..
BUILD_CONFIG=debug ./etc/testbed/testbed.sh up  # debug build; later commands keep using it (switch on a stopped testbed)
SOAK_VICTIMS=validator-5 ./etc/testbed/testbed.sh test soak   # keep bouncing just the added validator
```

## Commands

| Command | What it does |
|---|---|
| `up` | Build, run genesis if needed, start all nodes, wait for the first block |
| `down` / `clean` | Stop all nodes (SIGTERM) / stop them and delete the data |
| `status` | PID, RPC, block, epoch and role of each node |
| `start\|stop\|pause\|resume <node>` | One node: `1`..`5` or `rpc-N`. `stop` = SIGTERM, `pause`/`resume` = SIGSTOP/SIGCONT |
| `add-validator` | Add validator-5 and stake it into the committee (fund, mint RLS, allowlist, stake, activate) |
| `add-rpc` | Add another RPC (observer) node to the running chain (`rpc-3`, ...) |
| `logs <node>` | Follow a node's log (`data/<node>.log`) |
| `load` | Steady tps-checker load until Ctrl-C (`data/load.log`) |
| `test [plan\|plan/test ...]` | Run plans or single tests (default `basic`) |

## Test cases

Each test is one small file in `tests/<plan>/` and runs in file order.

| Plan | Test | Checks |
|---|---|---|
| basic | `01-liveness` | Every node is up and its chain grows |
| | `02-consistency` | All nodes have the same block hashes |
| | `03-node-status` | Validators `active_cvv`, RPC nodes `observer`, all caught up, same epoch |
| | `04-transfer` | A transfer sent through each node lands, and all nodes see it |
| | `05-restart` | Restart one validator; the chain continues and the node catches up |
| | `06-epoch` | An epoch closes and its certified record is served |
| chaos | `01-pause` | Freeze one validator (SIGSTOP), then resume it |
| | `02-lose-quorum` | Stop enough validators to break quorum; the chain halts, then resumes without a fork |
| | `03-flicker` | 5 quick freeze/resume cycles |
| | `04-rolling-restart` | Restart every validator in turn; the chain never stops |
| | `05-full-restart` | Stop and start all nodes; the chain resumes from the same height |
| | `06-rapid-restart` | Stop and start one validator 3 times; the stop lands while it is still starting |
| | `07-stop-during-sync` | Stop a validator while it is catching up |
| | `08-long-downtime` | Down for 2 epochs; it syncs the missed epochs and rejoins the committee |
| | `09-stop-at-epoch-boundary` | Stop a validator right after an epoch change |
| | `10-restart-loop` | Stop/start once caught up, `CYCLES` times |
| extended | `01-add-validator` | validator-5 is staked, becomes `active_cvv`, and the committee has 5 members |
| | `02-rpc-node` | A late RPC node joins, syncs from genesis, relays a transfer, survives a restart |
| | `03-faults-with-new-nodes` | `chaos/01` and `chaos/02` with 5 validators and 3 RPC nodes |
| | `04-erc20-precompile` | `etc/test-network/erc20_precompile_test.sh` against validator-1 |
| | `05-tps` | tps-checker burst through the RPC nodes; every transfer is accepted and lands on-chain (checked by nonces) |
| soak | `01-faults-under-load` | Steady load, and each round stops or freezes a random node (any node except rpc-1, the reference). Stops at the first fork, stall or load gap |
| rust | `e2e`, `chaos` | The `e2e-tests` and `chaos-framework` crates. `chaos` needs Linux and root |

After `extended` adds validator-5 and rpc-3, every plan includes them.

## Where the scenarios come from

The testbed takes its scenarios from these scripts, which stay as they are. Paths are relative to
the repo root.

| Existing script | Testbed command | Difference |
|---|---|---|
| `etc/test-network/local-testnet.sh` (plain mode), `etc/test-network/start-local-validator.sh`, `etc/test-network/start-local-observer.sh`, `etc/test-network/clear-datadir.sh`, `etc/test-network/clear-validators-db.sh` | `./etc/testbed/testbed.sh up`, `start`, `stop`, `add-rpc`, `clean` | |
| `etc/chaos-network/scripts/run-chaos.sh rapid-cycling` | `./etc/testbed/testbed.sh test chaos/06-rapid-restart` | SIGTERM, not `docker kill` |
| `etc/chaos-network/scripts/run-chaos.sh full-restart` | `./etc/testbed/testbed.sh test chaos/05-full-restart` | `docker stop -t 3` sends SIGKILL after 3s; the testbed waits up to `STOP_TIMEOUT` |
| `etc/chaos-network/scripts/run-chaos.sh kill-during-sync` | `./etc/testbed/testbed.sh test chaos/07-stop-during-sync` | SIGTERM, not `docker kill` |
| `etc/test/edge-cases/net-01.sh` | `./etc/testbed/testbed.sh test chaos/01-pause` | net-01 cuts one node off the network; the testbed freezes it |
| `etc/test/edge-cases/net-02.sh` | `./etc/testbed/testbed.sh test chaos/02-lose-quorum` | net-02 cuts a third of the nodes off; the testbed stops enough validators to lose quorum |
| `etc/test/edge-cases/net-04.sh` | `./etc/testbed/testbed.sh test chaos/03-flicker` | net-04 cuts a quarter of the nodes off and reconnects them 5 times; the testbed freezes one node 5 times |
| `fork_test_configs/kill.sh` | `CYCLES=10 ./etc/testbed/testbed.sh test chaos/10-restart-loop` | SIGTERM only, no SIGKILL fallback |
| `fork_test_configs/bounce-node.sh` | `SOAK_VICTIMS=validator-5 ./etc/testbed/testbed.sh test extended soak` | |
| `etc/test-network/stake-relay-node.sh`, `etc/validator/create-validator.sh` (staking) | `./etc/testbed/testbed.sh add-validator` | |
| `etc/test-network/erc20_precompile_test.sh` | `./etc/testbed/testbed.sh test extended/04-erc20-precompile` | Runs the same script |
| `etc/tps` | `./etc/testbed/testbed.sh test extended/05-tps` | tps-checker instead of `etc/tps` |
| `crates/testing/e2e-tests`, `crates/testing/chaos-framework` | `./etc/testbed/testbed.sh test rust` | Runs the same crates |

**A freeze or a stop is not a network cut.** The `net-*` scripts disconnect a running node's
container, so the node keeps running, keeps its state, and sees the outage. A frozen node (SIGSTOP)
runs nothing until it is resumed, and a stopped node restarts from disk. The testbed runs every node
on one host without root, so it can't cut one off.

Not covered here:

- **Network cuts.** The `net-*` scripts need a docker network and a `net.env` that isn't in the
  repo. `rust/chaos` (`test_network_partition_single_node`, Linux and root) and
  `etc/chaos-network/scripts/run-chaos.sh partition` cut one node off. Nothing else cuts half of the
  nodes off the way `etc/test/edge-cases/net-03.sh` does. `chaos/02-lose-quorum` takes the same 2 of
  4 validators away, but by stopping them.
- **Latency and packet loss.** Use `rust/chaos` or `etc/chaos-network/scripts/run-chaos.sh latency`
  / `packet-loss`.
- **Clock skew:** `etc/test/edge-cases/clk-01.sh`.
- **Relay and `/dnsaddr` topologies:** `etc/test-network/local-testnet.sh --relay` / `--relay-dns`.

## Settings

`up` records the binary, `HTTP_ADDR`, `NODE_ARGS`, `LOG_LEVEL` and the genesis `EPOCH_SECS` in `data/env`.
Every later command, node restarts during tests included, reuses them unless you set a value
explicitly. So `HTTP_ADDR=0.0.0.0 ./etc/testbed/testbed.sh up` on the VM keeps restarted nodes
reachable from outside, and a debug testbed stays on debug. To change them, run `down` first:
`up` refuses new settings while nodes are running.

| Variable | Default | Purpose |
|---|---|---|
| `BUILD_CONFIG` | `release` | Cargo profile to build and run: `debug`, or another workspace profile such as `profiling`. Remembered by `up` (see below) |
| `RAYLS_BIN` | builds `target/$BUILD_CONFIG/rayls-network` | Use this binary instead of building |
| `RPC_NODES` | `2` | RPC (observer) nodes created at genesis |
| `TESTBED_DATA` | `etc/testbed/data` | Data and log directory |
| `EPOCH_SECS` | `30` | Epoch length; applied at genesis |
| `HTTP_ADDR` | `127.0.0.1` | RPC bind address (`0.0.0.0` on the VM) |
| `LOG_LEVEL` / `NODE_ARGS` / `GENESIS_ARGS` | `vvv` / - / - | Node verbosity and extra flags |
| `STOP_TIMEOUT` | `120` | Seconds a node gets to exit after SIGTERM |
| `LOAD_TPS` / `LOAD_WALLETS` | `200` / `4` | Background load rate and wallet count |
| `COLLIDE_SECS` | `10` | Average seconds between deliberate nonce collisions; `0` = off |
| `LOAD_CYCLE_SECS` | `0` | Restart tps-checker every N seconds; `0` = only when it exits |
| `SOAK_MINUTES` / `SOAK_INTERVAL` | `480` / `60` | Soak length and seconds between rounds |
| `SOAK_VICTIMS` | all but rpc-1 | Nodes the soak may stop or freeze, e.g. `validator-5` |
| `TPS_TXS` / `CYCLES` | `3000` / `3` | Burst size in `extended/05`; loops in `chaos/10` |
| `TPS_CHECKER_DIR` / `TPS_CHECKER_BIN` | `../tps-checker` | Where tps-checker lives; an existing checkout is used as is |
| `TPS_CHECKER_REPO` / `TPS_CHECKER_REF` | GitHub (SSH) / `d7ea8e4` | Clone URL and commit used when the testbed clones it |

## Notes

- **Crashes are caught.** Every consistency check (most tests, every soak round) fails if any node
  is not running, so a node that dies on its own is reported instead of skipped.
- **No SIGKILL, ever.** Nodes are stopped with SIGTERM and must shut down cleanly. If a node is
  still running after `STOP_TIMEOUT`, the stop fails. The node is left running for inspection, and
  `clean` refuses to delete its data.
- **Load goes through the RPC nodes only, as real users' traffic does.** tps-checker runs in
  continuous mode across all RPC nodes and fails over between them. Its wallets come from the test
  mnemonic starting at index 10, are funded up front, and pay twice the base fee. If tps-checker
  exits, it is restarted. Validators get no load, so the soak can fault any of them, validator-1
  included.
- **Nonce collisions.** Every ~`COLLIDE_SECS`, `cast` sends a transfer from the same wallet
  tps-checker is using. Rejections in `load.log` are expected; the chain must not care.
- **Network.** RPC ports: validator-N is on `8546-N`, and rpc-N on `8540-(N-1)` (rpc-1 = 8540,
  rpc-2 = 8539). Consensus metrics are on `9100` and up, in the same order. p2p (UDP): validator-N
  uses `31000+N` (primary) and `31100+N` (worker), and rpc-N uses `31005+N` / `31105+N`. These sit
  below the OS ephemeral port ranges, so no two nodes can end up on the same port. The dev/admin account is anvil #0 (`0xf39F…2266`). Validator
  operators are anvil #5–#9.
- **Rust plan.** `rust/e2e` stops the testbed while it runs, because it uses ports 8545–8542. Run
  `rust/chaos` with the testbed stopped, via `sudo -E env "PATH=$PATH" ./etc/testbed/testbed.sh test rust/chaos`.
  Afterwards it hands `target/` back to your user.
- **One run at a time.** `data/test.lock` stops a second `test`, `down` or `clean` while a run is
  active. Killing the runner also stops its test and load.
- **Failed additions are set aside.** If `extended` fails to bring up rpc-N, or validator-5 before
  it is staked, the node is stopped and its data moved to `data/failed-<node>-<time>`. The rest of
  the run then sees the network that is actually there. A validator-5 that was already staked
  stays, because it is on-chain.
- **Adding a test.** Drop a file into `tests/<plan>/`. Its first line is a `# description`. Each
  test runs with `set -e` and `lib.sh` loaded, using helpers such as `wait_blocks`, `wait_synced`,
  `check_consistency`, `stop_node`, `fail` and `skip`. After a failure, the runner restores the
  nodes before the next test.
