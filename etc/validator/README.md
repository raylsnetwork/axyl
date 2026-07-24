# Provisioning a Rayls validator node

This directory ships the operator workflow for adding a validator to an
existing Rayls network. It is the validator counterpart to
[`etc/observer/`](../observer/README.md) and uses the same general layout
(scripts + `.env` + a local datadir).

A validator's life on the network has three explicit stages, each driven by
a separate script:

| Script | What it does |
|---|---|
| [`create-validator.sh`](create-validator.sh) | Generate keys, fund the operator address, allowlist it on `ConsensusRegistry`, and submit the stake transaction. |
| [`activate-validator.sh`](activate-validator.sh) | Submit `ConsensusRegistry.activate()` (moves the validator into `PendingActivation`); with `--start`, also launch the node so it is ready to vote when the next epoch promotes it to `Active`. |
| [`exit-validator.sh`](exit-validator.sh) | Submit `ConsensusRegistry.beginExit()` to put the validator in the exit queue at the next epoch boundary. |

The on-chain side of the lifecycle (stake → allowlist → activate → exit →
unstake) is documented in [`rayls-contracts/README.md`](../../rayls-contracts/README.md).

## Prerequisites

Before you start you need, from the network operator:

- A reachable **RPC URL** of an existing node on the network you are joining.
- A **`genesis/`** directory containing `genesis.yaml` and `committee.yaml`,
  with a sibling `parameters.yaml` one level above it.
- An **admin key** with `MAINTAINER` / `DEFAULT_ADMIN_ROLE` permission on
  `ConsensusRegistry` (`ADMIN_PRIVATE_KEY` below). This key funds the new
  validator address and allowlists it on-chain. In typical operator setups
  this is held by the team running the network, not by the validator
  operator.

You also need the Rust toolchain matching the workspace `rust-toolchain` /
`Cargo.toml` and Foundry's `cast` (for sending the on-chain transactions
from the scripts).

## Configuration — `.env`

```sh
cp .env.example .env
```

Populate `.env` with:

| Variable | Description |
|---|---|
| `ADMIN_PRIVATE_KEY` | Key authorised to allowlist validators on `ConsensusRegistry` and to fund the new validator address. Hex, with or without the `0x` prefix — `cast send` accepts both. |
| `PRIVATE_KEY` | Private key of the new validator's operator address (the address that will hold the stake). Hex, with or without the `0x` prefix. |
| `ADDRESS` | The validator's operator address (`0x...`). Must match `PRIVATE_KEY`. |
| `GENESISDIR` | Absolute path to the directory containing `genesis.yaml` and `committee.yaml`. `parameters.yaml` is read from `${GENESISDIR}/..`. |
| `RPC_URL` | RPC endpoint of an existing network node, used during bootstrap. If unset, scripts will prompt for it. |
| `STAKE_AMOUNT` | Stake to lock, in wei (e.g. `1000000000000000000000000` for 1M RLS at 18 decimals). |
| `REGISTRY_CONTRACT_ADDRESS` | *(Optional)* Defaults to the canonical address `0x07E17e17E17e17E17e17E17E17E17e17e17E17e1`. Override only if the network operator has deployed `ConsensusRegistry` elsewhere. |
| `RPC_PORT` | *(Optional)* Only used in a log line printed by `activate-validator.sh --start`; the node itself starts on its default HTTP port regardless. |
| `VALIDATOR` | *(Optional)* Human-readable label used in log lines for this validator (e.g. `val1`). |
| `BUILD_CONFIG` | Build configuration passed to `rayls-network` compilation (e.g. `debug` or `release`). Defaults to `debug` if unset. |
| `RAYLS_NETWORK` | *(Optional)* Network identifier for the `rayls-network` binary (e.g. `local`). |

The same `.env` is read by all three scripts.

## Step 1 — `./create-validator.sh`

```sh
./create-validator.sh
```

What it does:

1. Compiles `rayls-network` in release mode (skipped if the build is already
   up to date).
2. Runs `rayls-network keytool generate validator --datadir local-validator
   --address ${ADDRESS}` to create the validator's BLS and network keys
   under `local-validator/node-keys/` and write `node-info.yaml`.
3. Copies `${GENESISDIR}/{genesis,committee}.yaml` into
   `local-validator/genesis/` and `${GENESISDIR}/../parameters.yaml` into
   `local-validator/`.
4. **Funding** — `cast send` from `ADMIN_PRIVATE_KEY` transfers
    `${STAKE_AMOUNT}` wei (native tokens) to `${ADDRESS}` to cover gas for
    subsequent on-chain calls. RLS tokens are minted separately.
5. **Allowlisting** — `cast send` calls
   `ConsensusRegistry.allowlistValidator(address)` from
   `ADMIN_PRIVATE_KEY` to add the new operator address to the registry's
   allowlist.
6. **Stake** — runs `rayls-network keytool stake-calldata` to produce the
   ABI-encoded `stake(...)` calldata (with the proof-of-possession), then
   `cast send` submits the stake transaction signed by `PRIVATE_KEY`.

> Note: `create-validator.sh` accepts a `--start` flag for backwards
> compatibility but ignores it (legacy no-op). To launch the node, use
> `activate-validator.sh --start` in Step 2.

After this step the validator is **staked but not yet active** — its status
on `ConsensusRegistry` is the post-stake state, prior to `activate()`.

If `local-validator/` already exists, the script prints a skip message and
exits 0 without re-running any steps. Remove the directory and re-run if
you want a fresh provisioning.

## Step 2 — `./activate-validator.sh`

```sh
./activate-validator.sh             # send activate(), don't launch the node
./activate-validator.sh --start     # send activate() AND launch the node
```

`activate-validator.sh` is **always** the step that submits
`ConsensusRegistry.activate()` — the create script does not do this. After
the transaction confirms, the validator is in `PendingActivation` and will
be promoted to `Active` at the next `concludeEpoch()` system call.

With `--start` the script also launches the node so it is up and following
consensus when activation completes:

```
rayls-network node \
  --datadir local-validator \
  --instance 99 \
  --metrics 127.0.0.1:9109 \
  --log.stdout.format log-fmt \
  --txpool.pending-max-count 1000000 \
  --txpool.pending-max-size 1242880000 \
  ... (other --txpool.* limits) \
  --txpool.minimal-protocol-fee 0 \
  -vvv \
  --http
```

Use `--start` only when you intend the same machine that ran the
provisioning scripts to also run the node. For Docker / remote-host
deployments, omit `--start`, copy the `local-validator/` directory to the
target host, and launch `rayls-network node` there.

### Cold-start sequencing

A newly-activated validator must catch up to the network's current epoch
before it can vote. While catching up it sits in `CvvInactive` mode and
runs the state-sync subscriber instead of participating directly in
consensus. Once it has caught the chain up it transitions to `CvvActive`
automatically. See [`doc/node-lifecycle.md`](../../doc/node-lifecycle.md)
for the full transition state machine.

## Step 3 (when retiring) — `./exit-validator.sh`

```sh
./exit-validator.sh
```

Sends `ConsensusRegistry.beginExit()` signed by `PRIVATE_KEY`. The
validator stays selectable in voter committees until it has been excluded
from the committee for two consecutive epochs (handled by `concludeEpoch()`);
only then is the validator moved to `Exited`. After one further epoch in
`Exited`, `unstake()` can be called to recover the stake and any accrued
rewards.

The `exit-validator.sh` script does **not** stop the running node — bring
it down separately (`kill <pid>` or your service manager) once the on-chain
exit has been finalised.

## Monitoring a running validator

- Prometheus metrics are on `127.0.0.1:9109` by default (override with
  `--metrics`). The execution layer adds its own metrics; the consensus
  layer adds the `tx_*_total` counters documented in
  [`doc/crates/consensus/primary-metrics.md`](../../doc/crates/consensus/primary-metrics.md).
- A `--healthcheck <PORT>` flag exposes a TCP liveness probe; not enabled
  by the `--start` path of `activate-validator.sh`, but recommended for
  Kubernetes / systemd setups.
- The standard `eth_*` JSON-RPC and the `rayls_*` namespace (see
  [`doc/crates/execution/rpc.md`](../../doc/crates/execution/rpc.md)) are
  served from `--http.addr / --http.port`.

## Troubleshooting

- **`Error: .env file not found`** — every script reads
  `etc/validator/.env`; copy from `.env.example` first.
- **`AllowlistValidator: caller is not allowed`** — the `ADMIN_PRIVATE_KEY`
  does not hold `MAINTAINER` on `ConsensusRegistry`. Ask the network
  operator for the right key.
- **Activation transaction reverts with `not staked`** — `activate-validator.sh`
  was invoked before `create-validator.sh` finished. Re-run the create
  script and re-attempt activation.
- **Node refuses to start, "BLS passphrase required"** — the scripts
  hardcode `RL_BLS_PASSPHRASE="local"` for convenience. For any non-local
  deployment, change this in the scripts (or set `RL_BLS_PASSPHRASE`
  externally and remove the hardcoded export) and use a strong passphrase.
