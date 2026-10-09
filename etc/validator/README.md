# Provisioning a Rayls validator node

This directory ships the operator workflow for adding a validator to an
existing Rayls network. It is the validator counterpart to
[`etc/observer/`](../observer/README.md) and uses the same general layout
(scripts + `.env` + a local datadir).

`create-validator.sh` supports two modes:

| Mode | Command | Use it when |
|---|---|---|
| **Config only** | `./create-validator.sh --config-only` | You are preparing a validator that will run on its own host and join an existing network. Generates keys and config files only; no on-chain calls, **no private keys and no RPC URL required**. Allowlisting, staking and activation are done later, once the node is running. |
| **Full flow** | `./create-validator.sh` | You are testing a validator locally and hold the admin key. Generates the config, then funds, allowlists and stakes the validator on-chain. |

The remaining lifecycle steps each have their own script:

| Script | What it does |
|---|---|
| [`activate-validator.sh`](activate-validator.sh) | Submit `ConsensusRegistry.activate()` (moves the validator into `PendingActivation`); with `--start`, also launch the node locally. |
| [`exit-validator.sh`](exit-validator.sh) | Submit `ConsensusRegistry.beginExit()` to put the validator in the exit queue at the next epoch boundary. |

The on-chain side of the lifecycle (stake → allowlist → activate → exit →
unstake) is documented in [`rayls-contracts/README.md`](../../rayls-contracts/README.md).

## Prerequisites

From the network operator:

- A **`genesis/`** directory containing `genesis.yaml` and `committee.yaml`,
  with a sibling `parameters.yaml` one level above it.
- *(Full flow only)* A reachable **RPC URL** of an existing node, and an
  **admin key** with `MAINTAINER` / `DEFAULT_ADMIN_ROLE` permission on
  `ConsensusRegistry` (`ADMIN_PRIVATE_KEY`). This key funds the new
  validator address and allowlists it on-chain. It is normally held by the
  team running the network, not by the validator operator.

Locally you need the Rust toolchain matching the workspace `rust-toolchain` /
`Cargo.toml` (the scripts build `rayls-network`). Foundry's `cast` is only
needed for the full flow and for `activate-validator.sh` / `exit-validator.sh`.

## Configuration — `.env`

```sh
cp .env.example .env
```

All scripts read `etc/validator/.env`. Every variable in it is exported, so
`RL_*` variables are passed straight through to `rayls-network`.

| Variable | Config only | Full flow | Description |
|---|---|---|---|
| `ADDRESS` | required | required | The validator's operator address (`0x...`). Becomes the fee/reward recipient in `node-info.yaml` and is the address that later holds the stake. |
| `GENESISDIR` | required | required | Absolute path to the directory containing `genesis.yaml` and `committee.yaml`. `parameters.yaml` is read from `${GENESISDIR}/..`. |
| `RL_BLS_PASSPHRASE` | required | optional | Passphrase that encrypts the BLS key in `node-keys/`. The **same** value must be set when starting the node. Defaults to `local` in the full flow (throwaway nodes only). |
| `RL_EXTERNAL_PRIMARY_ADDR` | optional | optional | Public primary p2p multiaddr written into `node-info.yaml`, e.g. `/ip4/<PUBLIC_IP>/udp/49001/quic-v1`. Set it to the host the validator will run on; the default (`127.0.0.1` with a random port) is only useful for local tests. |
| `RL_EXTERNAL_WORKER_ADDRS` | optional | optional | Comma-separated public worker multiaddrs, e.g. `/ip4/<PUBLIC_IP>/udp/49101/quic-v1`. |
| `BUILD_CONFIG` | optional | optional | `debug` (default) or `release`. |
| `COMPILER_THREADS` | optional | optional | Passed to `cargo build -j`. |
| `ADMIN_PRIVATE_KEY` | **not needed** | required | Key authorised to allowlist validators on `ConsensusRegistry` and to mint/fund the new validator. |
| `PRIVATE_KEY` | **not needed** | required | Private key of `ADDRESS`; signs the RLS `approve` and `stake` transactions. Also used by `activate-validator.sh` / `exit-validator.sh`. |
| `RPC_URL` | **not needed** | required | RPC endpoint of an existing network node. Prompted for if unset. |
| `STAKE_AMOUNT` | **not needed** | required | Native tokens (wei) sent to `ADDRESS` to cover gas. The RLS stake itself is read from `getCurrentStakeConfig()`. |
| `REGISTRY_CONTRACT_ADDRESS` | **not needed** | optional | Defaults to `0x07E17e17E17e17E17e17E17E17E17e17e17E17e1`. |
| `RAYLS_NETWORK` | — | — | *(Optional)* Network identifier used by `activate-validator.sh --start` when launching the node. |
| `RPC_PORT`, `VALIDATOR` | — | — | *(Optional)* Only used in log lines of `activate-validator.sh --start`. |

## Step 1a — generate config only (joining a network)

```sh
./create-validator.sh --config-only
```

1. Builds `rayls-network` (`-p rayls-network --bin rayls-network`).
2. Runs `rayls-network keytool generate validator --datadir local-validator
   --address ${ADDRESS}`, encrypting the BLS key with `RL_BLS_PASSPHRASE`.
3. Copies `${GENESISDIR}/{genesis,committee}.yaml` into
   `local-validator/genesis/` and `${GENESISDIR}/../parameters.yaml` into
   `local-validator/`.
4. Writes the `ConsensusRegistry.stake(...)` calldata (BLS public key +
   proof of possession) to `local-validator/stake-calldata.txt`, so staking
   can be done later from any machine with `cast`.
5. Packs everything into `validator-bundle.tar.gz`.

Output:

```
local-validator/
├── node-info.yaml        # public validator info — share with the network operator
├── node-keys/            # BLS + network keys — keep private
├── genesis/
│   ├── genesis.yaml
│   └── committee.yaml
├── parameters.yaml
└── stake-calldata.txt
validator-bundle.tar.gz   # the directory above, ready to upload
```

Upload the bundle to the validator host, extract it and start the node there
with the same passphrase:

```sh
tar -xzf validator-bundle.tar.gz
RL_BLS_PASSPHRASE='<same passphrase>' rayls-network node \
  --datadir ./local-validator \
  --full --storage.v2 \
  --http
```

(See the full flag list used by `activate-validator.sh --start` below.)

`node-keys/` contains the validator's identity. The BLS key is encrypted with
`RL_BLS_PASSPHRASE`, the network key is not — treat the bundle as a secret
and do not commit it. If the passphrase is lost the validator must be
re-provisioned.

### Later: allowlist, stake and activate

Once the node is running, the remaining on-chain steps are done by whoever
holds the relevant keys:

```sh
# network operator (admin key)
cast send $REGISTRY "allowlistValidator(address)" $ADDRESS --private-key $ADMIN_PRIVATE_KEY --rpc-url $RPC_URL

# validator operator (key of $ADDRESS), after acquiring the required RLS stake
cast send $RLS_TOKEN "approve(address,uint256)(bool)" $REGISTRY $REQUIRED_STAKE --private-key $PRIVATE_KEY --rpc-url $RPC_URL
cast send $REGISTRY "$(cat local-validator/stake-calldata.txt)" --private-key $PRIVATE_KEY --rpc-url $RPC_URL
cast send $REGISTRY "activate()" --private-key $PRIVATE_KEY --rpc-url $RPC_URL
```

`$RLS_TOKEN` is `cast call $REGISTRY "rlsToken()(address)"` and
`$REQUIRED_STAKE` is the first value of
`cast call $REGISTRY "getCurrentStakeConfig()(uint256,uint256,uint32)"`.
`activate-validator.sh` (without `--start`) sends the `activate()` call for you.

## Step 1b — full flow (local testing)

```sh
./create-validator.sh
```

Performs steps 1–4 of the config-only mode (no bundle), then:

5. **Funding** — `cast send` from `ADMIN_PRIVATE_KEY` transfers
   `${STAKE_AMOUNT}` wei (native tokens) to `${ADDRESS}` to cover gas.
6. **Allowlisting** — `ConsensusRegistry.allowlistValidator(address)` from
   `ADMIN_PRIVATE_KEY`.
7. **Mint + approve** — mints the required RLS stake to `${ADDRESS}`
   (admin has `MINTER_ROLE`) and approves the registry to spend it, signed by
   `PRIVATE_KEY`.
8. **Stake** — submits the stake calldata signed by `PRIVATE_KEY`.

After this step the validator is **staked but not yet active**.

> `create-validator.sh` accepts `--start` for backwards compatibility but
> ignores it. To launch the node, use `activate-validator.sh --start`.

In both modes, if `local-validator/` already exists the script prints a skip
message and exits 0 without re-running any steps. Remove the directory (and
`validator-bundle.tar.gz`) to provision a fresh validator.

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
  was invoked before the validator was staked (full flow not finished, or
  the stake step of the config-only flow not done yet).
- **`RL_BLS_PASSPHRASE must be set in .env for --config-only`** — set a
  passphrase in `.env`; it protects the BLS key and is needed at node start.
- **Node refuses to start / cannot decrypt BLS key** — the node must be
  started with the same `RL_BLS_PASSPHRASE` that was in `.env` when the keys
  were generated. `activate-validator.sh --start` reads it from `.env`
  (falling back to `local`). To change it later use
  `rayls-network keytool rotate-passphrase`.
