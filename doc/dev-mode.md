# Run a local dev chain

`rayls-network dev` spins up a **working single-node local chain** — one command, no
key/genesis ceremony, RPC enabled, with well-known accounts pre-funded so you can
send transactions immediately.

> **NOT FOR PRODUCTION.** Dev mode is a 1-of-1 committee (no Byzantine fault
> tolerance), gasless, on a non-production chain-id, using publicly-known account
> keys. It refuses to run against a production chain-id. Use it only for local
> development, demos, and tests.
>
> Dev mode is gated behind the `dev-single-node-setup` **Cargo feature**, which is **off by default**,
> so release/production binaries (built without `--features dev-single-node-setup`) contain no dev
> code at all — no `dev` subcommand, no `--dev` flag, no dashboard. Build with
> `--features dev-single-node-setup` to use it.

## Quick start

```sh
# build once — dev mode lives behind the `dev-single-node-setup` Cargo feature (off by default)
cargo build --bin rayls-network --release --features dev-single-node-setup

# start a local chain (bootstraps an empty datadir automatically)
target/release/rayls-network dev --datadir /tmp/rayls-dev
```

On first run against an empty datadir this:

1. generates the validator key,
2. creates a single-validator genesis + `committee.yaml`,
3. starts the node with **HTTP RPC on `http://127.0.0.1:8545`** and **WS on
   `ws://127.0.0.1:8546`** (permissive local CORS),
4. pre-funds the dev accounts below,
5. serves a **dashboard at `http://127.0.0.1:8550`** so you can see the chain is
   running.

Re-running against the same datadir reuses the existing chain (the bootstrap step
is a no-op). To start fresh, delete the datadir.

You don't need to set `RL_BLS_PASSPHRASE` — `dev` uses a default local passphrase.

## Dashboard

Open **`http://127.0.0.1:8550`** in a browser. The embedded dashboard is a tiny
self-contained block explorer that polls the RPC and shows:

- a **RUNNING / OFFLINE** status pill,
- chain id, block height, average block time, recent tx count, gas price, peers,
- a **recent blocks** table (click a block to expand its transactions),
- a **recent transactions** feed.

It's read-only, localhost, and dev-only. Configure it with:

- `--dashboard-port <PORT>` — change the dashboard port (default `8550`),
- `--no-dashboard` — disable it.

The page has an editable RPC field, so if you ran with a non-default RPC port
(e.g. via `--instance` or `--http.port`) you can point the dashboard at it.

## Chain parameters

| Setting | Value |
|---|---|
| Chain ID | `487` (`0x1e3`, `local`) |
| RPC (HTTP) | `http://127.0.0.1:8545` |
| RPC (WS) | `ws://127.0.0.1:8546` |
| Gas / fees | gasless (base fee `0`, no floor) |
| Committee | 1 validator |
| Block timing | fast headers (~125–250 ms) |

## Pre-funded dev accounts

These are the standard **Hardhat / Anvil** test accounts — their private keys are
public and identical everywhere, so they import into any wallet (MetaMask, `cast`,
ethers, web3.py) with no extra setup. Each is pre-funded with native USDr.

> ⚠️ These keys are public. **Never** use them on any network you care about.

| # | Address | Private key |
|---|---|---|
| 0 | `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` | `0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80` |
| 1 | `0x70997970C51812dc3A010C7d01b50e0d17dc79C8` | `0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d` |
| 2 | `0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC` | `0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a` |
| 3 | `0x90F79bf6EB2c4f870365E785982E1f101E93b906` | `0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6` |

Account 0 is also the validator's fee recipient, so block rewards accrue there.

## Connect a wallet

Add a custom network:

- **RPC URL:** `http://127.0.0.1:8545`
- **Chain ID:** `487`
- **Currency symbol:** `USDr`

Then import one of the private keys above.

## Send a transaction

With [Foundry](https://book.getfoundry.sh/)'s `cast`:

```sh
# check a balance
cast balance 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 --rpc-url http://127.0.0.1:8545

# transfer 1 USDr from dev account 0 to account 1
cast send 0x70997970C51812dc3A010C7d01b50e0d17dc79C8 \
    --value 1ether \
    --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
    --rpc-url http://127.0.0.1:8545
```

## Common options

`dev` accepts all the normal `node` flags, so you can override the defaults:

```sh
# run a second instance on shifted ports (e.g. HTTP 8546)
rayls-network dev --datadir /tmp/rayls-dev2 --instance 1

# pick an explicit HTTP port
rayls-network dev --datadir /tmp/rayls-dev --http.port 9545
```

## `node --dev` equivalence

`dev` is a thin, RPC-on wrapper. The lower-level path is:

```sh
rayls-network node --dev --datadir /tmp/rayls-dev
```

`node --dev` also auto-bootstraps an empty datadir, but does not force the WS server
or permissive CORS, and expects the BLS passphrase via the usual
`--bls-passphrase-source` (env/stdin/ask). Like the `dev` subcommand, the `--dev`
flag only exists in a binary built with `--features dev-single-node-setup`.

## Resetting

Dev chains are throwaway — to wipe state and re-bootstrap, just remove the datadir:

```sh
rm -rf /tmp/rayls-dev
```
