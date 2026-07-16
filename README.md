# Rayls Network

[![License: BUSL-1.1](https://img.shields.io/badge/License-BUSL--1.1-blue.svg)](./LICENSE)

Consensus layer (CL) is an implementation of Narwhal and Bullshark.
Execution layer (EL) produces EVM blocks compatible with Ethereum.

Requires Rust 1.91

Crate index: [`doc/crates/index.md`](doc/crates/index.md)

### Supported Platforms

The Rayls Network protocol client supports Linux and MacOS operating systems. For Windows users, use WSL to run a Linux environment in which the client compiles and runs properly.

### Recommended minimum system requirements

Target performance: 10 000+ TPS

**CPU:** 8 x physical CPU cores

**RAM:** 16 GiB RAM

**Storage:** 500 GiB SSD Disk

**Network:** 10+ Gbps Network Bandwidth

## Quick Start

### Run a local dev chain (one command)

The fastest way to get a working chain for local development — no key/genesis
ceremony, RPC enabled, well-known accounts pre-funded:

```sh
cargo build --bin rayls-network --release --features dev-single-node-setup
target/release/rayls-network dev --datadir /tmp/rayls-dev
```

This bootstraps an empty datadir into a single-validator, gasless chain (chain-id
`2017`) with HTTP RPC on `http://127.0.0.1:8545` and a status/explorer dashboard at
`http://127.0.0.1:8550`. Dev mode lives behind the `dev-single-node-setup` Cargo feature (off by
default, so production builds exclude it). See [`doc/dev-mode.md`](doc/dev-mode.md)
for pre-funded accounts and wallet setup. For local development only — not for
production.

### Run an observer against testnet

Build a release version of the node software:

```sh
cargo build --bin rayls-network --release
```

Generate a config and keys for your observer node:

```sh
target/release/rayls-network keytool generate observer \
    --datadir DATADIR \
    --address 0x4444444444444444444444444444444444444444 \
    --bls-passphrase-source ask
```

This will use DATADIR for storage and set your "execution" address to 0x4444444444444444444444444444444444444444. Note an observer does not recieve credit for execution but this option needs to be set anyway (at time of writing). Use an address you control or a dummy like above. This will also ask for the password for your nodes BLS key, this will need to be entered when started (or it can be put in an ENV var for injection).

Start your observer node:

```sh
target/release/rayls-network node -vvv \
    --http \
    --observer \
    --chain testnet \
    --bls-passphrase-source ask \
    --datadir DATADIR
```

The only valid values for `--chain` are `testnet` and `mainnet`; the embedded chain spec is selected from the value.

Make sure DATADIR matches the config command above and use the same password for reading the key.

### Start a local development network

Run the test network script to start four local validators and begin advancing the chain. The
script reads `etc/test-network/.env` (copy from `.env.example` first if you have not already)
for tunable parameters like RPC ports and gas limits:

```sh
cp etc/test-network/.env.example etc/test-network/.env  # first time only
etc/test-network/local-testnet.sh --start --dev-funds 0xADDRESS
```

Note: the script will compile a release build, which may take a few minutes.
This configures and creates genesis for a new network and starts it. See the output for the RPC endpoints.
0xADDRESS above should be a valid address prepended with 0x. Make sure you have the key for this address,
it will be funded with 1billion RLS on your test network. After configuration you can run the script with
just the --start option (--dev-funds is only used when configuring and CAN NOT be used later to fund
an account). Nodes run in the backgound and should be killed with the `kill` or `killall` commands.

The best docs for running a test network will currently be this script. It is short and pretty basic,
it configures each node, brings together the configs to create genesis and then shares this with each node.
This is the same basic procedure used to create nodes on diffent machines (NOTE- do not use the instance
option if not running on the same machine, it is to avoid port conflicts).

Once started you can use the RPC endpoint for any node with your favorate Ethereum tooling to test.
You will have test funds in your dev funds account set during config. The network can be restarted
by shutting down, `killall rayls-network` is good for this, deleting ./etc/test-network/local-validators/ and
rerunning the script.

The defaults should build a block roughly every 10 seconds, see comments on the script if you want to
speed this up for testing.

## CLI Usage

The CLI is used to create validator information, join a committee, and start the network.
The following `.env` variables are useful but not required:

- `RL_EXTERNAL_PRIMARY_ADDR`: The multi address of the primary libp2p network.
- `RL_EXTERNAL_WORKER_ADDRS`: The multi address(es) of the worker libp2p networks. This is a comma seperated list.
  All of these multi addresses will default to /ip4/127.0.0.1/udp/[PORT]/quic-v1 with an unused port for PORT. This is really only useful for tests (but is very useful for testing).
  You MUST supply quic-v1 and udp to work with the rayls-network (although if you were setting up your own network other protocols may work but are untested).
  References for multiaddr:
  https://github.com/multiformats/multiaddr
  https://github.com/multiformats/rust-multiaddr
  These are used with libp2p2 so also see the Rust libp2p docs.

## Example RPC request

### get chain id

```sh
curl 127.0.0.1:8545 \
    -X POST \
    -H "Content-Type: application/json" \
    --data '{"method":"eth_chainId","params":[],"id":1,"jsonrpc":"2.0"}'
```

## Security Audits

The protocol has been independently audited by [Halborn](https://www.halborn.com/) —
three assessments covering the consensus protocol, the network node, and the smart
contracts. The full reports and their outcomes are in the [audits/](./audits/) directory.

## Acknowledgements

Rayls Network is an EVM-compatible blockchain built with DAG-based consensus.
While building the protocol, we studied and explored many different projects to identify what worked well and where we could make improvements.

We want to extend our sincere appreciation to the following teams:

- [reth](https://github.com/paradigmxyz/reth): Reth stands out for their dedication to implementing the Ethereum protocol with clean, well-written code. Their unwavering commitment to building a strong open-source community has reached far beyond the Ethereum ecosystem. We are truly grateful for their leadership and the inspiration they continue to provide.
- [sui](https://github.com/MystenLabs/sui): Rayls Network uses a version of Bullshark that was heavily derived from Mysten Lab's Sui codebase under Apache 2.0 license. Because this code was already released under the Apache License, we decided to start with a derivation of their work to iterate more quickly. We thank the Mysten Labs team for pioneering BFT consensus protocols and publishing their libraries.
- [Telcoin Network](https://github.com/Telcoin-Association/telcoin-network): Rayls' Rust consensus workspace is derived from Telcoin Network — which builds on Sui's Narwhal/Bullshark consensus — and has been substantially reorganized and modified by Rayls. Their work is released under the Apache 2.0 / MIT licenses. We thank the Telcoin Association and its contributors for their open-source work.

## License

Rayls Network is licensed under the **Business Source License 1.1 (BUSL-1.1)** — see [LICENSE](./LICENSE). The Change Date is four years after each version's first public release, with the **Apache License, Version 2.0** as the Change License.

This repository is a derivative work that incorporates and modifies code from Telcoin Network, Sui, and reth (all permissively licensed), plus the LayerZero OFT interfaces kept under their original MIT license. See [NOTICE](./NOTICE) for full attribution — including the MIT terms for the LayerZero interfaces — alongside the third-party [Apache 2.0 license text](./licenses/Apache-2.0.txt).
