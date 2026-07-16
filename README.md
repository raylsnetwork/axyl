<div align="center">

<img src="./assets/rayls-logo.svg" alt="Rayls" width="120">

# Axyl

**The Rayls Network protocol client — EVM execution built on [reth], DAG-based BFT consensus (Narwhal + Bullshark).**

[![CI][ci-badge]][ci-url]
[![Nightly E2E][e2e-badge]][e2e-url]
[![Nightly Fuzz][fuzz-badge]][fuzz-url]
[![Version][version-badge]][version-url]
[![MSRV][msrv-badge]][msrv-url]
[![License: BUSL-1.1][license-badge]][license-url]

[![Discord][discord-badge]][discord-url]
[![X][x-badge]][x-url]
[![LinkedIn][linkedin-badge]][linkedin-url]
[![YouTube][youtube-badge]][youtube-url]

[Run a node](#running-a-node) | [System Overview](doc/index.md) | [Crate Docs](doc/crates/index.md) | [Dev Mode](doc/dev-mode.md)

</div>

## What is Axyl?

Axyl is the protocol client for the Rayls Network, an EVM-compatible blockchain built on DAG-based consensus. A single binary (`rayls-network`) runs both halves of the protocol:

- **Consensus layer** — an implementation of [Narwhal](https://arxiv.org/abs/2105.11827) and [Bullshark](https://arxiv.org/abs/2201.05677), a DAG-based BFT consensus protocol, communicating over libp2p (QUIC).
- **Execution layer** — an Ethereum-compatible EVM built on [reth] and [alloy]. It produces EVM blocks and serves the standard Ethereum JSON-RPC API, so existing Ethereum tooling works out of the box.

Nodes run as **validators**, which participate in consensus, or as **observers**, which follow the chain via state-sync without joining the committee. Chain specs for the public `testnet` and `mainnet` are embedded in the binary. The protocol targets 10,000+ TPS.

For the architecture and transaction flow, see the [system overview](doc/index.md); for a map of the workspace, see the [crate index](doc/crates/index.md).

## Status

Releases are tagged `v<version>` from `main` — the version badge above tracks the latest tag.

The protocol has been independently audited by [Halborn](https://www.halborn.com/): three assessments performed between February and March 2026, covering the consensus protocol, the network node, and the smart contracts, with remediation reviews completed in April–May 2026. The full reports and their outcomes are in [audits/](./audits/).

## Running a node

### System requirements

|             | Recommended minimum        |
| ----------- | -------------------------- |
| CPU         | 8 physical cores           |
| RAM         | 16 GiB                     |
| Storage     | 500 GiB SSD                |
| Network     | 10+ Gbps bandwidth         |

Axyl supports Linux and macOS. On Windows, use WSL to run a Linux environment in which the client compiles and runs properly.

### Run a local dev chain (one command)

The fastest way to get a working chain for local development — no key/genesis ceremony, RPC enabled, well-known accounts pre-funded:

```sh
cargo build --bin rayls-network --release --features dev-single-node-setup
target/release/rayls-network dev --datadir /tmp/rayls-dev
```

This bootstraps an empty datadir into a single-validator, gasless chain (chain-id `2017`) with HTTP RPC on `http://127.0.0.1:8545` and a status/explorer dashboard at `http://127.0.0.1:8550`. Dev mode lives behind the `dev-single-node-setup` Cargo feature (off by default, so production builds exclude it). See [`doc/dev-mode.md`](doc/dev-mode.md) for pre-funded accounts and wallet setup. For local development only — not for production.

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

This uses `DATADIR` for storage and sets the node's execution address. Observers do not receive credit for execution, but the option is still required — use an address you control, or a dummy one as above. The command prompts for a passphrase for the node's BLS key; you will need it again on startup (or supply it via the `RL_BLS_PASSPHRASE` environment variable with `--bls-passphrase-source env`).

Start your observer node, with a `DATADIR` and passphrase matching the step above:

```sh
target/release/rayls-network node -vvv \
    --http \
    --observer \
    --chain testnet \
    --bls-passphrase-source ask \
    --datadir DATADIR
```

The only valid values for `--chain` are `testnet` and `mainnet`; the embedded chain spec is selected from the value.

### Run a local multi-validator network

The test network script configures four local validators, creates genesis, and starts them in the background:

```sh
cp etc/test-network/.env.example etc/test-network/.env  # first time only
etc/test-network/local-testnet.sh --start --dev-funds 0xADDRESS
```

`0xADDRESS` must be an address you hold the key for; it is funded with 1 billion RLS at genesis (`--dev-funds` only applies during initial configuration and cannot fund accounts later). The script compiles a release build, which may take a few minutes, and prints the RPC endpoints — point your favorite Ethereum tooling at any of them. Tunables (RPC ports, gas limits, block cadence — roughly one block every 10 seconds by default) live in `etc/test-network/.env`.

After the first run, `--start` alone restarts the network. To reset from scratch: stop the nodes (`killall rayls-network`), delete `etc/test-network/local-validators/`, and rerun the script. The same basic procedure — configure each node, combine the configs into genesis, share it with every node — is used to create networks across machines; the script itself is short and is currently the best reference for it.

For multi-machine setups, `RL_EXTERNAL_PRIMARY_ADDR` and `RL_EXTERNAL_WORKER_ADDRS` (comma-separated) set the [multiaddrs](https://github.com/multiformats/multiaddr) the primary and worker libp2p networks advertise. They default to `/ip4/127.0.0.1/udp/[PORT]/quic-v1` with unused ports; the addresses must use `quic-v1` over UDP.

### Example RPC request

```sh
curl 127.0.0.1:8545 \
    -X POST \
    -H "Content-Type: application/json" \
    --data '{"method":"eth_chainId","params":[],"id":1,"jsonrpc":"2.0"}'
```

## For Developers

Build from source:

```sh
git clone https://github.com/raylsnetwork/axyl
cd axyl
cargo build --release
```

The toolchain is pinned to Rust 1.91 via [`rust-toolchain.toml`](./rust-toolchain.toml). `make pr` runs the same fmt + clippy + test gate as CI (fmt and clippy require a nightly toolchain); `make test` runs the test suite alone.

Documentation lives in [`doc/`](doc/):

- [System overview](doc/index.md) — architecture, transaction flow, database tables, RPC.
- [Crate index](doc/crates/index.md) — every workspace crate with per-domain overviews.
- [Dev mode](doc/dev-mode.md), [gasless mode](doc/gasless-mode.md), [node lifecycle](doc/node-lifecycle.md).

Contributions are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md) and our [Code of Conduct](./CODE_OF_CONDUCT.md).

## Getting Help

- Join the chat on [Discord][discord-url].
- Found a bug or have a feature request? [Open an issue](https://github.com/raylsnetwork/axyl/issues).
- Follow Rayls on [X][x-url], [LinkedIn][linkedin-url], and [YouTube][youtube-url] for announcements.

## Security

To report a security vulnerability, see [SECURITY.md](./SECURITY.md) — please do not open a public issue. Completed third-party audits are listed under [Status](#status).

## Acknowledgements

Axyl is an EVM-compatible blockchain client built with DAG-based consensus. While building the protocol, we studied and explored many different projects to identify what worked well and where we could make improvements.

We want to extend our sincere appreciation to the following teams:

- [reth](https://github.com/paradigmxyz/reth): Reth stands out for their dedication to implementing the Ethereum protocol with clean, well-written code. Their unwavering commitment to building a strong open-source community has reached far beyond the Ethereum ecosystem. We are truly grateful for their leadership and the inspiration they continue to provide.
- [sui](https://github.com/MystenLabs/sui): Axyl uses a version of Bullshark that was heavily derived from Mysten Labs' Sui codebase under the Apache 2.0 license. Because this code was already released under the Apache License, we decided to start with a derivation of their work to iterate more quickly. We thank the Mysten Labs team for pioneering BFT consensus protocols and publishing their libraries.
- [Telcoin Network](https://github.com/Telcoin-Association/telcoin-network): Axyl's Rust consensus workspace is derived from Telcoin Network — which builds on Sui's Narwhal/Bullshark consensus — and has been substantially reorganized and modified by Rayls. Their work is released under the Apache 2.0 / MIT licenses. We thank the Telcoin Association and its contributors for their open-source work.

## License

Axyl is licensed under the **Business Source License 1.1 (BUSL-1.1)** — see [LICENSE](./LICENSE). Production use is permitted for operating nodes — validator, observer, relayer, and RPC — on the Rayls public mainnet and its official test networks; other production use requires a commercial license. The Change Date is four years after each version's first public release, with the **Apache License, Version 2.0** as the Change License.

This repository is a derivative work that incorporates and modifies code from Telcoin Network, Sui, and reth (all permissively licensed), plus the LayerZero OFT interfaces kept under their original MIT license. See [NOTICE](./NOTICE) for full attribution — including the MIT terms for the LayerZero interfaces — alongside the third-party [Apache 2.0 license text](./licenses/Apache-2.0.txt).

[reth]: https://github.com/paradigmxyz/reth
[alloy]: https://github.com/alloy-rs/alloy

[ci-badge]: https://github.com/raylsnetwork/axyl/actions/workflows/pr.yaml/badge.svg
[ci-url]: https://github.com/raylsnetwork/axyl/actions/workflows/pr.yaml
[e2e-badge]: https://github.com/raylsnetwork/axyl/actions/workflows/nightly-e2e.yml/badge.svg
[e2e-url]: https://github.com/raylsnetwork/axyl/actions/workflows/nightly-e2e.yml
[fuzz-badge]: https://github.com/raylsnetwork/axyl/actions/workflows/nightly-fuzz.yml/badge.svg
[fuzz-url]: https://github.com/raylsnetwork/axyl/actions/workflows/nightly-fuzz.yml
[version-badge]: https://img.shields.io/github/v/tag/raylsnetwork/axyl?label=version
[version-url]: https://github.com/raylsnetwork/axyl/tags
[msrv-badge]: https://img.shields.io/badge/MSRV-1.91-orange.svg
[msrv-url]: ./rust-toolchain.toml
[license-badge]: https://img.shields.io/badge/License-BUSL--1.1-blue.svg
[license-url]: ./LICENSE
[discord-badge]: https://img.shields.io/badge/Discord-join%20chat-5865F2?logo=discord&logoColor=white
[discord-url]: https://discord.gg/6THZ96357r
[x-badge]: https://img.shields.io/badge/X-%40RaylsLabs-000000?logo=x&logoColor=white
[x-url]: https://x.com/RaylsLabs
[linkedin-badge]: https://img.shields.io/badge/LinkedIn-Rayls-0A66C2?logo=linkedin&logoColor=white
[linkedin-url]: https://www.linkedin.com/company/rayls/
[youtube-badge]: https://img.shields.io/badge/YouTube-Rayls-FF0000?logo=youtube&logoColor=white
[youtube-url]: https://www.youtube.com/@Rayls_blockchain
