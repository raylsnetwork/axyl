# Rayls Network Smart Contracts

## Overview

This Rayls Network smart contracts repository contains various system and standard contracts that play crucial roles for the Rayls Network, including the **ConsensusRegistry** system contract, staking infrastructure, and fee distribution.

## Get Started

This repository does not use Foundry git submodules due to dependencies that do not properly support them. Instead of the `lib` directory, all dependencies are kept in `node_modules`

Requires Node version >= 18, which can be installed like so:

`nvm install 18`

And then install using `npm`:

`npm install`

To build the smart contracts:

`forge b`

To run the smart contract tests, which will run for a bit to fuzz thoroughly, use:

`forge test`

The fork tests will require you to add a Sepolia and rayls-network RPC url to the .env file.

## Static Analysis

CI runs [Slither](https://github.com/crytic/slither) on every PR that touches `rayls-contracts/` (the `Slither` job in `.github/workflows/pr.yaml`) and posts the results as a PR comment. The job fails on any Medium or High finding; Low and Informational findings are listed but don't fail it.

To run it locally (Python >= 3.10):

```sh
pip install slither-analyzer==0.11.6
slither . --config-file slither.config.json
```

Slither runs `forge clean` before compiling, so `out/` and `cache/` are rebuilt afterwards.

### Ignore list

Findings that don't apply to these contracts are suppressed in one of two places, always with a reason.

**Whole detectors** are excluded in `slither.config.json` (`detectors_to_exclude`):

| Detector | Why it's excluded |
| --- | --- |
| `naming-convention`, `similar-names`, `too-many-digits`, `pragma`, `solc-version` | Style and compiler-version lints; the compiler is pinned in `foundry.toml`. |
| `assembly`, `low-level-calls` | Informational markers for ERC-7201 storage access, the BLS precompile calls and native transfers, all by design. |
| `uninitialized-local` | Flags the `uint256 total;` idiom; Solidity zero-initializes locals. |
| `calls-loop` | Epoch-close loops run over the bounded active validator set and only call our own system contracts. |
| `reentrancy-events` | Only about event ordering after external calls; the state-changing reentrancy detectors stay enabled. |
| `costly-loop`, `cyclomatic-complexity`, `boolean-equal` | Gas and style hints. |

Optimization-level detectors are off too (`exclude_optimization`), and `test/`, `script/` and `src/mocks/` are not analyzed (`filter_paths`).

**Individual findings** are suppressed inline, directly above the flagged line, with the reason in the comment before the directive:

```solidity
// Emergency withdrawal: `to` is chosen by DEFAULT_ADMIN_ROLE by design.
// slither-disable-next-line arbitrary-send-eth
(bool success,) = to.call{value: amount}("");
```

`grep -rn -B2 'slither-disable' src/` lists them all with their reasons. Inline directives move with the code; Slither's central triage file (`slither.db.json`) keys findings by byte offset, so unrelated edits would silently invalidate it.

When Slither flags new code, fix it or add a directive with a reason. `slither . --config-file slither.config.json --warn-unused-ignores` reports directives that no longer suppress anything (ignore the one it flags in OpenZeppelin's `Panic.sol`).

## ConsensusRegistry Contract

### Overview

The ConsensusRegistry system contract serves as a single onchain source of truth for consensus-related items which need to be easily accessible across all RL nodes.

It plays a pivotal role in maintaining the integrity and functionality of the network by:

1. **Overseeing RLS Staking Mechanisms**: Handling the locking of stakes for governance-approved validators, as well as tracking, distributing, and slashing rewards for validation services.
2. **Managing the Active Validator Set**: Processing validators through activation and exit queues.
3. **Storing Historical Epoch Information**: Recording epoch block heights and voting validator committees, which are predetermined and stored for future epochs.

### Key Features

- **Validator Lifecycle Management**: Handles the activation, operation, and exit of validators in an efficient manner.
- **Epoch Management**: Utilizes system calls to maintain up-to-date contract state at the end of each epoch through `concludeEpoch()`.
- **Rewards and Slashing**: Implements mechanisms for distributing staking rewards and applying penalties.

### Validator Onboarding

Below, we follow the general lifecycle of a new validator in roughly chronological order.

1. **Staking**: This can be done by calling the `stake()` function, where the validator or a delegator provides the validator BLS public key and address.

2. **Initiating Activation**: Once staked, validators can enter the pending activation queue by calling the `activate()` function. This sets their status to `PendingActivation`, with their activation epoch designated as the next epoch.

3. **Activation**: At the end of each epoch, the protocol system calls `concludeEpoch()`, processing the `PendingActivation` queue. Validators with `PendingActivation` status are transitioned to the `Active` state, allowing them to begin their duties in the network.

4. **Exit Requests**: Active validators may choose to retire by calling the `beginExit()` function (`ConsensusRegistry.sol:524`), which places them in the exit queue where they remain active and eligible for selection in voter committees until their exit is finalized.

5. - **Protocol-Determined Exit**: The protocol manages exit finalization. A validator is fully exited after being excluded from voter committees for two consecutive epochs handled at the epoch boundary during the `concludeEpoch()` system call. This is a permissioned transition of the validator from `PendingExit` to `Exited` status, at which point the validator becomes eligible for unstaking after waiting one additional epoch.

6. **Unstaking**: Validators that have elapsed one full epoch in the `Exited` state (or their delegators) may call the `unstake()` function to reclaim the original stake and any accrued rewards. This process releases the stake and rewards. Once unstaked, the validator's address enters an `UNSTAKED` state, making the retirement irreversible.

This detailed lifecycle ensures that validators are properly integrated into the Rayls Network, maintaining the integrity and reliability of the network's consensus mechanism. For further technical details, refer to the [consensus/design.md](./design.md) file.

## Get Involved

We welcome contributions and feedback from the community. If you're interested in contributing to the Rayls Network, please refer to our contribution guidelines and join our discussions on governance and protocol improvements.

## License

These contracts are licensed under the Business Source License 1.1 (BUSL-1.1) — see [LICENSE](./LICENSE) — except the LayerZero OFT interfaces (`IMintableBurnable`, `IOFT`), which keep their original MIT license. Contracts derived from Telcoin `tn-contracts` are under BUSL with attribution preserved in the repository [NOTICE](../NOTICE), which also reproduces the MIT terms for the LayerZero interfaces. See each file's SPDX header and the third-party [Apache 2.0 license text](./licenses/Apache-2.0.txt).
