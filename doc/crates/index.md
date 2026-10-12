# Crate Index

This workspace uses a single Cargo workspace. The crates below are listed in `Cargo.toml` under `[workspace].members`.

See the top-level README for setup and usage: [`README.md`](../../README.md).

For a system-level overview (transaction flow, architecture, database tables, RPC): [`doc/index.md`](../index.md).

## Common

- [`bin/rayls-network`](crates/rayls-network.md)
- [`bin/rayls-db-inspect`](../../bin/rayls-db-inspect/README.md) — read-only consensus-db inspection CLI for operators

## Consensus

- [`consensus overview`](consensus/overview.md)
- [`crates/consensus/state-sync`](consensus/state-sync.md)
- [`crates/consensus/network`](consensus/network.md)
- [`crates/consensus/consensus-metrics`](consensus/consensus-metrics.md)
- [`crates/consensus/primary`](consensus/primary.md)
- [`crates/consensus/worker`](consensus/worker.md)
- [`crates/consensus/primary-metrics`](consensus/primary-metrics.md)
- [`crates/consensus/worker/src/batch-builder`](consensus/batch-builder.md)
- [`crates/consensus/worker/src/batch-validator`](consensus/batch-validator.md)

## Middleware

- [`middleware overview`](middleware/overview.md)
- [`crates/middleware/bridge`](middleware/overview.md#rayls-middleware-bridge)
- [`crates/middleware/orchestrator`](middleware/overview.md#rayls-middleware-orchestrator)
- [`crates/middleware/processor`](middleware/overview.md#rayls-middleware-processor)

## Execution

- [`execution overview`](execution/overview.md)
- [`crates/execution/evm`](execution/evm.md)
- [`crates/execution/faucet`](execution/faucet.md)
- [`crates/execution/rpc`](execution/rpc.md)
- [`crates/execution/execution-metrics`](execution/execution-metrics.md)

## Infrastructure

- [`infrastructure overview`](infrastructure/overview.md)
- [`crates/infrastructure/types`](infrastructure/overview.md#rayls-infrastructure-types)
- [`crates/infrastructure/storage`](infrastructure/overview.md#rayls-infrastructure-storage)
- [`crates/infrastructure/config`](infrastructure/overview.md#rayls-infrastructure-config)
- [`crates/infrastructure/network-cli`](infrastructure/overview.md#rayls-network-cli)
- [`crates/infrastructure/network-types`](infrastructure/overview.md#rayls-infrastructure-network-types)
- [`crates/infrastructure/utils`](infrastructure/overview.md#rayls-infrastructure-utils)

## Testing

- [`testing overview`](testing/overview.md)
- [`crates/testing/test-utils-committee`](testing/overview.md#rayls-testing-test-utils-committee)
- [`crates/testing/test-utils`](testing/overview.md#rayls-testing-test-utils)
- [`crates/testing/e2e-tests`](testing/overview.md#rayls-testing-e2e-tests)
