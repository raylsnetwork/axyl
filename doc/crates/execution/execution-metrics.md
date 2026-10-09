# `rayls-execution-metrics`

[`crates/execution/execution-metrics/`](../../../crates/execution/execution-metrics/)

Hosts the Prometheus metrics endpoint for the execution layer. The crate is a
small adapter around `reth-node-metrics`: it wires reth's metrics recorder
and DB hooks into a long-running HTTP server, and is otherwise a single
function call.

## API surface

A single public function, `start_reth_metrics_server`, is exposed:

```rust
pub async fn start_reth_metrics_server<N: NodeTypesWithDB>(
    addr: SocketAddr,
    task_executor: TaskExecutor,
    provider_factory: &ProviderFactory<N>,
    pprof_dumps: PathBuf,
    chain_name: &str,
    build: &BuildMetadata,
) -> eyre::Result<()>
```

It installs the Prometheus recorder, builds a `MetricServerConfig` (binding
the supplied `addr`, advertising `VersionInfo` from `BuildMetadata`, and
declaring the chain via `ChainSpecInfo`), then serves `GET /metrics`.

## Hooks

The crate registers three periodic hooks on the configured `ProviderFactory`:

- A throttled DB metrics emitter (`db.report_metrics()` every 5 minutes).
- The static-file provider's metrics emitter (throttled to 5 minutes).
- A throttled RocksDB metrics emitter (`rocksdb.report_metrics()` every 5 minutes).

All three are bridged through reth's `Hooks::builder()`, so they participate
in the standard reth metrics pipeline without any Rayls-specific glue.

## Inputs

| Argument | Source |
|---|---|
| `addr` | Whatever the operator passes to `rayls-network node --reth-metrics <socket>` — the flag is intentionally renamed from upstream reth's `--metrics` to avoid colliding with the consensus `--metrics` endpoint (see `reth_env/config.rs:28`). |
| `task_executor` | The shared reth task executor created at node startup. |
| `provider_factory` | The reth provider that owns the execution DB. |
| `pprof_dumps` | Filesystem path for on-demand pprof dumps (`/debug/pprof/...`). |
| `chain_name` | The literal string `"Axyl"` — hardcoded at the call site (`reth_env/init.rs:113`). |
| `build` | Build metadata embedded at compile time (version, git SHA, target triple). |

## Output

The exposed endpoint is `http://<addr>/metrics`, in standard Prometheus
text format. It contains all reth-native metrics plus the consensus
counters/gauges defined elsewhere in the workspace
(see [`consensus/primary-metrics.md`](../consensus/primary-metrics.md)
and [`consensus/consensus-metrics.md`](../consensus/consensus-metrics.md)).

## When to depend on this crate

Almost never directly — the binary already does the wiring. The only
expected callers are:

- `bin/rayls-network` at startup, when `--metrics <socket>` is supplied.
- Test harnesses that need to assert metric values without spinning up
  the full reth node.

The crate does not own any consensus-layer metrics; for those, see the
[`consensus-metrics`](../consensus/consensus-metrics.md) crate.
