# Profiling PoC: eBPF profiler + tokio-metrics

Proof of concept for finding **where the node spends time**: which crates and functions burn CPU,
where threads block, and which async tasks hog or wait for the tokio runtime. It runs on the
4-validator network from `etc/docker-network`, so there is real QUIC/libp2p traffic between
validators.

Two complementary sources:

| | eBPF profiler (Alloy `pyroscope.ebpf`) | tokio-metrics (`--tokio-metrics`) |
|---|---|---|
| Code changes | none, only an unstripped binary | small: task manager hook + CLI flag (below) |
| Answers | which **functions/crates** use CPU (on-CPU) and where threads **block** (off-CPU) | which **async tasks** take long polls and how long woken work **waits for a worker** |
| Sees | every thread: tokio workers, rayon pools, `spawn_blocking`, the kernel (`udp_sendmsg`, …) | the tokio runtime and every task spawned through `TaskManager`/`TaskSpawner` |
| Doesn't see | time a future spends awaiting I/O (no thread is on a CPU) | what code runs inside a slow poll |
| Output | flame graphs in Grafana/Pyroscope | Prometheus metrics, Grafana dashboard |

Use them together: the dashboard says *which task* holds a worker too long, the flame graph
(filtered to that validator and time range) says *which functions* are inside it.

## TL;DR

```sh
make profiling-up            # or: make profiling-up LOAD=1  (adds a transfer load generator)
# first run compiles the workspace inside Docker (profiling profile, tokio_unstable): it takes a while

open http://localhost:3000   # Grafana, anonymous admin
#  - Dashboards → "Rayls / tokio runtime & tasks"
#  - Explore → Pyroscope → profile type `process_cpu` → `cpu` (or `offcpu`), label validator="validator1"
make profiling-down          # tear down, deletes volumes
```

Equivalent without make:

```sh
docker compose -f etc/docker-network/compose.yaml -f etc/profiling/compose.yaml up -d --build
docker compose -f etc/docker-network/compose.yaml -f etc/profiling/compose.yaml --profile load up -d  # + load
```

| URL | What |
|---|---|
| http://localhost:3000 | Grafana: dashboard + flame graphs |
| http://localhost:4040 | Pyroscope |
| http://localhost:9090 | Prometheus |
| http://localhost:12345 | Alloy UI: profiling targets and their labels (check here first if no profiles arrive) |
| http://localhost:7545 … 7542 | validator1 … 4 JSON-RPC |

The stack runs under its own compose project (`rayls-profiling`) but on the same subnet as
`make up`, so don't run both at once.

## What's in the stack

```
validator1..4 (local-rayls-network:profiling)
  │  --reth-metrics :9001  → reth_* + reth_tokio_* + reth_tokio_task_*   ──► Prometheus ──► Grafana
  │  --metrics      :9101  → consensus metrics                           ──►      ↑           │
  │                                                                                           │
  └─ stacks sampled by the kernel ◄── Alloy pyroscope.ebpf (privileged, pid: host) ──► Pyroscope
```

- `etc/profiling/Dockerfile`: the node built with `--profile profiling` (release + `debug = true`,
  not stripped) and `RUSTFLAGS="--cfg tokio_unstable"`. Separate from the production Dockerfile.
- `etc/profiling/compose.yaml`: override on `etc/docker-network/compose.yaml`. Swaps the image,
  binds the metrics endpoints to `0.0.0.0`, adds `--tokio-metrics=5s`, and adds Alloy, Pyroscope,
  Prometheus, Grafana, and the optional `loadgen` (`cast send` transfers at about 20/s into validator1).
- `etc/profiling/alloy.config`: keeps processes whose executable is `/usr/local/bin/rayls`, labels
  them `service_name="rayls-network"` and `validator="<compose service>"`. It samples on-CPU stacks
  at 97 Hz and records 5% of context switches as off-CPU samples.

## Code changes for tokio-metrics

- `crates/infrastructure/types/src/task_metrics.rs` (new) wraps every task spawned through
  `TaskManager` / `TaskSpawner`, including reth's tasks spawned via our `TaskSpawner`, in a
  [`tokio_metrics::TaskMonitor`](https://docs.rs/tokio-metrics) shared per task kind. It also runs
  the runtime-wide reporter.
  - **Task kind:** the task name with identifier-like tokens replaced by `*`, so label
    cardinality stays bounded. For example `VoteRequest-0x3f…` becomes `VoteRequest-*`, and
    `ProcessGossip-tn-primary-12D3…` becomes `ProcessGossip-tn-primary-*`. At most 256 kinds;
    anything beyond that is labelled `other`.
  - **When off:** one atomic load per spawn.
- **Flag:** `--tokio-metrics[=INTERVAL]` (default `5s`). It is wired through
  `RaylsBuilder::with_tokio_metrics`. `launch_node` installs reth's Prometheus recorder, then
  starts the reporters, so the metrics are published on the **`--reth-metrics` endpoint**. With
  `--cfg tokio_unstable` it also enables the runtime's poll-time histogram (doubling buckets,
  16 µs to 1 s).
- **Dependency:** `tokio-metrics = 0.5.2`, with the `metrics-rs-integration` feature, against the
  same `metrics` 0.24 as reth's recorder.

Running a profiling-build node outside Docker works the same way:

```sh
RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=target/tokio-unstable \
  cargo build -p rayls-network --bin rayls-network --profile profiling
rayls-network node … --reth-metrics 0.0.0.0:9001 --tokio-metrics
```

Without `tokio_unstable` the flag still works. You get worker busy time, the global queue depth,
live tasks, and all per-task metrics, but not mean poll time, the poll-time histogram, local
queue depths, or steal counts.

## Reading the tokio metrics

All names carry reth's `reth_` prefix. Durations are in µs.

- **Counters** (`total_*_duration`) are cumulative; use `rate()`.
- **`*_count` gauges** hold the count for the last sampling interval.
- **Ratios** are gauges in the range 0..1.

| Metric | Meaning | Worry when |
|---|---|---|
| `rate(reth_tokio_total_busy_duration[1m]) / 1e6` vs `reth_tokio_workers_count` | worker threads busy on average | approaches the worker count |
| `reth_tokio_global_queue_depth`, `reth_tokio_total_local_queue_depth` | tasks woken and waiting for a worker | sustained > 0 |
| `reth_tokio_poll_time_histogram{quantile=…}` | single-poll duration across the runtime (bucket lower bounds) | p99 in the ms range (a worker is blocked that long) |
| `reth_tokio_task_total_poll_duration{task}` | runtime time a task kind consumes | one kind dominates |
| `reth_tokio_task_total_scheduled_duration{task}` | time a task kind sat woken, waiting for a worker (**scheduling delay**) | consensus-critical kinds wait ms |
| `reth_tokio_task_slow_poll_ratio{task}` | share of polls > 50 µs | high on async code doing CPU or blocking work |
| `reth_tokio_task_long_delay_ratio{task}` | share of wakes waiting > 50 µs | high across many kinds: the runtime is starved |

The dashboard computes the per-poll and per-wake means as
`rate(total_X_duration) / (avg_over_time(total_X_count[1m]) / 5)`. It assumes the 5s sampling
interval.

### First look (single dev node on macOS, idle chain)

A one-minute smoke test of a `rayls-network dev` node already showed the kind of signal this gives.

- **`propose-header-*` took about 70% of the runtime's busy time**, nearly all of it in slow polls:
  roughly 1.27 s of 1.8 s total polling.
- **The runtime's p95 poll time was about 4 ms.**

That task runs `Certifier::spawn_header_proposal`, which calls
`state_sync.process_own_certificate(…)` and `publish_certificate(…)` inline on a tokio worker.
Some part of that path (signing, verification, or a synchronous DB write) holds the worker for
milliseconds per round. The flame graph for that window is how to tell which one. Treat this as a
lead, not a measurement: it was one dev node on a laptop, not the 4-validator network under load.

## Reading the flame graphs

- **`process_cpu:cpu`** (on-CPU) shows the frames that were running, from `main` and the tokio
  worker loop down to the kernel.
  - Filter with `validator="validator1"`.
  - Search for a crate (`libp2p`, `quinn`, `reth_trie`, `revm`, `blst`, `rayls_consensus_primary`) to
    see its total share.
  - Async functions show up as `{{closure}}` / `{async_fn_env#0}` frames inside their parent
    function.
- **`offcpu`** shows where threads went to sleep, weighted by time asleep.
  - **tokio workers:** expect them to dominate this view as parked threads (`epoll_wait`, futex
    inside `tokio::runtime::scheduler`). That is idle time, not latency of any particular future.
  - **What's worth looking at** is everything else: `std`/`parking_lot` mutex waits, synchronous
    file or DB I/O, `spawn_blocking` and rayon threads waiting on each other.
- **Networking cost** shows up as the CPU spent in `quinn`/`quinn_proto`, `rustls`/`ring`,
  `libp2p_*`, the (de)serialization code, and the kernel's UDP send/receive path. Neither tool
  measures **bytes on the wire** or per-request network latency.

## Limits and known gaps

- **Untested on Docker Desktop for Mac.**
  - eBPF needs a Linux kernel (5.10+). Docker Desktop's VM has a recent one, but nobody has
    confirmed this stack works there.
  - If no profiles arrive, check the Alloy UI targets first. On a Linux host it is the supported
    setup.
  - Build the image natively for the host architecture (the default). An amd64 image under
    Rosetta would profile the translator.
- **No inlined frames or line numbers.** Alloy symbolizes from the ELF symbol table, so heavily
  inlined reth/revm code is attributed to its caller (grafana/pyroscope#4704). `debug = true`
  doesn't change that today; it only helps other tools (`perf`, `samply`).
- **tokio-metrics covers only `TaskManager`/`TaskSpawner` tasks.**
  - Direct `tokio::spawn` calls (about 15 in our non-test code, plus libp2p's and reth's internal
    ones) count
    toward the runtime-wide metrics but have no per-task series.
  - Rayon and `spawn_blocking` work is not tokio-polled at all; use the flame graphs for it.
- **Why not the pure OTel Collector profiler** (`otel/opentelemetry-collector-ebpf-profiler`)?
  As of 0.162.0 it doesn't fit this PoC:
  - Native frames leave the profiler unsymbolized. Pyroscope then needs `-symbolizer.enabled`
    plus a `profilecli debuginfo upload` of every build.
  - Rust names stay mangled.
  - Off-CPU isn't in the released image.
  - Alloy's `pyroscope.ebpf` runs the same profiler engine (Grafana's fork) and has none of these
    gaps.

## Profiling a real host (devnet/testnet)

On a host that runs a profiling build (or any unstripped build):

1. Run the Alloy container from `compose.yaml`, with `alloy.config` pointed at your Pyroscope.
   Drop the `discovery.docker` join if the node isn't containerized.
2. Add `--reth-metrics … --tokio-metrics` to the node.

Overhead is low (97 Hz sampling, plus two clock reads per poll for tokio-metrics), but measure it
before leaving either on in production.
