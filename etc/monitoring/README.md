# Rayls node monitoring

A ready-to-run Prometheus + Grafana + node_exporter setup for observing a Rayls node.

Background and the reuse-vs-build strategy are in the Network Observability research
(`axyl-private#381`, under the observability initiative `#429`).

## What a node exposes

A Rayls node has **two** Prometheus endpoints (separate registries), plus host metrics via
node_exporter:

| Endpoint | Enable with | Exposes |
|---|---|---|
| **Consensus / Narwhal** | `metrics_address` in parameters.yaml, or `--metrics <socket>` | `ConsensusMetrics`, `PrimaryMetrics`, `WorkerMetrics`, `NetworkMetrics`, `ExecutorMetrics` — round progress, commit latency, certificate throughput, leader election, DAG depth, epoch, peer connectivity, storage gauges |
| **Reth execution** | `reth_metrics_address` in parameters.yaml, or `--reth-metrics <socket>` | reth execution-layer metrics — block processing, txpool, DB/static-file, process |
| **Host** | run node_exporter | RSS / CPU / disk / network |

Both endpoints are **off by default**. The CLI flags override the parameters.yaml values when
passed.

## 1. Enable the endpoints

Add to the node's `parameters.yaml`:

```yaml
metrics_address: "0.0.0.0:9184"        # consensus / Narwhal suite
reth_metrics_address: "0.0.0.0:9001"   # reth execution layer
```

Restart the node. It logs the active endpoints at startup (`… metrics enabled`).

## 2. Point Prometheus at your node

Edit [`prometheus.yml`](./prometheus.yml) so the `rayls-consensus` / `rayls-execution` targets
match the addresses above. The defaults assume the node runs on the docker host
(`host.docker.internal`).

## 3. Run the stack

```sh
docker compose -f etc/monitoring/docker-compose.yml up -d
```

- Prometheus → http://localhost:9090 (check **Status → Targets**: all three jobs `UP`)
- Grafana → http://localhost:3000 (`admin` / `admin`)

The Prometheus datasource is auto-provisioned in Grafana.

## 4. Dashboards

- **Execution layer:** in Grafana, *Dashboards → Import → 20638* — the official
  [reth dashboard](https://grafana.com/grafana/dashboards/20638-reth/). It works as-is against the
  `rayls-execution` metrics.
- **Consensus / validator health:** a Rayls-specific dashboard (per-validator liveness, committee/
  epoch state, round lag) is the next initiative deliverable — see
  [#428](https://github.com/raylsnetwork/axyl-private/issues/428). Until then, the consensus
  metrics are queryable directly in Prometheus (e.g. `current_round`, `last_committed_round`,
  `consensus_dag_rounds`, `leader_election`, `connected_peers_count`).

## Notes

- **Two targets per node, on purpose.** The consensus and execution metrics live in separate
  registries, so they are scraped as two jobs. The `layer` label (`consensus` / `execution` /
  `host`) distinguishes them.
- This stack is a **local/operator convenience**, not a hardened production deployment.
