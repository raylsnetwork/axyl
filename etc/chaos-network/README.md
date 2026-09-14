# Chaos Testing Network

Docker Compose setup for chaos engineering tests. Works on Linux and macOS.

## Quick Start

```bash
# Build and start the 4-validator testnet
docker compose -f etc/chaos-network/compose.yaml up --build -d

# Wait for the network to be ready (~30s for genesis + first blocks)
# Check logs:
docker compose -f etc/chaos-network/compose.yaml logs -f validator1

# Run all chaos scenarios
etc/chaos-network/scripts/run-chaos.sh all

# Run a single scenario
etc/chaos-network/scripts/run-chaos.sh node-crash
etc/chaos-network/scripts/run-chaos.sh latency
etc/chaos-network/scripts/run-chaos.sh partition
etc/chaos-network/scripts/run-chaos.sh packet-loss
etc/chaos-network/scripts/run-chaos.sh combined

# Tear down
docker compose -f etc/chaos-network/compose.yaml down -v
```

## How It Works

Validators run in Docker containers with `cap_add: [NET_ADMIN]` and network
tools (`iproute2`, `iptables`) pre-installed. Chaos faults are injected via
`docker exec` running `tc` or `iptables` inside the target container.

This works on any platform because the network manipulation happens inside
the Linux container, not on the host.

## Manual Fault Injection

```bash
# Add 200ms latency to validator2
etc/chaos-network/scripts/inject-latency.sh chaos-validator2 200 50

# Remove latency
etc/chaos-network/scripts/remove-latency.sh chaos-validator2

# Partition validator3 (block all peer traffic, keep RPC open)
etc/chaos-network/scripts/inject-partition.sh chaos-validator3

# Heal partition
etc/chaos-network/scripts/remove-partition.sh chaos-validator3

# Kill validator (graceful)
etc/chaos-network/scripts/kill-validator.sh chaos-validator2

# Kill validator (hard crash)
etc/chaos-network/scripts/kill-validator.sh chaos-validator2 --hard

# Restart killed validator
etc/chaos-network/scripts/restart-validator.sh chaos-validator2

# Add 15% packet loss
etc/chaos-network/scripts/inject-packet-loss.sh chaos-validator4 15
```

## RPC Endpoints

| Validator | RPC URL |
|-----------|---------|
| validator1 | http://127.0.0.1:7545 |
| validator2 | http://127.0.0.1:7544 |
| validator3 | http://127.0.0.1:7543 |
| validator4 | http://127.0.0.1:7542 |

## Configuration

- Chain ID: `487` (local)
- Epoch duration: 15 seconds (short, to exercise epoch transitions)
- Consensus ports: UDP 49590 (primary), 49595 (worker)
- Network: bridge `10.20.0.0/16`

## Using from Rust Tests

The `chaos-framework` crate provides Docker-aware fault injectors:

```rust
use chaos_framework::fault::{network_latency, network_partition};

// Inject latency into a container
let guard = network_latency::add_latency_docker("chaos-validator2", 200, 50)?;
// ... test ...
guard.clean(); // or just drop it — auto-cleans

// Partition a container
let guard = network_partition::partition_container("chaos-validator3")?;
// ... test ...
// guard auto-cleans on drop
```
