# Restart recovery: live reproduction

A validator that crashes after it records a new block's header, and before it saves the block, restarts one block behind. When no new block arrives, it never executes the missing block. If the chain needs that validator to make blocks, the chain stays stopped.

After the restart, the block never reaches the forward streamer, the part of the node that executes missing blocks. This happens in two ways:
- If the block's header is in the header cache, the backwards walk stops at it and never passes it on.
- If the walk fetches the header from a peer, it can pass it on before the forward streamer starts listening, and the streamer only reacts to later headers.

`run.sh` shows this end to end on a local network of real nodes.

## Run

```
etc/test/restart-recovery/run.sh            # 5 validators, builds the node first
etc/test/restart-recovery/run.sh -n 7       # 7 validators
etc/test/restart-recovery/run.sh -n 10 -b target/release/rayls-network
```

| Option | Meaning | Default |
|---|---|---|
| `-n` | Validators in the committee, at least 4 | 5 |
| `-b` | Node binary | builds `target/release/rayls-network` with cargo |
| `-d` | Empty or new directory for datadirs and logs | `target/restart-recovery/<timestamp>` |
| `-w` | Seconds to watch the restarted validator | 120 |
| `-a` | Attempts when the kill misses the window | 3 |
| `-i` | First `--instance` number, which sets the RPC ports | 20 |

Needs Linux, bash 4.4 or later, `curl`, and `gdb` with Python support. No root: gdb starts the node as its own child process, which `ptrace_scope` 0 and 1 allow (1 is the Ubuntu default). In a container, run with `--cap-add SYS_PTRACE`. The script checks gdb before it starts any node.

Validator `v` uses `--instance <i + v>`, so its RPC port is `8546 - i - v`. The script stops if one of these ports is already in use, for example by another local network; pick other numbers with `-i`.

Each run leaves its datadirs and logs behind, about 130 MB for 10 validators.

## What it does

With N validators, a block needs `2N/3 + 1` votes (the quorum Q).
1. Creates a new network of N validators and starts validators 1 to Q. Validators Q+1 to N stay down, so every running validator is needed.
2. Runs validator Q under gdb. Once the chain reaches block 20, gdb kills it with SIGKILL as it enters `save_consensus`, just before it saves the next block. The window between recording the header and saving the block is under a millisecond, so a kill timed from the logs misses it. The node binary is not modified.
3. Checks that the chain stops with every other validator at the same block k, then restarts validator Q normally and watches it.
4. After a `BUG` verdict, starts validators Q+1 to N and checks that the chain moves on and validator Q catches up.
5. Stops every node. Datadirs and logs stay in the run directory, with a `summary.txt`.

The breakpoint needs the `save_consensus` symbol in the binary. A release build from this repository has it. A build that inlines it everywhere, for example with full LTO, fails the symbol check or reports that the validator was not killed.

## Result

| Verdict | Meaning | Exit |
|---|---|---|
| `HEALTHY` | Validator Q restarted with block k only in its header cache, and the chain moved on | 0 |
| `BUG` | Validator Q stayed behind and the chain stayed at block k for the whole watch, through one of the two paths above; the summary names which | 1 |
| `MISSED` | The chain moved on, but block k was not left only in the header cache, so the run is retried | |
| `INCONCLUSIVE` | Setup failed, the kill did not happen, the chain did not stop, another validator stopped answering, the stall had a cause the script does not recognise, or every attempt missed | 2 |

A fixed node that needs longer than the watch to catch up is reported as `BUG`; raise `-w` for slow builds.

On unfixed code, the restarted validator logs one of these and the chain does not move:
```
derived walk coverage from cached headers anchor=<k>
starting backwards walk current_latest=0
forward streamer: no new consensus headers received, catch-up idle
```
```
notifying watchers header_number=<k>
stream handoff: starting forward streamer
forward streamer: no new consensus headers received, catch-up idle
```
