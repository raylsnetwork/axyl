# probe-tip-fork: live reproduction of the latest-block probe fork

A following node (observer / inactive validator, without gossip) learns the chain tip by probing
every committee member for its latest consensus header and keeping the highest number. The reply
is checked only for BLS signatures. The commit timestamp is hashed into the header digest but is
not covered by any signature, so a single committee member can take its own real latest block,
raise the commit timestamp, and serve that on the probe. The follower takes the forged header as
its catch-up target, executes it, and forks: its tip never advances past the forged block because
every real successor fails to link. A restart does not heal it, because the forged header is
persisted canonically on the follower and is replayed from the database.

## What the harness does

1. Builds (or reuses) the node binary with `--features byzantine-test-hooks,fault-injection-test-hooks`.
2. Generates keys and a genesis with four validators, plus a fifth key pair for an observer that
   only follows (not in the committee).
3. Starts everything honest, warms the chain up, then sends transfers through validator-1 so
   blocks carry batches. A poison target must carry batches so its bumped commit timestamp
   executes to a distinct EVM block that the harness can see on the RPC.
4. Stops the observer while it is at a block whose successor carries batches. This defines
   `PROBE_NUMBER` (first block above the observer's tip).
5. Lets the chain advance `GAP` more blocks so the observer is clearly behind.
6. (default) Routes the observer's committee connections to validator-4 (the attacker), keeping
   the real BLS keys but pointing every member's network address at the attacker: the attacker
   answers the probe and every follow-up request. With `-n`, no eclipse: the observer keeps its
   honest connections and only its probe goes to the attacker.
7. Arms validator-4 with `RL_BYZ_FORK_TIP=1`, `RL_BYZ_FORK_NUMBER=<N>`,
   `RL_BYZ_FORK_BUMP=1000000`, and restarts it.
8. Restarts the observer with its probe restricted to the attacker (`RL_FAULT_PROBE_ONLY_PEER`):
   the feature-gated fault models the startup window in which the attacker answers the first
   probe before honest peers do. Watches for the fork: the observer's block at its current tip
   having a timestamp exactly `BUMP` above the canonical validator's block of the same number.
9. On a fork: disarms the attacker, restarts the observer again, and confirms it stays stalled
   (restart does not heal).

## Attacker hook (compiled only under byzantine-test-hooks)

Armed by environment on the attacker process only:

- `RL_BYZ_FORK_TIP=1` serves a forged header on the follower's "latest" probe (number and
  hash both absent) and on a by-hash request for the forged header's own digest.
- `RL_BYZ_FORK_NUMBER=<N>` picks the block to wrap: the attacker's first real block at or above N
  that carries batches.
- `RL_BYZ_FORK_BUMP=<secs>` raises the commit timestamp (default 1000000).

The forgery keeps the real quorum-signed leader and certificates, so the follower's signature
re-verify passes, but the digest differs (the commit timestamp is inside the digest) and the
executed EVM block differs (the timestamp lands in the block header).

The follower's fault `RL_FAULT_PROBE_ONLY_PEER` (compiled only under fault-injection-test-hooks)
restricts its probe to one peer, which models the startup window deterministically. Without it the
test would depend on connection-order timing.

## Running

```
./run.sh -a on        # the bug (exit 1 when reproduced)
./run.sh -a off       # control: attacker honest, observer catches up (exit 0)
./run.sh -n           # no eclipse: the probe goes to the attacker, other peers stay honest
```

Options: `-b binary` to reuse a built binary, `-d dir` for the workdir, `-w secs` for the watch
window, `-e secs` for the epoch duration, `-i n` for the first `--instance` (RPC ports), `-l n`
for the number of transfer senders.

Exit status: 0 healthy, 1 reproduced, 2 inconclusive (including a run where the armed attacker
served no poison).

Logs (`summary.txt`, per-node `<name>.log`, observer slices `<victim>.armed.log`,
`<victim>.no-heal.log`) and every node's consensus-db datadir are kept under the run directory.
