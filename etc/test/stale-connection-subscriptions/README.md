# Live reproduction: lost gossip subscriptions after a fast restart

This directory reproduces, on a real local network, the issue where a validator that is killed and
restarted within the 30 s QUIC idle timeout cannot publish gossip afterwards.

A killed process sends no QUIC close, so every peer keeps the old, dead connection until the idle
timeout. When the restarted process connects, each peer holds two connections to it. libp2p
gossipsub sends a node's topic subscriptions only on the first connection to a peer, and does not
resend them when the dead one closes, so the restarted validator never learns that anyone
subscribes to anything. Every publish it makes fails with `NoPeersSubscribedToTopic`. libp2p
request-response also spreads requests across both connections, so about half the requests to the
restarted validator hang until the dead connection times out.

The same state needs no crash: an on-path attacker that drops every datagram one validator
receives for a little over the idle timeout, while that validator's own packets still go out,
makes the validator drop its connections while its peers keep theirs. When it redials, each peer
again holds a dead connection next to the new one.

The script has a mode for each trigger:

- `crash` (default): an ordinary crash followed by a supervised restart. No test hook is needed.
- `oneway`: no crash. The on-path attacker is played by `udp_drop.c`, an LD_PRELOAD shim the
  script builds with `cc` and loads only into the victim. The node binary is unchanged: libp2p-quic
  creates and owns its UDP sockets, so nothing inside the binary can drop datagrams.

## Run it

From a fresh clone, with a Rust toolchain and `curl` available:

```
./etc/test/stale-connection-subscriptions/run.sh                  # crash
./etc/test/stale-connection-subscriptions/run.sh -m oneway -s 40  # on-path drop (also needs cc)
```

The script is self-contained. It builds the node binary, generates keys and genesis for a fresh
local network under `target/stale-connection-subscriptions/<timestamp>/`, starts the nodes, drives
the scenario, and cleans up the processes on exit. It needs no `.env` and no prior setup. Pass
`-b <binary>` to skip the build.

What it does, per round (two rounds by default):

1. Puts validator 3 behind a stale connection:
   - `crash`: kills it with SIGKILL and restarts it at once, well inside the 30 s idle timeout.
   - `oneway`: drops everything it receives for 32 s, then lets traffic through again on its next
     QUIC handshake (its redial).
2. Gives it 20 s (`-s`; use about 40 in `oneway`, where it also has to catch up) to reconnect.
3. Stops validator 5 gracefully, so exactly a quorum (4 of 5) is left.
4. Watches for 60 s (`-w`): counts the `gossipsub PUBLISH FAILED` lines validator 3 logs and the
   blocks validator 1 commits.
5. Restarts validator 5 and waits until both catch up.

## Reading the result

- Exit 1, `REPRODUCED`: validator 3 failed to publish after reconnecting. Its log shows
  `gossipsub PUBLISH FAILED ... res=Err(NoPeersSubscribedToTopic) ... all_topic_peer_count=0`
  although it is connected to every other validator.
- Exit 0, `HEALTHY`: validator 3 published without a failure and the chain committed at least 20
  blocks in every round.
- Exit 2, `INCONCLUSIVE`: the network did not start, a node did not come back, or validator 3
  gossiped but the chain still committed too few blocks (a different problem).

Logs are `validator-<n>.log` in the run directory, with a `summary.txt` of the run.

The crash scenario also runs as an ignored e2e test:

```
STALE_KEEP=1 STALE_ROUNDS=2 cargo test -p e2e-tests --test it restart_within_idle_timeout_regossips -- --ignored --test-threads 1 --nocapture
```
