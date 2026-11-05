# Live reproduction: crash form of the gap-fetch issue

This directory reproduces, on a real local network, the crash form of the by-number consensus
gap-fetch issue. A node that follows the chain is made to stop with a fatal error by one connected
peer, and the stop repeats on every restart.

## What it shows

A follower that is missing a consensus block fetches it from a connected peer by its number. The
reply is accepted after a check on its number and parent link only, not on whether it is the real
block the committee produced (this is the core gap the issue describes). Each certificate in the
block is then verified against the keys of the block's epoch, and each certificate's author is
looked up in the follower's own committee to find the block-reward address. An author that is not
in that committee is a fatal `UnexpectedAuthority`, and the follower's subscriber stops.

A real block never carries a certificate from outside its committee, so the crash needs a crafted
reply. The peer that serves it is node5, a validator that joins the committee after genesis.

## Why node5 can build the certificate

Every real certificate carries its author's own vote, because a validator signs its own block
before asking others to vote. Such a certificate from node5 only verifies against node5's own
epoch keys, so a follower on an older committee would just reject it and move on, without stopping.

node5 builds a different certificate. Once it is a member of epoch N, it certifies one of its own
blocks. It takes the votes the other members gave that block, which are the four validators of
epoch N-1, and assembles a second certificate from those votes alone, leaving its own vote out.
That certificate is signed only by members of epoch N-1, and the list of who signed is not covered
by the signature, so it verifies against the epoch N-1 keys. Its author, node5, is not in the
epoch N-1 committee.

node5 adds this certificate to the older blocks (from before it joined) that a follower asks it for
by number. The follower verifies the block against the older epoch's keys, where the added
certificate passes, then looks node5 up in the older committee, does not find it, and stops.

## The test-only behaviour

node5's behaviour is compiled only with the `byzantine-test-hooks` feature, never in a production
build, and is armed per process by environment. The other nodes run the same binary and stay
honest.

- `RL_BYZ_EXTRA_CERT=selfless` (set on node5): after node5 certifies one of its own blocks, save
  the certificate built from the other members' votes only, as described above.
- `RL_BYZ_GAP_TARGET` and `RL_BYZ_GAP_MODE=crash` (set on node5): add the saved certificate to the
  older blocks node5 serves for a by-number request. The hook acts only once the certificate is
  saved, and only on blocks from an epoch before node5 joined that carry batches.

`RL_BYZ_EXTRA_CERT=honest` saves node5's real certificate instead, which carries its own vote, and
the follower rejects it without stopping. This is a control.

## Run it

From a fresh clone, with a Rust toolchain, `curl` and Foundry's `cast` available:

```
./etc/test/gap-fetch-crash/run.sh            # node5 adds the selfless certificate (the issue)
./etc/test/gap-fetch-crash/run.sh -a honest  # node5 adds its real certificate (control, healthy)
./etc/test/gap-fetch-crash/run.sh -a off     # node5 is honest (control, healthy)
```

By default the observer's backward walk stores none of the older blocks, so the observer fetches
every one of them by number. A fix that accepts a block fetched by number only once a stored block
links onto it has nothing to link to in that setup, so the observer could never catch up. To test
such a fix, pass `-k`: the walk stores every block and the observer skips only the odd-numbered
stored older blocks, so it still fetches those from node5 by number.

The script is self-contained. It builds the node binary with the hooks and `panic=abort` (as in
release) under `target/panic-abort/`, generates keys and genesis for a fresh local network under
`target/gap-fetch-crash/<timestamp>/`, starts the nodes, drives the scenario, and cleans up the
processes on exit. It needs no `.env` and no prior setup. Pass `-b <binary>` to skip the build; a
binary built with the default `panic=unwind` keeps running after the panic, which the script
reports separately.

What it does:

1. Starts four genesis validators, node5 (which joins later), and an observer that follows.
2. Starts a stream of transfers (`-l` senders), so blocks carry batches: the subscriber looks a
   certificate's author up only for a block that has batches.
3. Stops the observer to open a gap of blocks from before node5 joins.
4. Stakes and activates node5 through the on-chain registry, and waits until it is seated and has
   saved the certificate it adds.
5. Restarts the observer up to `-r` times, as a supervisor would. Each time the observer catches up
   through the gap and fetches older blocks by number, some of them from node5.

## Reading the result

- Exit 1, `REPRODUCED`: the observer's subscriber stopped on the added certificate. Each
  `observer.restart-<n>.log` shows `Authority missing from committee` followed by the panic.
- Exit 0, `HEALTHY`: the observer caught up without stopping.
- Exit 2, `INCONCLUSIVE`: the network did not start, node5 was never seated, the observer exited
  for another reason, or it neither stopped nor caught up.

Logs are `<node>.log` in the run directory, with a `summary.txt` of the run.

The same failure runs in-process, with no network, as a regression test:
`cargo test -p rayls-middleware-bridge --test it -- mixed_epoch_cert`.
