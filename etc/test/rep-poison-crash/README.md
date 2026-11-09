# rep-poison-crash: a following node crash-loops on an unsigned reputation score

Finding N1. A consensus block carries a `reputation_score` that is deliberately excluded from the
block digest, so it is not covered by any signature and not checked against anything a following
node already trusts. A peer can set it to anything.

## What the harness shows

A node that follows the chain fetches missing consensus blocks from peers by number and caches each
reply, `reputation_score` included, in its canonical `ConsensusBlocks` store. On its next start the
node rebuilds its leader schedule from the newest stored final-of-schedule row through
`LeaderSchedule::from_store`, and `LeaderSwapTable::new` divides by the number of scored
authorities. A peer that answers a by-number gap fetch with an empty, final-of-schedule score map
drives that count to zero. The follower divides by zero and aborts. The poisoned row persists, so
every restart repeats the abort. The result is a crash-loop that needs operator DB surgery to
clear, delivered by any single connected peer.

Because `reputation_score` is not part of the digest, the poisoned reply keeps the honest digest
and parent link, so it passes every hash and parent check on the way in.

## Topology

N validators and one observer. Validators 1 to K are armed from their first start to answer every
consensus-header request with the poison, for every block number after the warm-up. That covers the
by-number gap fetch, the by-hash fetch and the latest-block probe. K defaults to all N, and `-k 1`
shows what a single misbehaving peer achieves among honest ones.

The observer is stopped so it falls behind, then restarts, catches up through the gap and stores
what the peers served. The observer is then stopped with its DB kept and restarted several times
while the validators keep running. An observer with no peer waits before starting consensus and
never reaches the leader-schedule rebuild, so the validators must stay up. Each start rebuilds the
leader schedule from the newest stored final row and aborts if that row is the poison.

The attacker behaviour is compiled only with the `byzantine-test-hooks` cargo feature and armed per
process by environment, so the armed validators lie while their own consensus is unaffected. The
hook only rewrites replies served to a follower.

## Scenarios

`-s` picks the scenario. The default is `observer`, the flow above.

- `observer-attacker`: every validator is honest. A second observer is the attacker. It is armed
  from its first start, follows the chain like any observer and advertises a dialable address,
  which any operator may choose to do, so the victim observer can reach it. The victim runs the
  same flow as above. This asks whether a peer outside the committee can deliver the poison.
- `rolling`: no observer. Validators 1 to K are armed. The validators listed with `-v`, 2 and 3 by
  default, are restarted one after another the way an operator rolls an upgrade. Each one is
  stopped, the chain moves `-g` blocks on, it starts and catches up, and the harness waits until
  it rejoins the committee or aborts. It is then restarted `-r` more times. Every start gets its
  own log, and the chain height of every validator is logged after each step, so a halt shows as
  a height that stops moving. With `-l <seconds>` the armed validators 1 to K are then stopped for
  that long while the chain height is watched, and started again. This shows whether the liar can
  halt the chain once the victims are down and whether the chain resumes when it returns.
- `rolling-observer-attacker`: the same rolling restart, but every validator is honest. The
  attacker is one armed observer with a dialable address, set up as in `observer-attacker`.

A victim asks its peers for the gap one number at a time, rotating over every connected peer, so
the attacker answers about one number in every peer count. The default gap `-g` is six numbers per
peer the victim can fetch from and at least 40, so the attacker is asked several times per victim.
A smaller `-g` that gives under three numbers per peer is logged as a warning.

In the rolling scenarios every validator height is sampled every five seconds into `heights.log`.
The summary ends with a line per start, a line per victim and every halt found in `heights.log`.
A victim line gives the altered replies the armed nodes logged from the moment the victim was
stopped to the end of its last start, the block numbers those replies covered, how many starts
aborted and whether the victim crash-looped. The hook does not log who asked, so this count
belongs to the victim only while no other node is catching up, which holds when the roll is one
validator at a time. A start that aborts names the panic site, `leader_schedule.rs:93:9` for this
bug. A halt is any stretch of at least 20 seconds in which the highest height did not grow.

## Run

```
# reproduced: one armed validator, the other three honest, observer crash-loops at startup
etc/test/rep-poison-crash/run.sh -k 1

# every validator armed: the observer may not get past the first altered block before the
# restart test, so this case can end inconclusive
etc/test/rep-poison-crash/run.sh

# control: validators honest, observer catches up and restarts cleanly
etc/test/rep-poison-crash/run.sh -a off

# attacker outside the committee: one armed observer, the victim is the other observer
etc/test/rep-poison-crash/run.sh -s observer-attacker

# one lying validator, rolling restart of validators 2 and 3, two more restarts each
etc/test/rep-poison-crash/run.sh -s rolling -k 1 -r 2

# control for the rolling restart: every validator honest, all of them rejoin
etc/test/rep-poison-crash/run.sh -s rolling -k 1 -r 2 -a off

# seven validators, one lying validator, roll the two honest validators f can cover, then stop
# the liar for three minutes and start it again
etc/test/rep-poison-crash/run.sh -s rolling -n 7 -k 1 -v 2,3 -r 1 -l 180

# seven honest validators, one armed observer, roll f plus one validators
etc/test/rep-poison-crash/run.sh -s rolling-observer-attacker -n 7 -v 2,3,4 -r 1
```

Exit status: 1 reproduced, 0 healthy, 2 inconclusive.

The binary is built with `panic=abort` into `target/panic-abort`, so the divide-by-zero aborts the
process the way a release build does. Pass a prebuilt binary with `-b`.

## Evidence kept per run

Under `target/rep-poison-crash/<timestamp>`:
- `summary.txt`: the run log, including the poison-served count and the abort line per restart.
- `observer-catchup.log`: the observer log for the catch-up start.
- `observer-crash-<n>.log`: the observer log for each crashed restart, with the divide-by-zero.
- `attacker.log`: the attacker observer log in the observer-attacker scenarios, one warn line
  per altered reply.
- `validator-<n>-start-<m>.log`: in the rolling scenarios, the log of each start of a restarted
  validator, start 0 being the catch-up start.
- `validator-<n>-liar-restart.log`: with `-l`, the log of the armed validator started again after
  the liar step.
- `heights.log`: in the rolling scenarios, one line per sample with the epoch second, the UTC
  time, the highest height and every validator height.
- every node's datadir, stopped at the end of the run, so the observer's consensus DB can be
  inspected with the `../axyl-144` db-inspect CLI. The newest final-of-schedule row in `ConsensusBlocks` has an empty
  `scores_per_authority` map and `final_of_schedule: true`.
