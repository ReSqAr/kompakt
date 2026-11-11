# The "acked entries never reached the stable log" failure — investigation log

Append-only. The failure: once, in a 24-case run of the generated-faults proptest (stressed regime, a script with `Kill`
and `Replace`), "N of M acked entries never reached the stable log". It did not reproduce (the saved seed replays clean;
about 540 further generated cases passed). It is treated as a possible data-safety defect until explained.

## What an entry has to survive

```text
  writer ──ack from a quorum──▶ replica logs ──leader snapshot of (watermark, cutoff]──▶ stable log
                                      ▲
   write admission: hlc must be strictly above the cutoff, or the write is acked into no snapshot at all
```

A write acked by a quorum with hlc at or below the committed cutoff is on the replicas, the stable watermark passes it,
and the leader never integrates it. That is the failure's signature ("on a replica, watermark passed, never arrives").

## Ruled out by reading (no change needed)

- **Persistence order of the promise.** `apply_to_state_machine` persists and fences `last_applied` before it applies to
  the in-memory state the gate follows, so a restarted replica restores at least every cutoff it ever enforced. A stale
  restart cannot forget a promise it made.
- **Replacement.** A replica set collapses only once the stable watermark passes a bound sampled from a quorum of the
  outgoing set (`change.rs`, `drain_satisfied`); quorums of the old set intersect, so the bound covers anything acked
  against it. No hole found.

## Found

A replica admitted a write at or below the cutoff of its own replicated state whenever the gate lagged that state. The
gate holds no cutoff until the applier task installs one, and the applier only follows the state:

- a restarted replica restores its committed cutoff from disk, binds its server, and the applier task has not run yet;
- a joiner's cutoff arrives with its first snapshot, before the applier has run;
- writers resend what they still hold the moment a replica returns, and those writes can be older than the cutoff the
  cluster has since committed.

Fix: `WriteSubmitter::gate` also refuses at or below the cutoff of the state it already reads. The gate stays for the
drain its guards enforce. Test: `a_replica_refuses_writes_below_the_cutoff_its_state_holds_before_its_gate_does` (red
without the change, green with it).

## Not established

The window is real and now closed, but it is **not shown to be the cause** of the observed failure. An experiment
widened it on purpose (applier start delayed 400 ms, the fix reverted) and ran the proptest at 48 cases: no failure. A
loss needs more than the window: a write older than the cutoff, in flight at the moment of a restart, admitted by a
quorum of replicas that are all in the window at once.

## Still open

- Whether writers resend old-HLC pending writes across a quorum outage, and what the cluster does with them when it
  returns (the one place a below-cutoff write is likely to exist at all).
- The failure report now classifies each missing entry (on a replica or not, watermark passed or not, late or never):
  the next occurrence will say which stage lost it. `CLUSTER_TEST_DURABILITY_DEADLINE_MS` shortens the deadline.
