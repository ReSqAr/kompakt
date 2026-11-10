#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments,
    clippy::result_large_err,
    reason = "test code"
)]
//! Generated fault-and-timescale scripts over a real three-node cluster.
//!
//! # What this adds over the hand-written cluster tests
//!
//! Each hand-written test pins one schedule: one restart at one moment, one
//! timescale. The invariants in [`crate::common`] are crate-wide laws,
//! so the interesting question is not "does this schedule hold" but "does
//! *any* schedule hold". This module generates the schedule and the timescale
//! and asserts the same battery over the result.
//!
//! # Two rules that shape everything here
//!
//! **Nothing is asserted mid-flight.** Every claim is about the *quiesced*
//! cluster, after the script has run, everything down has been relaunched and
//! the replica set has re-converged. A mid-flight assertion over a cluster
//! with a node down and an election in progress would be asserting about a
//! race, and would fail for reasons that are not bugs. The one exception is
//! [`CutoffObserver`], which is inherently temporal: it records a history and
//! checks it after the fact.
//!
//! **A failed write proves nothing.** `zslg_log::Writer::write` documents that "an
//! error means quorum was not observed, not that nothing was written" — the
//! write may well be durable on a quorum with the ACK lost on the way back. So
//! a `DataId` is recorded only when `write` returns `Ok`, and the absence of a
//! failed write's data is never asserted. Under [`Regime::Stressed`] writes
//! failing is a *correct outcome*, not a test failure.
//!
//! # Faults available here, and one that is not
//!
//! `common::Partition` cuts links in `MemRouter`, the in-process raft
//! transport used by `start_cluster_with_partition`. A [`Cluster`] built by
//! `Cluster::new` runs real `ReplicaNode`s over real TCP and never constructs
//! a `MemRouter`, so `Partition` is not reachable from one. Reaching it would
//! mean threading a partition handle through `ReplicaNodeConfig` into the
//! production router — new surface for a test's benefit — so partition ops are
//! **omitted** and the fault vocabulary is a *sustained* node-down window
//! (opened by [`Op::Down`] or [`Op::Kill`], closed by [`Op::Up`]), node
//! *replacement* (which drives a real membership change) and timing.
//!
//! # A graceful stop and a crash are different faults, and both are here
//!
//! [`Op::Down`] is a *graceful* stop: `Cluster::remove` cancels the shutdown
//! token, awaits `run()`, and waits for the port. Everything is flushed, so the
//! state that makes CLAUDE.md §8's crash recovery interesting — an un-fsynced
//! tail, a batch interrupted between `Storage::write` and `Storage::fence` —
//! never exists after it. A suite whose only fault is that one leaves
//! CLAUDE.md §8's recovery paths to RocksDB's own WAL replay and never in
//! composition with cluster-level quorum and membership, which is what
//! [`Op::Kill`] is here for.
//!
//! [`Op::Kill`] is the crash analogue, and it needed **no production surface**:
//! `ReplicaNode` has no `Drop` impl, so `Cluster::kill` simply declines to call
//! the shutdown path and drops the node, whose `Shutdown` holds a `JoinSet` that
//! aborts its tasks where they stand. Both directions of the feasibility
//! question were *measured*, not assumed, by
//! [`hard_killed_node_can_be_relaunched`] before the op was added: the aborted
//! listener releases its port in 0.2–21 ms (the spread is scheduler contention,
//! not a wait on anything), and the relaunch on the same db-path succeeds in
//! ~6 ms. The second one is the one that could have failed —
//! `spawn_blocking` tasks cannot be aborted and they hold RocksDB's LOCK file
//! for the node's root — and it holds because those lanes hold `Weak` store
//! references and exit once the last `Arc<Storage>` drops, with `Storage::open`
//! retrying a held LOCK for up to 10 s if they have not yet. That retry is pre-existing production code written for exactly this
//! race, not something added for the test.
//!
//! What a kill may cost is **un-acked** writes, which is permitted and is never
//! asserted against (see the second rule above). It cannot cost an acked one: an
//! ack means a quorum of three observed the write, so killing one node leaves at
//! least one durable copy.
//!
//! # A node stays down until the script brings it back
//!
//! [`Op::Down`] and [`Op::Up`] are separate ops rather than one bounce, so the
//! ops between them run against a **two-node cluster — still a quorum of
//! three**. That is the difference that matters: a bounce only degrades the
//! cluster for as long as a relaunch takes, whereas a `Down` window can carry
//! writes, settles and a snapshot interval or two. An immediate bounce is still
//! in the language (`Down` immediately followed by `Up`) and the generator still
//! produces it, so nothing was lost by splitting.
//!
//! **At most one of three may be down, and that floor lives in
//! [`script_strategy`], not in the driver.** A script taking two down is asking
//! whether a cluster without a quorum is unavailable, which is not an invariant
//! question. It is enforced by rewriting the illegal op *in the generated
//! script* because `tally.attempted` is incremented before a burst is issued —
//! any runtime skip of a generated op would silently break the
//! `acked == attempted` claim. `enforce_script_constraints` owns that rewrite
//! and `script_constraints_hold_for_every_short_script` checks it exhaustively.
//!
//! **What the split bought, re-measured over 67 cases with [`Op::Kill`] in the
//! vocabulary.** 53 of them ran at least one *multi-op* down window (74 windows
//! in all), so the degraded cluster is genuinely exercised rather than bounced,
//! and 45 of them executed at least one kill (53 kills). Under `Healthy`,
//! **1037 of 1079** writes issued while a node was down were acked across 34
//! faulted cases, and **no faulted case in either regime acked zero** — the
//! claim the `(Healthy, faulted) => acked > 0` branch below actually makes. The
//! lowest faulted-`Healthy` ack count was 3, from a script that only attempted
//! 3; among cases attempting 10 or more the lowest was 30 of 30. Under
//! `Stressed` the while-down figure is 1185 of 1555, which is the
//! outcome-channel shedding [`capacity_ranges`] describes and not a fault
//! effect.
//!
//! The while-down figure is not 100%, and `Kill` is not why: of the three lossy
//! faulted-`Healthy` cases here, the worst (32 of 42 while-down writes lost)
//! executed no kill and no replace at all. Taking a *leader* down — gracefully
//! or not — costs the writes in flight across the re-election, so the figure is
//! a run-specific observation rather than a property, which is why it is
//! reported and never asserted. What is asserted is that a quorate cluster keeps
//! admitting *something*, and that held in 65 of 65 faulted cases.
//!
//! # The membership change is the point, not a bonus fault
//!
//! [`Op::Replace`] mints a node with a new `NodeId`, so the cluster runs the
//! full joint-config sequence: enter joint, arm the exit bound, drain, collapse.
//! Because `acked` accumulates across the whole script and the durability chain
//! is checked once at the end, a script with writes on both sides of a
//! `Replace` is an end-to-end test of CLAUDE.md §8's joint-drain invariant —
//! a write acked against a quorum of the *outgoing* set alone must still be
//! readable after that set is gone. Nothing extra is asserted for it; the
//! existing end-of-case check is the assertion.
//!
//! **What that coverage is worth today, measured rather than assumed.** The
//! drain gate was relaxed and the suite re-run at 64 cases, twice: once with
//! only the write-point validator (`raft_storage::validators`, the
//! `JointExitTooEarly` branch) returning `Ok(())`, and once with the leader's
//! own step-3 check in `leader/jobs/membership/change.rs` removed as well.
//! **Neither run failed.** The break was live — the joint window measurably
//! shrank (summed `joint_polls` over 67 cases: 412 with step 3 intact, 300
//! without) — so this is not a break that failed to land.
//!
//! The reason is visible in the trace: every exit condition these scripts arm
//! is [`zslg_consensus::Exit::AnyTime`], never `WatermarkAbove` (9 of 9 in a
//! traced run). `AnyTime` means every replica in the sampled quorum of `old`
//! reported an *empty* log, so nothing can be stranded and
//! `meta::drain_satisfied` is unconditionally true. A gate that is always
//! trivially satisfied cannot be caught by removing it. The logs are empty
//! because `Op::Replace` stops the victim *gracefully* and the change only
//! begins once the port is released and the liveness probe notices — hundreds
//! of milliseconds, against a 50–200 ms snapshot interval, so the watermark has
//! long since covered every write.
//!
//! So: these scripts exercise the membership *machinery* — joint entry,
//! retarget, arming, collapse, and the durability chain across it — but they do
//! **not** discriminate the drain bound itself, and no amount of timing,
//! killing or partitioning would change that. The reason is structural.
//!
//! `compute_target` (`leader/jobs/membership/change.rs:426`) seeds the target
//! set from `current.intersection(live)` and fills only the *deficit* from
//! spares, so `next` always contains `old ∩ live`. For the bound to be
//! load-bearing some quorum of `old` must lie wholly *outside* `next` —
//! otherwise every quorum of `old` intersects `next` and nothing acked against
//! `old` alone can be stranded. Given the seed, that requires
//! `old ∩ live = ∅`: every member of the outgoing set dead. And in that case
//! step 2's generation quorum, which needs a quorum of *both* sets, can never
//! be met, so the change never reaches the arming step at all. The two
//! conditions are mutually exclusive by construction.
//!
//! At `replica_node_count = 3` with one-at-a-time replacement the point is
//! sharper still: every 2-subset of a 3-set intersects every other, so no
//! quorum of `old` can lie outside a `next` that differs by one member. The
//! drain bound is a defensive write-point check guarding a transition shape no
//! current code path can initiate; exercising it needs an operator API for
//! wholesale cluster migration that does not exist. Claiming the drain
//! invariant is covered here would be claiming more than the evidence supports.

mod driver;
mod regime;
mod script;
mod tests;

pub(crate) use self::driver::{GENERATED_RECOVERY_BUDGET_MS, run_case, spawn_observer};
pub(crate) use self::regime::{Regime, capacity_ranges, config_for, pick, regime_ranges};
pub(crate) use self::script::{Op, enforce_script_constraints, script_strategy};
