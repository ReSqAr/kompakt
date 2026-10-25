#![allow(
    dead_code,
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
//! timescale. The invariants in [`crate::tests::common`] are crate-wide laws,
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
//! **A failed write proves nothing.** `api::Writer::write` documents that "an
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
//! is [`crate::meta::Exit::AnyTime`], never `WatermarkAbove` (9 of 9 in a
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

use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use proptest::prelude::{Just, ProptestConfig, Strategy, prop_oneof, proptest};
use proptest::{prop_assert, prop_assert_eq};

use crate::ids::{DataId, NodeId};
use crate::replica_node::ReplicaNodeConfig;
use crate::tests::{
    Cluster, ClusterConfig, CutoffObserver, NodePlacement, assert_cluster_invariants,
    assert_cluster_invariants_with_acks, assert_durability_chain_on,
};

/// Which regime a case runs in — a stratum over both the timescales
/// ([`regime_ranges`]) and the channel capacities ([`capacity_ranges`]).
///
/// Generated explicitly and stratified: proptest samples the enum uniformly,
/// so both regimes appear across a run by construction. Sampling from one wide
/// range instead would concentrate cases in the middle and reach the
/// interesting extremes rarely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Regime {
    /// Timescales comfortably above operation cost, queues with headroom.
    /// Writes are expected to succeed; both safety *and* liveness are asserted.
    Healthy,
    /// Timescales near or below operation cost and queues at the shed
    /// boundary: elections may thrash and writes may time out. Only safety is
    /// asserted — "everything errored" is a correct outcome here, not a
    /// failure.
    ///
    /// This is a real stratum and the capacities are what make it one, measured
    /// rather than assumed: over 64 cases it acks 83.28% of attempted writes
    /// against `Healthy`'s 99.98%, with 24 of 29 `Stressed` cases losing writes
    /// against 1 of 38 `Healthy` ones. The separation needs concurrency to
    /// appear at all — [`Op::WriteBurst`] is what fills the queues
    /// [`capacity_ranges`] undersizes; see there for the mechanism.
    Stressed,
}

/// The generated timescale bounds per regime, as
/// `(snapshot_interval_ms, raft_election_timeout_min_ms)`.
///
/// The 50 ms floor on both is deliberate: below it an fsync and a tokio timer
/// tick are the same order as the interval itself, so the configured number
/// stops describing anything the system actually does.
///
/// `Stressed` was **measured, not assumed**, to be non-vacuous: at its most
/// aggressive corner (50 ms snapshot interval, 60 ms election minimum, so a
/// 15 ms heartbeat) a three-node cluster still admits writes — see
/// [`stressed_regime_admits_writes`], which pins that corner together with the
/// capacity floor. No widening toward `Healthy` was needed.
///
/// These bounds are **not** what separates the two regimes in practice — with
/// an in-memory object store a per-op cost is ~1 ms, so 60 ms and 400 ms are
/// both comfortably above it. [`capacity_ranges`] is what separates them; see
/// there for the measurement.
fn regime_ranges(regime: Regime) -> (RangeInclusive<u64>, RangeInclusive<u64>) {
    match regime {
        Regime::Healthy => (50..=200, 400..=1500),
        Regime::Stressed => (50..=80, 60..=200),
    }
}

/// The generated capacity dials per regime, as
/// `(replica_log_max_flush_batch, write_channel_capacity)`.
///
/// # These dials bite only because writes are concurrent
///
/// The timescales in [`regime_ranges`] do not separate the regimes: with an
/// in-memory object store and three in-process nodes a per-op cost is ~1 ms, so
/// a 60 ms election window and a 400 ms one are both comfortably above it. What
/// separates them is backpressure, which is stress that does not depend on
/// wall-clock cost — and a queue only applies backpressure when something fills
/// it. That is why [`Op::WriteBurst`] issues its writes concurrently, and the
/// control measurement is direct: driven one write at a time these dials measure
/// as **inert**, `Healthy` acking 262/263 (99.6%) against `Stressed`'s 239/239
/// (100%) — the stressed stratum acking *everything* — with a whole
/// `Stressed`-floor case logging zero `replica log: evicted subscriber(s)`
/// warnings. A script that awaits each write before issuing the next never puts
/// a second record in any of these queues.
///
/// At the same floor (`stressed_regime_admits_writes`, four bursts of 32 at
/// `max_flush_batch = 1, write_capacity = 2`):
///
/// | | at concurrency 1 | with bursts of 32 |
/// |---|---|---|
/// | acked | 4 / 4 | **24 / 128** |
/// | outcomes dropped | — | **229** |
/// | writer hard deadlines | 0 | **97** |
/// | subscribers shed | 0 | **0** |
///
/// **The path that bites is `replica_log_write_out_capacity`, not the
/// subscriber fan-out.** The flush loop hands each completed write back over a
/// bounded per-connection outcome channel with `try_send`, and drops the
/// outcome when it is full — safe by construction, because the write *is*
/// durable and the writer's reconnect path resends (CLAUDE.md §8). At
/// `write_capacity = 2` a burst of 32 overruns it, the client never observes
/// quorum, and the write fails on its own deadline. That is the designed
/// degradation, and it is why the ack rates below are not a bug: **a failed
/// write here licenses no claim that its data is absent**, and none is made.
///
/// Subscriber shedding stays at zero even so, and always will: reaching it
/// needs a flush batch larger than the subscriber depth, and
/// `ReplicaLogConfig::validate` requires `subscribe >= max_flush_batch`. The
/// fan-out shed is unreachable from *any* valid config, which is why the
/// subscriber depth is derived here rather than generated — a dial that cannot
/// change an outcome is decoration, and reads as though it can.
/// What `Stressed` measures is **undersizing, not load**. `replica::mod`'s
/// `write` documents `write_out_capacity` as needing to be at least the
/// writer's in-flight window — "a Full here is a sizing bug, not a steady
/// state". `Stressed`'s `2..=16` against bursts of up to 32 deliberately
/// violates that, so the shed path it exercises is the one a misconfigured
/// deployment hits, not one ordinary load produces. That is a legitimate thing
/// to test — the config passes `validate()`, the drop is safe by construction,
/// and the invariant battery must hold either way — but it is not evidence that
/// the system sheds acks under load.
fn capacity_ranges(regime: Regime) -> (RangeInclusive<u64>, RangeInclusive<u64>) {
    match regime {
        Regime::Healthy => (32..=256, 256..=1024),
        Regime::Stressed => (1..=8, 2..=16),
    }
}

/// Build a node config from the generated timescales and capacities. Derives the
/// constrained fields rather than generating them independently, so every
/// config satisfies [`ReplicaNodeConfig::validate`] by construction — a config
/// rejected at validation tells us nothing about the system.
///
/// The regime is not a parameter: it selects the *ranges* the arguments are
/// drawn from ([`regime_ranges`], [`capacity_ranges`]) and has no further
/// effect on the config, so taking it here would only invite the reader to look
/// for one.
///
/// # One capacity is deliberately not generated
///
/// `stable_log_subscriber_queue_capacity` looks like a peer of the fields
/// below and is not one: `StableLogConfig::validate` floors it at the
/// stable-*load* batch (a private constant, 256 today), because a shallower
/// queue is overrun by a single load batch regardless of how fast subscribers
/// drain. That floor sits above the whole `Stressed` range, so generating it
/// would produce configs that fail validation rather than configs that apply
/// backpressure. Left at its default.
fn config_for(
    snapshot_ms: u64,
    election_min_ms: u64,
    max_flush_batch: u64,
    write_capacity: u64,
) -> ReplicaNodeConfig {
    let mut cfg = crate::tests::default_fast_config();
    cfg.snapshot_interval = Duration::from_millis(snapshot_ms);
    cfg.raft_election_timeout_min = Duration::from_millis(election_min_ms);
    // Derived, not generated: max must exceed min.
    cfg.raft_election_timeout_max = Duration::from_millis(election_min_ms * 2);
    // Derived: must exceed raft_election_timeout_max, so a write issued during
    // an election can survive it.
    cfg.raft_client_write_timeout = cfg.raft_election_timeout_max * 3;
    // Derived: comfortably inside the election window so a healthy leader is
    // never timed out by its own followers.
    //
    // Be aware this knob has a second job the ratio does not account for.
    // openraft 0.9.24 also spends `heartbeat_interval` as the **hard deadline
    // of every AppendEntries RPC** (`replication/mod.rs`), so `/ 4` sets a
    // network+fsync budget as a side effect of choosing a cadence — 15 ms at
    // the `Stressed` floor of 60, 37 ms at 150. The two roles pull opposite
    // ways: cadence wants to be far below the election window, the deadline
    // wants to be far above a loaded round trip. Combined with openraft's
    // fixed 500 ms replication backoff, a regime whose
    // `election_timeout_max` is under 500 ms cannot absorb a single slow RPC
    // without an election. That is survivable for the generated cases here —
    // 3 in-process nodes, in-memory object store, ~1 ms per op, and no node
    // restarts mid-change — and it is not for
    // [`full_replacement_transfers_membership_and_data`], which passes 600
    // for exactly this reason. See that test's header before tightening
    // either number.
    cfg.raft_heartbeat_interval = Duration::from_millis(election_min_ms / 4);
    // Derived, and it has to be: `default_fast_config` pins the client's hard
    // deadline at a flat 30 s, which is 25x the largest timescale any regime
    // here generates. Left flat, a write whose outcome is dropped under
    // backpressure — the expected path once [`Op::WriteBurst`] outruns
    // `replica_log_write_out_capacity` — parks the burst for 30 s to report
    // something the timescales already implied, and a `Stressed` case spends
    // minutes doing it. Twice `raft_client_write_timeout` keeps the ordering
    // that matters (the client deadline outlives the raft one below it, so a
    // write that raft could still land is not cut off by its caller) while
    // scaling the wait with the regime.
    cfg.write_timeout = cfg.raft_client_write_timeout * 2;
    cfg.snapshot_compaction_tiers = vec![cfg.snapshot_interval.into()];
    // Membership change must complete inside a case. `default_fast_config`'s
    // 500 ms / 300 ms are too slow for that; these are the values
    // `rolling_replacement.rs::fast_rolling_config` already runs a full node
    // rotation on.
    cfg.membership_poll_interval = Duration::from_millis(100);
    cfg.liveness_probe_interval = Duration::from_millis(100);

    let mfb = nz(max_flush_batch);
    cfg.replica_log_max_flush_batch = mfb;
    // Derived, not generated, and pinned to the minimum `validate` allows.
    // `ReplicaLogConfig::validate` requires subscribe >= max_flush_batch,
    // because a smaller value sheds every subscriber on one flush batch — the
    // evicted subscriber then reconnects and re-scans, burning the cores that
    // caused the shed. Equality is therefore the closest a valid config gets to
    // that boundary, and no headroom above it changes any outcome: the shed
    // needs a batch *larger* than the depth, which validation forbids outright.
    let sub = mfb;
    cfg.replica_log_subscribe_channel_capacity = sub;
    // The client-side receive buffer for the same stream: matching it keeps the
    // bottleneck at the replica's fan-out rather than one hop downstream.
    cfg.subscribe_channel_capacity = sub;

    let wc = nz(write_capacity);
    cfg.replica_log_write_channel_capacity = wc;
    cfg.replica_log_write_out_capacity = wc;
    cfg.learner_local_store_integrate_channel_capacity = wc;
    cfg
}

/// Generated capacities are bounded well below `usize::MAX` and never zero.
fn nz(v: u64) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(usize::try_from(v).expect("capacity fits usize"))
        .expect("capacity is non-zero")
}

/// One step of a generated script.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// `n` writes of `len` bytes each, issued concurrently to the node at
    /// `target` (resolved modulo the live node list) and joined. `n = 1` is a
    /// plain single write, and is what this op shrinks toward — so a minimal
    /// counterexample is the simple case.
    ///
    /// Each write's `DataId` is recorded only on success.
    ///
    /// The target is part of the op rather than derived from a seed so it
    /// shrinks with the rest of the script and appears in the failing input.
    ///
    /// # Why a burst, and why against one node
    ///
    /// The capacity dials in [`config_for`] are per-node queue depths, so they
    /// only bite when more than one write is in flight against the same node. A
    /// burst against a single target is what fills them; spreading it across
    /// nodes would divide the pressure by three and reproduce the concurrency-1
    /// case on each. See [`capacity_ranges`] for what that measurably changed.
    ///
    /// This is also the only op that exercises CLAUDE.md §8's write-batch
    /// drain, which folds additional queued items into one append + fsync via
    /// `try_recv`. At concurrency 1 that drain always batches exactly one item.
    WriteBurst { n: usize, len: usize, target: usize },
    /// Issue single writes against node 0 until one is acked, or until
    /// `within_ms` elapses. Records whether it ever got one, how many attempts
    /// it took, and how long — see [`CaseResult::recovery`].
    ///
    /// # Why this is a separate op and not a `WriteBurst { n: 1 }`
    ///
    /// A burst asks "does the cluster ack *right now*", which is a question
    /// about the moment it lands. This asks "does the cluster ack *eventually*",
    /// which is a question about whether it is stressed or broken — and those
    /// are different claims with different answers under load. A burst issued
    /// into an election in flight can legitimately ack nothing (CLAUDE.md §20:
    /// a failed write licenses no claim); a cluster that cannot ack a single
    /// write in a whole minute of retries is wedged.
    ///
    /// It keeps its own accounting and deliberately does **not** touch
    /// [`Tally`]: its retries would inflate `attempted` with attempts made
    /// *after* the stress the tally is measuring, and the ratio printed by the
    /// stressed-floor test would stop meaning what it says. The one write that
    /// does ack is still pushed onto the case's `acked` ground truth, so the
    /// durability chain covers it like any other.
    ///
    /// Never generated: [`script_strategy`] does not emit it, because the
    /// question it asks is about a script's end state rather than about a step
    /// that composes with faults. Hand-written scripts use it.
    WriteUntilAcked { within_ms: u64 },
    /// Take the node at this index down **gracefully** (resolved modulo the
    /// live node list) and **leave it down**. Every op that follows, until the
    /// matching [`Op::Up`], runs against a two-node cluster — still a quorum of
    /// three, so writes must continue to be acked.
    Down(usize),
    /// Take the node at this index down the way a crash does: drop it without
    /// cancelling its shutdown token and without awaiting `run()`, so its
    /// `JoinSet` aborts its tasks mid-flight and whatever was not yet fsynced
    /// stays that way. Opens the same down window as [`Op::Down`] and is closed
    /// by the same [`Op::Up`], so the quorum floor covers both identically.
    ///
    /// The difference from `Down` is what the *relaunch* then has to do: a
    /// graceful stop leaves a clean store, a kill leaves the crash debris
    /// CLAUDE.md §8's recovery rules exist for (a torn WAL tail, a batch with no
    /// commit marker), and the node has to repair it before it can rejoin and
    /// serve. See the module header for why this needs no production surface and
    /// what was measured before it was added.
    Kill(usize),
    /// Bring the downed node back on its original address and db-path, so it
    /// resumes its persisted raft state and its `NodeId` — the same thing an
    /// operator's rolling bounce does. That the `NodeId` is unchanged is
    /// asserted, and is what distinguishes this pair from [`Op::Replace`].
    ///
    /// Closes a window opened by either [`Op::Down`] or [`Op::Kill`]; the driver
    /// does not distinguish, because a node that resumes from crash debris must
    /// come back the same way one that stopped cleanly does.
    ///
    /// Takes no index: [`script_strategy`] guarantees at most one node is down,
    /// so there is only ever one node to bring back.
    Up,
    /// Replace the node at this index with a fresh one on the same address.
    /// The new node has a new `NodeId`, so this drives a real membership
    /// change: joint config, drain, collapse.
    ///
    /// The proptest accumulates `acked` across the whole script and checks the
    /// durability chain at the end, so a `Replace` with writes on both sides of
    /// it *is* the test of CLAUDE.md §8's drain invariant — every write acked
    /// before the change must still be readable after the outgoing node is
    /// gone. No separate assertion is needed.
    Replace(usize),
    /// A bounded pause, letting timer-driven work (snapshots, cutoff advances,
    /// compaction) interleave with the faults.
    Settle(u64),
}

/// Node indices are generated in `0..3` and resolved against the live node
/// list at execution time, so every script is well-formed by construction
/// rather than filtered.
///
/// Snapshots and cutoff advances are deliberately **not** ops: no trigger
/// affordance exists for them, and the short `snapshot_interval` makes them
/// interleave on their own. That they interleave *safely* is the property
/// under test; their exact timing is not.
fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Writes are weighted up: they are what the durability chain is keyed
        // on, and a script of nothing but restarts asserts very little.
        6 => (1usize..=32, 1usize..=256, 0usize..3)
            .prop_map(|(n, len, target)| Op::WriteBurst { n, len, target }),
        // The two window-openers weigh 1 each and `Up` weighs 2, so openers and
        // closers are drawn about equally often. The imbalance a given script
        // draws is what produces the interesting shapes: an `Up` with no open
        // window becomes a `Settle`, and a window left open at the end is closed
        // by an appended `Up` — so a surplus of openers lengthens the degraded
        // window rather than breaking the script. Weighting `Up` at 1 against
        // two openers would instead spend most of the vocabulary on openers
        // rewritten to `Settle`, thinning the fault coverage.
        1 => (0usize..3).prop_map(Op::Down),
        1 => (0usize..3).prop_map(Op::Kill),
        2 => Just(Op::Up),
        // The most expensive op in the vocabulary: a whole node teardown plus a
        // fresh launch plus a two-phase membership change. Weighted down to 1
        // accordingly, and capped at two per script by `script_strategy`.
        1 => (0usize..3).prop_map(Op::Replace),
        3 => (50u64..=300).prop_map(Op::Settle),
    ]
}

/// A script, guaranteed to contain at least one [`Op::WriteBurst`].
///
/// The guarantee is structural rather than a filter, and it is load-bearing
/// for the assertions: every write claim in the proptest body is about `acked`,
/// which only means something if the script tried to write at all. `op_strategy`
/// picks a non-write with probability 0.4, so an all-restart-and-settle script
/// of 8 ops arises about once per 1500 cases — rare enough to survive review and
/// frequent enough to fire spuriously on a nightly 256-case triple run.
/// Appending a write when none was generated removes that case entirely,
/// which is preferable to guarding the claim with `attempted > 0` — a guard
/// would silently skip those cases rather than test them. The appended burst is
/// `n = 1`: the guarantee is "the script writes at all", and adding pressure
/// that the generator did not ask for would make the fallback path behave
/// differently from the generated one.
fn script_strategy() -> impl Strategy<Value = Vec<Op>> {
    (
        proptest::collection::vec(op_strategy(), 8..=20),
        1usize..=256,
        0usize..3,
    )
        .prop_map(|(mut ops, len, target)| {
            if !ops.iter().any(|op| matches!(op, Op::WriteBurst { .. })) {
                ops.push(Op::WriteBurst { n: 1, len, target });
            }
            enforce_script_constraints(ops)
        })
}

/// The two script-level constraints, in one walk. Split out of
/// [`script_strategy`] so [`script_constraints_hold_for_every_short_script`] can
/// check them exhaustively — a violation here does not fail an assertion, it
/// wedges a case below quorum for the full 30 s deadline.
///
/// Both belong here and not in [`run_case`]: `tally.attempted` is incremented
/// before a burst is issued, so any runtime `continue` over a generated op
/// silently breaks the `acked == attempted` claim. Constraints on the script are
/// the strategy's job.
///
/// 1. **The quorum floor.** At most one of three nodes is down at a time. A
///    second window-opener (`Down` *or* `Kill` — the walk does not distinguish
///    them, because a killed node is exactly as absent as a stopped one), or an
///    `Up` with no window open, becomes a `Settle` — a rewrite rather than a
///    filter, so no sample is rejected and shrinking stays clean. A window still
///    open at the end is closed by an appended `Up`.
/// 2. **The replace cap.** A script that replaces every node repeatedly is
///    `rolling_replacement.rs`, which already exists as a named scenario.
///
/// `Replace` interacts with the floor because it *is* a node-down window: it
/// stops the victim before launching its successor, so a `Replace` inside a
/// `Down` window leaves one live node of three and the successor's raft join
/// blocks forever. The window is therefore **closed ahead of it** rather than
/// the `Replace` being dropped — dropping it would silently thin out the
/// membership coverage this module exists for, in proportion to how much of a
/// script is spent degraded.
///
/// The two rewrites are ordered: the cap is applied **first**, so a `Replace`
/// that becomes a `Settle` does not also close an open window. Closing first
/// would spend a degraded window — the thing the `Down`/`Up` split exists to
/// produce — on an op that then never runs.
fn enforce_script_constraints(ops: Vec<Op>) -> Vec<Op> {
    let mut replaces = 0;
    let mut down = false;
    let mut out: Vec<Op> = Vec::with_capacity(ops.len().saturating_add(2));
    for op in ops {
        match op {
            Op::Down(_) | Op::Kill(_) if down => out.push(Op::Settle(50)),
            Op::Down(idx) => {
                down = true;
                out.push(Op::Down(idx));
            }
            Op::Kill(idx) => {
                down = true;
                out.push(Op::Kill(idx));
            }
            Op::Up if !down => out.push(Op::Settle(50)),
            Op::Up => {
                down = false;
                out.push(Op::Up);
            }
            Op::Replace(_) if replaces >= 2 => out.push(Op::Settle(50)),
            Op::Replace(idx) => {
                if down {
                    down = false;
                    out.push(Op::Up);
                }
                replaces += 1;
                out.push(Op::Replace(idx));
            }
            other => out.push(other),
        }
    }
    if down {
        out.push(Op::Up);
    }
    // 3. **The recovery probe**, appended last — after the `Up` above, so it
    //    always runs against a whole cluster rather than a degraded one. This is
    //    the liveness claim every case carries (see the `match (regime,
    //    faulted)` in the proptest body): the script's own bursts answer "did
    //    writes land *while* this was happening", which is a question about how
    //    fast the host is, and this answers "does it admit writes once the
    //    script stops pushing", which is a question about the system. The settle
    //    is not the recovery window — the probe carries its own — it just keeps
    //    the first attempt out of the tail of the last op.
    out.push(Op::Settle(500));
    out.push(Op::WriteUntilAcked {
        within_ms: GENERATED_RECOVERY_BUDGET_MS,
    });
    out
}

/// Recovery budget for a *generated* case.
///
/// Shorter than the 60 s [`stressed_regime_admits_writes`] gives its floor
/// corner, and deliberately: that test sits at the most aggressive dials the
/// generator can produce and is the one expected to be near the edge, while a
/// generated case is somewhere in the range and should not be able to spend a
/// minute proving it is wedged — the proptest runs dozens of them. Only a case
/// that never recovers pays this in full; a healthy one returns on the first
/// attempt in milliseconds.
const GENERATED_RECOVERY_BUDGET_MS: u64 = 30_000;

// ── driver ────────────────────────────────────────────────────────────

/// How many writes a case attempted and how many were acked. Printed per case
/// so a stratum that quietly stops admitting writes is visible rather than
/// silently vacuous.
#[derive(Debug, Default, Clone, Copy)]
struct Tally {
    attempted: usize,
    acked: usize,
    /// The same two counts, restricted to bursts issued while a node was down.
    /// Reported, never asserted: two of three nodes are a quorum, so a
    /// degraded-but-quorate cluster is *expected* to keep acking — but a burst
    /// issued into the re-election that a leader's `Down` triggers may still
    /// fail, and a failed write licenses no claim (CLAUDE.md §20). The
    /// assertion that carries the degraded-liveness claim is the recovery
    /// probe every script ends with ([`Op::WriteUntilAcked`]); these two make
    /// it visible how much of the ack count was earned while degraded.
    attempted_while_down: usize,
    acked_while_down: usize,
    /// How many `Op::Replace`s actually executed. Printed for the same reason
    /// as the write counts: a `Replace` that never fires is silent vacuity,
    /// and the drain invariant this module exists to cover is only under test
    /// when this is non-zero.
    replaced: usize,
    /// How many of the executed down-windows were opened by [`Op::Kill`] rather
    /// than [`Op::Down`]. Printed for the same vacuity reason: the crash-recovery
    /// composition this op exists for is only under test when this is non-zero,
    /// and it shares every other counter with the graceful stop, so nothing else
    /// would reveal a run in which no kill was drawn.
    killed: usize,
}

fn spawn_observer(cluster: &Cluster) -> CutoffObserver {
    CutoffObserver::spawn_for(cluster)
}

/// Wait for any in-flight membership change to collapse before taking a node
/// down. **A precondition of the fault, not a constraint on the script**, which
/// is why it is here and not in [`script_strategy`]: no op is skipped, no
/// generated value is discarded, and the tally is untouched.
///
/// The reason is a real deadlock, found by this proptest on its first run and
/// reproduced deterministically before being fixed here. While a config is
/// joint, `old` is immutable by design (CLAUDE.md §8 — it is what makes the
/// drain bound sound), and the leader's step 2 needs a generation quorum in
/// *both* `old` and `next`. Taking a second member of `old` down leaves it at
/// 1-of-3: the change can never progress, the joint config can never collapse,
/// and a node started into it blocks forever in `quorum_applied_generation`
/// (raft startup is unbounded by design — `run_phase` escalates but never
/// aborts). The observed script was `Replace(1)` then `Replace(0)` two writes
/// later, ending in `old = {A, B, leader}` with both `A` and `B` gone.
///
/// So it is an operator-contract violation, not a system bug: the system's
/// response — block, lose nothing — is correct, and "a cluster with no quorum
/// in `old` is unavailable" is not an invariant question. That is the same
/// judgement [`script_strategy`]'s quorum floor makes. It applies to
/// [`Op::Down`] as well as [`Op::Replace`]: taking down the one remaining live
/// `old` member is equally fatal while joint, so both fence on this.
///
/// # `downed` is why this takes an argument
///
/// The predicate is "the agreed non-joint set is exactly the *membership*",
/// and `Cluster::node_ids` is the set of *running* nodes, which is one short
/// while a node is down. A node being down changes nothing about the replica
/// set — with three members, one dead and no spare, `compute_target` cannot
/// reach the target size and declines to initiate — so the membership to
/// compare against is the live nodes plus the downed one. Passing
/// `node_ids()` alone would spin for the full deadline and then panic.
///
/// What it deliberately does *not* do is wait after the change is *started*.
/// Writes still race the joint config — the concurrency this module exists to
/// exercise — because only the next *fault* waits, never the next write.
///
/// # Why not `Cluster::wait_for_convergence`
///
/// Because it does not answer this question. It asks only "does every node
/// agree on some non-joint set", which is *already true* in the window between
/// `Cluster::replace` returning and the leader's membership job even starting:
/// the committed set is still the old one, nobody is joint, everyone agrees —
/// and the second `Replace` fires straight into it. That is fragility note 4 on
/// `rolling_replacement.rs`, and it is why that test pairs the same call with a
/// blind two-second sleep. The predicate below is the precise one instead: the
/// agreed non-joint set must be *exactly the set of live nodes*, which is false
/// until the outgoing node is out and the incoming one is in. No sleep needed.
async fn settle_membership(cluster: &Cluster, downed: Option<NodeId>) {
    let mut want = cluster.node_ids();
    want.extend(downed);
    crate::tests::wait_until_sync("membership settled", Duration::from_secs(30), || {
        cluster.nodes().all(|n| {
            let s = n.state_handle().snapshot();
            !s.replica.is_joint() && *s.replica.current() == want
        })
    })
    .await;
}

/// Outcome of running one generated script. Returned rather than asserted
/// inside so the proptest body can use `prop_assert!` (which shrinks) instead
/// of a bare panic.
struct CaseResult {
    tally: Tally,
    /// Whether any node reached a state where `assert_unstable_above_watermark`
    /// had something to check: a promoted watermark *and* a non-empty
    /// `unstable_tail`. Reported so a permanently vacuous boundary check is
    /// visible.
    boundary_exercised: bool,
    /// Whether the script actually executed a `Down` or a `Replace`. Scopes the
    /// liveness claim — see the note on it in the proptest body.
    faulted: bool,
    /// How many `Down` … `Up` windows had at least one op inside them.
    ///
    /// Reported as the vacuity guard on the [`Op::Down`]/[`Op::Up`] split: a
    /// window with nothing inside it degrades the cluster only for as long as a
    /// relaunch takes, which is the immediate bounce the split exists to
    /// improve on. Zero across a whole run means it bought nothing.
    degraded_windows: usize,
    /// How many observer polls across the case saw a non-`Single` replica
    /// membership.
    ///
    /// Reported rather than asserted, and it is a vacuity guard on the *script*,
    /// not evidence about the invariant battery. These are samples taken
    /// *during* a script; the battery runs only after `wait_for_convergence`,
    /// whose predicate (`Cluster::all_converged`) is false unless every node is
    /// non-joint. So a non-zero count here says nothing about whether
    /// `assert_membership_well_formed` or `assert_no_satisfied_drain_left_armed`
    /// — both no-ops on a `Single` membership — ever had anything to check, and
    /// a zero count would say nothing either. What it does say is whether a
    /// membership change was ever in flight while writes and faults were still
    /// running: zero means the concurrency this module exists for never
    /// happened.
    joint_polls: usize,
    /// Outcome of the script's [`Op::WriteUntilAcked`], if it ran one.
    recovery: Option<Recovery>,
}

/// What an [`Op::WriteUntilAcked`] observed.
#[derive(Debug, Clone, Copy)]
struct Recovery {
    /// Whether a write was ever acked before the deadline. This is the whole
    /// point: `false` means the cluster could not admit a single write in the
    /// entire window, which is "broken" rather than "stressed".
    admitted: bool,
    /// How many single writes it took. `1` means the cluster was already
    /// healthy; a large number means it recovered, and says how hard.
    attempts: usize,
    /// How long the probe ran before it succeeded or gave up.
    elapsed: Duration,
}

/// Runs one generated script and returns what happened. Takes the dials rather
/// than the `Regime` they came from: the regime selects the ranges and scopes
/// the *assertions*, both of which live in the caller, so the driver itself is
/// regime-agnostic.
async fn run_case(
    snapshot_ms: u64,
    election_min_ms: u64,
    max_flush_batch: u64,
    write_capacity: u64,
    ops: Vec<Op>,
) -> CaseResult {
    let cluster_res = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(config_for(
            snapshot_ms,
            election_min_ms,
            max_flush_batch,
            write_capacity,
        )),
    })
    .await;
    let mut cluster = cluster_res.expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    // One observer for the whole case. A `RaftStateHandle` belongs to one
    // `ReplicaNode` instance, so every fault that mints a new instance calls
    // `refresh` to swap the handle set; the recorded history is keyed by
    // `NodeId` and carries across, which is what lets a regression *straddling*
    // a fault be seen at all — a node resuming from persisted state is where a
    // backwards cutoff would matter most.
    let observer = spawn_observer(&cluster);

    let mut acked: Vec<(DataId, Bytes)> = Vec::new();
    let mut tally = Tally::default();
    let mut faulted = false;
    // The node the script has taken down, if any, with the placement needed to
    // put it back on the same address and db-path. `script_strategy` guarantees
    // at most one at a time, which is why this is an `Option` and not a set —
    // and the `expect`s below turn a generator that broke that guarantee into a
    // clear failure rather than a cluster that wedges below quorum.
    let mut downed: Option<(NodeId, NodePlacement)> = None;
    // How many ops the current `Down` window has carried, and how many windows
    // carried at least one. See `CaseResult::degraded_windows`.
    let mut window_ops = 0usize;
    let mut degraded_windows = 0usize;
    let mut recovery: Option<Recovery> = None;

    for op in ops {
        if downed.is_some() && !matches!(op, Op::Up) {
            window_ops += 1;
        }
        match op {
            Op::WriteBurst { n, len, target } => {
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                // Incremented before anything is issued, so nothing below this
                // line may `continue` or return early — see the note on
                // `script_strategy`. The burst is issued unconditionally.
                tally.attempted += n;
                if downed.is_some() {
                    tally.attempted_while_down += n;
                }
                let target = ids[target % ids.len()];
                let writer = cluster.node(target).writer();
                // `Arc<dyn Writer>` is `Send + Sync`, so every future in the
                // burst can hold its own handle and they can all be in flight
                // at once. `join_all` polls all of them from this task, so all
                // `n` reach their submit point before the first one completes —
                // which is the whole point: that is what puts more than one
                // record in the per-node queues.
                let burst = futures::future::join_all((0..n).map(|i| {
                    let writer = Arc::clone(&writer);
                    // Distinct payloads. `DataId` is minted from 64 random bits
                    // plus the creation HLC, so uniqueness is already
                    // guaranteed by construction (CLAUDE.md §8) — varying the
                    // first byte makes a dumped blob traceable to its position
                    // in the burst, and gives the durability chain's payload
                    // comparison something that actually differs per write.
                    // Nothing here reuses a `DataId`.
                    let mut bytes = vec![0xABu8; len];
                    bytes[0] = (i % 256) as u8;
                    // Two handles to the same bytes: `write` consumes one, the
                    // other is recorded as the ground truth the chain verifies
                    // against. The `async move` must not reach back into an
                    // outer collection — that would move it into the first
                    // future and fail to compile on the second.
                    let payload = Bytes::from(bytes);
                    let recorded = payload.clone();
                    async move { writer.write(payload).await.map(|id| (id, recorded)) }
                }));
                // Bounded so a wedged cluster cannot hang the case. The bound
                // covers the *whole joined burst*, not each write: they are
                // submitted together, so their individual deadlines run
                // concurrently and the burst costs about one `write_timeout`.
                // 30 s sits above the largest `write_timeout` `config_for`
                // derives (18 s), so this is a backstop against a wedge, not
                // the thing that normally ends a stalled write — that is the
                // writer's own deadline, which returns a per-write `Err`
                // instead of discarding the burst's other results.
                let res = tokio::time::timeout(Duration::from_secs(30), burst).await;
                // An outer timeout drops the whole burst; an inner `Err` is one
                // failed write. Both mean "quorum not observed", and neither
                // licenses any claim about absence — so only `Ok`s are counted
                // and nothing is asserted about the rest.
                if let Ok(results) = res {
                    for (data_id, payload) in results.into_iter().flatten() {
                        tally.acked += 1;
                        if downed.is_some() {
                            tally.acked_while_down += 1;
                        }
                        acked.push((data_id, payload));
                    }
                }
            }
            Op::Down(idx) | Op::Kill(idx) => {
                // The two openers differ only in *how* the node stops; every
                // piece of bookkeeping below is deliberately shared, so the
                // quorum floor, the observation window and the `Up` that closes
                // the window cannot drift between them.
                let hard = matches!(op, Op::Kill(_));
                assert!(
                    downed.is_none(),
                    "script_strategy must never emit a second Down/Kill while one is open — \
                     two of three down is a cluster without a quorum, which asserts nothing"
                );
                // A fault must not start while a membership change is in
                // flight; see `settle_membership`. Nothing is down yet, so the
                // membership is exactly the live set.
                settle_membership(&cluster, None).await;
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                let victim = ids[idx % ids.len()];
                faulted = true;
                if hard {
                    tally.killed += 1;
                }
                let placement = if hard {
                    cluster.kill(victim).await
                } else {
                    cluster.remove(victim).await.expect("remove node")
                };
                downed = Some((victim, placement));
                window_ops = 0;
                observer.refresh(&cluster);
            }
            Op::Up => {
                let (victim, placement) = downed
                    .take()
                    .expect("script_strategy must never emit an Up with no Down open");
                if window_ops > 0 {
                    degraded_windows += 1;
                }
                // The downed node is still a replica-set member — with three
                // members, one dead and no spare, `compute_target` cannot reach
                // the target size, so nothing was initiated on its account — so
                // it belongs in the expected membership.
                settle_membership(&cluster, Some(victim)).await;
                let relaunched = cluster.relaunch(placement).await.expect("relaunch node");
                assert_eq!(
                    relaunched, victim,
                    "relaunch on the same address and db-path must resume the same NodeId — \
                     otherwise this is a Replace and the persisted raft state was not reused"
                );
                observer.refresh(&cluster);
            }
            Op::Replace(idx) => {
                assert!(
                    downed.is_none(),
                    "script_strategy must close a Down window before a Replace — a Replace \
                     while a node is down leaves one live node of three and the successor's \
                     raft join blocks forever"
                );
                settle_membership(&cluster, None).await;
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                let victim = ids[idx % ids.len()];
                faulted = true;
                tally.replaced += 1;
                let fresh = cluster.replace(victim).await.expect("replace node");
                assert_ne!(
                    fresh, victim,
                    "replace must mint a new NodeId — otherwise no membership change occurs \
                     and this op silently degenerates into a restart"
                );
                observer.refresh(&cluster);
            }
            Op::WriteUntilAcked { within_ms } => {
                let started = tokio::time::Instant::now();
                let deadline = started
                    .checked_add(Duration::from_millis(within_ms))
                    .expect("Instant + within_ms does not overflow");
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                let mut attempts = 0usize;
                let mut admitted = false;
                loop {
                    attempts = attempts.saturating_add(1);
                    // Re-resolved per attempt, and through the leader when
                    // there is one: `Writer::write` fans out over the router
                    // and is never gated on the coordinating node's own
                    // membership (CLAUDE.md §12), so any node can serve it —
                    // but a fixed node picked once can be the one the script
                    // just replaced, and re-reading who leads keeps the retries
                    // from all going to the same unlucky place.
                    let target = cluster
                        .nodes()
                        .find_map(|n| n.state_handle().snapshot().leader)
                        .filter(|id| ids.contains(id))
                        .unwrap_or(ids[0]);
                    let writer = cluster.node(target).writer();
                    // A distinct payload per attempt, so a retry never looks
                    // like a resend of the one before it.
                    let mut bytes = vec![0xCDu8; 64];
                    bytes[0] = (attempts % 256) as u8;
                    let payload = Bytes::from(bytes);
                    let recorded = payload.clone();
                    if let Ok(data_id) = writer.write(payload).await {
                        acked.push((data_id, recorded));
                        admitted = true;
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    // The write's own `write_timeout` already paced the
                    // attempt; this is just enough of a gap for an election in
                    // flight to finish rather than to be interrupted again.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                recovery = Some(Recovery {
                    admitted,
                    attempts,
                    elapsed: started.elapsed(),
                });
            }
            Op::Settle(ms) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
        }
    }

    // ── quiesce ───────────────────────────────────────────────────────
    // Every fault is dropped *before* anything is waited on, so none survives
    // into the assertions: nothing below this line runs against a two-node
    // cluster. `script_strategy` closes any open window with an appended `Up`,
    // so for a generated script this is already a no-op — it is reachable from
    // the hand-written scripts (`stressed_regime_admits_writes`), which is
    // exactly the case where forgetting it would silently assert the battery
    // against a degraded cluster.
    if let Some((victim, placement)) = downed.take() {
        if window_ops > 0 {
            degraded_windows += 1;
        }
        let relaunched = cluster.relaunch(placement).await.expect("relaunch node");
        assert_eq!(
            relaunched, victim,
            "relaunch on the same address and db-path must resume the same NodeId"
        );
        observer.refresh(&cluster);
    }
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;
    // `wait_for_convergence` alone is not quiescence: it asks only whether
    // everyone agrees on some non-joint set, which is already true in the window
    // between a `Replace` returning and the leader's membership job starting
    // (see `settle_membership`). A script ending in `Replace` could clear the
    // fence before its change began, and the battery would then assert over a
    // cluster whose last op contributed nothing. The stronger predicate — the
    // agreed non-joint set is *exactly* the live nodes — is the one that waits
    // for the change to finish. Nothing is down here, so the live set is the
    // membership.
    settle_membership(&cluster, None).await;

    // ── assert ────────────────────────────────────────────────────────
    let boundary_exercised = assert_cluster_invariants_with_acks(&cluster, &acked).await;
    observer.assert_monotonic();
    let joint_polls = observer.joint_polls();
    observer.stop().await;

    cluster.shutdown().await.expect("clean shutdown");

    CaseResult {
        tally,
        boundary_exercised,
        faulted,
        degraded_windows,
        joint_polls,
        recovery,
    }
}

// ── tests ─────────────────────────────────────────────────────────────

/// Every config the generator can produce must pass `validate()`. Checked at
/// every corner of both regimes' ranges — the two timescales and the three
/// capacity dials — so a validation failure surfaces here as a clear unit
/// failure rather than as a mysterious cluster-start error inside a generated
/// case.
///
/// This is also the guard on the one cross-field constraint `config_for`
/// satisfies by derivation rather than by range choice
/// (`subscribe >= max_flush_batch`): the derivation pins them equal, so any
/// loosening of it shows up here.
#[test]
fn generated_configs_validate_at_every_regime_corner() {
    for regime in [Regime::Healthy, Regime::Stressed] {
        let (snap, elec) = regime_ranges(regime);
        let (mfb_r, wc_r) = capacity_ranges(regime);
        for &snapshot_ms in &[*snap.start(), *snap.end()] {
            for &election_min_ms in &[*elec.start(), *elec.end()] {
                for &mfb in &[*mfb_r.start(), *mfb_r.end()] {
                    for &wc in &[*wc_r.start(), *wc_r.end()] {
                        let mut cfg = config_for(snapshot_ms, election_min_ms, mfb, wc);
                        // `validate` requires a non-empty peer list that
                        // includes this node's own server_name, plus a
                        // db_path; the cluster harness fills both in. Supply
                        // stand-ins so this test exercises the *tunings*,
                        // which is what the generator varies. The stand-in
                        // peer's server_name must match `cfg.network_server_name`
                        // (set by `default_fast_config` to "placeholder") or
                        // this trips the "own config omits self" check.
                        cfg.peers = vec![crate::ids::PeerAddr {
                            addr: "127.0.0.1:1".into(),
                            server_name: cfg.network_server_name.clone(),
                        }];
                        cfg.db_path = std::path::PathBuf::from("/tmp/placeholder.db");
                        cfg.validate().unwrap_or_else(|e| {
                            panic!(
                                "{regime:?} corner (snapshot {snapshot_ms}ms, election_min \
                                 {election_min_ms}ms, max_flush_batch {mfb}, \
                                 write_capacity {wc}) fails validation: {e}"
                            )
                        });
                    }
                }
            }
        }
    }
}

/// Every script the generator can emit must satisfy the two constraints
/// [`enforce_script_constraints`] exists to impose: at most one node down at
/// any point, no `Replace` inside a down window, and no window left open at the
/// end.
///
/// Checked exhaustively over every sequence of the five *shape-relevant* ops up
/// to length 5 — `WriteBurst` and `Settle` are folded into one `Settle`
/// representative because neither reads or writes the walk's state, so they
/// cannot affect its outcome. 5^0 + … + 5^5 = 3906 scripts, instant to run.
/// `Kill` is enumerated alongside `Down` rather than folded into it: they are
/// two arms of the walk and folding them would stop checking that the second
/// one maintains the floor.
///
/// It earns its place because a violation is not a failed assertion but a
/// *hang*, and where the case blocks decides whether it ever comes back. Two of
/// three down has no quorum. If it blocks in a `wait_until_sync` it panics after
/// 30 s, somewhere unrelated to the generator that caused it. If it blocks
/// inside a node's raft startup — which is what an `Up` or a `Replace` into a
/// quorum-less config does — it never comes back at all: raft startup is
/// unbounded by design, and `raft/startup/bootstrap.rs::run_phase` escalates its
/// logging the longer it waits but never aborts, so `raft_startup_phase_timeout`
/// sets an escalation cadence and not a deadline. That is CLAUDE.md §17's hazard
/// exactly — a hung test needs manual intervention — so cheap deterministic
/// coverage of the walk is worth far more than the seed that eventually
/// reproduces one.
#[test]
fn script_constraints_hold_for_every_short_script() {
    let alphabet = [
        Op::Down(0),
        Op::Kill(0),
        Op::Up,
        Op::Replace(0),
        Op::Settle(50),
    ];
    let mut scripts: Vec<Vec<Op>> = vec![Vec::new()];
    // Extend only the previous round's frontier. Re-extending everything
    // accumulated so far would re-check each shorter script once per remaining
    // round — the same 1365 distinct scripts, evaluated 3125 times.
    let mut frontier: Vec<Vec<Op>> = vec![Vec::new()];
    for _ in 0..5 {
        let mut next = Vec::new();
        for script in &frontier {
            for op in alphabet {
                let mut extended = script.clone();
                extended.push(op);
                next.push(extended);
            }
        }
        scripts.extend(next.iter().cloned());
        frontier = next;
    }
    assert_eq!(scripts.len(), 3906, "every script of length 0..=5");

    for script in scripts {
        let out = enforce_script_constraints(script.clone());
        let mut down = false;
        let mut replaces = 0;
        for op in &out {
            match op {
                Op::Down(_) | Op::Kill(_) => {
                    assert!(!down, "two nodes down at once in {out:?} (from {script:?})");
                    down = true;
                }
                Op::Up => {
                    assert!(down, "Up with no node down in {out:?} (from {script:?})");
                    down = false;
                }
                Op::Replace(_) => {
                    assert!(
                        !down,
                        "Replace inside a down window in {out:?} (from {script:?})"
                    );
                    replaces += 1;
                }
                Op::WriteBurst { .. } | Op::Settle(_) | Op::WriteUntilAcked { .. } => {}
            }
        }
        assert!(
            !down,
            "script ends with a node down: {out:?} (from {script:?})"
        );
        assert!(replaces <= 2, "replace cap exceeded in {out:?}");
    }
}

/// The stressed regime must be *stressed*, not simply broken.
///
/// "Everything errored" is a correct outcome under `Stressed` but a vacuous
/// one, and a stratum that always errors everything burns CI to assert
/// nothing. This runs one fixed `Stressed` script at the most aggressive corner
/// of *both* range sets — the timescale floor and the capacity floor, the
/// latter being where the stress now lives — and then requires the cluster to
/// **recover**: after the bursts, a patient probe must get a single write
/// admitted.
///
/// # Why recovery, and not "the bursts acked something"
///
/// This asserted `tally.acked > 0` over the bursts themselves until it was
/// found to fail on a slower machine with `attempted=128 acked=0` and a raft
/// term that had walked past 30. Both halves of that are worth separating:
///
/// - **Acking nothing under maximal burst pressure is a legitimate `Stressed`
///   outcome, not a defect.** Four concurrent bursts of 32 against a two-slot
///   outcome channel, at a timescale floor whose `write_timeout` derives to
///   720 ms, is a regime where whether *any* write beats its deadline depends
///   on how fast the host is. Asserting on it measures the host.
/// - **Never acking again is a defect**, and that is the property the
///   assertion was reaching for all along: the difference between stressed and
///   broken. [`Op::WriteUntilAcked`] tests it directly and does not care how
///   fast the host is — a fast host recovers in one attempt, a slow one in
///   twenty, and both pass; only a wedged cluster fails.
///
/// So the stress is unchanged — same bursts, same floors, both ranges are
/// still confirmed by this test rather than widened by it — and the claim on
/// top of it got stronger rather than weaker. The burst tally is still printed,
/// because the thin margin is informative even when it is not a gate: on the
/// reference host four bursts of 32 ack about 25 of 128, while the same floor
/// driven one write at a time acks 4 of 4 and sits nowhere near the edge.
///
/// The failures are outcome-channel drops, and a dropped outcome is a durable
/// write whose ack was shed (CLAUDE.md §8): nothing here treats that as an
/// error, and nothing asserts the data is absent.
///
/// # Known fragility at this floor
///
/// `election_min = 60` derives a **15 ms** AppendEntries deadline
/// (`config_for` sets `heartbeat_interval = election_min / 4`, which openraft
/// 0.9.24 spends as the RPC hard deadline) against openraft's **fixed 500 ms**
/// replication backoff. A host that cannot serve a raft round trip inside 15 ms
/// therefore pays a full election for every slow RPC, which is the term churn
/// seen above. That is why the recovery probe is given a whole minute: it must
/// outlast an election storm, not merely a slow write. See
/// [`full_replacement_transfers_membership_and_data`] for the same coupling
/// where it was not survivable.
#[tokio::test(flavor = "multi_thread")]
async fn stressed_regime_admits_writes() {
    crate::tests::init_logging();
    let (snap, elec) = regime_ranges(Regime::Stressed);
    let (mfb_r, wc_r) = capacity_ranges(Regime::Stressed);
    // Bursts at the top of the generated `n` range, not single writes: the
    // capacity floor is only a floor if something is pushing against it, and a
    // burst of 32 against a two-slot write channel is that. This is also the
    // script the shed-warning count in `capacity_ranges` was measured on.
    let script = vec![
        Op::Settle(200),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        // Then stop pushing and ask whether it comes back. The settle is not
        // the recovery window — the probe below carries its own — it just means
        // the first attempt is not issued into the tail of the last burst.
        Op::Settle(500),
        Op::WriteUntilAcked { within_ms: 60_000 },
    ];
    // Boxed at the call site (CLAUDE.md §2): `run_case` owns a whole cluster's
    // worth of locals, and this is a one-off heavy await.
    let result = Box::pin(run_case(
        *snap.start(),
        *elec.start(),
        *mfb_r.start(),
        *wc_r.start(),
        script,
    ))
    .await;
    let recovery = result.recovery.expect("the script ran a WriteUntilAcked");
    println!(
        "[stressed floor] attempted={} acked={} boundary_exercised={} \
         recovery={{admitted={} attempts={} elapsed={:?}}}",
        result.tally.attempted,
        result.tally.acked,
        result.boundary_exercised,
        recovery.admitted,
        recovery.attempts,
        recovery.elapsed,
    );
    assert!(
        recovery.admitted,
        "the Stressed regime never recovered at its floor \
         (snapshot {}ms, election_min {}ms, max_flush_batch {}, write_capacity {}): \
         after the bursts, {} single writes over {:?} were all refused. The bursts \
         acking nothing is fair at this floor; never acking again is not — this is \
         a wedged cluster, not a stressed one. Check the raft term in the log: a \
         term that keeps climbing means the election storm never ended, and the \
         floor is below what this host can serve (see the header).",
        snap.start(),
        elec.start(),
        mfb_r.start(),
        wc_r.start(),
        recovery.attempts,
        recovery.elapsed,
    );
}

/// Can a hard-killed node be relaunched on its own db-path?
///
/// Two things must hold and neither may be assumed. `spawn_blocking` tasks
/// cannot be aborted — they run to completion — and the engine's blocking
/// loops hold RocksDB's LOCK file, so a relaunch may fail with the DB still
/// open. The listener must also release its port.
#[tokio::test(flavor = "multi_thread")]
async fn hard_killed_node_can_be_relaunched() {
    crate::tests::init_logging();
    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(config_for(100, 400, 32, 256)),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    let leader = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(crate::replica_node::ReplicaNode::node_id)
        .expect("no leader");
    let pre_kill = Bytes::from_static(b"pre-kill");
    let acked = cluster
        .node(leader)
        .writer()
        .write(pre_kill.clone())
        .await
        .expect("write");

    let victim = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a non-leader node");
    let killed_at = std::time::Instant::now();
    // `kill` returns once the aborted listener has given the port back, so this
    // elapsed time *is* the port-free spike.
    let placement = cluster.kill(victim).await;
    println!("[kill spike] port freed after {:?}", killed_at.elapsed());

    let relaunch_at = std::time::Instant::now();
    let relaunched = cluster.relaunch(placement).await.unwrap_or_else(|e| {
        panic!(
            "relaunch after a hard kill failed after {:?} — a failure here is the finding: \
             the engine still holds the db-path, so a crash is not reachable at this tier: {e}",
            relaunch_at.elapsed()
        )
    });
    println!(
        "[kill spike] relaunch took {:?} (kill to relaunched: {:?})",
        relaunch_at.elapsed(),
        killed_at.elapsed()
    );
    assert_eq!(relaunched, victim, "relaunch must resume the same NodeId");

    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;
    let _ = assert_cluster_invariants_with_acks(&cluster, &[(acked, pre_kill)]).await;
    cluster.shutdown().await.expect("clean shutdown");
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Low by default so CI cost stays bounded; the nightly job raises it
        // via PROPTEST_CASES.
        cases: 8,
        // Each case starts a real three-node cluster over real TCP. Shrinking
        // by re-running dozens of variants would take longer than the run
        // itself; the failing seed plus the printed script is the diagnostic.
        max_shrink_iters: 4,
        .. ProptestConfig::default()
    })]

    #[test]
    fn cluster_upholds_invariants_under_generated_faults(
        regime in prop_oneof![Just(Regime::Healthy), Just(Regime::Stressed)],
        snapshot_pick in 0u64..=1000,
        election_pick in 0u64..=1000,
        flush_batch_pick in 0u64..=1000,
        write_capacity_pick in 0u64..=1000,
        ops in script_strategy(),
    ) {
        let (snap_r, elec_r) = regime_ranges(regime);
        let snapshot_ms = pick(&snap_r, snapshot_pick);
        let election_min_ms = pick(&elec_r, election_pick);
        let (mfb_r, wc_r) = capacity_ranges(regime);
        let max_flush_batch = pick(&mfb_r, flush_batch_pick);
        let write_capacity = pick(&wc_r, write_capacity_pick);

        // A fresh runtime per case: a case restarts nodes and drops a whole
        // cluster, and sharing a runtime across cases would let one case's
        // stragglers land in the next one's.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt.block_on(run_case(
            snapshot_ms,
            election_min_ms,
            max_flush_batch,
            write_capacity,
            ops,
        ));

        println!(
            "[case] regime={regime:?} snapshot={snapshot_ms}ms election_min={election_min_ms}ms \
             flush_batch={max_flush_batch} write_cap={write_capacity} \
             attempted={} acked={} down_attempted={} down_acked={} replaced={} killed={} \
             faulted={} degraded_windows={} boundary_exercised={} joint_polls={} \
             recovery_attempts={}",
            result.tally.attempted, result.tally.acked, result.tally.attempted_while_down,
            result.tally.acked_while_down, result.tally.replaced, result.tally.killed,
            result.faulted, result.degraded_windows, result.boundary_exercised, result.joint_polls,
            result.recovery.map_or(0, |r| r.attempts)
        );

        // The write claim, three ways. What is asserted is always `acked` —
        // never `attempted`, which is incremented before the write is issued
        // and so is a property of the generated script, not of the cluster.
        // `script_strategy` guarantees `attempted >= 1`, so each branch below
        // is a real claim about the system rather than about the generator.
        match (regime, result.faulted) {
            // Generous timescales and no fault: every write must land. This is
            // the only place liveness is claimed in full, and it holds under
            // concurrency — 506 of 506 acked across the fault-free `Healthy`
            // cases of a 64-case run, with bursts up to 32. That is only **4 of
            // 38** `Healthy` cases, because the branch is scoped by `faulted`
            // and most scripts fault, so it is thinly sampled and the figure
            // reads narrower than its denominator suggests. The reason it holds
            // is structural rather than lucky: `Healthy`'s `write_capacity`
            // floor is 256, 8x the largest burst the generator can produce, so
            // the outcome channel that costs `Stressed` its writes never fills
            // here.
            (Regime::Healthy, false) => prop_assert_eq!(
                result.tally.acked, result.tally.attempted,
                "fault-free Healthy script lost a write: {:?}", result.tally
            ),
            // A `Down`, a `Kill` or a `Replace` bounces the leader and costs the
            // writes issued inside the re-election (or membership-change) window
            // — but not all of them: **the cluster is a quorum throughout**, since a
            // script may only take one of three down, so a sustained down
            // window is degraded, not unavailable. Zero acks here would mean a
            // fault permanently wedged a quorate cluster, which is a real
            // failure and must be investigated rather than assertion-adjusted.
            // A `Down`, a `Kill` or a `Replace` bounces the leader and costs the
            // writes issued inside the re-election (or membership-change)
            // window. What must NOT happen is that the fault costs every write
            // from then on — the cluster is a quorum throughout, since a script
            // may only take one of three down, so a sustained down window is
            // degraded, not unavailable.
            //
            // That claim is carried by the recovery probe below rather than by
            // a whole-script `acked > 0`, which was what stood here. The two
            // differ on slow hosts and only one of them is about the system:
            // `acked > 0` asks whether some burst happened to land *while* the
            // fault was in flight, which a host slow enough to put every burst
            // inside the re-election window fails without anything being wrong.
            // The recovery probe asks whether writes are admitted once the
            // script stops pushing, which is the actual difference between
            // "degraded" and "wedged".
            (Regime::Healthy, true) => {}
            // Stressed makes no write claim: timescales are near operation cost
            // and the queues are at the shed boundary by design, so "everything
            // errored" is a correct outcome. It is now measurably the stratum
            // that loses writes — 83.3% acked, 24 of 29 cases lossy. A loss here
            // means only that quorum was not observed: most are durable writes
            // whose ack was shed, but a write can also expire on the client
            // deadline while still in flight, or fail its append. Nothing may
            // therefore assert that an unacked Stressed write is *absent* —
            // which is exactly why no claim is made here (CLAUDE.md §20).
            // `stressed_regime_admits_writes` still pins the floor corner
            // specifically, which a generated case reaches only by chance.
            (Regime::Stressed, _) => {}
        }

        // The claim every case carries, in every regime and with or without a
        // fault: once the script stops pushing, the cluster admits a write
        // again.
        //
        // This is what `Stressed` asserted nothing in place of. Raft-level
        // liveness was already covered — `run_case`'s quiesce panics if no
        // leader is elected or the replica set does not converge — but a
        // cluster that holds an election and converges while never acking
        // another write satisfied both and every write branch above, so the
        // whole stratum passed on it. It is also the honest version of the
        // `(Healthy, faulted)` branch, for the reason given there.
        let recovery = result
            .recovery
            .expect("enforce_script_constraints appends a WriteUntilAcked to every script");
        prop_assert!(
            recovery.admitted,
            "the cluster never admitted another write after the script: {} attempts over {:?} \
             were all refused ({regime:?}, faulted={}). The script's own writes failing is a \
             fair outcome under these dials; never accepting one again is not — that is a \
             wedged cluster, and the tally {:?} is context, not the failure.",
            recovery.attempts,
            recovery.elapsed,
            result.faulted,
            result.tally,
        );
    }
}

/// Map a generated 0..=1000 dial onto an inclusive range. Keeps the strategy
/// independent of the regime (the range is chosen after the regime is known)
/// without needing a dependent strategy.
fn pick(range: &RangeInclusive<u64>, dial: u64) -> u64 {
    let (lo, hi) = (*range.start(), *range.end());
    let span = hi - lo;
    lo + (span * dial.min(1000)) / 1000
}

/// Every op in the vocabulary, in one deterministic `Healthy` script.
///
/// The generator reaches every op probabilistically; nothing pinned a single
/// script that uses all of them together, so no fixed case covered the
/// *interplay* — a write burst inside a graceful-down window, then inside a
/// hard-kill window, then a membership change on top. This is that case.
///
/// It also asserts every op actually executed. A script whose ops silently
/// no-op asserts nothing, and the battery would still pass: `tally.killed`,
/// `tally.replaced` and `degraded_windows` are the vacuity guards, and each is
/// checked to fail if the corresponding op is removed.
///
/// # Why `boundary_exercised` is reported and not asserted
///
/// It measures `false` here, and that is correct rather than a gap. `Healthy`
/// snapshots at 50 ms (`regime_ranges`), so by the time the script's trailing
/// `Settle` and `run_case`'s quiesce are done, every write has been promoted
/// and `unstable_tail` is empty — the boundary check has nothing left to
/// compare. That it is genuinely "all promoted" and not "no watermark" follows
/// from the durability chain in the same battery passing for all 20 acked
/// entries on every node, which requires a watermark that covers them.
///
/// Asserting it would therefore be asserting that snapshots are *slow*. The
/// flag exists so a vacuous check is reported rather than mistaken for a
/// passing one, and this is the reporting.
#[tokio::test(flavor = "multi_thread")]
async fn full_vocabulary_smoke_script_holds_the_battery() {
    crate::tests::init_logging();
    // Healthy dials; the script itself drives the faults.
    let (snap, elec) = regime_ranges(Regime::Healthy);
    let (mfb_r, wc_r) = capacity_ranges(Regime::Healthy);
    // Valid by construction against `run_case`'s own assertions: at most one
    // node down at a time, every Down/Kill window closed by an Up, and no
    // Replace inside a window.
    let script = vec![
        Op::Settle(100),
        Op::WriteBurst {
            n: 8,
            len: 64,
            target: 0,
        },
        Op::Down(1),
        Op::WriteBurst {
            n: 4,
            len: 32,
            target: 2,
        },
        Op::Up,
        Op::Kill(2),
        Op::WriteBurst {
            n: 4,
            len: 32,
            target: 0,
        },
        Op::Up,
        Op::Replace(1),
        Op::WriteBurst {
            n: 8,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
    ];
    // Boxed at the call site (CLAUDE.md §2): `run_case` owns a whole cluster's
    // worth of locals, and this is a one-off heavy await.
    let result = Box::pin(run_case(
        *snap.start(),
        *elec.start(),
        *mfb_r.start(),
        *wc_r.start(),
        script,
    ))
    .await;
    println!(
        "[full vocabulary] attempted={} acked={} killed={} replaced={} \
         degraded_windows={} joint_polls={} boundary_exercised={}",
        result.tally.attempted,
        result.tally.acked,
        result.tally.killed,
        result.tally.replaced,
        result.degraded_windows,
        result.joint_polls,
        result.boundary_exercised,
    );

    // The battery and the observer already ran inside `run_case`. What is left
    // is vacuity: prove the script did what it says.
    assert!(
        result.tally.acked > 0,
        "smoke script acked nothing: {:?}",
        result.tally
    );
    assert!(
        result.tally.killed >= 1,
        "smoke script never executed a Kill: {:?}",
        result.tally
    );
    assert!(
        result.tally.replaced >= 1,
        "smoke script never executed a Replace: {:?}",
        result.tally
    );
    assert!(
        result.degraded_windows >= 1,
        "no Down/Kill window carried ops: {}",
        result.degraded_windows
    );
}

/// The full replacement: a 7-node cluster whose 3 replicated members are all
/// killed at once, driving `Joint { old: R, next: N }` with `R ∩ N = ∅`.
///
/// This is the shape `docs/TESTING.md` recorded as unreachable at this tier,
/// and the reason the drain bound was untestable: `compute_target` seeds from
/// `current ∩ live`, so `next` always retains the live members of `old` and
/// every quorum of `old` intersects `next` — under which the bound is never
/// load-bearing and every armed exit measures `Exit::AnyTime`.
///
/// Seven nodes break the deadlock that made it unreachable before. Killing all
/// 3 replicas forces `old ∩ live = ∅`, so `next` is disjoint; the 4 surviving
/// voters are still a quorum of 7, so Raft keeps committing; and the killed
/// nodes are relaunched (never evicted, so they resume as voters) in time to
/// satisfy step 2's generation quorum of `old`, which is what previously could
/// never be met once `old` was wholly dead.
///
/// Step 2b then samples a *real* `WatermarkAbove` — the relaunched nodes' logs
/// still hold W, the write acked against `R` alone — and step 3 must wait for
/// a snapshot to cover it before collapsing.
///
/// Liveness, not just safety: every stage carries an explicit deadline, so a
/// stalled membership change is a failure that names its stage rather than a
/// hang.
///
/// The 30 s snapshot interval is load-bearing. W must stay above the manifest
/// watermark for the whole dance; a snapshot covering it early would satisfy
/// the bound trivially and make the teeth check meaningless. Do not "speed up"
/// this test by shortening it.
///
/// # The election window is a floor, not a speed dial
///
/// `election_min_ms` here is 600, not the 150 the other `config_for` callers
/// use, and both of openraft 0.9.24's undocumented couplings to
/// `heartbeat_interval` are why. `config_for` derives that interval as
/// `election_min / 4`, and openraft spends the same number twice:
///
/// - it is the **hard deadline of every AppendEntries RPC**
///   (`replication/mod.rs`: `let the_timeout =
///   Duration::from_millis(self.config.heartbeat_interval)`), not just the
///   heartbeat cadence `config_for`'s comment reasons about. At
///   `election_min = 150` that deadline is **37 ms** — for a real-TCP round
///   trip whose far end fsyncs the entries it is appending.
/// - on an RPC error openraft backs replication off by a **fixed 500 ms**
///   (`network/network.rs`: `Backoff::new(repeat(Duration::from_millis(500)))`),
///   which no config value can influence.
///
/// Together those make any regime with `election_timeout_max < 500 ms` a trap
/// rather than merely a tight budget: one slow RPC parks replication for longer
/// than a follower's whole election window, so the follower campaigns, and the
/// cluster churns terms instead of recovering. At `election_min = 150` the max
/// is 300 ms and that is guaranteed.
///
/// Stage 3 is exactly where it bites. It relaunches the three killed nodes —
/// each opening RocksDB and replaying its WAL — and those same nodes are the
/// `old` quorum step 2b must sample, so the RPC deadline is tightest precisely
/// when its targets are slowest. The observed failure is a leader that never
/// arms the drain bound while the log fills with `timeout after 37ms when
/// AppendEntries` and a vote walking T1 → T5.
///
/// 600 ms buys a 150 ms RPC deadline and a 1200 ms election window that clears
/// the 500 ms backoff. Nothing here measures election speed — the only timing
/// assertion is the `drain_wait >= 5 s` floor below, which a slower election
/// can only lengthen — so this costs the test a few seconds of failover and
/// nothing else.
#[tokio::test(flavor = "multi_thread")]
async fn full_replacement_transfers_membership_and_data() {
    crate::tests::init_logging();
    let mut base = config_for(30_000, 600, 32, 256);
    // Deterministic spare choice, so `next` is not a coin flip across runs.
    base.membership_spare_selection = crate::raft::SpareSelectionStrategy::LowestLatency;
    let mut cluster = Cluster::new(ClusterConfig {
        count: 7,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    // Discover the replicated set R and the voter set from applied state —
    // join order decides which three form R.
    let snap = cluster
        .nodes()
        .next()
        .expect("node")
        .state_handle()
        .snapshot();
    let r: BTreeSet<NodeId> = snap.replica.current().clone();
    let voters: BTreeSet<NodeId> = snap.voters.clone();
    assert_eq!(r.len(), 3, "replica_node_count = 3");
    assert_eq!(voters.len(), 7, "all seven are voters: {voters:?}");
    assert!(r.is_subset(&voters), "replicas are voters");

    // One write, acked against R, and above the watermark for the whole dance.
    let leader_id = cluster
        .nodes()
        .find_map(|n| n.state_handle().snapshot().leader)
        .expect("leader");
    let w_payload = Bytes::from_static(b"replacement-write");
    let w_id = cluster
        .node(leader_id)
        .writer()
        .write(w_payload.clone())
        .await
        .expect("write W");

    let observer = spawn_observer(&cluster);

    // ── Stage 1: kill all three replicated members ───────────────────────
    // Sequential: each `kill` borrows `&mut cluster`.
    let mut placements: Vec<NodePlacement> = Vec::new();
    for id in &r {
        placements.push(cluster.kill(*id).await);
    }
    observer.refresh(&cluster);
    let t_killed = std::time::Instant::now();

    // ── Stage 2: a joint config with a DISJOINT next must be committed ────
    // 4 of 7 voters survive, so the raft layer keeps working.
    let mut next: Option<BTreeSet<NodeId>> = None;
    crate::tests::wait_until_sync("joint proposed", Duration::from_secs(20), || {
        cluster.nodes().any(|n| {
            let s = n.state_handle().snapshot();
            match s.replica.next() {
                Some(nx) if nx.len() == 3 && nx.is_disjoint(&r) => {
                    next = Some(nx.clone());
                    true
                }
                _ => false,
            }
        })
    })
    .await;
    let n = next.expect("joint config with disjoint next");
    let t_joint = std::time::Instant::now();

    // ── Stage 3: relaunch the old nodes ──────────────────────────────────
    // Step 2's generation quorum needs a quorum of `old` alive and applied,
    // and step 2b needs their logs to sample a real bound from. They were
    // never evicted from the voters, so they resume as voters.
    // All bound before any is awaited for readiness, and with the full peer
    // list — see `relaunch_many`. A loop of `relaunch` (start *and* wait, per
    // node) deadlocks even after the start/readiness split: each node's gate
    // waits for a quorum of R, which is exactly the set being relaunched.
    let relaunched = cluster
        .relaunch_many(placements)
        .await
        .expect("relaunch old nodes");
    assert_eq!(
        relaunched.iter().copied().collect::<BTreeSet<NodeId>>(),
        r,
        "relaunch must resume the same NodeIds (all of R)"
    );
    observer.refresh(&cluster);

    // Step 2b is done when the leader arms the bound. It must be a REAL
    // bound: `AnyTime` would mean every responder reported an empty log, so
    // nothing could be stranded and the gate would be doing no work.
    let mut armed: Option<crate::meta::Exit> = None;
    crate::tests::wait_until_sync("drain bound armed", Duration::from_secs(30), || {
        cluster
            .nodes()
            .any(|node| match node.state_handle().snapshot().replica.exit() {
                Some(e) => {
                    armed = Some(e);
                    true
                }
                None => false,
            })
    })
    .await;
    let t_armed = std::time::Instant::now();
    assert!(
        matches!(armed, Some(crate::meta::Exit::WatermarkAbove(_))),
        "the drain bound must be a real watermark, not AnyTime — an AnyTime arm \
         means the old logs were empty and the gate did no work: {armed:?}"
    );

    // ── Stage 4: drain — the watermark must pass the sampled bound ────────
    crate::tests::wait_until_sync("collapse", Duration::from_secs(60), || {
        cluster.nodes().all(|node| {
            let s = node.state_handle().snapshot();
            !s.replica.is_joint() && *s.replica.current() == n
        })
    })
    .await;
    let t_collapsed = std::time::Instant::now();

    let drain_wait = t_collapsed.saturating_duration_since(t_armed);
    println!(
        "[replacement] armed={armed:?} kill->joint {:?}, joint->armed {:?}, armed->collapse {:?}",
        t_joint.saturating_duration_since(t_killed),
        t_armed.saturating_duration_since(t_joint),
        drain_wait,
    );

    // **This is the assertion that detects a relaxed drain gate**, and it is
    // deliberately about the wait rather than about lost data.
    //
    // Measured both ways: with the gate intact the collapse waits ~28 s for a
    // snapshot to cover W (bounded by the 30 s snapshot interval); with both
    // gates disabled — the write-point validator and the leader's step 3 — it
    // collapses in ~89 ms. 5 s separates those cleanly.
    //
    // The durability chain below does NOT detect the relaxation, and that is a
    // finding rather than an oversight: with the gates off the chain still
    // passes, because the outgoing nodes are relaunched and stay live, so the
    // incoming set catches up W straight from their replica logs — the joint
    // window it had to do that in was short, but the subscription replays from
    // the start of the log, and W predates the kills. Exhibiting actual loss
    // needs the outgoing set to become unreachable before the incoming set has
    // caught up, which is a different (and racy) scenario. See docs/TESTING.md.
    assert!(
        drain_wait >= Duration::from_secs(5),
        "the collapse did not wait for the drain — the joint config went from armed \
         ({armed:?}) to collapsed in {drain_wait:?}. The bound is armed but nothing is \
         honouring it: check the write-point validator (`validate_transition`'s \
         JointExitTooEarly arm) and the leader's step 3 drain wait."
    );

    // ── Assert: battery + durability chain + observer ────────────────────
    // The battery covers all 7 nodes; they all share the raft state. The
    // durability chain is scoped to N — the claim is "W reached every node of
    // the new replicated set". A voter-only node outside the replicated set
    // has no local-store subscription to the replica logs (the subscriber
    // manager subscribes to `current ∪ next` only), so it is not a stable-log
    // carrier for W and asserting against it would assert the wrong thing.
    assert_cluster_invariants(&cluster);
    assert_durability_chain_on(&cluster, &[(w_id, w_payload)], Duration::from_secs(30), &n).await;
    observer.assert_monotonic();
    observer.stop().await;

    cluster.shutdown().await.expect("clean shutdown");
}
