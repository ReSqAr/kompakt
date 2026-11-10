//! The timescale regimes a generated case runs under, and the configs they produce.

use std::ops::RangeInclusive;
use std::time::Duration;
use zslg::ReplicaNodeConfig;

/// Which regime a case runs in — a stratum over both the timescales
/// ([`regime_ranges`]) and the channel capacities ([`capacity_ranges`]).
///
/// Generated explicitly and stratified: proptest samples the enum uniformly,
/// so both regimes appear across a run by construction. Sampling from one wide
/// range instead would concentrate cases in the middle and reach the
/// interesting extremes rarely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Regime {
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
pub(crate) fn regime_ranges(regime: Regime) -> (RangeInclusive<u64>, RangeInclusive<u64>) {
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
pub(crate) fn capacity_ranges(regime: Regime) -> (RangeInclusive<u64>, RangeInclusive<u64>) {
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
pub(crate) fn config_for(
    snapshot_ms: u64,
    election_min_ms: u64,
    max_flush_batch: u64,
    write_capacity: u64,
) -> ReplicaNodeConfig {
    let mut cfg = crate::default_fast_config();
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
pub(crate) fn nz(v: u64) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(usize::try_from(v).expect("capacity fits usize"))
        .expect("capacity is non-zero")
}

/// Map a generated 0..=1000 dial onto an inclusive range. Keeps the strategy
/// independent of the regime (the range is chosen after the regime is known)
/// without needing a dependent strategy.
pub(crate) fn pick(range: &RangeInclusive<u64>, dial: u64) -> u64 {
    let (lo, hi) = (*range.start(), *range.end());
    let span = hi - lo;
    lo + (span * dial.min(1000)) / 1000
}
