//! Canonical metric registry (design doc §"Observability").
//!
//! Every metric the crate emits is declared here as a [`MetricDef`] in
//! [`ALL_METRICS`] — name, kind, unit, and help text. Emission sites refer to
//! the name constants and the label-value enums below; nothing constructs a
//! metric name or label value ad hoc. [`describe_all`] registers the help
//! text and units with the installed recorder (call it once after
//! `observability::init_prometheus`).
//!
//! Conventions:
//! - every metric this crate owns is exported under the `zslg_` namespace,
//!   applied by the registry macro so a row never writes it. The one exemption
//!   is the Prometheus-standard `process_*` collector namespace
//!   (`metrics!(process, unprefixed; ..)`), which every client library exports
//!   under exactly these names — prefixing them would hide this process from
//!   tooling that expects the standard. `tokio_*` comes from `tokio-metrics`
//!   and is not ours to name at all.
//! - counters end in `_total`; histograms end in a unit/quantity suffix
//!   (`_seconds`, `_bytes`, or `_entries` for count distributions); gauges carry
//!   a unit suffix where one applies (`_bytes`, `_ms`, `_seconds`).
//! - durations are recorded as `tokio::time::Duration` /
//!   `std::time::Duration` via `metrics`' native `IntoF64` impl — no float
//!   arithmetic at call sites.
//! - HLC-valued gauges export the **physical-millisecond part**
//!   (`Hlc::parts().physical_ms`, ≤ 2^48), never `Hlc::raw()`: the raw
//!   `u64` exceeds f64's 53-bit integer range and would silently lose
//!   precision in the gauge.
//! - label keys are the `LABEL_*` constants; label values come from the
//!   enums below via `as_str()`, so every `{label="value"}` combination that
//!   can occur is enumerable at compile time.

// `expect` calls here are `u64::try_from(usize)` on bounded values (counts,
// lengths) that cannot exceed u64::MAX.
#![allow(
    clippy::expect_used,
    reason = "u64::try_from(usize) on bounded metric values (counts, lengths)"
)]

use metrics::Unit;
use std::time::Duration;

use crate::hlc::Hlc;

// ---------------------------------------------------------------------------
// Registry types
// ---------------------------------------------------------------------------

// Metrics using manual Instant are now either Pattern 5 (snapshot multi-lap
// timing, `leader/snapshot/create.rs`) or Pattern 6 (stored-Instant fields for
// queue-wait measurement). Everything else uses Probe or RpcServerGuard.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// One canonical metric: everything a recorder or a dashboard needs to know.
#[derive(Debug, Clone, Copy)]
struct MetricDef {
    name: &'static str,
    kind: MetricKind,
    unit: Option<Unit>,
    help: &'static str,
}

// ---------------------------------------------------------------------------
// Registry macros
// ---------------------------------------------------------------------------

/// `Seconds` → `Some(Unit::Seconds)`, `None` → `None`. Unit slot for a row.
macro_rules! unit {
    (None) => {
        None
    };
    ($u:ident) => {
        Some(Unit::$u)
    };
}

/// The namespace every metric this crate owns is exported under.
///
/// Test-only: the live value is the literal in [`metrics`], and this exists so
/// `tests::every_metric_is_namespaced` asserts against a name rather than
/// re-deriving one. The two are kept in step by that test.
#[cfg(test)]
pub(crate) const METRIC_PREFIX: &str = "zslg_";

/// Declare a subsystem group of metrics as rows (see the file's group blocks).
/// Each row declares one metric: emission fn name, const name, metric string,
/// (kind+unit per shape), help prose, and signature. The block expands each
/// row to its `pub(crate) const` and (except `custom`) its `#[doc = help]`
/// emission fn, and defines a zero-arg macro named `<group>` that expands to a
/// `[MetricDef; N]` array for `ALL_METRICS` (assembled as `&[ &group!(), .. ]`,
/// a `&[&[MetricDef]]` — see the assembly step).
macro_rules! metrics {
    ($group:ident; { $($row:tt)* }) => {
        metrics_impl!($group, "zslg_", [], $($row)*);
    };

    // Opt out of the namespace for names that are a shared convention rather
    // than ours — the Prometheus-standard `process_*` collector namespace,
    // which every client library exports under exactly these names. Prefixing
    // them would hide this process from tooling that expects the standard.
    ($group:ident, unprefixed; { $($row:tt)* }) => {
        metrics_impl!($group, "", [], $($row)*);
    };
}

// Accumulated `MetricDef` fragments are captured as `tt` (not `expr`) and each
// is wrapped in parentheses so it is a single token tree; the base case emits
// the group's defs as a bracketed array literal.

macro_rules! metrics_impl {
    // base case: emit the group's `[MetricDef; N]` array as a zero-arg macro
    ($group:ident, $prefix:literal, [$($defs:tt),*] $(,)?) => {
        macro_rules! $group { () => { [ $($defs),* ] } }
    };

    // gauge:   set_gauge(CONST, v)                    arg u64,  unit required
    ($group:ident, $prefix:literal, [$($defs:tt),*], gauge $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: u64); $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn($arg: u64) { set_gauge($const, $arg); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Gauge, unit: unit!($unit), help: $help })], $($rest)*);
    };

    // flag:    set_flag(CONST, on)                    arg bool, no unit
    ($group:ident, $prefix:literal, [$($defs:tt),*], flag $fn:ident $const:ident $name:literal $help:literal ($arg:ident: bool); $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn($arg: bool) { set_flag($const, $arg); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Gauge, unit: None, help: $help })], $($rest)*);
    };

    // counter: counter!(CONST).increment(1)           no args, no unit
    ($group:ident, $prefix:literal, [$($defs:tt),*], counter $fn:ident $const:ident $name:literal $help:literal; $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn() { metrics::counter!($const).increment(1); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Counter, unit: None, help: $help })], $($rest)*);
    };

    // labeled_counter: counter!(CONST, <labels>).increment(1)
    ($group:ident, $prefix:literal, [$($defs:tt),*], labeled_counter $fn:ident $const:ident $name:literal $help:literal ($arg:ident: $ty:ty) [$($label:tt)*]; $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn($arg: $ty) { metrics::counter!($const, $($label)*).increment(1); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Counter, unit: None, help: $help })], $($rest)*);
    };

    // hlc:     set_hlc_gauge(CONST, h)                arg Hlc, unit required
    ($group:ident, $prefix:literal, [$($defs:tt),*], hlc $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: Hlc); $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn($arg: Hlc) { set_hlc_gauge($const, $arg); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Gauge, unit: unit!($unit), help: $help })], $($rest)*);
    };

    // gauge_usize: set_gauge_usize(CONST, v)          arg usize, unit required
    ($group:ident, $prefix:literal, [$($defs:tt),*], gauge_usize $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: usize); $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn($arg: usize) { set_gauge_usize($const, $arg); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Gauge, unit: unit!($unit), help: $help })], $($rest)*);
    };

    // hist:    record_usize_hist(CONST, n)            arg usize, unit required
    ($group:ident, $prefix:literal, [$($defs:tt),*], hist $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: usize); $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn($arg: usize) { record_usize_hist($const, $arg); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Histogram, unit: unit!($unit), help: $help })], $($rest)*);
    };

    // hist_duration: histogram!(CONST).record(d)      arg Duration, unit Seconds implicit
    ($group:ident, $prefix:literal, [$($defs:tt),*], hist_duration $fn:ident $const:ident $name:literal $help:literal ($arg:ident: Duration); $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn($arg: Duration) { metrics::histogram!($const).record($arg); }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Histogram, unit: Some(Unit::Seconds), help: $help })], $($rest)*);
    };

    // probe:   Probe::start(CONST, OpState::Failure)  no args, unit Seconds implicit
    ($group:ident, $prefix:literal, [$($defs:tt),*], probe $fn:ident $const:ident $name:literal $help:literal; $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn() -> Probe<OpState> { Probe::start($const, OpState::Failure) }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Histogram, unit: Some(Unit::Seconds), help: $help })], $($rest)*);
    };

    // probe_unlabeled: Probe::unlabeled(CONST)        no args, unit Seconds implicit
    ($group:ident, $prefix:literal, [$($defs:tt),*], probe_unlabeled $fn:ident $const:ident $name:literal $help:literal; $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        pub(crate) fn $fn() -> Probe<NoOutcome> { Probe::unlabeled($const) }
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::Histogram, unit: Some(Unit::Seconds), help: $help })], $($rest)*);
    };

    // custom: const + MetricDef only; the emission fn is hand-written below
    // the block and references $const. $fn is documentation-only.
    ($group:ident, $prefix:literal, [$($defs:tt),*], custom $fn:ident $const:ident $name:literal $kind:ident $unit:tt $help:literal; $($rest:tt)*) => {
        pub(crate) const $const: &str = concat!($prefix, $name);
        metrics_impl!($group, $prefix, [$($defs,)* (MetricDef { name: $const, kind: MetricKind::$kind, unit: unit!($unit), help: $help })], $($rest)*);
    };
}

/// Fieldless label-vocabulary enum with a `const fn as_str`. `$name` takes the
/// standard `Debug, Clone, Copy, PartialEq, Eq` derive; extra attributes pass
/// through before the name. Variant `///` doc comments pass through to the
/// enum variants. Enum-level docs must be given as `#[doc = "..."]` INSIDE the
/// invocation (a `///` before the invocation would warn `unused_doc_comments`
/// under `-D warnings`).
macro_rules! label_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $value:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            #[must_use]
            pub(crate) const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $value ),+
                }
            }
        }
    };
}

/// Like [`label_enum!`] plus `impl ProbeOutcome for $name`.
macro_rules! probe_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $value:literal),+ $(,)? }) => {
        label_enum!($(#[$meta])* $name { $($(#[$vmeta])* $variant => $value),+ });

        impl ProbeOutcome for $name {
            fn as_str(self) -> &'static str {
                $name::as_str(self)
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Label keys
// ---------------------------------------------------------------------------

const LABEL_STATE: &str = "state";
const LABEL_LOG_TYPE: &str = "log_type";
const LABEL_STEP: &str = "step";
const LABEL_OP: &str = "op";
const LABEL_METHOD: &str = "method";
const LABEL_SOURCE: &str = "source";
const LABEL_REASON: &str = "reason";
const LABEL_KIND: &str = "kind";
const LABEL_DIRECTION: &str = "direction";
const LABEL_OUTCOME: &str = "outcome";
const LABEL_PEER_ID: &str = "peer_id";
/// Which side of the replication path shed a record (see [`ShedSite`]).
const LABEL_SITE: &str = "site";
/// Peer identity (`NodeId::to_string()`). High-cardinality but bounded by cluster
/// size; the enum-valued-label rule targets bounded vocabularies, not node ids.
const LABEL_NODE_ID: &str = "node_id";
/// Which leader background job a `leader_job_*` metric describes (see
/// [`LeaderJob`]). Deliberately **not** `job`: Prometheus attaches its own
/// `job=<job_name>` label at scrape time and, without `honor_labels: true`,
/// renames any exposed `job` to `exported_job` — which would silently break
/// every per-job breakdown.
const LABEL_JOB: &str = "leader_job";
/// Which RocksDB column family a per-CF storage gauge describes. Value comes
/// from `Subsystem::cf_name()` (CLAUDE.md §7: label values from an enum,
/// never ad-hoc strings) — a node has one DB, so `cf` is the axis that
/// distinguishes one memtable/SST/compaction reading from another.
const LABEL_CF: &str = "cf";

/// Every label key this crate emits. A new `LABEL_*` constant belongs here, so
/// that `no_label_key_collides_with_prometheus_scrape_labels` keeps checking
/// the whole set rather than whichever subset was current when it was written.
#[cfg(test)]
const ALL_LABEL_KEYS: &[&str] = &[
    LABEL_STATE,
    LABEL_LOG_TYPE,
    LABEL_STEP,
    LABEL_OP,
    LABEL_METHOD,
    LABEL_SOURCE,
    LABEL_REASON,
    LABEL_KIND,
    LABEL_DIRECTION,
    LABEL_OUTCOME,
    LABEL_PEER_ID,
    LABEL_SITE,
    LABEL_NODE_ID,
    LABEL_JOB,
    LABEL_CF,
    LABEL_NONE,
];

// ---------------------------------------------------------------------------
// Metrics: each name constant is immediately followed by its emission
// helper(s). Helpers are `pub(crate)` on purpose — an unwired metric is then
// an unused function, which `clippy -D warnings` rejects (dead_code).
// ---------------------------------------------------------------------------

// ── raft / consensus ────────────────────────────────────────────────────────

metrics!(raft; {
    flag    raft_is_member   RAFT_IS_MEMBER   "raft_is_member"
        "1 if this node is a voting member of the raft config, else 0. \
         Cluster bootstrap alert: sum() staying 0." (is_member: bool);
    flag    raft_is_leader   RAFT_IS_LEADER   "raft_is_leader"
        "1 while this node is the raft leader. sum() != 1 for a sustained \
         period means no (or a split) leader." (is_leader: bool);
    flag    raft_has_leader  RAFT_HAS_LEADER  "raft_has_leader"
        "1 if this node currently observes an elected leader." (has_leader: bool);
    gauge   raft_term        RAFT_TERM        "raft_term" None
        "Current raft term as observed by this node." (term: u64);
    gauge   raft_applied_log_index RAFT_APPLIED_LOG_INDEX "raft_applied_log_index" None
        "Last applied raft log index. Spread across nodes = apply lag." (index: u64);
    counter raft_leader_change RAFT_LEADER_CHANGES_TOTAL "raft_leader_changes_total"
        "Leader changes observed by this node. Sustained rate = election churn.";
    flag    raft_in_joint_config RAFT_IN_JOINT_CONFIG "raft_in_joint_config"
        "1 while the applied membership is a joint config. Stuck at 1 = \
         membership change wedged (doc: leader must resume from joint)."
        (in_joint: bool);
    gauge   raft_config_generation RAFT_CONFIG_GENERATION "raft_config_generation" None
        "RaftGeneration of the applied state. Spread across nodes = \
         state-replication lag." (generation: u64);
    counter raft_fenced_write  RAFT_FENCED_WRITES_TOTAL "raft_fenced_writes_total"
        "Fenced state writes rejected by the apply-time generation CAS \
         (StaleStateWrite). Expected around leadership churn; sustained rate \
         without churn = bug.";
    counter raft_client_write_timeout RAFT_CLIENT_WRITE_TIMEOUTS_TOTAL
        "raft_client_write_timeouts_total" "Fenced state writes abandoned because \
         openraft's client_write did not resolve within raft_client_write_timeout. \
         openraft has no deadline of its own, so this is the backstop against a \
         partitioned leader stalling the integrator — and with it every leader \
         job. Any non-zero rate warrants investigation.";
    gauge   replica_nodes_count REPLICA_NODES_COUNT "replica_nodes_count" Count
        "Replica nodes in the applied config (both sets counted once under a \
         joint config)." (count: u64);
    labeled_counter membership_change MEMBERSHIP_CHANGES_TOTAL "membership_changes_total"
        "Completed membership-change attempts by outcome."
        (state: OpState) [LABEL_STATE => state.as_str()];
    counter membership_retarget MEMBERSHIP_RETARGETS_TOTAL "membership_retargets_total"
        "Joint membership changes that recomputed their incoming set because a \
         member of it stopped being live. Each retarget disarms the drain bound \
         and discards the step-2 generation quorum, so a sustained rate means \
         the change is restarting, not progressing.";

    custom  leader_job_ran  LEADER_JOB_LAST_RUN_TIMESTAMP_SECONDS
        "leader_job_last_run_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds when each leader job last completed an iteration. \
         Grafana shows staleness as time() - this; a job that is wedged stops \
         advancing it. Only meaningful on the leader.";
    custom  leader_job_ran  LEADER_JOB_RUNS_TOTAL "leader_job_runs_total" Counter None
        "Completed iterations per leader job.";
    custom  leader_job_ran  LEADER_JOB_DURATION_SECONDS "leader_job_duration_seconds"
        Histogram Seconds "How long each leader-job iteration took. The snapshot \
        and compaction jobs are the long ones; they run concurrently, so a long \
        tail here does not imply other jobs are blocked.";
    custom  leader_intent   LEADER_INTENTS_TOTAL "leader_intents_total" Counter None
        "Raft-state intents resolved by the leader integrator, by outcome \
         (applied/already_satisfied/superseded/failed).";
    custom  leader_intent_batch LEADER_INTENT_BATCHES_TOTAL "leader_intent_batches_total"
        Counter None "Raft proposals issued by the leader integrator. Divide \
        leader_intents_total{state=\"applied\"} by this for the coalescing factor.";
    custom  membership_joint_exit_hlc_physical_ms MEMBERSHIP_JOINT_EXIT_HLC_PHYSICAL_MS
        "membership_joint_exit_hlc_physical_ms" Gauge Milliseconds
        "Physical-millisecond part of the armed joint-config drain bound (0 = \
         not draining). A joint config may not collapse until \
         manifest_end_hlc_physical_ms reaches this.";
    custom  node_rejoin   NODE_REJOINS_TOTAL  "node_rejoins_total"  Counter None
        "Nodes with no local state that joined an existing cluster. Covers \
         initial cluster formation, scale-out, and an operator wiping a node \
         and rejoining it under a new NodeId after data loss — \
         indistinguishable from inside the process. A LIFECYCLE counter, not an \
         alarm: expect n-1 on a healthy n-node startup. Alert on unexpected \
         CHANGES, not on a non-zero value.";
    custom  node_last_seen NODE_LAST_SEEN_TIMESTAMP_SECONDS "node_last_seen_timestamp_seconds"
        Gauge Seconds "Unix-epoch seconds of the last successful liveness probe \
        reply from each peer (label node_id), per observing node. Cluster \
        liveness is `time() - node_last_seen_timestamp_seconds`.";
    custom  node_live     NODE_LIVE  "node_live"  Gauge None
        "1 while a peer (label node_id) is in the observing node's live set, 0 \
         once probe failures drop it or it is evicted.";
});

// ── raft / consensus: hand-written emission helpers ────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

/// One leader job's iteration finished: stamp the wall clock, count the run, and
/// record how long it took.
///
/// The stamp is the liveness signal for the leader's background work. Each job is
/// a single serial task, so "it has not run recently" is the only way a wedged
/// job shows up — there is no queue to inspect. Grafana derives staleness as
/// `time() - leader_job_last_run_timestamp_seconds`, which keeps climbing even if
/// the job (and therefore the emitter) is stuck, unlike a duration histogram that
/// simply stops receiving samples.
pub(crate) fn leader_job_ran(job: LeaderJob, elapsed: std::time::Duration) {
    metrics::gauge!(LEADER_JOB_LAST_RUN_TIMESTAMP_SECONDS, LABEL_JOB => job.as_str())
        .set(lossy_f64(unix_now_secs()));
    metrics::counter!(LEADER_JOB_RUNS_TOTAL, LABEL_JOB => job.as_str()).increment(1);
    metrics::histogram!(LEADER_JOB_DURATION_SECONDS, LABEL_JOB => job.as_str())
        .record(elapsed.as_secs_f64());
}

/// One intent resolved by the integrator, by outcome.
pub(crate) fn leader_intent(outcome: IntentState) {
    metrics::counter!(LEADER_INTENTS_TOTAL, LABEL_STATE => outcome.as_str()).increment(1);
}

/// One Raft proposal issued by the integrator, carrying one or more intents.
///
/// The coalescing signal is the ratio:
/// `rate(leader_intents_total{state="applied"}) / rate(leader_intent_batches_total)`
/// — above 1 means several jobs' writes rode one Raft entry instead of
/// contending for the generation fence. Sustained 1 is fine at low load and
/// worth a look under it. Counters rather than a histogram because the registry
/// requires histograms to carry a real unit, and "intents" is a count.
pub(crate) fn leader_intent_batch() {
    metrics::counter!(LEADER_INTENT_BATCHES_TOTAL).increment(1);
}

label_enum!(#[doc = "`state` label of [`LEADER_INTENTS_TOTAL`]."] IntentState {
    /// Folded into a proposal that committed.
    Applied => "applied",
    /// Already true in the committed state; nothing proposed.
    AlreadySatisfied => "already_satisfied",
    /// The premise no longer held, so the submitter must recompute.
    Superseded => "superseded",
    /// The proposal itself failed (Raft error).
    Failed => "failed",
    /// The proposal lost the generation fence. The integrator is the only
    /// writer of replicated state and it writes serially, so within a leader
    /// epoch this cannot happen: a non-zero rate outside an election means
    /// either a deposed leader is still submitting or something else is
    /// writing `EntryPayload::Normal`.
    StaleFence => "stale_fence",
});

/// The armed joint-config drain bound, or 0 when not draining. Compare against
/// `manifest_end_hlc_physical_ms`: while this is non-zero and above the
/// watermark, the membership change is correctly blocked waiting for the
/// outgoing replica set to be snapshotted (designdoc §"Membership Change" step
/// 3). A bound that stays above the watermark for many snapshot intervals means
/// snapshot creation is stalled, not that membership is stuck.
pub(crate) fn membership_joint_exit_hlc_physical_ms(exit: Option<crate::meta::Exit>) {
    // `AnyTime` reports 0, same as "not draining": both mean the collapse is
    // not waiting on a snapshot, which is what the gauge is read for.
    let exit_hlc = match exit {
        Some(crate::meta::Exit::WatermarkAbove(h)) => Some(h),
        Some(crate::meta::Exit::AnyTime) | None => None,
    };
    match exit_hlc {
        Some(hlc) => set_hlc_gauge(MEMBERSHIP_JOINT_EXIT_HLC_PHYSICAL_MS, hlc),
        None => set_gauge(MEMBERSHIP_JOINT_EXIT_HLC_PHYSICAL_MS, 0),
    }
}

/// A node with **no local state** joined an existing cluster. Three causes,
/// indistinguishable from inside the process (all are "empty state, cluster
/// already exists"): initial cluster formation, scale-out, and an operator
/// wiping a node and rejoining it under a new `NodeId` after data loss.
///
/// So this is a **lifecycle counter, not an alarm** — it reads non-zero on
/// every healthy multi-node startup (n-1 on formation of an n-node cluster).
/// Alert on it *changing when you did not deploy anything*, never on it being
/// non-zero.
///
/// The `HardCrashRestart` decision is deliberately not counted here: that path
/// refuses to start, so any metric it emits is only exported if a scrape lands
/// before the process exits. Its `error!` log is the reliable signal.
pub(crate) fn node_rejoin() {
    metrics::counter!(NODE_REJOINS_TOTAL).increment(1);
}

/// Unix-epoch seconds when this node last received a successful liveness probe
/// reply from `node_id`. Emitted from the liveness worker; Grafana derives
/// "time since last seen" as `time() - node_last_seen_timestamp_seconds`.
pub(crate) fn node_last_seen(node_id: crate::ids::NodeId) {
    metrics::gauge!(NODE_LAST_SEEN_TIMESTAMP_SECONDS, LABEL_NODE_ID => node_id.to_string())
        .set(lossy_f64(unix_now_secs()));
}

/// 1 while this node considers `node_id` live (probes succeeding), 0 once it has
/// been dropped from the live set (probe failures past threshold, or evicted).
pub(crate) fn node_live(node_id: crate::ids::NodeId, live: bool) {
    metrics::gauge!(NODE_LIVE, LABEL_NODE_ID => node_id.to_string()).set(if live {
        1.0
    } else {
        0.0
    });
}

// ── write path (writer side) ────────────────────────────────────────────────

metrics!(write_path_writer; {
    custom  write_fanout_response  WRITE_FANOUT_RESPONSES_TOTAL
        "write_fanout_responses_total" Counter None
        "Per-replica fan-out responses (ack / rejected / unreachable). \
         Persistent unreachable share = degraded replica set.";
    custom  writes_in_flight_guard  WRITES_IN_FLIGHT  "writes_in_flight"
        Gauge Count "Writes between fan-out start and quorum resolution.";
    custom  writer_inflight_permits_set  WRITER_INFLIGHT_PERMITS
        "writer_inflight_permits" Gauge Count
        "Held in-flight permits in the write coordinator. Pinned at the \
         MAX_INFLIGHT cap = quorum can't keep up (backpressure).";
    counter writer_fan_full  WRITER_FAN_FULL_TOTAL  "writer_fan_full_total"
        "Feeder dropped an id because a replica's fan was full/gone (id \
         resent on that replica's next reconnect prime). ~0 in steady \
         state; sustained = a chronically-behind replica.";
    counter writer_outcome_dropped  WRITER_OUTCOME_DROPPED_TOTAL
        "writer_outcome_dropped_total"
        "Outcome dropped because the per-connection outcome channel was \
         full. ~0 in steady state; sustained = consumer (coordinator \
         per-replica loop) is falling behind and writes will time out.";
    counter writer_submit_dropped  WRITER_SUBMIT_DROPPED_TOTAL
        "writer_submit_dropped_total"
        "Write dropped because the coordinator's submit channel was full. \
         ~0 in steady state; sustained = the coordinator feeder is blocked \
         and writes are backing up.";
    counter writer_replica_reopen  WRITER_REPLICA_REOPEN_TOTAL
        "writer_replica_reopen_total"
        "Per-replica write connections (re)opened. Steady-state growth = \
         flapping connections.";
});

// ── write path (writer side): hand-written emission helpers ─────────────────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn write_fanout_response(state: FanoutResponseState) {
    metrics::counter!(
        WRITE_FANOUT_RESPONSES_TOTAL,
        LABEL_STATE => state.as_str(),
    )
    .increment(1);
}

/// RAII guard for the `writes_in_flight` gauge: `+1` on construction, `-1` on
/// `Drop`. A guard (not bare inc/dec calls) keeps the gauge correct even when the
/// holding future is cancelled/dropped mid-flight — the previous bare pair leaked
/// `+1` on every write that hit the `write_timeout` (the drop skipped the dec).
#[must_use = "hold the guard for the write's lifetime; dropping it decrements the gauge immediately"]
pub(crate) struct WritesInFlightGuard;

pub(crate) fn writes_in_flight_guard() -> WritesInFlightGuard {
    metrics::gauge!(WRITES_IN_FLIGHT).increment(1.0);
    WritesInFlightGuard
}

impl Drop for WritesInFlightGuard {
    fn drop(&mut self) {
        metrics::gauge!(WRITES_IN_FLIGHT).decrement(1.0);
    }
}

/// Held in-flight permits (`quorum::coordinator`). Pinned at `MAX_INFLIGHT` = the
/// global cap is saturated (quorum can't keep up): the backpressure signal.
/// Set from the coordinator's semaphore rather than tracked incrementally, so
/// the gauge is re-derived every tick instead of accumulating drift.
pub(crate) fn writer_inflight_permits_set(held: usize) {
    set_gauge(
        WRITER_INFLIGHT_PERMITS,
        u64::try_from(held).unwrap_or(u64::MAX),
    );
}

// ── write path (replica side) + HLC cutoff ──────────────────────────────────

metrics!(write_path_replica; {
    labeled_counter replica_write_request REPLICA_WRITE_REQUESTS_TOTAL
        "replica_write_requests_total"
        "Replica-side write requests by outcome. fail_hlc_cutoff spikes are \
         expected briefly after snapshot cutoffs. (Wall-clock skew is \
         enforced at ingress as hlc_drift_too_large on the fresh per-RPC \
         HLC, not here.)"
        (state: ReplicaWriteState) [LABEL_STATE => state.as_str()];
    custom  hlc_guards_inc  HLC_GUARDS_IN_FLIGHT  "hlc_guards_in_flight"
        Gauge Count "Outstanding HlcGuards (writes admitted but not yet \
         durable + broadcast). Cutoff drain blocks on these.";
    labeled_counter hlc_cutoff_set HLC_CUTOFF_SETS_TOTAL "hlc_cutoff_sets_total"
        "Cutoff set attempts on this replica. rejected_backwards is expected \
         once per leader change; alert on sustained rate."
        (state: CutoffSetState) [LABEL_STATE => state.as_str()];
    hist_duration hlc_drift HLC_DRIFT_SECONDS "hlc_drift_seconds"
        "Absolute drift between a peer HLC physical-ms and local wall clock \
         observed during integrate(). Values beyond the configured skew limit \
         are rejected; the histogram reveals the normal operating band. Under \
         load this is dominated by receive-buffer latency, not clock skew."
        (drift: Duration);
    labeled_counter hlc_drift_rejection HLC_DRIFT_REJECTIONS_TOTAL
        "hlc_drift_rejections_total"
        "Peer HLC integrations rejected as hlc_drift_too_large, by direction. \
         `behind` (older peer stamp) is usually receive-buffer latency under \
         load; `ahead` points at a genuinely fast peer clock."
        (direction: DriftDirection) [LABEL_DIRECTION => direction.as_str()];
});

// ── write path (replica side) + HLC cutoff: hand-written emission helpers ───
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn hlc_guards_inc() {
    metrics::gauge!(HLC_GUARDS_IN_FLIGHT).increment(1.0);
}

pub(crate) fn hlc_guards_dec() {
    metrics::gauge!(HLC_GUARDS_IN_FLIGHT).decrement(1.0);
}

label_enum!(#[doc = "`direction` label of [`HLC_DRIFT_REJECTIONS_TOTAL`]."] DriftDirection {
    /// Peer HLC physical-ms is behind local (older) — usually buffer latency.
    Behind => "behind",
    /// Peer HLC physical-ms is ahead of local (newer) — peer clock ahead.
    Ahead => "ahead",
});

// ── logs and replication ────────────────────────────────────────────────────

metrics!(logs; {
    custom  log_writes  LOG_WRITES_TOTAL  "log_writes_total"  Counter None
        "Entries appended/integrated, by log (replica = single-writer \
         replica log, local = learner local store).";
    custom  log_index_frontier  LOG_INDEX_FRONTIER  "log_index_frontier"
        Gauge None
        "Highest log index per log type. replica-vs-local spread on a \
         learner = replication lag in entries.";
    gauge_usize log_subscriptions_count LOG_SUBSCRIPTIONS_COUNT
        "log_subscriptions_count" Count
        "Active subscribers to this replica's log broadcast." (count: usize);
    counter subscription_reconnect LOG_SUBSCRIPTION_RECONNECTS_TOTAL
        "log_subscription_reconnects_total"
        "Subscription reconnects (learner side). Sustained rate = flaky \
         link or replica restarts.";
    counter subscription_reorder LOG_SUBSCRIPTION_REORDERS_TOTAL
        "log_subscription_reorders_total"
        "Subscription streams closed because a replica's delivered index \
         went BACKWARDS (a reorder or duplicate). The replica log is \
         gapless and single-writer and is read in index order, so this \
         should always be zero: any non-zero value means local \
         replica-log corruption on the serving replica, which warrants \
         the wipe + new NodeId procedure.";
    custom  subscription_messages LOG_SUBSCRIPTION_MESSAGES_TOTAL
        "log_subscription_messages_total" Counter None
        "Records received via log subscription (learner side). \
         rate() compared with log_writes_total{log_type=\"replica\"} on \
         the upstream = per-subscription catch-up lag.";
});

// ── logs and replication: hand-written emission helpers ─────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn log_writes(log_type: LogType, count: usize) {
    if count == 0 {
        return;
    }
    metrics::counter!(
        LOG_WRITES_TOTAL,
        LABEL_LOG_TYPE => log_type.as_str(),
    )
    .increment(u64::try_from(count).expect("log write count fits u64"));
}

pub(crate) fn log_index_frontier(log_type: LogType, index: u64) {
    metrics::gauge!(
        LOG_INDEX_FRONTIER,
        LABEL_LOG_TYPE => log_type.as_str(),
    )
    .set(lossy_f64(index));
}

pub(crate) fn subscription_messages(count: usize) {
    if count == 0 {
        return;
    }
    metrics::counter!(LOG_SUBSCRIPTION_MESSAGES_TOTAL)
        .increment(u64::try_from(count).expect("subscription message count fits u64"));
}

// ── subscription load shedding ──────────────────────────────────────────────
//
// The replication path sheds deliberately: a subscriber whose buffer is full is
// dropped rather than allowed to stall the replica's shared flush thread (or to
// buffer without bound). That is the intended availability trade — one slow
// consumer must not take the node down. These metrics make the trade *visible*,
// so a shed that is protecting the node can be told apart from a shed loop that
// is preventing a subscriber from ever catching up. Nothing here changes the
// shedding behaviour.

metrics!(subscription; {
    labeled_counter subscription_shed LOG_SUBSCRIPTION_SHED_TOTAL
        "log_subscription_shed_total"
        "Records dropped by deliberate load shedding on the replication \
         path. Only site=\"replica_fanout\" emits: the serving replica \
         evicted a subscriber whose live channel was full, rather than \
         block the single flush thread that serves every writer on the \
         node. By design, and it costs a resend rather than data — \
         delivery is index-addressed and the consumer dedups by data_id. \
         The learner does not shed: its reader thread is dedicated to \
         one subscription, so it waits instead, which lets TCP pace the \
         replica's catch-up scan. Expect this only under live write \
         load, never during a quiet catch-up."
        (site: ShedSite) [LABEL_SITE => site.as_str()];
    counter subscription_consumer_stalled LOG_SUBSCRIPTION_CONSUMER_STALLED_TOTAL
        "log_subscription_consumer_stalled_total"
        "Subscriptions ended because the consumer accepted nothing for \
         the whole stall timeout (30s). NOT backpressure: a slow \
         consumer is waited on and is not counted anywhere. This fires \
         only when an integrate loop has stopped making room entirely, \
         which is a bug — an fsync-fenced batch frees a slot in \
         milliseconds. No data is lost (the subscriber resumes from its \
         durable cursor); the thread and socket are released so a \
         wedged consumer cannot pin them forever. MUST be zero.";
    labeled_counter subscription_ended LOG_SUBSCRIPTION_ENDED_TOTAL
        "log_subscription_ended_total"
        "Subscription attempts that ended, by reason (learner side). \
         \"ended\": the stream stopped — a socket drop, a replica \
         restart, or a shed on either side, which are indistinguishable \
         on the wire; correlate log_subscription_shed_total on this node \
         and on the serving replica to tell them apart. \"peer_gone\": \
         the peer is unroutable or replaced (membership converging). \
         \"failed\": stream or integrate error. A high \"ended\" rate on \
         a healthy network means the buffers, not the peers, are ending \
         connections."
        (reason: SubscriptionEndReason) [LABEL_REASON => reason.as_str()];
    hist_duration subscription_connection_seconds LOG_SUBSCRIPTION_CONNECTION_SECONDS
        "log_subscription_connection_seconds"
        "Lifetime of one subscription attempt (learner side), from \
         connect to stream end. Healthy = long-lived (minutes to hours). \
         A tight cluster of sub-second lifetimes means connections are \
         being ended by buffer pressure and every reconnect re-pays the \
         replica's catch-up scan."
        (elapsed: Duration);
    custom  subscription_records_retired LOG_SUBSCRIPTION_RECORDS_RETIRED
        "log_subscription_records_retired_entries" Histogram Count
        "Durable progress made by one subscription attempt (learner \
         side): the advance of this peer's persisted subscription cursor \
         between connect and stream end. The progress-per-connection \
         measure. A distribution pinned near the combined \
         receive-buffer capacity means each connection dies exactly when \
         its buffers fill, so a reconnect never retires more than a \
         bufferful and the replica's catch-up scan is re-paid every \
         time. Near zero with a non-zero reconnect rate means no forward \
         progress at all.";
    gauge   subscription_catchup_backlog LOG_SUBSCRIPTION_CATCHUP_BACKLOG_ENTRIES
        "log_subscription_catchup_backlog_entries" Count
        "Entries a subscriber had to replay at its most recent connect \
         (serving replica side): the replica's frontier minus the \
         subscriber's resume cursor. Rising across successive connects \
         means the deficit is diverging — shedding is no longer just \
         protecting the node, it is preventing catch-up. Flat or falling \
         means shedding is working as intended."
        (entries: u64);
});

// ── subscription load shedding: hand-written emission helpers ───────────────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn subscription_records_retired(records: u64) {
    metrics::histogram!(LOG_SUBSCRIPTION_RECORDS_RETIRED).record(lossy_f64(records));
}

// ── snapshots / leader background tasks ─────────────────────────────────────

metrics!(snapshots; {
    custom  purged_entries  PURGED_ENTRIES_TOTAL  "purged_entries_total"  Counter None
        "Entries removed by purge workers, by log type.";
    labeled_counter snapshot_creation SNAPSHOT_CREATION_TOTAL "snapshot_creation_total"
        "Snapshot creation attempts by outcome (see SnapshotState). \
         waiting_for_stable_log is backpressure, not failure: the leader's \
         local stable log has not re-ingested its own last snapshot yet."
        (state: SnapshotState) [LABEL_STATE => state.as_str()];
    custom  snapshot_creation_step SNAPSHOT_CREATION_DURATION_SECONDS
        "snapshot_creation_duration_seconds" Histogram Seconds
        "Snapshot creation latency per step (set_cutoff, await_quorum, \
         write_object, commit) plus total.";
    gauge   snapshot_size SNAPSHOT_SIZE_BYTES "snapshot_size_bytes" Bytes
        "Size of the last successful snapshot object." (bytes: u64);
    gauge_usize snapshot_entries SNAPSHOT_ENTRIES_COUNT "snapshot_entries_count" Count
        "Entries in the last successful snapshot." (n: usize);
    gauge   manifest_entries_count MANIFEST_ENTRIES_COUNT "manifest_entries_count" Count
        "Snapshot files in the applied manifest. Steady growth without \
         compaction = compaction wedged." (count: u64);
    custom  snapshot_compression_ratio SNAPSHOT_COMPRESSION_RATIO
        "snapshot_compression_ratio" Gauge None
        "Compression ratio of the last written snapshot \
         (uncompressed_bytes / compressed_bytes). >1 means compression \
         saved space; 1 means incompressible data.";
    labeled_counter compaction_run COMPACTION_RUNS_TOTAL "compaction_runs_total"
        "Manifest compaction runs by outcome."
        (state: OpState) [LABEL_STATE => state.as_str()];
    flag    leader_tasks_active LEADER_TASKS_ACTIVE "leader_tasks_active"
        "1 while the leader-only background loop runs on this node. \
         Disagreement with raft_is_leader = task lifecycle bug \
         (start-on-promotion / stop-on-demotion)." (active: bool);
});

// ── snapshots / leader background tasks: hand-written emission helpers ──────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn purged_entries(log_type: LogType, purged: usize) {
    if purged == 0 {
        return;
    }
    metrics::counter!(
        PURGED_ENTRIES_TOTAL,
        LABEL_LOG_TYPE => log_type.as_str(),
    )
    .increment(u64::try_from(purged).expect("purge count fits u64"));
}

pub(crate) fn snapshot_creation_step(step: SnapshotStep, elapsed: Duration) {
    metrics::histogram!(
        SNAPSHOT_CREATION_DURATION_SECONDS,
        LABEL_STEP => step.as_str(),
    )
    .record(elapsed);
}

pub(crate) fn snapshot_compression_ratio(uncompressed: u64, compressed: u64) {
    if compressed == 0 || uncompressed == 0 {
        return;
    }
    let pct = uncompressed
        .saturating_mul(100)
        .checked_div(compressed)
        .unwrap_or(u64::MAX);
    metrics::gauge!(SNAPSHOT_COMPRESSION_RATIO).set(lossy_f64(pct));
}

// ── object store ────────────────────────────────────────────────────────────

metrics!(object_store; {
    custom  object_store_transfer_bytes OBJECT_STORE_TRANSFER_BYTES_TOTAL
        "object_store_transfer_bytes_total" Counter Bytes
        "Bytes moved to/from the object store, by operation and by *why* the \
         transfer happened. The `reason` split is the one that matters: reads \
         run at a multiple of writes (26.7 GB GET against 12.3 GB PUT on one \
         harness run), and `op` alone cannot say whether that is compaction \
         re-reading what it just wrote or learners re-fetching a purged prefix \
         — which have opposite fixes.";
});

// ── object store: hand-written emission helpers ─────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn object_store_transfer_bytes(
    op: ObjectStoreOp,
    reason: ObjectStoreReason,
    bytes: u64,
) {
    metrics::counter!(
        OBJECT_STORE_TRANSFER_BYTES_TOTAL,
        LABEL_OP => op.as_str(),
        LABEL_REASON => reason.as_str(),
    )
    .increment(bytes);
}

// ── HLC / clock ──────────────────────────────────────────────────────────────

metrics!(hlc; {
    hlc     hlc_now_physical_ms HLC_NOW_PHYSICAL_MS "hlc_now_physical_ms" Milliseconds
        "Physical-millisecond part of the node's current HLC." (hlc: Hlc);
    gauge   hlc_logical HLC_LOGICAL "hlc_logical" Count
        "Logical component of the node's current HLC. Sustained high \
         values = wall clock stalled or heavy same-millisecond traffic."
        (logical: u64);
    custom  hlc_wall_clock_drift_ms HLC_WALL_CLOCK_DRIFT_MS "hlc_wall_clock_drift_ms"
        Gauge Milliseconds
        "Signed difference between the HLC physical part and the wall \
         clock. Approaching max_clock_skew = imminent write rejections.";
    hlc     hlc_cutoff_physical_ms HLC_CUTOFF_PHYSICAL_MS "hlc_cutoff_physical_ms" Milliseconds
        "Physical-millisecond part of this replica's HLC cutoff." (cutoff: Hlc);
});

// ── HLC / clock: hand-written emission helpers ──────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn hlc_wall_clock_drift_ms(drift_ms: i64) {
    metrics::gauge!(HLC_WALL_CLOCK_DRIFT_MS).set(lossy_f64_signed(drift_ms));
}

// ── read path / learner stores ───────────────────────────────────────────────

metrics!(read_path; {
    hlc     manifest_end_hlc_physical_ms MANIFEST_END_HLC_PHYSICAL_MS
        "manifest_end_hlc_physical_ms" Milliseconds
        "Physical-millisecond part of the stable watermark. Distance to \
         hlc_now_physical_ms = stabilization lag." (watermark: Hlc);
    custom  manifest_watermark_advanced MANIFEST_WATERMARK_ADVANCED_TIMESTAMP_SECONDS
        "manifest_watermark_advanced_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds of the last applied manifest-watermark advance \
         (a snapshot committed + replicated), emitted by the raft state \
         publisher on every node. THE snapshot alert per the design doc, \
         evaluated as `time() - manifest_watermark_advanced_timestamp_seconds`: \
         individual failure states are expected; that age growing is not.";
    custom  read_request READS_TOTAL "reads_total" Counter None
        "Read requests served, by source (stable vs unstable).";
    counter stable_read_resync STABLE_READ_RESYNCS_TOTAL
        "stable_read_resyncs_total"
        "Stable reads (`stable_tail` / `subscribe_stable_log`) that fell \
         behind the append broadcast and re-entered historical replay from \
         their own cursor. Benign by construction — the consumer sees no gap \
         and no error, only latency — but a sustained rate means consumers \
         are slower than the write rate or their subscriber queue \
         (`stable_log_subscriber_queue_capacity`) is too shallow for the \
         burst size, and each resync costs a re-scan of the durable log.";
    counter unstable_read_resync UNSTABLE_READ_RESYNCS_TOTAL
        "unstable_read_resyncs_total"
        "Unstable-tail subscriptions (`subscribe_unstable_tail`) the local \
         store's fan-out hung up on, which re-entered their cycle: re-read \
         the watermark and rescan the whole unstable window. The consumer \
         sees duplicates, never a gap. The under-pressure signal: a steadily \
         rising rate means consumers cannot keep up with their queue depth \
         and are paying an O(window) rescan each time.";
    custom  unstable_fanout_hangups UNSTABLE_FANOUT_HANGUPS_TOTAL
        "unstable_fanout_hangups_total" Counter None
        "Unstable-tail subscribers the local store's fan-out dropped because \
         their queue was full or their consumer had gone — the two are the \
         same case to the fan-out. Read with unstable_read_resyncs_total: \
         hang-ups a resync follows are slow consumers; the rest departed.";
    counter read_token_wait_timeout READ_TOKEN_WAIT_TIMEOUTS_TOTAL
        "read_token_wait_timeouts_total"
        "Read-token waits that hit their timeout.";
    labeled_counter quorum_read QUORUM_READS_TOTAL "quorum_reads_total"
        "Debounced quorum end_index reads by outcome."
        (state: OpState) [LABEL_STATE => state.as_str()];
});

// ── read path / learner stores: hand-written emission helpers ───────────────
// (custom rows above; each fn below emits the metrics named in its row)

/// Wall-clock stamp (Unix epoch seconds) of the last time the applied manifest
/// watermark advanced — i.e. a snapshot committed and replicated. Emitted by the
/// owner ([`crate::raft::state::state_publisher`]) on every node. The snapshot-age alert
/// is the PromQL `time() - manifest_watermark_advanced_timestamp_seconds`, which
/// keeps climbing regardless of whether the emitter is running.
pub(crate) fn manifest_watermark_advanced() {
    set_gauge(
        MANIFEST_WATERMARK_ADVANCED_TIMESTAMP_SECONDS,
        unix_now_secs(),
    );
}

pub(crate) fn unstable_fanout_hangups(count: usize) {
    metrics::counter!(UNSTABLE_FANOUT_HANGUPS_TOTAL)
        .increment(u64::try_from(count).expect("usize fits u64"));
}

pub(crate) fn read_request(source: ReadSource) {
    metrics::counter!(
        READS_TOTAL,
        LABEL_SOURCE => source.as_str(),
    )
    .increment(1);
}

// ── RPC server ───────────────────────────────────────────────────────────────

metrics!(rpc_server; {
    custom  rpc_server_request  RPC_SERVER_REQUESTS_TOTAL
        "rpc_server_requests_total" Counter None
        "RPC requests handled, by method and outcome.";
    custom  rpc_server_request_duration RPC_SERVER_REQUEST_DURATION_SECONDS
        "rpc_server_request_duration_seconds" Histogram Seconds
        "Server-side RPC handler latency by method.";
    gauge   rpc_connections_active RPC_CONNECTIONS_ACTIVE
        "rpc_connections_active" Count
        "Open TCP connections on the RPC server (unary keep-alive \
         connections + persistent write/subscribe connections)."
        (count: u64);
    labeled_counter rpc_connect_failed RPC_CONNECT_FAILED_TOTAL
        "rpc_connect_failed_total"
        "Failed outbound dials by reason. \"refused\" = the host answered \
         but nothing is listening (peer down, or its accept loop is \
         dead). \"local_fd_exhausted\" = EMFILE/ENFILE — OUR resource \
         exhaustion, not a peer fault, and every dial fails until fds are \
         released. \"reset\" = established then torn down, typically the \
         peer's accept backlog overflowing. \"timeout\" = no answer \
         within connect_timeout (wedged peer or blackholed route). \
         \"dns\" = resolution failed, no socket was ever attempted. \
         \"unreachable\" = no route. A sustained non-zero rate with a \
         live peer means connections cannot be re-established at all — \
         distinct from, and far more serious than, streams that end."
        (reason: ConnectFailure) [LABEL_REASON => reason.as_str()];
    counter raft_log_read_empty_range RAFT_LOG_READ_EMPTY_RANGE_TOTAL
        "raft_log_read_empty_range_total"
        "Raft-log reads for a non-empty index range that returned no \
         entries, i.e. the whole range was already purged. openraft's \
         DEFAULT limited_get_log_entries would unwrap that empty result \
         and abort the process (our release profile sets panic=abort); \
         we override it to return a StorageError instead, so openraft \
         falls back to snapshot replication. Non-zero therefore means \
         that fallback is being exercised — a follower fell behind the \
         purge floor — not that anything crashed.";
    labeled_counter rpc_accept_failed RPC_ACCEPT_FAILED_TOTAL
        "rpc_accept_failed_total"
        "Failed accept() calls on the RPC server, same `reason` \
         vocabulary as rpc_connect_failed_total. The server-side half of \
         a connectivity failure: peers reporting \"refused\"/\"reset\" \
         while this counter climbs locates the fault on THIS node. \
         \"local_fd_exhausted\" additionally means the accept loop is \
         hot-spinning, since it retries immediately on error."
        (reason: ConnectFailure) [LABEL_REASON => reason.as_str()];
});

// ── RPC server: hand-written emission helpers ────────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn rpc_server_request(method: &'static str, state: OpState) {
    metrics::counter!(
        RPC_SERVER_REQUESTS_TOTAL,
        LABEL_METHOD => method,
        LABEL_STATE => state.as_str(),
    )
    .increment(1);
}

pub(crate) fn rpc_server_request_duration(method: &'static str, elapsed: Duration) {
    metrics::histogram!(
        RPC_SERVER_REQUEST_DURATION_SECONDS,
        LABEL_METHOD => method,
    )
    .record(elapsed);
}

label_enum!(#[doc = "`reason` label of [`RPC_CONNECT_FAILED_TOTAL`]. Bounded by \
construction: every `std::io::Error` maps into exactly one of these."] ConnectFailure {
    /// The connect did not complete within `connect_timeout`. Peer alive but
    /// wedged, or a blackholed route (no RST comes back).
    Timeout => "timeout",
    /// DNS resolution failed — we never got as far as a socket.
    Dns => "dns",
    /// `ECONNREFUSED`: the host answered, nothing is listening on the port.
    /// The peer's accept loop is dead or the process is down.
    Refused => "refused",
    /// `EMFILE`/`ENFILE`: **we** ran out of file descriptors. A local resource
    /// exhaustion, not a peer fault — every dial fails until fds are released.
    LocalFdExhausted => "local_fd_exhausted",
    /// `ECONNRESET`/`ECONNABORTED`: the connection was established and then
    /// torn down. Typically the peer's accept backlog overflowed.
    Reset => "reset",
    /// `EHOSTUNREACH`/`ENETUNREACH`/`ENETDOWN`: no route.
    Unreachable => "unreachable",
    /// Anything else. A non-trivial rate here means this enum needs another arm.
    Other => "other",
});

impl ConnectFailure {
    /// Classify a connect `io::Error`. `ErrorKind` covers most cases on stable;
    /// fd exhaustion still has no stable `ErrorKind`, so it is matched on the
    /// raw errno (EMFILE 24 / ENFILE 23 on Linux and macOS alike) — and it is
    /// worth the special case, because it is the one failure that is *ours*.
    #[must_use]
    pub(crate) fn classify(e: &std::io::Error) -> Self {
        use std::io::ErrorKind as K;
        const EMFILE: i32 = 24;
        const ENFILE: i32 = 23;
        if matches!(e.raw_os_error(), Some(EMFILE | ENFILE)) {
            return Self::LocalFdExhausted;
        }
        match e.kind() {
            K::ConnectionRefused => Self::Refused,
            K::ConnectionReset | K::ConnectionAborted => Self::Reset,
            K::HostUnreachable | K::NetworkUnreachable | K::NetworkDown => Self::Unreachable,
            K::TimedOut => Self::Timeout,
            _ => Self::Other,
        }
    }
}

// ── RPC client (writer/learner outbound; async-path investigation) ───────────
// Additive decomposition of one client-side unary RPC:
//   roundtrip = conn_checkout(on miss: dial) + send + response_wait
// `conn_checkout` is the fresh-dial cost paid on a keep-alive pool miss (a hit
// reuses an idle connection and pays ~0); `rpc_conn_reuse_total{state}` counts
// hits vs misses; `open_stream` is a dedicated stream/subscribe dial; `roundtrip`
// is the whole `send_unary` exchange.

metrics!(rpc_client; {
    custom  replica_write_rpc  REPLICA_WRITE_RPC_DURATION_SECONDS
        "replica_write_rpc_duration_seconds" Histogram Seconds
        "Per-replica write-RPC latency + outcome (peer_id, outcome = \
         success / rejected / failed). A failed bucket at ~unary_timeout = a \
         black-hole/slow peer; a fast failed = refused/broken conn; rejected = \
         replica-side reject. Attributes a write-rate dip to a specific replica.";
    hist_duration coordinator_submit_wait COORDINATOR_SUBMIT_WAIT_SECONDS
        "coordinator_submit_wait_seconds"
        "Time a write sat in the coordinator's submit channel before the feed \
         loop picked it up (submit() -> feed recv). Grows when the feed loop \
         is backed up (writes not being absorbed)."
        (elapsed: Duration);
    hist_duration coordinator_permit_wait COORDINATOR_PERMIT_WAIT_SECONDS
        "coordinator_permit_wait_seconds"
        "Time the feed loop waited for an in-flight permit (the MAX_INFLIGHT \
         semaphore). THE backpressure indicator: when writes accumulate in \
         pending faster than quorum completes, this grows."
        (elapsed: Duration);
    hist_duration coordinator_fan_wait COORDINATOR_FAN_WAIT_SECONDS
        "coordinator_fan_wait_seconds"
        "Time from a write entering pending to the coordinator picking it up \
         to fan it to a follower (pending enqueue -> fan pickup)."
        (elapsed: Duration);
    hist_duration coordinator_quorum_latency COORDINATOR_QUORUM_LATENCY_SECONDS
        "coordinator_quorum_latency_seconds"
        "Time from a write entering pending to quorum being reached (the \
         writer's oneshot fires). What the client's write() actually waits \
         for. Compare to the per-follower replica_write_rpc: a gap means the \
         quorum waits on a path the per-follower round trip misses."
        (elapsed: Duration);
    hist_duration write_stream_send WRITE_STREAM_SEND_SECONDS
        "write_stream_send_seconds"
        "Time to feed + flush one batch of frames on the write-stream socket \
         (leader -> follower). Grows when the follower's server isn't reading \
         (TCP backpressure on the leader's send)."
        (elapsed: Duration);
    hist_duration write_stream_input_wait WRITE_STREAM_INPUT_WAIT_SECONDS
        "write_stream_input_wait_seconds"
        "Time a write sat in the write stream's input channel (coordinator \
         fan pickup -> the blocking writer loop dequeues it). Directly \
         timestamped on the queued object; ~0 when the writer loop keeps up."
        (elapsed: Duration);
    hist_duration server_dispatch_wait SERVER_DISPATCH_WAIT_SECONDS
        "server_dispatch_wait_seconds"
        "Time a replicated write sat in the follower's server -> raft-append \
         dispatch channel (frame received -> the raft submit picks it up). \
         Directly timestamped on the queued object; grows under follower \
         backpressure."
        (elapsed: Duration);
});

// ── RPC client: hand-written emission helpers ────────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

/// Latency + outcome of a single write RPC to ONE replica (the enqueue→response
/// exchange in the per-peer write pool), keyed by target peer. Distinguishes a
/// slow-to-ack replica from a fast-failing one, so a write-rate dip during churn
/// can be attributed to a specific replica rather than the writer.
pub(crate) fn replica_write_rpc(
    peer_id: crate::ids::NodeId,
    outcome: WriteRpcOutcome,
    latency: Duration,
) {
    metrics::histogram!(
        REPLICA_WRITE_RPC_DURATION_SECONDS,
        LABEL_PEER_ID => peer_id.to_string(),
        LABEL_OUTCOME => outcome.as_str(),
    )
    .record(latency);
}

label_enum!(#[doc = "`outcome` label of [`REPLICA_WRITE_RPC_DURATION_SECONDS`]. A \
`failed` bucket at ~`unary_timeout` latency = a black-hole/timed-out peer; a fast \
`failed` = a refused/broken connection; `rejected` = a replica-side reject (HLC cutoff \
etc.)."] WriteRpcOutcome {
    Success => "success",
    Rejected => "rejected",
    Failed => "failed",
});

// ── storage write paths: the replicated logs (stable log, replica log) ─────────
// Wrapper-level metrics keep their stable_log_*/replica_log_* names;
// throughput + entry count ride on the existing `log_writes{log_type}` /
// `log_index_frontier{log_type}`. Engine-level behavior is on the storage_* series.

metrics!(storage; {
    hlc     stable_log_end_hlc_physical_ms STABLE_LOG_END_HLC_PHYSICAL_MS
        "stable_log_end_hlc_physical_ms" Milliseconds
        "Physical-millisecond part of the stable log's end HLC (highest \
         stabilized HLC). Distance to hlc_now_physical_ms = stabilization lag."
        (end_hlc: Hlc);
    hlc     stable_log_purge_threshold_physical_ms STABLE_LOG_PURGE_THRESHOLD_PHYSICAL_MS
        "stable_log_purge_threshold_physical_ms" Milliseconds
        "Physical-millisecond part of the HLC the stable log is being purged \
         up to (manifest_watermark − purge_ttl, before the frontier clamp). \
         Only moves when TTL purge is enabled; 0/absent means purge off or \
         nothing aged out yet. Gap to stable_log_end_hlc_physical_ms = the \
         live retention window still held locally (design doc §794)."
        (threshold: Hlc);
    custom  stable_log_end_advanced STABLE_LOG_END_HLC_ADVANCED_TIMESTAMP_SECONDS
        "stable_log_end_hlc_advanced_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds of the last stable-log upper-watermark (HLC end) \
         advance on this node (new data going quorum-stable). Paired with the \
         manifest stamp it separates 'idle' (both flat) from 'snapshot stalled' \
         (this advancing, manifest flat).";
    custom  read_entry  READ_ENTRIES_TOTAL  "read_entries_total"  Counter None
        "Entries streamed to readers, by the storage tier that served each \
         (source=local_store|stable_log|object_store). Per-entry, unlike \
         reads_total (per-request). object_store is non-zero only when a \
         read reaches below the stable-log purge floor (design doc §794).";
    hist_duration replica_log_commit_elapsed REPLICA_LOG_COMMIT_SECONDS
        "replica_log_commit_seconds"
        "Segment-WAL append + fsync latency per replica-log flush batch \
         (the engine's own durability fence). High values point at the \
         disk, not at compaction."
        (elapsed: Duration);
    probe replica_log_flush_cycle REPLICA_LOG_FLUSH_CYCLE_SECONDS
        "replica_log_flush_cycle_seconds"
        "Total time of one flush_one_batch_blocking body — batch build + \
         append/fsync + fanout + completions. The flush thread's serial \
         per-batch cost; the write-path serialization suspect."
        ;
    probe replica_log_batch_build REPLICA_LOG_BATCH_BUILD_SECONDS
        "replica_log_batch_build_seconds"
        "Time to drain + encode one batch (BatchBuilder::build). The phase \
         of the flush cycle no metric covered before."
        ;
    hist_duration replica_append_queue_wait REPLICA_APPEND_QUEUE_WAIT_SECONDS
        "replica_append_queue_wait_seconds"
        "Time a write waited in the mpsc channel from append() enqueue to \
         flush-loop dequeue (the batching wait). The non-storage part of \
         replica_log_append_duration_seconds."
        (elapsed: Duration);
    hist    replica_log_flush_bytes REPLICA_LOG_FLUSH_BYTES
        "replica_log_flush_bytes" Bytes
        "Payload bytes per replica-log flush (one engine append batch). \
         _sum is byte throughput, _count the number of flushes."
        (bytes: usize);
    hist    replica_log_flush_entries REPLICA_LOG_FLUSH_ENTRIES
        "replica_log_flush_entries" Count
        "Entries batched into one replica-log flush. Distribution near 1 \
         under load = batching starvation; near the cap = healthy batching."
        (count: usize);
    labeled_counter replica_log_flush_trigger REPLICA_LOG_FLUSH_TRIGGER_TOTAL
        "replica_log_flush_trigger_total"
        "Replica-log flushes by trigger reason (batch_full vs idle). A high \
         idle share under load = the writer is starving the batch."
        (reason: FlushTrigger) [LABEL_REASON => reason.as_str()];
    gauge_usize replica_log_queue_depth REPLICA_LOG_QUEUE_DEPTH
        "replica_log_queue_depth" Count
        "Pending writes queued to the replica-log flush loop. Sustained \
         growth = the flush loop cannot keep up (durability backpressure)."
        (depth: usize);
    custom  replica_log_flushed REPLICA_LOG_LAST_FLUSH_TIMESTAMP_SECONDS
        "replica_log_last_flush_timestamp_seconds" Gauge Seconds
        "Unix timestamp of the last replica-log flush. Grafana shows \
         'time since last flush' as time() minus this; a stall = stuck writer.";
    counter replica_outcome_dropped REPLICA_OUTCOME_DROPPED_TOTAL
        "replica_outcome_dropped_total"
        "Write outcomes the flush loop could not hand back because the \
         connection's outcome channel was full. The write IS durable — \
         the writer resends it on reconnect and the downstream dedups by \
         data_id — so this costs a resend, never correctness. Non-zero \
         means a connection's consumer stalled a full in-flight window \
         deep, or its outcome channel is sized below the client's \
         in-flight cap.";
    hist    local_store_entry_bytes LOCAL_STORE_ENTRY_BYTES
        "local_store_entry_bytes" Bytes
        "Encoded size of full-data entries written to the local store. \
         _bucket is the payload-size distribution."
        (bytes: usize);
    hist    local_store_flush_bytes LOCAL_STORE_FLUSH_BYTES
        "local_store_flush_bytes" Bytes
        "Bytes written per local-store flush (one WriteBatch). _sum \
         is byte throughput, _count the number of flushes."
        (bytes: usize);
    hist    local_store_flush_entries LOCAL_STORE_FLUSH_ENTRIES
        "local_store_flush_entries" Count
        "Operations folded into one local-store flush. Near 1 under load = \
         batching starvation; near the cap = healthy batching."
        (count: usize);
    custom  local_store_ops  LOCAL_STORE_OPS_TOTAL  "local_store_ops_total"  Counter None
        "Local-store write operations by kind (integrate).";
    custom  local_store_integrate_skipped_stale LOCAL_STORE_INTEGRATE_SKIPPED_STALE_TOTAL
        "local_store_integrate_skipped_stale_total" Counter None
        "Subscription records skipped at integrate because the stable prefix \
         already covers them (HLC <= end_hlc). Catch-up redundancy avoided; \
         near-zero in steady state.";
    custom  local_store_integrate_skipped_duplicate LOCAL_STORE_INTEGRATE_SKIPPED_DUPLICATE_TOTAL
        "local_store_integrate_skipped_duplicate_total" Counter None
        "Duplicate copies of an already-written data_id dropped before the LSM \
         write. Every learner subscribes to every replica, so each write \
         arrives once per replica; this counts copies 2..n. Expect roughly \
         (replicas - 1)/replicas of arrivals. Distinct from \
         skipped_stale, which is about the stable prefix, not duplication.";
    custom  local_store_flushed LOCAL_STORE_LAST_FLUSH_TIMESTAMP_SECONDS
        "local_store_last_flush_timestamp_seconds" Gauge Seconds
        "Unix timestamp of the last local-store flush (Grafana: time() minus \
         this for 'time since last flush').";
    custom  local_store_integrate_queue_depth LOCAL_STORE_INTEGRATE_QUEUE_DEPTH
        "local_store_integrate_queue_depth" Gauge None
         "Depth of the node's shared local-store integration channel, which \
         every subscription forwards into. Non-zero means the single \
         integration worker is falling behind the incoming streams. Not \
         labeled by peer: there is one queue per node, not one per \
         subscription (it replaced the per-peer subscription_buffer_depth \
         when the per-subscription queue was removed).";
    custom  hlc_skew  HLC_SKEW_SECONDS  "hlc_skew_seconds" Gauge Seconds
        "Per-peer HLC physical-ms skew (|local − peer| / 1000) observed on the \
         replication subscriber path. Creeps toward the configured HLC skew \
         limit (`hlc_max_clock_skew`) as a peer approaches rejection — a \
         leading indicator of the drift-reject/reconnect churn loop. \
         Labeled by peer_id.";
    // RocksDB engine-state gauges. Per-CF: a node has one DB, so `cf`
    // (Subsystem::cf_name()) is the only axis that distinguishes one
    // memtable/SST/compaction reading from another.
    custom  storage_num_immutable_memtable STORAGE_NUM_IMMUTABLE_MEMTABLE
        "storage_num_immutable_memtable" Gauge Count
        "rocksdb.num-immutable-mem-table: memtables that are full and \
         waiting to be flushed, per column family (cf). A sustained climb \
         means flush cannot keep up with the write rate.";
    custom  storage_memtable_flush_pending STORAGE_MEMTABLE_FLUSH_PENDING
        "storage_memtable_flush_pending" Gauge Count
        "rocksdb.mem-table-flush-pending: 1 if a memtable flush is queued \
         for this column family (cf), 0 otherwise.";
    custom  storage_compaction_pending STORAGE_COMPACTION_PENDING
        "storage_compaction_pending" Gauge Count
        "rocksdb.compaction-pending: 1 if compaction is queued for this \
         column family (cf), 0 otherwise. Sustained 1 alongside a growing \
         storage_live_sst_files_size_bytes means compaction is falling \
         behind.";
    custom  storage_estimate_num_keys STORAGE_ESTIMATE_NUM_KEYS
        "storage_estimate_num_keys" Gauge Count
        "rocksdb.estimate-num-keys: estimated live key count per column \
         family (cf). An estimate (tombstones and duplicates are not \
         resolved), not an exact count.";
    custom  storage_live_sst_files_size_bytes STORAGE_LIVE_SST_FILES_SIZE_BYTES
        "storage_live_sst_files_size_bytes" Gauge Bytes
        "rocksdb.live-sst-files-size: on-disk size of the live SST files in \
         this column family (cf). The disk-space counterpart to \
         storage_memory_charged_bytes, which covers only the in-memory \
         budget.";
    custom  storage_block_cache_usage_bytes STORAGE_BLOCK_CACHE_USAGE_BYTES
        "storage_block_cache_usage_bytes" Gauge Bytes
        "rocksdb.block-cache-usage: bytes currently held by the block \
         cache, DB-wide (one cache per node, not per column family). A \
         breakdown of storage_memory_charged_bytes, which also folds in \
         memtables charged to the same cache.";
    custom  storage_actual_delayed_write_rate STORAGE_ACTUAL_DELAYED_WRITE_RATE
        "storage_actual_delayed_write_rate" Gauge Count
        "rocksdb.actual-delayed-write-rate: the write-rate cap RocksDB is \
         currently enforcing on this node's DB (bytes/sec), 0 when no \
         throttle is active. Non-zero means the ENGINE is throttling \
         writers — backpressure from the memtable/L0 limits, not the host. \
         Cross-check against scrape duration to tell this apart from host \
         starvation: the 2026-08-17 harness run could not, because nothing \
         exported this.";
});

// ── storage write paths: hand-written emission helpers ───────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

/// Wall-clock stamp (Unix epoch seconds) of the last time this node's stable-log
/// upper watermark (HLC end) advanced — new data going quorum-stable, via append
/// or empty-window promotion. Grafana: `time() - stable_log_end_hlc_advanced_timestamp_seconds`.
pub(crate) fn stable_log_end_advanced() {
    set_gauge(
        STABLE_LOG_END_HLC_ADVANCED_TIMESTAMP_SECONDS,
        unix_now_secs(),
    );
}

/// One entry streamed to a reader, labeled by the storage tier that served it:
/// `local_store` (unstable tail), `stable_log` (local stable range), or
/// `object_store` (purged prefix re-streamed from a snapshot — design doc §794).
/// Per-entry, unlike the per-request `reads_total`.
pub(crate) fn read_entry(source: ReadEntrySource) {
    metrics::counter!(
        READ_ENTRIES_TOTAL,
        LABEL_SOURCE => source.as_str(),
    )
    .increment(1);
}

pub(crate) fn replica_log_flushed() {
    set_gauge(REPLICA_LOG_LAST_FLUSH_TIMESTAMP_SECONDS, unix_now_secs());
}

pub(crate) fn local_store_ops(kind: LocalWriteKind, count: u64) {
    if count == 0 {
        return;
    }
    metrics::counter!(LOCAL_STORE_OPS_TOTAL, LABEL_KIND => kind.as_str()).increment(count);
}

pub(crate) fn local_store_integrate_skipped_stale(count: u64) {
    if count == 0 {
        return;
    }
    metrics::counter!(LOCAL_STORE_INTEGRATE_SKIPPED_STALE_TOTAL).increment(count);
}

pub(crate) fn local_store_integrate_skipped_duplicate(count: u64) {
    if count == 0 {
        return;
    }
    metrics::counter!(LOCAL_STORE_INTEGRATE_SKIPPED_DUPLICATE_TOTAL).increment(count);
}

pub(crate) fn local_store_flushed() {
    set_gauge(LOCAL_STORE_LAST_FLUSH_TIMESTAMP_SECONDS, unix_now_secs());
}

pub(crate) fn local_store_integrate_queue_depth(depth: usize) {
    let v: u64 = u64::try_from(depth).unwrap_or(0);
    metrics::gauge!(LOCAL_STORE_INTEGRATE_QUEUE_DEPTH).set(lossy_f64(v));
}

pub(crate) fn hlc_skew(peer_id: crate::ids::NodeId, skew_s: f64) {
    metrics::gauge!(HLC_SKEW_SECONDS, LABEL_PEER_ID => peer_id.to_string()).set(skew_s);
}

/// `rocksdb.num-immutable-mem-table` for one column family.
pub(crate) fn storage_num_immutable_memtable(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_NUM_IMMUTABLE_MEMTABLE, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.mem-table-flush-pending` for one column family.
pub(crate) fn storage_memtable_flush_pending(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_MEMTABLE_FLUSH_PENDING, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.compaction-pending` for one column family.
pub(crate) fn storage_compaction_pending(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_COMPACTION_PENDING, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.estimate-num-keys` for one column family.
pub(crate) fn storage_estimate_num_keys(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_ESTIMATE_NUM_KEYS, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.live-sst-files-size` for one column family.
pub(crate) fn storage_live_sst_files_size_bytes(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_LIVE_SST_FILES_SIZE_BYTES, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.block-cache-usage`, DB-wide (one cache per node).
pub(crate) fn storage_block_cache_usage_bytes(value: u64) {
    set_gauge(STORAGE_BLOCK_CACHE_USAGE_BYTES, value);
}

/// `rocksdb.actual-delayed-write-rate`, DB-wide. Non-zero means the engine
/// itself is throttling writers.
pub(crate) fn storage_actual_delayed_write_rate(value: u64) {
    set_gauge(STORAGE_ACTUAL_DELAYED_WRITE_RATE, value);
}

// The replica-log / local-store probe factories live in the `probes!` group
// (generated from its rows), along with the consts they emit.

// ---------------------------------------------------------------------------
// Label-value vocabularies
// ---------------------------------------------------------------------------

probe_enum!(#[doc = "Generic success/failure outcome (`state` label)."] OpState {
    Success => "success",
    Failure => "failure",
});

impl OpState {
    /// Convenience: derive the label value from any `Result`.
    #[must_use]
    pub(crate) const fn from_ok(ok: bool) -> Self {
        if ok { Self::Success } else { Self::Failure }
    }
}

label_enum!(#[doc = "`job` label of the `leader_job_*` metrics — one value per leader task (`crate::leader::jobs`). Label values come from this enum, never ad-hoc strings (CLAUDE.md §7)."] LeaderJob {
    /// Publishes the HLC cutoff (snapshot protocol step 1).
    Cutoff => "cutoff",
    /// Builds and publishes snapshots (steps 2-5).
    Snapshot => "snapshot",
    /// Merges snapshot objects and splices the manifest.
    Compaction => "compaction",
    /// Deletes unreferenced snapshot objects.
    Cleanup => "cleanup",
    /// Drives replica-set membership changes.
    Membership => "membership",
    /// Evicts Raft voters absent from `config.peers`.
    Voters => "voters",
    /// Integrates every leader intent into Raft — the single writer.
    Integrator => "integrator",
});

label_enum!(#[doc = "`state` label of [`REPLICA_WRITE_REQUESTS_TOTAL`]."] ReplicaWriteState {
    Success => "success",
    FailHlcCutoff => "fail_hlc_cutoff",
    FailNotReplica => "fail_not_replica",
    FailStorage => "fail_storage",
});

label_enum!(#[doc = "`state` label of [`WRITE_FANOUT_RESPONSES_TOTAL`]."] FanoutResponseState {
    Ack => "ack",
    Rejected => "rejected",
    Unreachable => "unreachable",
});

label_enum!(#[doc = "`state` label of [`HLC_CUTOFF_SETS_TOTAL`]. `rejected_backwards` is \
*expected* after a leader change (design doc gotcha: a new leader's first proposed cutoff \
may precede its predecessor's); alert on sustained rate, not occurrence."] CutoffSetState {
    Accepted => "accepted",
    RejectedBackwards => "rejected_backwards",
});

label_enum!(#[doc = "`log_type` label ([`LOG_WRITES_TOTAL`], [`LOG_INDEX_FRONTIER`], \
[`PURGED_ENTRIES_TOTAL`])."] LogType {
    Local => "local",
    Replica => "replica",
    Stable => "stable",
    Raft => "raft",
});

label_enum!(#[doc = "`state` label of [`SNAPSHOT_CREATION_TOTAL`]."] SnapshotState {
    Success => "success",
    /// The (watermark, cutoff] window was empty — nothing to snapshot.
    NoopEmptyWindow => "noop_empty_window",
    /// The replica quorum did not confirm the cutoff in time.
    FailureQuorumTimeout => "failure_quorum_timeout",
    /// The final fenced commit lost the generation CAS (leadership churn).
    FailureStaleGeneration => "failure_stale_generation",
    FailureObjectStore => "failure_object_store",
    FailureStorage => "failure_storage",
    /// The leader's local stable log has not caught up with the manifest yet,
    /// so index assignment cannot proceed. Backpressure, not failure — it
    /// clears once the leader re-ingests its own last snapshot.
    WaitingForStableLog => "waiting_for_stable_log",
});

label_enum!(#[doc = "`step` label of [`SNAPSHOT_CREATION_DURATION_SECONDS`]."] SnapshotStep {
    SetCutoff => "set_cutoff",
    AwaitQuorum => "await_quorum",
    WriteObject => "write_object",
    Commit => "commit",
    Total => "total",
});

label_enum!(#[doc = "`op` label of the object-store metrics."] ObjectStoreOp {
    Get => "get",
    Put => "put",
    Head => "head",
});

label_enum!(
    #[doc = "`reason` label of [`OBJECT_STORE_TRANSFER_BYTES_TOTAL`] — why the \
             transfer happened, named by the caller rather than inferred. \
             Snapshot objects are immutable and never GC'd, so the same object \
             is legitimately read many times; this says by whom."]
    ObjectStoreReason {
        /// A snapshot the leader created from a cutoff window.
        SnapshotCreate => "snapshot_create",
        /// Manifest compaction: reads the snapshots it merges, writes the merge.
        /// Both halves land here, so the ratio is readable directly.
        Compaction => "compaction",
        /// A read below the stable-log purge floor, served just-in-time from
        /// the never-GC'd snapshots (`learner/stable_archive`).
        ArchiveRead => "archive_read",
        /// The `raft-bootstrap-fence` marker — the cluster-initialized check.
        Bootstrap => "bootstrap",
    }
);

label_enum!(#[doc = "`source` label of [`READS_TOTAL`]."] ReadSource {
    Stable => "stable",
    /// The one-shot `unstable_tail` scan.
    Unstable => "unstable",
    /// A `subscribe_unstable_tail` subscription, counted once at the call.
    UnstableSubscription => "unstable_subscription",
});

label_enum!(#[doc = "`source` label of [`READ_ENTRIES_TOTAL`]: which storage tier actually served a streamed entry. Distinct from [`ReadSource`], which is the per-request entry-point label; this one is per-entry and names the tier the data came from, so a single stable read that dips below the purge floor is attributed partly to `object_store` and partly to `stable_log`."] ReadEntrySource {
    LocalStore => "local_store",
    StableLog => "stable_log",
    ObjectStore => "object_store",
});

label_enum!(#[doc = "`reason` label of the per-store flush-trigger counters: why a flush ran."] FlushTrigger {
    /// The pending batch reached the max batch size.
    BatchFull => "batch_full",
    /// The flush ran because the channel went idle (fewer than a full batch).
    Idle => "idle",
});

label_enum!(#[doc = "`site` label of [`LOG_SUBSCRIPTION_SHED_TOTAL`] — which side dropped the \
record. Shedding happens on a *full* buffer, never on a disconnect (a disconnect is a \
stream end, counted by [`LOG_SUBSCRIPTION_ENDED_TOTAL`]).\n\nOne site, and the label is \
kept because *which* side shed is the first question asked when it fires. The learner does \
not shed at all: it has no shared resource to protect, so it waits — see \
`network::client::subscribe_stream::send_or_stall`."] ShedSite {
    /// The serving replica's broadcast fan-out: the subscriber's live channel
    /// was full, so it was evicted rather than block the shared flush thread
    /// that serves every writer on the node.
    ReplicaFanout => "replica_fanout",
});

label_enum!(#[doc = "`reason` label of [`LOG_SUBSCRIPTION_ENDED_TOTAL`] — why one subscription attempt ended. A stream that ended because *we* shed is indistinguishable on the wire from one that ended because the peer went away, so the reason has to be recorded here or it is unrecoverable."] SubscriptionEndReason {
    /// The stream simply ended. On a stream that is infinite by contract this
    /// is never "the peer finished"; it is a socket drop, a replica restart, or
    /// a shed on **either** side — all identical on the wire. Which one it was
    /// is answered by correlating [`LOG_SUBSCRIPTION_SHED_TOTAL`] on this node
    /// (`site="client_buffer"`) and on the serving replica
    /// (`site="replica_fanout"`) over the same window.
    Ended => "ended",
    /// The peer is gone, unroutable, or replaced (membership still converging).
    PeerGone => "peer_gone",
    /// The stream carried an error, or a record failed to integrate.
    Failed => "failed",
});

label_enum!(#[doc = "`kind` label of [`LOCAL_STORE_OPS_TOTAL`] — the local-store write payload type."] LocalWriteKind {
    Integrate => "integrate",
});

// ---------------------------------------------------------------------------
// RAII timing probe
// ---------------------------------------------------------------------------

/// Outcome label vocabulary usable as a [`Probe`] `state` label.
pub(crate) trait ProbeOutcome: Copy {
    #[must_use]
    fn as_str(self) -> &'static str;
}

/// Marker type for probes that record an unlabeled histogram (no outcome label).
///
/// See [`Probe::unlabeled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NoOutcome;

impl ProbeOutcome for NoOutcome {
    fn as_str(self) -> &'static str {
        unreachable!("NoOutcome is never serialised into a label value")
    }
}

/// Sentinel `label_key` value for unlabeled probes.
const LABEL_NONE: &str = "";

/// Times a code region and records a `*_duration_seconds` histogram labeled by
/// outcome when it drops — replacing the manual
/// `let t0 = Instant::now(); …; metrics::foo(t0.elapsed())` pattern.
///
/// The histogram's `_count{state}` doubles as the outcome-breakdown counter and
/// `_bucket` carries the latency, so one probe usually stands in for a histogram
/// *and* a counter. The `outcome` defaults to a failure-ish variant so an early
/// `return`/`?`/panic still records (as that default) rather than vanishing.
#[must_use = "a Probe records its metric on drop; bind it to a name, do not discard it"]
pub(crate) struct Probe<O: ProbeOutcome> {
    t0: tokio::time::Instant,
    hist: &'static str,
    label_key: &'static str,
    outcome: O,
    /// One extra static label pair (e.g. `(LABEL_OP, "put")` for dual-label
    /// metrics). `None` → single outcome label or unlabeled histogram.
    extra: Option<(&'static str, &'static str)>,
    armed: bool,
}

impl<O: ProbeOutcome> Probe<O> {
    fn new(
        hist: &'static str,
        label_key: &'static str,
        extra: Option<(&'static str, &'static str)>,
        default_outcome: O,
        armed: bool,
    ) -> Self {
        Self {
            t0: tokio::time::Instant::now(),
            hist,
            label_key,
            outcome: default_outcome,
            extra,
            armed,
        }
    }

    fn start(hist: &'static str, default_outcome: O) -> Self {
        Self::new(hist, LABEL_STATE, None, default_outcome, true)
    }

    /// Like `start` but with one extra static label pair baked in at
    /// construction (e.g. `(LABEL_OP, "put")`).
    fn start_with_extra(
        hist: &'static str,
        label_key: &'static str,
        extra: (&'static str, &'static str),
        default_outcome: O,
    ) -> Self {
        Self::new(hist, label_key, Some(extra), default_outcome, true)
    }

    /// Update the outcome that will be recorded on drop; keeps timing.
    #[allow(dead_code, reason = "used when probes replace StreamMetricsGuard")]
    pub(crate) fn set(&mut self, outcome: O) {
        self.outcome = outcome;
    }

    /// Set the final outcome and record immediately (consumes the probe).
    pub(crate) fn finish(mut self, outcome: O) {
        self.outcome = outcome;
        // `drop` does the emission.
    }

    /// Suppress emission entirely (e.g. clean shutdown, not a real operation).
    #[allow(
        dead_code,
        reason = "available for callers that need to suppress emission on clean exit"
    )]
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<O: ProbeOutcome> Probe<O> {
    /// Record the histogram now, regardless of `armed`. Shared by `Drop` and
    /// the probe impl.
    #[allow(clippy::expect_used, reason = "elapsed is infallible")]
    fn emit(&self) {
        let elapsed = self.t0.elapsed();
        if self.label_key == LABEL_NONE {
            if let Some((ek, ev)) = self.extra {
                // One static label, no outcome (Pattern 4: per-store timings).
                metrics::histogram!(self.hist, ek => ev).record(elapsed);
            } else {
                // Unlabeled histogram (Pattern 3).
                metrics::histogram!(self.hist).record(elapsed);
            }
        } else if let Some((ek, ev)) = self.extra {
            // Dual-label (Pattern 2): outcome state + one extra static label.
            metrics::histogram!(self.hist, self.label_key => self.outcome.as_str(), ek => ev)
                .record(elapsed);
        } else {
            // Single outcome label (Pattern 1, status quo).
            metrics::histogram!(self.hist, self.label_key => self.outcome.as_str()).record(elapsed);
        }
    }
}

/// zslg's RAII probe: a caller binds one and lets it drop, and `Drop` records
/// the histogram.
impl<O: ProbeOutcome> Drop for Probe<O> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.emit();
    }
}

// ── NoOutcome-specific constructors ─────────────────────────────────────

impl Probe<NoOutcome> {
    /// Unlabeled histogram: records just `histogram!(hist).record(elapsed)`.
    fn unlabeled(hist: &'static str) -> Self {
        Self::new(hist, LABEL_NONE, None, NoOutcome, true)
    }

    /// Record the histogram immediately and disarm (consumes the probe).
    /// Useful when the timing boundary doesn't align with scope exit.
    #[allow(
        dead_code,
        reason = "used for explicit timing boundaries; will be adopted by more callers"
    )]
    pub(crate) fn record(mut self) {
        self.armed = false;
        metrics::histogram!(self.hist).record(self.t0.elapsed());
    }
}

// The stable/replica-log/local-store probe factories moved into the `probes!`
// group below (they are generated from its rows).

// ── Process-level resource gauges ──────────────────────────────────────

// ── Memory budget gauges: zslg's own, so namespaced — but read beside the
// unprefixed process_* gauges below, which is why they sit here.

metrics!(storage_budget; {
    gauge   storage_memory_charged_bytes STORAGE_MEMORY_CHARGED_BYTES "storage_memory_charged_bytes" Bytes
        "Bytes held by the storage engine's block cache — block, index and \
         filter blocks plus the memtables charged to it. This is what the \
         configured budget is judged against; read it beside \
         process_memory_rss_bytes, whose excess over this is everything \
         outside the budget (table readers, compaction buffers, iterator-\
         pinned blocks, allocator fragmentation)."
        (bytes: u64);
    gauge   storage_queue_capacity_bytes STORAGE_QUEUE_CAPACITY_BYTES "storage_queue_capacity_bytes" Bytes
        "Bytes this node's write-pipeline queues can hold at full depth: the \
         replica write queue and one subscriber's live-tail ring at a 2 KiB \
         reference record, plus the learner integrate channel and the \
         residency of its DataId dedup ring. Deliberately NOT part of \
         storage_memory_charged_bytes — that budget bounds RocksDB alone, and \
         the two are separate sizing decisions (the engine stalls on its \
         write-buffer manager, the pipeline blocks at submit). A planning \
         figure, not a measurement: the queues are sized in slots, so bytes \
         are depth times an assumed record size, and the per-subscriber ring \
         term multiplies with the live subscriber count."
        (bytes: u64);
});

metrics!(process, unprefixed; {
    custom  process_cpu_seconds PROCESS_CPU_SECONDS "process_cpu_seconds" Gauge Seconds
        "Cumulative user+system CPU seconds consumed by this process \
         (read from /proc/self/stat). Delta / wall-clock delta = CPU \
         utilisation ratio (1.0 = one full core).";
    gauge   process_memory_rss_bytes PROCESS_MEMORY_RSS_BYTES "process_memory_rss_bytes" Bytes
        "Resident set size (VmRSS, /proc/self/status) of this process — the \
         RAM working set. Climbs under write-stall backpressure and precedes \
         an OOM kill (137) if it reaches the container/cgroup limit."
        (bytes: u64);
    custom  process_memory_split_bytes PROCESS_MEMORY_ANON_BYTES "process_memory_anon_bytes"
        Gauge Bytes
        "Anonymous resident memory (/proc/self/smaps_rollup Anonymous): heap \
         + thread stacks — the pages the allocator and code own. The series \
         to chase in a leak hunt; file-backed RSS is reclaimable, this isn't.";
    custom  process_memory_split_bytes PROCESS_MEMORY_FILE_BYTES "process_memory_file_bytes"
        Gauge Bytes
        "Non-anonymous resident memory (RSS minus Anonymous, \
         /proc/self/smaps_rollup): file-backed mappings + shmem — the \
         binary, mapped page cache. Kernel-reclaimable under pressure.";
    gauge   process_threads_count PROCESS_THREADS_COUNT "process_threads_count" Count
        "OS threads in this process (Threads:, /proc/self/status). A climb \
         means a growing tokio blocking pool or leaked spawn_blocking work; \
         each thread's touched stack pages are anonymous RSS."
        (threads: u64);
    gauge   process_open_fds PROCESS_OPEN_FDS "process_open_fds" Count
        "Open file descriptors (/proc/self/fd). Sockets, listeners and \
         every RocksDB SST and WAL file. Compare against process_max_fds: at \
         the limit, dials AND accepts fail for a local reason, which \
         looks exactly like an unreachable peer but is not one."
        (fds: u64);
    gauge   process_max_fds PROCESS_MAX_FDS "process_max_fds" Count
        "RLIMIT_NOFILE soft limit (/proc/self/limits). Exported so \
         process_open_fds / process_max_fds is a query, not a hardcoded \
         alert threshold."
        (fds: u64);
});

// ── Process-level resource gauges: hand-written emission helpers ───────────
// (custom rows above; each fn below emits the metrics named in its row)

/// Cumulative user+system CPU seconds consumed by this process, read from
/// `/proc/self/stat` on a periodic tick. Delta between two readings divided
/// by wall-clock delta = CPU utilisation ratio (1.0 = one full core).
pub(crate) fn process_cpu_seconds(seconds: f64) {
    metrics::gauge!(PROCESS_CPU_SECONDS).set(seconds);
}

/// RSS split from `/proc/self/smaps_rollup`: anonymous (heap + thread stacks —
/// what the allocator and the code own) vs everything else (file-backed
/// mappings + shmem: the binary, mapped page cache). `anon` is the number to
/// chase in a leak hunt; `file` is reclaimable by the kernel under pressure.
pub(crate) fn process_memory_split_bytes(anon: u64, file: u64) {
    set_gauge(PROCESS_MEMORY_ANON_BYTES, anon);
    set_gauge(PROCESS_MEMORY_FILE_BYTES, file);
}

// ── jemalloc allocator gauges (only emitted with the `jemalloc` feature) ──

metrics!(jemalloc; {
    custom  jemalloc_stats JEMALLOC_ALLOCATED_BYTES "jemalloc_allocated_bytes"
        Gauge Bytes
        "jemalloc stats.allocated: bytes the application has live-allocated \
         (the true heap working set). Only emitted when the binary is built \
         with the `jemalloc` feature (the harness default).";
    custom  jemalloc_stats JEMALLOC_ACTIVE_BYTES "jemalloc_active_bytes"
        Gauge Bytes
        "jemalloc stats.active: bytes in whole pages backing allocations. \
         active - allocated = internal fragmentation. Only emitted with the \
         `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_METADATA_BYTES "jemalloc_metadata_bytes"
        Gauge Bytes
        "jemalloc stats.metadata: allocator bookkeeping overhead (page maps, \
         arena structures). Only emitted with the `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_RESIDENT_BYTES "jemalloc_resident_bytes"
        Gauge Bytes
        "jemalloc stats.resident: physical memory jemalloc holds (its share \
         of RSS). resident - active = dirty pages awaiting decay; RSS - \
         resident = non-heap memory (thread stacks, binary, mmaps). Only \
         emitted with the `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_MAPPED_BYTES "jemalloc_mapped_bytes"
        Gauge Bytes
        "jemalloc stats.mapped: total virtual address space jemalloc has \
         mapped (not physical). Only emitted with the `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_RETAINED_BYTES "jemalloc_retained_bytes"
        Gauge Bytes
        "jemalloc stats.retained: virtual address space retained after unmap \
         (not physical memory; cheap to hold). Only emitted with the \
         `jemalloc` feature.";
});

/// One reading of jemalloc's global stats (`stats.*` via mallctl), taken after
/// an `epoch` advance. All in bytes. The ladder for a leak hunt:
/// `allocated` (live application heap) ≤ `active` (whole pages backing it;
/// gap = internal fragmentation) ≤ `resident` (physical memory jemalloc holds;
/// gap = dirty pages awaiting decay) — and `process_memory_rss_bytes` minus
/// `resident` ≈ non-heap RSS (thread stacks, binary, mmapped engine files…).
#[cfg(feature = "jemalloc")]
pub(crate) struct JemallocStats {
    pub(crate) allocated: u64,
    pub(crate) active: u64,
    pub(crate) metadata: u64,
    pub(crate) resident: u64,
    pub(crate) mapped: u64,
    pub(crate) retained: u64,
}

#[cfg(feature = "jemalloc")]
pub(crate) fn jemalloc_stats(s: &JemallocStats) {
    set_gauge(JEMALLOC_ALLOCATED_BYTES, s.allocated);
    set_gauge(JEMALLOC_ACTIVE_BYTES, s.active);
    set_gauge(JEMALLOC_METADATA_BYTES, s.metadata);
    set_gauge(JEMALLOC_RESIDENT_BYTES, s.resident);
    set_gauge(JEMALLOC_MAPPED_BYTES, s.mapped);
    set_gauge(JEMALLOC_RETAINED_BYTES, s.retained);
}

// ── Raft storage probes (inside spawn_blocking — Probe uses Instant) ─
// Consts + factories generated from the rows; the two non-family factories
// (`object_store_request_probe`, `rpc_conn_reuse`) are hand-written below.

metrics!(probes; {
    probe_unlabeled rpc_dns_resolve_probe RPC_DNS_RESOLVE_SECONDS "rpc_dns_resolve_seconds"
        "DNS resolution time inside a dial. Called on every connect / \
         reconnect; slow = resolver issue or DNS server unreachable.";
    probe_unlabeled raft_log_append_probe RAFT_LOG_APPEND_DURATION_SECONDS
        "raft_log_append_duration_seconds"
        "Raft append_to_log latency (encode + batch + commit + fsync). \
         Slow values delay replication to followers.";
    probe_unlabeled raft_apply_probe RAFT_APPLY_DURATION_SECONDS "raft_apply_duration_seconds"
        "Raft apply_to_state_machine latency (membership writes + commit + \
         fsync). The hottest I/O path after the replica log.";
    probe_unlabeled raft_build_snapshot_probe RAFT_BUILD_SNAPSHOT_DURATION_SECONDS
        "raft_build_snapshot_duration_seconds"
        "Raft build_snapshot latency (read all meta + encode payload + \
         commit + fsync). The most expensive single storage operation.";
    probe_unlabeled raft_install_snapshot_probe RAFT_INSTALL_SNAPSHOT_DURATION_SECONDS
        "raft_install_snapshot_duration_seconds"
        "Raft install_snapshot latency (decode + batch write + commit + \
         fsync). Snapshot transfer cost on the receiving side.";
    probe_unlabeled stable_log_commit_probe STABLE_LOG_COMMIT_SECONDS
        "stable_log_commit_seconds"
        "Segment-WAL append + fsync latency inside StableLog::append \
         batches (the stable log's own durability fence).  High values = \
         slow WAL fsyncs during snapshot load.";
    probe_unlabeled rpc_conn_checkout_probe RPC_CONN_CHECKOUT_SECONDS
        "rpc_conn_checkout_seconds"
        "Fresh-dial cost paid on a unary keep-alive pool miss (DNS + TCP \
         connect + socket setup). A pool hit reuses an idle connection and \
         is not recorded here; see rpc_conn_reuse_total for the hit rate.";
    probe_unlabeled hlc_cutoff_drain_probe HLC_CUTOFF_DRAIN_DURATION_SECONDS
        "hlc_cutoff_drain_duration_seconds"
        "Time set_cutoff waited for in-flight HlcGuards to drain before \
         ACKing. Tail growth = slow write paths holding guards.";
    probe_unlabeled replica_append_postcommit_probe REPLICA_APPEND_POSTCOMMIT_SECONDS
        "replica_append_postcommit_seconds"
        "Post-fsync work per replica-log flush: frontier update, broadcast, \
         oneshot ACKs, guard drop.";
    probe   write_duration_probe WRITE_DURATION_SECONDS "write_duration_seconds"
        "End-to-end client write latency: fan-out start to quorum \
         resolution. Count per state doubles as the writes counter.";
    probe   rpc_roundtrip_probe RPC_ROUNDTRIP_SECONDS "rpc_roundtrip_seconds"
        "Whole client-side unary RPC: open to first response frame.";
    probe   rpc_open_stream_probe RPC_OPEN_STREAM_SECONDS "rpc_open_stream_seconds"
        "Client open: dedicated connection dial + request send for a \
         server-streaming RPC (subscribe); no response wait.";
    probe   read_token_wait_probe READ_TOKEN_WAIT_DURATION_SECONDS
        "read_token_wait_duration_seconds"
        "Time read-your-writes tokens waited for the store to catch up.";
    probe   stable_log_append_probe STABLE_LOG_APPEND_DURATION_SECONDS
        "stable_log_append_duration_seconds"
        "StableLog::append latency by outcome. _count is the append \
         throughput; _bucket the latency distribution.";
    probe   replica_log_append_probe REPLICA_LOG_APPEND_DURATION_SECONDS
        "replica_log_append_duration_seconds"
        "ReplicaLogStore::append latency by outcome (enqueue to durable + \
         broadcast). _count doubles as the append throughput.";
    probe   local_store_integrate_probe LOCAL_STORE_INTEGRATE_DURATION_SECONDS
        "local_store_integrate_duration_seconds"
        "LocalStore::integrate_batch latency by outcome. _count is the \
         integrate throughput.";
    custom  object_store_request_probe OBJECT_STORE_REQUEST_DURATION_SECONDS
        "object_store_request_duration_seconds" Histogram Seconds
        "Object-store request latency by operation and outcome. Count \
         per (op, state) doubles as the request counter.";
    custom  rpc_conn_reuse RPC_CONN_REUSE_TOTAL "rpc_conn_reuse_total" Counter None
        "Unary connection-pool checkouts by outcome. Label `state`: `hit` \
         reused an idle connection, `miss` required a fresh dial. A low hit \
         rate under load = connections churning (peer closing them, or the \
         idle cap too small).";
});

// ── probes: hand-written emission helpers ──────────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

/// Probe for [`OBJECT_STORE_REQUEST_DURATION_SECONDS`] with the given
/// operation baked into an extra `op` label.
pub(crate) fn object_store_request_probe(op: ObjectStoreOp) -> Probe<OpState> {
    Probe::start_with_extra(
        OBJECT_STORE_REQUEST_DURATION_SECONDS,
        LABEL_STATE,
        (LABEL_OP, op.as_str()),
        OpState::Failure,
    )
}

/// Count a unary connection-pool checkout as a reuse hit (`true`) or a miss
/// that required a fresh dial (`false`).
pub(crate) fn rpc_conn_reuse(hit: bool) {
    let state = if hit { "hit" } else { "miss" };
    metrics::counter!(RPC_CONN_REUSE_TOTAL, LABEL_STATE => state).increment(1);
}

// ── RPC server dual-metric guard ──────────────────────────────────────

/// RAII guard for RPC server handler metrics. Records one histogram
/// (`rpc_server_request_duration_seconds`) and one counter
/// (`rpc_server_requests_total`) on drop — the two-metric pattern that
/// [`Probe`] is not designed for.
#[must_use = "RpcServerGuard records metrics on drop"]
pub(crate) struct RpcServerGuard {
    t0: tokio::time::Instant,
    method: &'static str,
    armed: bool,
}

impl RpcServerGuard {
    pub(crate) fn new() -> Self {
        Self {
            t0: tokio::time::Instant::now(),
            method: "handshake",
            armed: true,
        }
    }

    /// Record the dispatched request kind so a cancellation mid-dispatch is
    /// attributed to the right method.
    pub(crate) fn set_method(&mut self, method: &'static str) {
        self.method = method;
    }

    /// Suppress emission entirely (e.g. clean shutdown, not a request failure).
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }

    /// Record both metrics with the given outcome and suppress drop emission.
    pub(crate) fn finish(mut self, ok: bool) {
        self.armed = false;
        let elapsed = self.t0.elapsed();
        rpc_server_request_duration(self.method, elapsed);
        rpc_server_request(self.method, OpState::from_ok(ok));
    }
}

impl Drop for RpcServerGuard {
    fn drop(&mut self) {
        if self.armed {
            let elapsed = self.t0.elapsed();
            rpc_server_request_duration(self.method, elapsed);
            rpc_server_request(self.method, OpState::Failure);
        }
    }
}

// ---------------------------------------------------------------------------
// The canonical registry
// ---------------------------------------------------------------------------

const ALL_METRICS: &[&[MetricDef]] = &[
    // --- storage memory budget (namespaced) ---
    &storage_budget!(),
    // --- process resource gauges (unprefixed: Prometheus standard) ---
    &process!(),
    // --- jemalloc allocator gauges ---
    &jemalloc!(),
    // --- raft / consensus ---
    &raft!(),
    // --- write path (writer side) ---
    &write_path_writer!(),
    // --- write path (replica side) + HLC cutoff ---
    &write_path_replica!(),
    // --- logs and replication ---
    &logs!(),
    // --- subscription load shedding ---
    &subscription!(),
    // --- snapshots / leader background tasks ---
    &snapshots!(),
    // --- object store ---
    &object_store!(),
    // --- HLC / clock ---
    &hlc!(),
    // --- read path / learner stores ---
    &read_path!(),
    // --- RPC server ---
    &rpc_server!(),
    // --- RPC client ---
    &rpc_client!(),
    // --- storage write paths: stable log / replica log / local store ---
    &storage!(),
    // --- timing probes (latency histograms by outcome/op/state) ---
    &probes!(),
];

// ---------------------------------------------------------------------------
// Recorder registration
// ---------------------------------------------------------------------------

/// Whether `name` is a registered histogram. For `observability`'s check that
/// every exported bucket override names one.
#[cfg(test)]
pub(crate) fn is_registered_histogram(name: &str) -> bool {
    ALL_METRICS
        .iter()
        .copied()
        .flatten()
        .any(|m| m.name == name && m.kind == MetricKind::Histogram)
}

/// Every registered histogram, as `(name, declared unit)`.
///
/// The unit is what `observability` derives bucket ladders from, so a `_bytes`
/// histogram never lands on a latency scale.
pub(crate) fn histograms() -> impl Iterator<Item = (&'static str, Option<Unit>)> {
    ALL_METRICS
        .iter()
        .copied()
        .flatten()
        .filter(|m| m.kind == MetricKind::Histogram)
        .map(|m| (m.name, m.unit))
}

/// Register help text and units for every metric with the installed recorder.
/// Call once after `observability::init_prometheus`.
pub(crate) fn describe_all() {
    for def in ALL_METRICS.iter().copied().flatten() {
        match (def.kind, def.unit) {
            (MetricKind::Counter, Some(unit)) => {
                metrics::describe_counter!(def.name, unit, def.help);
            }
            (MetricKind::Counter, None) => metrics::describe_counter!(def.name, def.help),
            (MetricKind::Gauge, Some(unit)) => {
                metrics::describe_gauge!(def.name, unit, def.help);
            }
            (MetricKind::Gauge, None) => metrics::describe_gauge!(def.name, def.help),
            (MetricKind::Histogram, Some(unit)) => {
                metrics::describe_histogram!(def.name, unit, def.help);
            }
            (MetricKind::Histogram, None) => metrics::describe_histogram!(def.name, def.help),
        }
    }
}

// ---------------------------------------------------------------------------
// Lint-safe emission helpers
// ---------------------------------------------------------------------------

/// The crate's single sanctioned lossy integer→float conversion, for gauge
/// readings only. Values above 2^53 lose precision; every gauge emitted
/// through this is bounded well below that (counts, byte sizes, indices,
/// HLC *physical-ms parts* — never `Hlc::raw()`).
#[must_use]
#[allow(
    clippy::as_conversions,
    reason = "audited lossy f64 conversion for bounded gauge values; see doc comment"
)]
fn lossy_f64(value: u64) -> f64 {
    value as f64
}

/// Signed variant of [`lossy_f64`] (e.g. clock drift).
#[must_use]
#[allow(
    clippy::as_conversions,
    reason = "audited lossy f64 conversion; see lossy_f64"
)]
fn lossy_f64_signed(value: i64) -> f64 {
    value as f64
}

/// Set an unlabeled gauge from a `u64` reading.
fn set_gauge(name: &'static str, value: u64) {
    metrics::gauge!(name).set(lossy_f64(value));
}

/// Set an unlabeled gauge from a `usize` reading (counts, queue depths).
fn set_gauge_usize(name: &'static str, value: usize) {
    set_gauge(
        name,
        u64::try_from(value).expect("usize gauge value fits u64"),
    );
}

/// Record an unlabeled histogram from a `usize` reading (byte sizes, batch counts).
fn record_usize_hist(name: &'static str, value: usize) {
    metrics::histogram!(name).record(lossy_f64(
        u64::try_from(value).expect("usize histogram value fits u64"),
    ));
}

/// Set an unlabeled 0/1 gauge.
fn set_flag(name: &'static str, on: bool) {
    metrics::gauge!(name).set(if on { 1.0 } else { 0.0 });
}

/// Set an HLC-valued gauge: exports the physical-millisecond part (raw HLCs
/// exceed f64's exact-integer range).
fn set_hlc_gauge(name: &'static str, hlc: Hlc) {
    set_gauge(name, hlc.parts().physical_ms);
}

/// Wall-clock seconds since the Unix epoch, for "last run at" timestamp gauges
/// (Grafana renders "time since" as `time() - metric`). Clock-skew safe.
#[must_use]
fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_metric_names_are_unique_and_non_empty() {
        let mut seen = std::collections::HashSet::new();
        for def in ALL_METRICS.iter().copied().flatten() {
            assert!(!def.name.is_empty(), "metric name must not be empty");
            assert!(seen.insert(def.name), "duplicate metric name: {}", def.name);
        }
    }

    /// Prometheus stamps `job` and `instance` on every scraped series from the
    /// scrape config. Without `honor_labels: true` (the harness does not set it,
    /// and should not have to) an exposed label of the same name is silently
    /// renamed to `exported_<name>`, so every dashboard selector on it breaks in
    /// production while passing locally. No emitted label key may collide.
    #[test]
    fn no_label_key_collides_with_prometheus_scrape_labels() {
        const RESERVED: [&str; 2] = ["job", "instance"];
        for key in ALL_LABEL_KEYS {
            assert!(
                !RESERVED.contains(key),
                "label key {key:?} collides with a Prometheus scrape label and \
                 would be exposed as exported_{key}",
            );
        }
    }

    #[test]
    fn rocksdb_properties_are_registered() {
        // `actual-delayed-write-rate` is the one that answers "was the seize-up
        // the 20 MiB budget or the host?" — the question the 2026-08-17 harness
        // run could not answer, because nothing exported it.
        for want in [
            "zslg_storage_num_immutable_memtable",
            "zslg_storage_memtable_flush_pending",
            "zslg_storage_compaction_pending",
            "zslg_storage_estimate_num_keys",
            "zslg_storage_live_sst_files_size_bytes",
            "zslg_storage_block_cache_usage_bytes",
            "zslg_storage_actual_delayed_write_rate",
        ] {
            assert!(
                ALL_METRICS
                    .iter()
                    .copied()
                    .flatten()
                    .any(|m| m.name == want),
                "{want} is not registered in ALL_METRICS",
            );
        }
    }

    /// The namespace is a contract with every dashboard and alert, so it is
    /// asserted rather than left to the macro's good behaviour. `process_*` is
    /// the documented exemption: those names are the Prometheus-standard
    /// collector namespace, shared with every other client library.
    #[test]
    fn every_metric_is_namespaced() {
        for def in ALL_METRICS.iter().copied().flatten() {
            assert!(
                def.name.starts_with(METRIC_PREFIX) || def.name.starts_with("process_"),
                "{} is outside the {METRIC_PREFIX} namespace and is not a \
                 standard process_* metric",
                def.name,
            );
        }
    }

    #[test]
    fn no_metric_refers_to_a_deleted_engine() {
        // The engine is gone from the binary; this stops its names creeping back
        // into the registry, where they would look alive to a dashboard.
        for m in ALL_METRICS.iter().copied().flatten() {
            assert!(
                !m.name.contains("strata"),
                "{} names an engine that no longer exists",
                m.name,
            );
        }
    }

    #[test]
    fn naming_conventions_hold() {
        for def in ALL_METRICS.iter().copied().flatten() {
            match def.kind {
                MetricKind::Counter => assert!(
                    def.name.ends_with("_total"),
                    "counter must end in _total: {}",
                    def.name,
                ),
                MetricKind::Histogram => assert!(
                    def.name.ends_with("_seconds")
                        || def.name.ends_with("_bytes")
                        || def.name.ends_with("_entries"),
                    "histogram must end in a unit suffix (_seconds/_bytes/_entries): {}",
                    def.name,
                ),
                MetricKind::Gauge => assert!(
                    !def.name.ends_with("_total"),
                    "gauge must not end in _total: {}",
                    def.name,
                ),
            }
        }
    }

    #[test]
    fn histograms_declare_units() {
        for def in ALL_METRICS.iter().copied().flatten() {
            if def.kind == MetricKind::Histogram {
                assert!(
                    def.unit.is_some(),
                    "histogram must declare a unit: {}",
                    def.name,
                );
            }
        }
    }

    #[test]
    fn every_metric_has_help_text() {
        for def in ALL_METRICS.iter().copied().flatten() {
            assert!(
                def.help.len() >= 20,
                "help text too short to be useful: {}",
                def.name,
            );
        }
    }

    #[test]
    fn all_design_doc_metrics_present() {
        let set: std::collections::HashSet<&str> = ALL_METRICS
            .iter()
            .copied()
            .flatten()
            .map(|d| d.name)
            .collect();
        // The design doc §"Observability" list, verbatim.
        for required in [
            "zslg_raft_is_member",
            "zslg_raft_is_leader",
            "zslg_raft_has_leader",
            "zslg_raft_term",
            "zslg_raft_applied_log_index",
            "zslg_raft_leader_changes_total",
            "zslg_raft_in_joint_config",
            "zslg_raft_config_generation",
            "zslg_raft_fenced_writes_total",
            "zslg_replica_nodes_count",
            "zslg_membership_changes_total",
            "zslg_node_rejoins_total",
            "zslg_write_duration_seconds",
            "zslg_write_fanout_responses_total",
            "zslg_writes_in_flight",
            "zslg_replica_write_requests_total",
            "zslg_hlc_guards_in_flight",
            "zslg_hlc_cutoff_sets_total",
            "zslg_hlc_cutoff_drain_duration_seconds",
            "zslg_log_writes_total",
            "zslg_log_index_frontier",
            "zslg_log_subscriptions_count",
            "zslg_log_subscription_reconnects_total",
            "zslg_local_store_integrate_queue_depth",
            "zslg_purged_entries_total",
            "zslg_manifest_watermark_advanced_timestamp_seconds",
            "zslg_stable_log_end_hlc_advanced_timestamp_seconds",
            "zslg_snapshot_creation_total",
            "zslg_snapshot_creation_duration_seconds",
            "zslg_snapshot_size_bytes",
            "zslg_snapshot_entries_count",
            "zslg_manifest_entries_count",
            "zslg_compaction_runs_total",
            "zslg_leader_tasks_active",
            "zslg_object_store_request_duration_seconds",
            "zslg_object_store_transfer_bytes_total",
            "zslg_hlc_now_physical_ms",
            "zslg_hlc_logical",
            "zslg_hlc_wall_clock_drift_ms",
            "zslg_hlc_cutoff_physical_ms",
            "zslg_manifest_end_hlc_physical_ms",
            "zslg_reads_total",
            "zslg_read_token_wait_duration_seconds",
            "zslg_read_token_wait_timeouts_total",
            "zslg_quorum_reads_total",
            "zslg_rpc_server_requests_total",
            "zslg_rpc_server_request_duration_seconds",
            "zslg_rpc_connections_active",
            "zslg_rpc_conn_checkout_seconds",
            "zslg_rpc_conn_reuse_total",
            "zslg_rpc_open_stream_seconds",
            "zslg_rpc_roundtrip_seconds",
        ] {
            assert!(
                set.contains(required),
                "missing required metric: {required}"
            );
        }
    }

    // ── emission smoke tests ─────────────────────────────────────────────

    #[test]
    fn set_flag_and_set_gauge_are_observable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            set_flag(RAFT_IS_LEADER, true);
            set_gauge(RAFT_TERM, 7);
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map.keys().any(|ck| ck.key().name() == RAFT_IS_LEADER);
        assert!(found, "raft_is_leader gauge should be present");
    }

    #[test]
    fn counter_emission_is_observable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            ::metrics::counter!(RAFT_FENCED_WRITES_TOTAL).increment(1);
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map
            .keys()
            .any(|ck| ck.key().name() == RAFT_FENCED_WRITES_TOTAL);
        assert!(found, "raft_fenced_writes_total counter should be present");
    }

    #[test]
    fn writes_in_flight_guard_nets_to_zero_after_drop() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            // Two concurrent writes, both complete (guards dropped) → net 0. This
            // is the leak guard: even a dropped/cancelled future runs Drop.
            let g1 = writes_in_flight_guard();
            let g2 = writes_in_flight_guard();
            drop(g1);
            drop(g2);
        });
        let value = snapshotter
            .snapshot()
            .into_hashmap()
            .into_iter()
            .find(|(ck, _)| ck.key().name() == WRITES_IN_FLIGHT)
            .map(|(_, (_, _, v))| v);
        let net_zero = matches!(
            value,
            Some(metrics_util::debugging::DebugValue::Gauge(g)) if g.into_inner() == 0.0
        );
        assert!(
            net_zero,
            "writes_in_flight must net to zero after both guards drop"
        );
    }

    #[test]
    fn labeled_histogram_emission_is_observable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            ::metrics::histogram!(
                SNAPSHOT_CREATION_DURATION_SECONDS,
                LABEL_STEP => SnapshotStep::Total.as_str(),
            )
            .record(std::time::Duration::from_millis(42));
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map
            .keys()
            .any(|ck| ck.key().name() == SNAPSHOT_CREATION_DURATION_SECONDS);
        assert!(
            found,
            "snapshot_creation_duration_seconds histogram should be present"
        );
    }

    #[test]
    fn probe_finish_records_chosen_outcome() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            stable_log_append_probe().finish(OpState::Success);
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map.keys().any(|ck| {
            ck.key().name() == STABLE_LOG_APPEND_DURATION_SECONDS
                && ck
                    .key()
                    .labels()
                    .any(|l| l.key() == LABEL_STATE && l.value() == OpState::Success.as_str())
        });
        assert!(found, "probe.finish should record the chosen outcome label");
    }

    #[test]
    fn probe_dropped_without_finish_records_default_outcome() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let _probe = stable_log_append_probe();
            // dropped at end of scope without finish -> default outcome (Failure).
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map.keys().any(|ck| {
            ck.key().name() == STABLE_LOG_APPEND_DURATION_SECONDS
                && ck
                    .key()
                    .labels()
                    .any(|l| l.key() == LABEL_STATE && l.value() == OpState::Failure.as_str())
        });
        assert!(
            found,
            "a dropped probe should record its default outcome label"
        );
    }

    #[test]
    fn watermark_advance_stamps_emit_epoch_timestamps() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            manifest_watermark_advanced();
            stable_log_end_advanced();
        });
        let map = snapshotter.snapshot().into_hashmap();
        // Each stamp must be a wall-clock epoch value. A near-zero reading
        // means it is measuring a duration instead, and Prometheus renders the
        // subtraction as nonsense.
        let is_epoch = |name: &str| {
            map.iter().any(|(ck, (_, _, v))| {
                ck.key().name() == name
                    && matches!(
                        v,
                        metrics_util::debugging::DebugValue::Gauge(g) if g.0 > 1_700_000_000.0
                    )
            })
        };
        assert!(
            is_epoch(MANIFEST_WATERMARK_ADVANCED_TIMESTAMP_SECONDS),
            "manifest_watermark_advanced should emit a Unix-epoch timestamp"
        );
        assert!(
            is_epoch(STABLE_LOG_END_HLC_ADVANCED_TIMESTAMP_SECONDS),
            "stable_log_end_advanced should emit a Unix-epoch timestamp"
        );
    }

    #[test]
    fn node_liveness_gauges_carry_node_id_label() {
        let node = crate::ids::NodeId::new_v4();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            node_last_seen(node);
            node_live(node, true);
        });
        let map = snapshotter.snapshot().into_hashmap();
        let labeled = |ck: &metrics_util::CompositeKey, name: &str| {
            ck.key().name() == name
                && ck
                    .key()
                    .labels()
                    .any(|l| l.key() == LABEL_NODE_ID && l.value() == node.to_string())
        };
        assert!(
            map.iter().any(
                |(ck, (_, _, v))| labeled(ck, NODE_LAST_SEEN_TIMESTAMP_SECONDS)
                    && matches!(
                        v,
                        metrics_util::debugging::DebugValue::Gauge(g) if g.0 > 1_700_000_000.0
                    )
            ),
            "node_last_seen should emit an epoch stamp labeled by node_id"
        );
        assert!(
            map.iter().any(|(ck, (_, _, v))| labeled(ck, NODE_LIVE)
                && matches!(v, metrics_util::debugging::DebugValue::Gauge(g) if g.0 > 0.5)),
            "node_live should be 1 for a healthy peer"
        );
    }

    #[test]
    fn node_live_reports_zero_when_dropped() {
        let node = crate::ids::NodeId::new_v4();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            node_live(node, false);
        });
        let map = snapshotter.snapshot().into_hashmap();
        assert!(
            map.iter().any(|(ck, (_, _, v))| {
                ck.key().name() == NODE_LIVE
                    && matches!(v, metrics_util::debugging::DebugValue::Gauge(g) if g.0 < 0.5)
            }),
            "node_live should be 0 once the peer is dropped from the live set"
        );
    }
}

#[cfg(test)]
mod macro_smoke_tests {
    use super::*;

    metrics!(synthetic; {
        gauge   synth_gauge   SYNTH_GAUGE   "synth_gauge"  Seconds  "A synthetic gauge." (v: u64);
        flag    synth_flag    SYNTH_FLAG    "synth_flag"   "A synthetic flag." (on: bool);
        counter synth_counter SYNTH_COUNTER "synth_counter" "A synthetic counter.";
        labeled_counter synth_led SYNTH_LED "synth_led" "A synthetic labeled counter."
            (s: OpState) [LABEL_STATE => s.as_str()];
        hlc     synth_hlc     SYNTH_HLC     "synth_hlc"    Milliseconds  "A synthetic hlc gauge." (h: Hlc);
        gauge_usize synth_gu  SYNTH_GU      "synth_gu"     Count  "A synthetic usize gauge." (n: usize);
        hist    synth_hist    SYNTH_HIST    "synth_hist"   Bytes  "A synthetic histogram." (n: usize);
        hist_duration synth_hd SYNTH_HD     "synth_hd"     "A synthetic duration histogram." (d: Duration);
        probe   synth_probe   SYNTH_PROBE   "synth_probe"  "A synthetic probe.";
        probe_unlabeled synth_up SYNTH_UP   "synth_up"     "A synthetic unlabeled probe.";
        custom  synth_custom  SYNTH_CUSTOM  "synth_custom" Gauge None "A synthetic custom.";
    });

    metrics!(synthetic_raw, unprefixed; {
        gauge   synth_raw_gauge SYNTH_RAW_GAUGE "synth_raw_gauge" Count
            "A synthetic gauge in an unprefixed group." (v: u64);
    });

    /// The `unprefixed` arm is what keeps `process_*` on the Prometheus
    /// standard names; without a test the exemption is one typo from silently
    /// namespacing them.
    #[test]
    fn unprefixed_group_keeps_its_bare_name() {
        assert_eq!(SYNTH_RAW_GAUGE, "synth_raw_gauge");
        assert_eq!(PROCESS_OPEN_FDS, "process_open_fds");

        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || synth_raw_gauge(1));
        assert!(
            snapshotter
                .snapshot()
                .into_hashmap()
                .keys()
                .any(|k| k.key().name() == "synth_raw_gauge"),
            "an unprefixed group must emit under its bare name",
        );

        let defs = synthetic_raw!();
        assert_eq!(defs[0].name, "synth_raw_gauge");
    }

    #[test]
    fn synthetic_group_defines_consts_and_emits() {
        // The namespace is applied by the macro, so the const and the emitted
        // name both carry it and a row never writes it.
        assert_eq!(SYNTH_GAUGE, "zslg_synth_gauge");
        assert_eq!(SYNTH_CUSTOM, "zslg_synth_custom");

        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            synth_gauge(7);
            synth_flag(true);
            synth_counter();
            synth_led(OpState::Success);
            synth_hlc(Hlc::ZERO);
            synth_gu(3);
            synth_hist(4);
            synth_hd(Duration::from_millis(1));
            synth_probe().finish(OpState::Success);
            let _probe = synth_up();
        });
        let map = snapshotter.snapshot().into_hashmap();
        for name in [
            "zslg_synth_gauge",
            "zslg_synth_flag",
            "zslg_synth_counter",
            "zslg_synth_led",
            "zslg_synth_hlc",
            "zslg_synth_gu",
            "zslg_synth_hist",
            "zslg_synth_hd",
            "zslg_synth_probe",
            "zslg_synth_up",
        ] {
            assert!(
                map.keys().any(|k| k.key().name() == name),
                "missing emitted metric {name}"
            );
        }
    }

    #[test]
    fn synthetic_group_assembles_metric_defs() {
        let defs = synthetic!();
        assert_eq!(defs.len(), 11);
        assert_eq!(defs[0].name, SYNTH_GAUGE);
        assert_eq!(defs[0].kind, MetricKind::Gauge);
        assert_eq!(defs[0].unit, Some(Unit::Seconds));
        assert_eq!(defs[2].unit, None); // counter
        assert_eq!(defs[4].unit, Some(Unit::Milliseconds)); // hlc — the arm takes a unit token
        assert_eq!(defs[5].unit, Some(Unit::Count)); // gauge_usize
        assert_eq!(defs[7].unit, Some(Unit::Seconds)); // hist_duration implicit
        assert_eq!(defs[8].unit, Some(Unit::Seconds)); // probe implicit
        assert_eq!(defs[10].kind, MetricKind::Gauge);
        assert_eq!(defs[10].unit, None); // custom None
    }
}
