//! What the raft state publisher emits: leadership, membership shape and the manifest watermark.

use metrics_kit::{MetricDef, metrics, set_gauge, unix_now_secs};
use zslg_time::Hlc;

metrics!(pub "zslg_", raft_state; {
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
    counter raft_leader_change RAFT_LEADER_CHANGES_TOTAL "raft_leader_changes_total"
        "Leader changes observed by this node. Sustained rate = election churn.";
    flag    raft_in_joint_config RAFT_IN_JOINT_CONFIG "raft_in_joint_config"
        "1 while the applied membership is a joint config. Stuck at 1 = \
         membership change wedged (doc: leader must resume from joint)."
        (in_joint: bool);
    gauge   raft_config_generation RAFT_CONFIG_GENERATION "raft_config_generation" None
        "RaftGeneration of the applied state. Spread across nodes = \
         state-replication lag." (generation: u64);
    gauge   replica_nodes_count REPLICA_NODES_COUNT "replica_nodes_count" Count
        "Replica nodes in the applied config (both sets counted once under a \
         joint config)." (count: u64);
    custom  membership_joint_exit_hlc_physical_ms MEMBERSHIP_JOINT_EXIT_HLC_PHYSICAL_MS
        "membership_joint_exit_hlc_physical_ms" Gauge Milliseconds
        "Physical-millisecond part of the armed joint-config drain bound (0 = \
         not draining). A joint config may not collapse until \
         manifest_end_hlc_physical_ms reaches this.";
    gauge   manifest_entries_count MANIFEST_ENTRIES_COUNT "manifest_entries_count" Count
        "Snapshot files in the applied manifest. Steady growth without \
         compaction = compaction wedged." (count: u64);
    gauge_via     manifest_end_hlc_physical_ms MANIFEST_END_HLC_PHYSICAL_MS
        "manifest_end_hlc_physical_ms" Milliseconds
        "Physical-millisecond part of the stable watermark. Distance to \
         hlc_now_physical_ms = stabilization lag." (watermark: Hlc => Hlc::physical_ms);
    custom  manifest_watermark_advanced MANIFEST_WATERMARK_ADVANCED_TIMESTAMP_SECONDS
        "manifest_watermark_advanced_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds of the last applied manifest-watermark advance \
         (a snapshot committed + replicated), emitted by the raft state \
         publisher on every node. THE snapshot alert per the design doc, \
         evaluated as `time() - manifest_watermark_advanced_timestamp_seconds`: \
         individual failure states are expected; that age growing is not.";
    flag    leader_tasks_active LEADER_TASKS_ACTIVE "leader_tasks_active"
        "1 while the leader-only background loop runs on this node. \
         Disagreement with raft_is_leader = task lifecycle bug \
         (start-on-promotion / stop-on-demotion)." (active: bool);
    gauge_via     hlc_cutoff_physical_ms HLC_CUTOFF_PHYSICAL_MS "hlc_cutoff_physical_ms" Milliseconds
        "Physical-millisecond part of this replica's HLC cutoff." (cutoff: Hlc => Hlc::physical_ms);
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &raft_state!();

/// The armed joint-config drain bound, or 0 when not draining. Compare against
/// `manifest_end_hlc_physical_ms`: while this is non-zero and above the
/// watermark, the membership change is correctly blocked waiting for the
/// outgoing replica set to be snapshotted (designdoc §"Membership Change" step
/// 3). A bound that stays above the watermark for many snapshot intervals means
/// snapshot creation is stalled, not that membership is stuck.
pub fn membership_joint_exit_hlc_physical_ms(exit: Option<zslg_consensus::Exit>) {
    // `AnyTime` reports 0, same as "not draining": both mean the collapse is
    // not waiting on a snapshot, which is what the gauge is read for.
    let exit_hlc = match exit {
        Some(zslg_consensus::Exit::WatermarkAbove(h)) => Some(h),
        Some(zslg_consensus::Exit::AnyTime) | None => None,
    };
    match exit_hlc {
        Some(hlc) => set_gauge(MEMBERSHIP_JOINT_EXIT_HLC_PHYSICAL_MS, hlc.physical_ms()),
        None => set_gauge(MEMBERSHIP_JOINT_EXIT_HLC_PHYSICAL_MS, 0),
    }
}

/// Wall-clock stamp (Unix epoch seconds) of the last time the applied manifest
/// watermark advanced — i.e. a snapshot committed and replicated. Emitted by the
/// owner (`crate::raft::state::state_publisher`) on every node. The snapshot-age alert
/// is the PromQL `time() - manifest_watermark_advanced_timestamp_seconds`, which
/// keeps climbing regardless of whether the emitter is running.
pub fn manifest_watermark_advanced() {
    set_gauge(
        MANIFEST_WATERMARK_ADVANCED_TIMESTAMP_SECONDS,
        unix_now_secs(),
    );
}
