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
//! Global invariants asserted over a quiesced cluster.
//!
//! Every cluster test ends by calling [`assert_cluster_invariants`]. These are
//! crate-wide laws (CLAUDE.md §8, §12), not scenario-specific claims: a test
//! that only checks its own recovery path can pass while the manifest is
//! corrupt or a joint config collapsed early.
//!
//! Asserted on *applied* state, which is what distinguishes this from
//! `validate_replicated_state`: that runs on a proposal, so it cannot catch a
//! bug on the apply path itself.

use crossbeam_combinators::testing::ChannelTestExt as _;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;

use crate::common::Cluster;
use zslg_consensus::NodeId;
use zslg_consensus::{RaftState, ReplicaMembership};
use zslg_log::DataId;
use zslg_log::StableLogEvent;

/// Assert every global invariant that holds of a quiesced cluster.
///
/// Call after the test's own quiesce. Panics with the offending node and state
/// on violation.
pub(crate) fn assert_cluster_invariants(cluster: &Cluster) {
    for node in cluster.nodes() {
        let id = node.node_id();
        let state = node.state_handle().snapshot();
        assert_manifest_contiguous(id, &state);
        assert_watermark_below_cutoff(id, &state);
        assert_membership_well_formed(id, &state);
        assert_no_satisfied_drain_left_armed(id, &state);
    }
}

/// Assert every global invariant, including the staged durability chain and
/// the unstable/stable boundary, keyed on `acked` — the `(DataId, Bytes)`
/// pairs of *successful* writes only.
///
/// Returns whether the unstable/stable boundary check had anything to compare,
/// so a caller that samples coverage can report a permanently vacuous check
/// instead of mistaking it for a passing one. Callers that do not care write
/// `let _ = …`.
/// How long the durability chain may take to reach every stable log: 10 s
/// unless `CLUSTER_TEST_DURABILITY_DEADLINE_MS` says otherwise. The override
/// exists to provoke the failure report on purpose (a short deadline turns
/// "slow" into a report that says whether an entry is late or lost).
fn durability_deadline() -> Duration {
    std::env::var("CLUSTER_TEST_DURABILITY_DEADLINE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(10), Duration::from_millis)
}

pub(crate) async fn assert_cluster_invariants_with_acks(
    cluster: &Cluster,
    acked: &[(DataId, Bytes)],
) -> bool {
    assert_cluster_invariants(cluster);
    let boundary_exercised = assert_unstable_above_watermark(cluster).await;
    assert_durability_chain(cluster, acked, durability_deadline()).await;
    boundary_exercised
}

/// The staged durability chain, per CLAUDE.md §8:
///
/// ```text
/// quorum-acked ⇒ in replica log ⇒ covered by a snapshot ⇒ in stable log
/// ```
///
/// Keyed strictly on `acked` — the writes that returned `Ok`, each paired with
/// the bytes that were written. A failed `Writer::write` proves nothing ("an
/// error means quorum was not observed, not that nothing was written"), so a
/// failed write's absence is never asserted.
///
/// **Content, not presence.** Carrying the payload is what lets this catch a
/// wrong-bytes-under-the-right-id bug; a `DataId`-only chain passes such a bug
/// silently. `durability_chain_rejects_wrong_payload` is the teeth check.
///
/// Every node must eventually carry every acked entry in its stable log. This
/// is the eventual half, so it polls to `deadline`.
pub(crate) async fn assert_durability_chain(
    cluster: &Cluster,
    acked: &[(DataId, Bytes)],
    deadline: Duration,
) {
    let all: BTreeSet<NodeId> = cluster.node_ids();
    assert_durability_chain_on(cluster, acked, deadline, &all).await;
}

/// The durability chain over an explicit node set.
///
/// The 7-node replacement spike scopes its claim to the new replicated set
/// `N`: a voter-only node outside the replicated set has no local-store
/// subscription to the replica logs (the subscriber manager subscribes to
/// `replica.current() ∪ next()` only), so it is not a stable-log carrier and
/// asserting against it would be asserting the wrong thing.
pub(crate) async fn assert_durability_chain_on(
    cluster: &Cluster,
    acked: &[(DataId, Bytes)],
    deadline: Duration,
    nodes: &BTreeSet<NodeId>,
) {
    if acked.is_empty() {
        return;
    }
    let want: BTreeMap<DataId, Bytes> = acked.iter().cloned().collect();
    for node_id in nodes {
        let id = *node_id;
        let node = cluster.node(id);
        let reader = node.reader();
        // `sync_quorum` fences the first link: after it returns, an immediate
        // read observes everything that was on a quorum when it started.
        reader.sync().await.expect("sync_quorum");

        // `seen` lives outside the timeout closure so a failure can report
        // the real difference — exactly which acked ids never arrived —
        // rather than the whole `want` set. `stable_tail` chains a
        // historical replay with a live tail that only ends when the node
        // shuts down, so a single pass that stops as soon as every wanted id
        // has been seen is the only shape that can terminate; there is no
        // second iteration to retry, so there is nothing to loop around.
        let seen: BTreeMap<DataId, Bytes> = BTreeMap::new();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(seen));
        let found = tokio::time::timeout(deadline, {
            let seen = seen.clone();
            let want = want.clone();
            async move {
                let mut stream = reader.subscribe_stable_log(None).into_channel().filter_map(|item| {
                    match item {
                        Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                        Ok(StableLogEvent::Watermark(_)) => None,
                        Err(e) => Some(Err(e)),
                    }
                }).into_test_stream();
                loop {
                    // `want ⊆ seen` — the direction matters. Written the other
                    // way round (`seen ⊆ want`) it is trivially true on the
                    // empty `seen` and this returns without reading anything.
                    let have_all = {
                        let g = seen
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        want.keys().all(|id| g.contains_key(id))
                    };
                    if have_all {
                        return;
                    }
                    let Some(item) = stream.next().await else {
                        return;
                    };
                    if let Ok((_, d)) = item {
                        if let Some(expected) = want.get(&d.data_id) {
                            assert_eq!(
                                &d.blob,
                                expected,
                                "node {id}: stable-log payload for {:?} does not match what was acked",
                                d.data_id
                            );
                        }
                        seen.lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(d.data_id, d.blob);
                    }
                }
            }
        })
        .await;
        // The guard's scope ends here: the report below awaits.
        let missing: BTreeSet<DataId> = {
            let seen = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            want.keys()
                .copied()
                .filter(|id| !seen.contains_key(id))
                .collect()
        };
        if !(found.is_ok() && missing.is_empty()) {
            let report = explain_missing(cluster, id, &missing, found.is_err()).await;
            panic!(
                "node {id}: {} of {} acked entries never reached the stable log: {missing:?}\n{report}",
                missing.len(),
                want.len()
            );
        }
    }
}

/// CLAUDE.md §12: `unstable_tail` emits only entries *strictly above* the
/// watermark — one at or below it would duplicate `stable_tail`.
///
/// Both sides of this check derive their bound from the same source:
/// `reader.stable_watermark()` and `unstable_tail`'s internal
/// `stream_above_hlc` bound are each `stable_log.end_hlc()`
/// (`learner/read.rs`). So this can only fail if the range comparison inside
/// `stream_above_hlc` itself is wrong — it cannot catch a watermark that was
/// committed to the wrong value, since both sides would agree on the wrong
/// value together. Immediate, not eventual: it must hold at every instant, so
/// it is asserted directly rather than polled.
///
/// Returns whether any node supplied a comparison at all: a node whose
/// `stable_watermark` is `None` is skipped and a node whose `unstable_tail` is
/// empty passes trivially, so `false` means nothing was checked. The walk is
/// the only place that knows this, which is why it reports it rather than
/// leaving a caller to re-walk the same two streams.
pub(crate) async fn assert_unstable_above_watermark(cluster: &Cluster) -> bool {
    let mut exercised = false;
    for node in cluster.nodes() {
        let id = node.node_id();
        let reader = node.reader();
        let Some(watermark) = reader.stable_watermark() else {
            continue; // nothing promoted yet; the boundary is vacuous
        };
        let mut stream = reader.unstable_tail().into_test_stream();
        while let Some(item) = stream.next().await {
            exercised = true;
            if let Ok(d) = item {
                let hlc = d.data_id.hlc();
                assert!(
                    hlc > watermark,
                    "node {id}: unstable_tail emitted {hlc:?} at or below \
                     watermark {watermark:?} — duplicates stable_tail"
                );
            }
        }
    }
    exercised
}

/// CLAUDE.md §8: the manifest tiles the index space from 0 with no gap and no
/// overlap. A consumer (`learner`'s seamless append) relies on this.
fn assert_manifest_contiguous(id: zslg_consensus::NodeId, state: &RaftState) {
    let mut expected: u64 = 0;
    for meta in &state.manifest_state.manifest {
        assert_eq!(
            meta.start_index.raw(),
            expected,
            "node {id}: manifest not contiguous at snapshot {:?}",
            meta.snapshot_id
        );
        assert!(
            meta.end_index >= meta.start_index,
            "node {id}: manifest entry {:?} ends before it starts",
            meta.snapshot_id
        );
        expected = meta
            .end_index
            .raw()
            .checked_add(1)
            .expect("manifest index overflow");
    }
}

/// CLAUDE.md §12: a committed `manifest_watermark > hlc_cutoff` would admit
/// writes no snapshot can ever cover.
fn assert_watermark_below_cutoff(id: zslg_consensus::NodeId, state: &RaftState) {
    if let (Some(w), Some(c)) = (
        state.manifest_state.manifest_watermark,
        state.manifest_state.hlc_cutoff,
    ) {
        assert!(
            w <= c,
            "node {id}: manifest_watermark {w:?} > hlc_cutoff {c:?}"
        );
    }
}

/// A joint config's two sets must differ — otherwise the transition is a no-op
/// that can never make progress toward collapse.
pub(crate) fn assert_membership_well_formed(id: zslg_consensus::NodeId, state: &RaftState) {
    match &state.replica {
        ReplicaMembership::Single { .. } => {}
        ReplicaMembership::Joint { old, next }
        | ReplicaMembership::JointDraining { old, next, .. } => {
            assert!(
                !old.is_empty(),
                "node {id}: joint config with empty old set"
            );
            assert!(
                !next.is_empty(),
                "node {id}: joint config with empty next set"
            );
            assert_ne!(old, next, "node {id}: joint config where old == next");
        }
    }
}

/// CLAUDE.md §8: a `JointDraining` whose drain is satisfied should have
/// collapsed. Observing one still armed at quiesce means the leader's step 3
/// is not firing — the drain bound is doing its job, but nothing is consuming
/// it, which stalls membership change indefinitely.
fn assert_no_satisfied_drain_left_armed(id: zslg_consensus::NodeId, state: &RaftState) {
    if let ReplicaMembership::JointDraining { exit, .. } = &state.replica {
        assert!(
            !zslg_consensus::drain_satisfied(state.manifest_state.manifest_watermark, *exit),
            "node {id}: drain satisfied but config still joint — collapse is not firing"
        );
    }
}

/// What the cluster knows about entries a node's stable log is missing, to tell
/// two very different failures apart:
///
/// - **not yet stable** — the entry is on the replicas (in the unstable tail)
///   and the stable watermark has not reached it: a liveness question;
/// - **skipped** — the stable watermark is at or past the entry's HLC, yet the
///   entry is in no stable log: a data-safety violation, whatever the timing.
async fn explain_missing(
    cluster: &Cluster,
    on: NodeId,
    missing: &BTreeSet<DataId>,
    timed_out: bool,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "  the wait {} (checking node {on})",
        if timed_out {
            "hit its deadline"
        } else {
            "ended because the stable stream ended"
        }
    );
    let nodes = cluster.node_ids();
    let mut stable_on: BTreeMap<NodeId, BTreeSet<DataId>> = BTreeMap::new();
    let mut unstable_on: BTreeMap<NodeId, BTreeSet<DataId>> = BTreeMap::new();
    let mut watermark: BTreeMap<NodeId, Option<zslg_time::Hlc>> = BTreeMap::new();
    for node_id in &nodes {
        let reader = cluster.node(*node_id).reader();
        watermark.insert(*node_id, reader.stable_watermark());
        let tail = tokio::time::timeout(Duration::from_secs(5), {
            let mut stream = reader.unstable_tail().into_test_stream();
            async move {
                let mut got = BTreeSet::new();
                while let Some(item) = stream.next().await {
                    if let Ok(d) = item {
                        got.insert(d.data_id);
                    }
                }
                got
            }
        })
        .await
        .unwrap_or_default();
        unstable_on.insert(*node_id, tail);
        let stable = tokio::time::timeout(Duration::from_secs(3), {
            let mut stream = reader
                .subscribe_stable_log(None)
                .into_channel()
                .into_test_stream();
            let missing = missing.clone();
            async move {
                let mut got = BTreeSet::new();
                while let Some(item) = stream.next().await {
                    if let Ok(StableLogEvent::Appended { data, .. }) = item
                        && missing.contains(&data.data_id)
                    {
                        got.insert(data.data_id);
                        if got.len() == missing.len() {
                            break;
                        }
                    }
                }
                got
            }
        })
        .await
        .unwrap_or_default();
        stable_on.insert(*node_id, stable);
    }
    // Is it late, or lost? Keep watching the checked node's stable log, with
    // the cluster quiet, and say when each missing entry shows up (if ever).
    let late = late_arrivals(cluster, on, missing, Duration::from_secs(60)).await;
    for (node_id, wm) in &watermark {
        let _ = writeln!(out, "  node {node_id}: stable watermark {wm:?}");
    }
    for id in missing {
        let hlc = id.hlc();
        let stable_nodes: Vec<_> = nodes
            .iter()
            .filter(|n| stable_on[*n].contains(id))
            .collect();
        let unstable_nodes: Vec<_> = nodes
            .iter()
            .filter(|n| unstable_on[*n].contains(id))
            .collect();
        let passed: Vec<_> = nodes
            .iter()
            .filter(|n| watermark[*n].is_some_and(|w| w >= hlc))
            .collect();
        let verdict = if stable_nodes.is_empty() && unstable_nodes.is_empty() {
            "IN NO LOG AT ALL"
        } else if !passed.is_empty() && stable_nodes.len() < nodes.len() {
            "WATERMARK PASSED IT, STABLE LOG LACKS IT"
        } else {
            "not stable yet"
        };
        let arrival = match late.get(id) {
            Some(after) => {
                format!("ARRIVED in node {on}'s stable log {after:?} after the deadline")
            }
            None => format!("NEVER ARRIVED in node {on}'s stable log within 60 s more"),
        };
        let _ = writeln!(
            out,
            "  {id:?} (hlc {hlc:?}): stable on {stable_nodes:?}, unstable on {unstable_nodes:?}, \
             watermark at/after it on {passed:?} => {verdict}; {arrival}"
        );
    }
    out
}

/// When each of `missing` appears in `on`'s stable log, measured from now;
/// entries that never appear within `within` are absent from the result.
async fn late_arrivals(
    cluster: &Cluster,
    on: NodeId,
    missing: &BTreeSet<DataId>,
    within: Duration,
) -> BTreeMap<DataId, Duration> {
    let started = std::time::Instant::now();
    let arrived = std::sync::Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let reader = cluster.node(on).reader();
    let _ = tokio::time::timeout(within, {
        let arrived = arrived.clone();
        let missing = missing.clone();
        let mut stream = reader
            .subscribe_stable_log(None)
            .into_channel()
            .into_test_stream();
        async move {
            while let Some(item) = stream.next().await {
                if let Ok(StableLogEvent::Appended { data, .. }) = item
                    && missing.contains(&data.data_id)
                {
                    let mut g = arrived
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    g.entry(data.data_id).or_insert_with(|| started.elapsed());
                    if g.len() == missing.len() {
                        return;
                    }
                }
            }
        }
    })
    .await;
    let g = arrived
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    g.clone()
}
