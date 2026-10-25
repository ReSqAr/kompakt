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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;

use crate::StableLogEvent;
use crate::ids::{DataId, LogIndex, NodeId};
use crate::meta::{RaftState, ReplicaMembership};
use crate::tests::common::Cluster;

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
pub(crate) async fn assert_cluster_invariants_with_acks(
    cluster: &Cluster,
    acked: &[(DataId, Bytes)],
) -> bool {
    assert_cluster_invariants(cluster);
    let boundary_exercised = assert_unstable_above_watermark(cluster).await;
    assert_durability_chain(cluster, acked, Duration::from_secs(10)).await;
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
                let mut stream = reader.subscribe_stable_log(LogIndex::ZERO).filter_map(|item| {
                    match item {
                        Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                        Ok(StableLogEvent::Watermark(_)) => None,
                        Err(e) => Some(Err(e)),
                    }
                }).into_stream();
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
        let seen = seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let missing: BTreeSet<DataId> = want
            .keys()
            .copied()
            .filter(|id| !seen.contains_key(id))
            .collect();
        assert!(
            found.is_ok() && missing.is_empty(),
            "node {id}: {} of {} acked entries never reached the stable log: {missing:?}",
            missing.len(),
            want.len()
        );
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
        let mut stream = reader.unstable_tail().into_stream();
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
fn assert_manifest_contiguous(id: crate::ids::NodeId, state: &RaftState) {
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
fn assert_watermark_below_cutoff(id: crate::ids::NodeId, state: &RaftState) {
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
pub(crate) fn assert_membership_well_formed(id: crate::ids::NodeId, state: &RaftState) {
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
fn assert_no_satisfied_drain_left_armed(id: crate::ids::NodeId, state: &RaftState) {
    if let ReplicaMembership::JointDraining { exit, .. } = &state.replica {
        assert!(
            !crate::meta::drain_satisfied(state.manifest_state.manifest_watermark, *exit),
            "node {id}: drain satisfied but config still joint — collapse is not firing"
        );
    }
}
