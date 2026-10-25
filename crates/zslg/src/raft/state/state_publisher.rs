//! Feeds the raft gauges and the leadership-transition logs from openraft's
//! metrics stream plus the applied [`RaftStateHandle`].
//!
//! The manifest-watermark gauge is stamped only on an *advance observed while
//! running*: the baseline is taken from the already-applied state so a restart
//! does not stamp a historical snapshot as "just now" (the snapshot-age alert
//! reads `time() - zslg_manifest_watermark_advanced_timestamp_seconds`).
use std::collections::BTreeSet;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::NodeId;
use crate::metrics;
use crate::raft::RaftStateHandle;

/// The voter set as openraft's metrics report it.
///
/// This is the *effective* membership: while openraft is itself in a joint
/// config, `voter_ids` yields the union of both of its configs, which is the
/// right answer for every consumer here — a node in either half can vote, so a
/// node in either half is one the replica set may target.
fn voters_of(metrics: &openraft::RaftMetrics<NodeId, crate::raft::NodeInfo>) -> BTreeSet<NodeId> {
    metrics.membership_config.membership().voter_ids().collect()
}

pub(in crate::raft) struct RaftStatePublisher {
    raft_state: RaftStateHandle,
    raft: Arc<crate::raft::RaftNode>,
    self_node_id: NodeId,
}

impl RaftStatePublisher {
    pub(in crate::raft) fn new(
        raft_state: RaftStateHandle,
        raft: Arc<crate::raft::RaftNode>,
        self_node_id: NodeId,
    ) -> Self {
        Self {
            raft_state,
            raft,
            self_node_id,
        }
    }

    pub(in crate::raft) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        tracing::info!("raft state publisher: starting");
        let mut metrics_rx = self.raft.inner().metrics();
        let mut state_rx = self.raft_state.subscribe();
        let mut prev_leader = metrics_rx.borrow().current_leader;
        {
            let metrics = metrics_rx.borrow().clone();
            self.raft_state.blend_raft_metrics(
                metrics.current_leader,
                metrics.current_term,
                voters_of(&metrics),
            );
        }
        // Baseline the manifest watermark from the already-applied state so a
        // restart doesn't stamp a historical snapshot as "just now": only
        // advances observed while running stamp the snapshot-age gauge.
        let mut prev_watermark = self.raft_state.snapshot().manifest_state.manifest_watermark;
        // Populate the gauges before the first scrape.
        self.emit_leadership_gauges(&mut prev_leader);
        self.emit_raft_state_gauges(&mut prev_watermark);
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::info!("raft state publisher: shutdown");
                    return Ok(());
                }
                changed = metrics_rx.changed() => {
                    if changed.is_err() {
                        tracing::info!("raft state publisher: raft metrics closed");
                        return Ok(());
                    }
                    let metrics = metrics_rx.borrow().clone();
                    self.raft_state.blend_raft_metrics(
                        metrics.current_leader,
                        metrics.current_term,
                        voters_of(&metrics),
                    );
                    self.emit_leadership_gauges(&mut prev_leader);
                }
                changed = state_rx.changed() => match changed {
                    // The state is read from the handle, so one publish covers
                    // however many changes were coalesced into this wake-up.
                    Ok(()) => self.emit_raft_state_gauges(&mut prev_watermark),
                    Err(_) => {
                        // Unreachable while we hold the handle (and thus the
                        // sender), but harmless to handle.
                        tracing::debug!("raft state publisher: state channel closed");
                        return Ok(());
                    }
                },
            }
        }
    }

    fn emit_leadership_gauges(&self, prev_leader: &mut Option<NodeId>) {
        let current_leader = self.raft.inner().metrics().borrow().current_leader;
        if current_leader != *prev_leader {
            // Leadership transition. The `zslg_raft_has_leader` gauge shows *current*
            // state across the fleet; these logs give the transition + timing so
            // "cluster fell apart" (a Some→None flip) is loud at RUST_LOG=warn.
            match (*prev_leader, current_leader) {
                (Some(prev), None) => tracing::warn!(
                    node_id = ?self.self_node_id,
                    prev_node_id = ?prev,
                    "raft: became leaderless — no current leader (lost quorum or leader down)"
                ),
                (None, Some(leader)) => tracing::info!(
                    node_id = ?self.self_node_id,
                    leader_node_id = ?leader,
                    "raft: leader acquired"
                ),
                (Some(prev), Some(leader)) => tracing::info!(
                    node_id = ?self.self_node_id,
                    prev_node_id = ?prev,
                    leader_node_id = ?leader,
                    "raft: leader changed"
                ),
                (None, None) => {}
            }
            // Preserve the existing metric semantics: only count a change away
            // from a previously-known leader.
            if prev_leader.is_some() {
                metrics::raft_leader_change();
            }
        }
        *prev_leader = current_leader;

        let is_leader = current_leader == Some(self.self_node_id);
        metrics::raft_is_leader(is_leader);
        metrics::raft_has_leader(current_leader.is_some());
        metrics::leader_tasks_active(is_leader);
    }

    #[allow(
        clippy::expect_used,
        reason = "usize→u64 conversion on bounded values (node counts, manifest length)"
    )]
    fn emit_raft_state_gauges(&self, prev_watermark: &mut Option<Hlc>) {
        let state = self.raft_state.snapshot();

        // Raft membership — read from the applied state snapshot we already
        // hold instead of allocating a fresh voter set per event.
        let is_member = state.replica.current().contains(&self.self_node_id)
            || state
                .replica
                .next()
                .is_some_and(|s| s.contains(&self.self_node_id));
        metrics::raft_is_member(is_member);
        metrics::raft_term(state.raft_term);
        metrics::raft_config_generation(state.generation.raw());
        metrics::raft_in_joint_config(state.replica.is_joint());
        metrics::membership_joint_exit_hlc_physical_ms(state.replica.exit());
        let replica_count: u64 = {
            let all = state.replica.current().len();
            let new = state.replica.next().map_or(0, |s| s.len());
            u64::try_from(if all >= new { all } else { new }).expect("usize ≤ u64")
        };
        metrics::replica_nodes_count(replica_count);

        // Manifest / cutoff / watermark
        metrics::manifest_entries_count(
            u64::try_from(state.manifest_state.manifest.len()).expect("manifest len ≤ u64"),
        );
        if let Some(cutoff) = state.manifest_state.hlc_cutoff {
            metrics::hlc_cutoff_physical_ms(cutoff);
        }
        if let Some(watermark) = state.manifest_state.manifest_watermark {
            metrics::manifest_end_hlc_physical_ms(watermark);
            // Stamp the snapshot-age gauge only on a genuine advance (a snapshot
            // committed + replicated). Grafana reads it as
            // `time() - zslg_manifest_watermark_advanced_timestamp_seconds`.
            if prev_watermark.is_none_or(|prev| watermark > prev) {
                metrics::manifest_watermark_advanced();
                *prev_watermark = Some(watermark);
            }
        }
    }
}
