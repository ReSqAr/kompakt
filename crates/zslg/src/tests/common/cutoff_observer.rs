//! Records each replica's committed HLC cutoff across a run.
//!
//! # What this proves
//!
//! CLAUDE.md §8: a replica's HLC cutoff may never go backwards. That is a
//! *temporal* property — a post-quiesce check sees only the final cutoff,
//! which is trivially consistent with itself — so it needs an observer.
//!
//! The invariant is now also enforced where the state is written
//! (`raft_storage::validate_cutoff_advance` → `RaftStateError::CutoffRegressed`),
//! which sees every transition with no channel in between. This observer is
//! therefore corroboration rather than the sole check: it exercises the rule
//! end to end, across real leader changes and faults, against states the fence
//! actually accepted.
//!
//! # Push, not poll
//!
//! The observer watches each node's published state
//! ([`RaftStateHandle::subscribe`]). One receiver per node, created at spawn
//! and re-created only by [`CutoffObserver::refresh`], so it wakes on a change
//! rather than polling for one.
//!
//! It does **not** see every individual transition: the channel is a watch, so
//! changes that land while this task is busy coalesce into one wake-up carrying
//! the latest value. That is deliberate, and the completeness it gives up is
//! made up for at the write point — a regressing cutoff is rejected before it
//! can be committed (`raft_storage::validate_cutoff_advance`), which sees every
//! transition with no channel in between. So a transient dip cannot exist for
//! this observer to miss.
//!
//! Scope is the *committed, observable* cutoff. It does not check the
//! in-memory `HlcGate` no-op on a backwards target (CLAUDE.md §12) — that is
//! `hlc/gate.rs`'s own tier-A concern and is covered there.
//!
//! # Why the history is keyed by `NodeId` and survives a fault
//!
//! A `RaftStateHandle` belongs to one `ReplicaNode` *instance*, and a restart
//! or a kill mints a new instance — same `NodeId`, new handle. Holding the
//! handles for the life of a run would therefore leave the observer listening
//! to a dead instance after the first fault, and rebuilding the whole observer
//! around each fault would split the history into per-window fragments, so a
//! regression *straddling* a fault would fall between two of them.
//!
//! [`CutoffObserver::refresh`] swaps the handle set in place instead. The
//! recorded sequences are keyed by `NodeId` and accumulate across the whole
//! run, so the value a node last published before it went down and the first
//! one it publishes after it returns are adjacent in one sequence — which is
//! exactly the comparison CLAUDE.md §8 cares about, since a node resuming from
//! persisted state is the case where a regression would matter most.
//!
//! A node that is down publishes nothing and is simply absent from the handle
//! set until it returns; its sequence resumes where it left off.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::Hlc;
use crate::ids::NodeId;
use crate::meta::RaftState;
use crate::raft::RaftStateHandle;
use crate::tests::common::Cluster;
use crate::tests::common::invariants::assert_membership_well_formed;

/// Every node's state handle, paired with its `NodeId`.
type Handles = Vec<(NodeId, RaftStateHandle)>;

/// The handle set plus the generation [`CutoffObserver::refresh`] bumps when
/// it swaps them, which is how the observer task knows to re-subscribe.
type HandleSet = (u64, Handles);

pub(crate) struct CutoffObserver {
    seen: Arc<Mutex<BTreeMap<NodeId, Vec<Hlc>>>>,
    /// How many observed states were in a non-`Single` replica membership.
    ///
    /// Piggybacked on this task rather than given its own: the two samples
    /// want the same event stream and the same handle set. It answers a
    /// question the caller cannot answer post-hoc — whether a generated script
    /// ever *reached* a joint config, or whether membership change completed
    /// between two observations and the joint-config assertions in
    /// [`super::invariants`] are dead.
    ///
    /// A sample count, not an exact event count: a joint config that is
    /// entered and left between two wake-ups is not counted, so a zero here
    /// means "no joint config was observed", which is the question the caller
    /// is asking — whether the joint-config assertions ran at all.
    joint_polls: Arc<Mutex<usize>>,
    /// `(generation, handles)`. [`Self::refresh`] bumps the generation and
    /// swaps the handle set; the task re-subscribes when it sees a new
    /// generation.
    handles: Arc<Mutex<HandleSet>>,
    stop: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

fn handles_of(cluster: &Cluster) -> Handles {
    cluster
        .nodes()
        .map(|n| (n.node_id(), n.state_handle().clone()))
        .collect()
}

impl CutoffObserver {
    /// Record one observed state into the histories.
    ///
    /// Runs `assert_membership_well_formed` on every joint observation — safe
    /// mid-flight because non-empty `old`/`next` and `old != next` are
    /// write-point invariants, not merely quiesced ones.
    fn observe(
        id: NodeId,
        snap: &RaftState,
        seen: &Arc<Mutex<BTreeMap<NodeId, Vec<Hlc>>>>,
        joint_polls: &Arc<Mutex<usize>>,
    ) {
        if snap.replica.is_joint() {
            assert_membership_well_formed(id, snap);
            let mut g = joint_polls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *g = g.saturating_add(1);
        }
        if let Some(c) = snap.manifest_state.hlc_cutoff {
            let mut g = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let e = g.entry(id).or_default();
            // Record transitions only; a repeated value is not a move and
            // would bloat the log.
            if e.last() != Some(&c) {
                e.push(c);
            }
        }
    }

    /// Subscribe to every node's state broadcast until stopped.
    ///
    /// One observer spans a whole run: call [`Self::refresh`] after a fault
    /// rather than building a second one, so the recorded history stays
    /// continuous across it.
    pub(crate) fn spawn_for(cluster: &Cluster) -> Self {
        Self::spawn_for_handles(handles_of(cluster))
    }

    /// [`Self::spawn_for`] over an explicit handle set.
    ///
    /// Split out for the observer's own tests, which need a handle nobody else
    /// writes to. Pointing this at a live node instead means racing that
    /// node's cutoff job, whose publishes are real physical-time HLCs — they
    /// interleave with any synthetic value and dwarf it, so an exact-sequence
    /// assertion is unwritable there.
    fn spawn_for_handles(initial: Handles) -> Self {
        let seen: Arc<Mutex<BTreeMap<NodeId, Vec<Hlc>>>> = Arc::default();
        let joint_polls: Arc<Mutex<usize>> = Arc::default();
        let handles = Arc::new(Mutex::new((0u64, initial)));
        let stop = CancellationToken::new();
        let handle = {
            let (seen, joint_polls, handles, stop) = (
                seen.clone(),
                joint_polls.clone(),
                handles.clone(),
                stop.clone(),
            );
            tokio::spawn(async move {
                let mut subscribed_generation: Option<u64> = None;
                let mut receivers: Vec<(NodeId, tokio::sync::watch::Receiver<Arc<RaftState>>)> =
                    Vec::new();
                loop {
                    // Cloned out under the lock so no guard is held across the
                    // `.await` below (CLAUDE.md §4).
                    let (generation, current): HandleSet = {
                        let g = handles
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        (g.0, g.1.clone())
                    };
                    if subscribed_generation != Some(generation) {
                        // Subscribe FIRST, then baseline from the snapshot: the
                        // receiver covers everything after the subscribe and
                        // the snapshot everything up to it, so the two together
                        // leave no window. The other order would drop anything
                        // published between the read and the subscribe.
                        receivers = current.iter().map(|(id, h)| (*id, h.subscribe())).collect();
                        subscribed_generation = Some(generation);
                        for (id, h) in &current {
                            Self::observe(*id, &h.snapshot(), &seen, &joint_polls);
                        }
                    }
                    if receivers.is_empty() {
                        tokio::select! {
                            () = stop.cancelled() => return,
                            () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
                        }
                        continue;
                    }
                    // Wait for the next publish on ANY node. The receivers are
                    // persistent across iterations, so a state published while
                    // this task was busy is seen on the next poll.
                    let mut pending = futures::stream::FuturesUnordered::new();
                    for (id, rx) in &mut receivers {
                        pending.push(async move { (*id, rx.changed().await, rx.borrow().clone()) });
                    }
                    let next = tokio::select! {
                        () = stop.cancelled() => return,
                        item = pending.next() => item,
                    };
                    // Drop the borrow of `receivers` before the next iteration
                    // re-subscribes.
                    drop(pending);
                    match next {
                        Some((id, Ok(()), state)) => {
                            Self::observe(id, &state, &seen, &joint_polls);
                        }
                        // A closed sender means that node's handle is gone.
                        // `refresh` is what replaces it; until then there is
                        // nothing to do but wait, and yielding here keeps this
                        // from spinning on a permanently-ready receiver.
                        Some((_, Err(_), _)) | None => {
                            tokio::select! {
                                () = stop.cancelled() => return,
                                () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
                            }
                        }
                    }
                }
            })
        };
        Self {
            seen,
            joint_polls,
            handles,
            stop,
            handle,
        }
    }

    /// Point the observer at `cluster`'s current node instances, keeping the
    /// history recorded so far.
    ///
    /// Call after any fault that replaces an instance — a relaunch, a hard
    /// kill's relaunch, or a node replacement. Without it the observer keeps
    /// listening to the departed instance's channel, which never publishes
    /// again.
    pub(crate) fn refresh(&self, cluster: &Cluster) {
        let mut g = self
            .handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.0 = g.0.saturating_add(1);
        g.1 = handles_of(cluster);
    }

    /// How many observed states were in a non-`Single` replica membership.
    /// See the field's note for what a zero here does and does not mean.
    pub(crate) fn joint_polls(&self) -> usize {
        *self
            .joint_polls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Assert the recorded sequence never decreases, per node.
    pub(crate) fn assert_monotonic(&self) {
        let g = self
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (id, seq) in g.iter() {
            for w in seq.windows(2) {
                assert!(
                    w[1] >= w[0],
                    "node {id}: committed cutoff regressed {:?} -> {:?}",
                    w[0],
                    w[1]
                );
            }
        }
    }

    pub(crate) async fn stop(self) {
        self.stop.cancel();
        let _ = self.handle.await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    /// Step 3: the observer has teeth. Feed it a deliberately decreasing
    /// synthetic sequence (bypassing the subscription entirely) and confirm
    /// `assert_monotonic` panics. Forcing a *real* cutoff regression would
    /// mean breaking the write-point validator — a much larger break than this
    /// assertion needs.
    #[tokio::test]
    #[should_panic(expected = "committed cutoff regressed")]
    async fn assert_monotonic_panics_on_decrease() {
        let node = crate::tests::nid(1);
        let mut seen = BTreeMap::new();
        seen.insert(node, vec![Hlc(10), Hlc(5)]);
        let observer = CutoffObserver {
            seen: Arc::new(Mutex::new(seen)),
            joint_polls: Arc::default(),
            handles: Arc::new(Mutex::new((0, Vec::new()))),
            stop: CancellationToken::new(),
            handle: tokio::spawn(async {}),
        };
        observer.assert_monotonic();
    }

    /// The observer is push-driven and converges on the published value
    /// without polling for it.
    ///
    /// It no longer claims to see *every* transition: the state channel is a
    /// watch, so two publishes in quick succession may coalesce into one
    /// wake-up carrying the later value. That claim moved to where it is
    /// unconditional — the write point rejects a regressing cutoff outright
    /// (`raft_storage::validate_cutoff_advance`), so a transient dip cannot be
    /// committed for anyone to miss. What remains here is corroboration: a
    /// history sampled across real leader changes and faults, checked for a
    /// regression between any two values it did record.
    #[tokio::test(flavor = "multi_thread")]
    async fn observer_converges_on_the_published_cutoff() {
        let node_id = crate::tests::nid(1);
        let handle = RaftStateHandle::new();
        let observer = CutoffObserver::spawn_for_handles(vec![(node_id, handle.clone())]);

        let publish = |hlc: Hlc| {
            let mut snap = (*handle.snapshot()).clone();
            snap.manifest_state.hlc_cutoff = Some(hlc);
            handle.replace(snap);
        };

        publish(Hlc(1));
        publish(Hlc(2));
        publish(Hlc(3));

        crate::tests::wait_until_sync("latest publish observed", Duration::from_secs(5), || {
            observer
                .seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&node_id)
                .is_some_and(|seq| seq.last() == Some(&Hlc(3)))
        })
        .await;

        {
            let g = observer
                .seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let seq = g.get(&node_id).expect("cutoff history");
            assert!(
                seq.iter().all(|h| [Hlc(1), Hlc(2), Hlc(3)].contains(h)),
                "only published values are recorded: {seq:?}"
            );
        }

        observer.assert_monotonic();
        observer.stop().await;
    }

    /// A joint state observed live must satisfy `assert_membership_well_formed`
    /// and be counted. The battery cannot see a joint config — it runs only
    /// after `wait_for_convergence`, whose predicate is "nobody is joint" — so
    /// this is where that assertion earns its keep.
    #[tokio::test(flavor = "multi_thread")]
    async fn joint_states_are_counted_and_checked_live() {
        use crate::meta::ReplicaMembership;

        let node_id = crate::tests::nid(1);
        let handle = RaftStateHandle::new();
        let observer = CutoffObserver::spawn_for_handles(vec![(node_id, handle.clone())]);

        let mut snap = (*handle.snapshot()).clone();
        snap.replica = ReplicaMembership::joint(
            [crate::tests::nid(1)].into_iter().collect(),
            [crate::tests::nid(2)].into_iter().collect(),
        );
        handle.replace(snap);

        crate::tests::wait_until_sync("joint state observed", Duration::from_secs(5), || {
            observer.joint_polls() > 0
        })
        .await;

        observer.stop().await;
    }
}
