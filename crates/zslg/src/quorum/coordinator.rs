//! Streaming write-quorum coordinator — one per writer.
//!
//! ```text
//!                                        ┌─> fan(n1) ─> run_replica ─┐
//!   submit ─> permit ─> pending ─────────┼─> fan(n2) ─> run_replica ─┼─> outcome
//!             (blocks)  DashMap<DataId>  └─> fan(n3) ─> run_replica ─┘      │
//!                          ^                                               │
//!                          └──────── ack / neg, then re-test quorum ────────┘
//!                                        quorum ─> remove entry, resp.send
//! ```
//!
//! Quorum is decided **as acks stream in**, and `pending`'s per-key lock *is* the
//! decision lock — so a write completes the instant a majority acks, and the
//! entry disappears. A replica that arrives late finds nothing to answer for,
//! which is how a slow node skips work the majority already committed rather
//! than falling further behind.
//!
//! Nothing here waits on a replica. The feeder only `try_send`s into a fan, so a
//! stalled node cannot block the others, and a dropped id is safe: it is resent
//! from `pending` on the next reconnect and deduped downstream by `data_id`
//! (CLAUDE.md §8). That is why there is no client-side in-flight tracking.
//!
//! A submit never blocks either: it `try_send`s onto a bounded queue and a full
//! queue drops the write (`writer_submit_dropped`, surfaced as
//! `QuorumUnreachable`). Capacity is instead held by the feeder's semaphore,
//! which bounds `pending` — and with it every per-stream channel.
//!
//! Shutdown is by reference count, not by token: every loop holds a
//! `Weak<Coordinator>` and blocks only on *owned* channel handles. When the last
//! [`crate::writer::WriterService`] drops, `fans` drops with the coordinator,
//! each replica input disconnects, and the loops cascade out.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry as MapEntry;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::api::{RaftStateTracker, Router, StampedWrite, WriteOutcome};
use crate::error::Error;
use crate::ids::{DataId, NodeId};
use crate::meta::Data;
use crate::metrics::{self, FanoutResponseState, WriteRpcOutcome};
use crate::quorum::{all_targets, is_quorum};
use crossbeam_combinators::Channels;

/// Global in-flight cap. A permit is held for a write's whole life, so this
/// bounds `pending` and every per-stream channel with it (a `DataId` is 32 bytes;
/// 2048 ids ≈ 64 KiB).
///
/// It is also the un-acked window a replica must absorb, so keep it
/// ≤ `crate::replica::WRITE_OUT_CAP` (currently equal). A replica with a smaller
/// outcome channel `try_send`s past the end of the window and counts
/// `zslg_replica_outcome_dropped_total`, costing this writer a resend on reconnect —
/// a tuning relationship, not a safety one.
const MAX_INFLIGHT: usize = 2048;
/// Per-replica fan depth. Live ids are capped by `MAX_INFLIGHT`; the slack absorbs
/// *stale* ids — already quorum'd, not yet pulled — so a replica must fall a full
/// window behind before the feeder drops for it.
const FAN_CAP: usize = 2 * MAX_INFLIGHT;
/// Reconnect backoff after a broken write connection.
const REOPEN_BACKOFF: Duration = Duration::from_millis(50);
const MAX_REOPEN_BACKOFF: Duration = Duration::from_secs(2);
/// How often abandoned entries are swept. A caller that gave up while *no* replica
/// ever answered is the one path that leaves an entry with nothing to remove it.
const REAP_INTERVAL: Duration = Duration::from_secs(1);

/// One outstanding write. Both tallies are load-bearing and neither derives from
/// the other: `acks` decides quorum, `negatives` decides when quorum has become
/// *impossible* and the write can fail early rather than wait out its deadline.
struct Entry {
    data: Data,
    resp: oneshot::Sender<Result<(), Error>>,
    /// The in-flight slot, returned to the semaphore when this drops — on
    /// quorum, fast-fail, or reap alike.
    _permit: OwnedSemaphorePermit,
    enqueued: Instant,
    acks: BTreeSet<NodeId>,
    negatives: BTreeSet<NodeId>,
}

/// One item in the coordinator's submit channel: the write, its completion
/// channel, and the enqueue time (for the submit_wait residency probe).
type SubmitItem = (
    Data,
    tokio::sync::oneshot::Sender<Result<(), Error>>,
    std::time::Instant,
);

pub(crate) struct Coordinator {
    pending: Arc<PendingWrites>,
    streams: StreamOrchestrator,
    submit_tx: mpsc::Sender<SubmitItem>,
}

impl Coordinator {
    #[must_use]
    pub(crate) fn new(router: Arc<dyn Router>, tracker: Arc<dyn RaftStateTracker>) -> Arc<Self> {
        let inflight = Arc::new(Semaphore::new(MAX_INFLIGHT));
        let pending = Arc::new(PendingWrites::new(tracker.clone()));
        let streams = StreamOrchestrator::new(router, tracker);
        let (submit_tx, mut submit_rx) = mpsc::channel(MAX_INFLIGHT);

        let coord = Arc::new(Self {
            pending: pending.clone(),
            streams,
            submit_tx,
        });

        for node in all_targets(&coord.streams.tracker().snapshot().replica) {
            let rx = coord.streams.ensure(node);
            let weak = Arc::downgrade(&coord);
            tokio::task::spawn_blocking(move || run_replica(weak, node, rx));
        }

        // Async, not `spawn_blocking`: nothing in this loop blocks — `insert` is a
        // DashMap write, `reconcile_if_newer` an atomic CAS, the fan sends are
        // `try_send`. Only the semaphore and the queue ever make it wait.
        let sem = inflight.clone();
        let weak_feed = Arc::downgrade(&coord);
        tokio::spawn(async move {
            while let Some((data, resp, enqueued)) = submit_rx.recv().await {
                metrics::coordinator_submit_wait(enqueued.elapsed());
                let permit_t0 = std::time::Instant::now();
                let Ok(permit) = sem.clone().acquire_owned().await else {
                    return;
                };
                metrics::coordinator_permit_wait(permit_t0.elapsed());
                let Some(this) = weak_feed.upgrade() else {
                    return;
                };
                this.streams.reconcile_if_newer(weak_feed.clone());
                let id = data.data_id;
                this.pending.insert(data, resp, permit);
                for (_, fan) in this.streams.fan_senders() {
                    if fan.try_send(id).is_err() {
                        metrics::writer_fan_full();
                    }
                }
            }
        });

        let weak_reap = Arc::downgrade(&coord);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REAP_INTERVAL);
            loop {
                tick.tick().await;
                let Some(this) = weak_reap.upgrade() else {
                    return;
                };
                this.pending.reap_abandoned();
                // Derived, not tracked: a gauge computed from the semaphore can't
                // drift the way a hand-maintained inc/dec pair can.
                metrics::writer_inflight_permits_set(
                    MAX_INFLIGHT.saturating_sub(inflight.available_permits()),
                );
            }
        });

        coord
    }

    #[must_use]
    pub(crate) fn submit(&self, data: Data) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        let enqueued = std::time::Instant::now();
        if self.submit_tx.try_send((data, tx, enqueued)).is_err() {
            metrics::writer_submit_dropped();
        }
        rx
    }
}

/// One replica's write connection, for the lifetime of the coordinator: prime
/// from `pending`, stream, apply outcomes, reconnect with backoff. Returns only
/// when the coordinator is gone or the node has left the replica set.
///
/// Priming before merging the fan is what makes a reconnect self-healing —
/// whatever this node missed while disconnected is still in `pending`.
fn run_replica(weak: Weak<Coordinator>, node: NodeId, mut ids_rx: Receiver<DataId>) {
    let mut backoff = REOPEN_BACKOFF;
    loop {
        let Some(this) = weak.upgrade() else {
            return;
        };
        let (prime_tx, prime_rx) = crossbeam_channel::bounded(MAX_INFLIGHT);
        for data in this.pending.iter_unacked_data(node) {
            let _ = prime_tx.try_send(data);
        }
        drop(prime_tx);

        let pending = this.pending.clone();
        // Both finite-by-drop rather than by declaration: the prime set is fully
        // sent above, and the fan is replaced wholesale on reopen. Nothing reads
        // this stream's verdict — an un-acked write is resent from `pending`.
        let (prime_ch, prime_closer) = Channels::from(prime_rx, "coordinator prime");
        prime_closer.close();
        // Infinite: replaced wholesale on reopen, never completed.
        let ids_ch = Channels::from_infinite(ids_rx, "coordinator fan");
        let prime_stamped = prime_ch.filter_map(|data| Some(StampedWrite::now(data)));
        let ids_stamped = ids_ch.filter_map(move |id| {
            if let Some(wait) = pending.elapsed_since_enqueued(&id) {
                metrics::coordinator_fan_wait(wait);
            }
            pending.get_data(&id).map(StampedWrite::now)
        });
        let input = prime_stamped.merge(ids_stamped);

        metrics::writer_replica_reopen();
        let mut outcomes = this.streams.router().remote(node).write(input);
        drop(this);

        while let Ok(Ok(outcome)) = outcomes.recv() {
            let Some(this) = weak.upgrade() else {
                return;
            };
            backoff = REOPEN_BACKOFF;
            let data_id = outcome.data_id();

            metrics::write_fanout_response(fanout_state(&outcome));
            if let Some(elapsed) = this.pending.elapsed_since_enqueued(&data_id) {
                metrics::replica_write_rpc(node, rpc_outcome(&outcome), elapsed);
            }

            match &outcome {
                WriteOutcome::Accepted { raft_state, .. }
                | WriteOutcome::Rejected { raft_state, .. } => {
                    this.streams.tracker().integrate(raft_state);
                }
                _ => {}
            }
            this.streams.reconcile_if_newer(weak.clone());

            match &outcome {
                WriteOutcome::Accepted { .. } => this.pending.ack(data_id, node),
                _ => this.pending.neg(data_id, node),
            }
        }

        let Some(this) = weak.upgrade() else {
            return;
        };
        this.pending.fast_fail_unacked(node);
        if this.streams.should_stop(node) {
            this.streams.remove(&node);
            return;
        }
        ids_rx = this.streams.ensure(node);
        drop(this);

        std::thread::sleep(crate::jitter(backoff));
        backoff = backoff.saturating_mul(2).min(MAX_REOPEN_BACKOFF);
    }
}

struct PendingWrites {
    tracker: Arc<dyn RaftStateTracker>,
    map: DashMap<DataId, Entry>,
}

impl PendingWrites {
    fn new(tracker: Arc<dyn RaftStateTracker>) -> Self {
        Self {
            tracker,
            map: DashMap::new(),
        }
    }

    fn insert(
        &self,
        data: Data,
        resp: oneshot::Sender<Result<(), Error>>,
        permit: OwnedSemaphorePermit,
    ) {
        self.map.insert(
            data.data_id,
            Entry {
                data,
                resp,
                _permit: permit,
                enqueued: Instant::now(),
                acks: BTreeSet::new(),
                negatives: BTreeSet::new(),
            },
        );
    }

    fn ack(&self, id: DataId, node: NodeId) {
        {
            let Some(mut e) = self.map.get_mut(&id) else {
                return;
            };
            e.acks.insert(node);
            e.negatives.remove(&node);
        }
        let replica = self.tracker.snapshot().replica.clone();
        if self
            .map
            .get(&id)
            .is_some_and(|e| is_quorum(&replica, &e.value().acks))
        {
            let quorum_latency = self.elapsed_since_enqueued(&id);
            if let Some((_, e)) = self.map.remove(&id) {
                if let Some(l) = quorum_latency {
                    metrics::coordinator_quorum_latency(l);
                }
                let _ = e.resp.send(Ok(()));
            }
            return;
        }
        self.reap_if_abandoned(id);
    }

    fn neg(&self, id: DataId, node: NodeId) {
        let replica = self.tracker.snapshot().replica.clone();
        let dead = {
            let Some(mut e) = self.map.get_mut(&id) else {
                return;
            };
            e.negatives.insert(node);
            let live: BTreeSet<NodeId> = all_targets(&replica)
                .difference(&e.negatives)
                .copied()
                .collect();
            !is_quorum(&replica, &live)
        };
        if dead {
            if let Some((_, e)) = self.map.remove(&id) {
                let _ = e.resp.send(Err(Error::QuorumUnreachable));
            }
        } else {
            self.reap_if_abandoned(id);
        }
    }

    fn reap_if_abandoned(&self, id: DataId) {
        if self
            .map
            .get(&id)
            .is_some_and(|e| e.value().resp.is_closed())
        {
            self.map.remove(&id);
        }
    }

    fn reap_abandoned(&self) {
        self.map.retain(|_, e| !e.resp.is_closed());
    }

    fn fast_fail_unacked(&self, node: NodeId) {
        let unacked: Vec<DataId> = self
            .map
            .iter()
            .filter(|e| !e.value().acks.contains(&node))
            .map(|e| *e.key())
            .collect();
        for id in unacked {
            self.neg(id, node);
        }
    }

    fn iter_unacked_data(&self, node: NodeId) -> Vec<Data> {
        self.map
            .iter()
            .filter_map(|e| {
                if !e.value().acks.contains(&node) {
                    Some(e.value().data.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_data(&self, id: &DataId) -> Option<Data> {
        self.map.get(id).map(|e| e.value().data.clone())
    }

    fn elapsed_since_enqueued(&self, id: &DataId) -> Option<Duration> {
        self.map.get(id).map(|e| e.value().enqueued.elapsed())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }
}

struct StreamOrchestrator {
    router: Arc<dyn Router>,
    tracker: Arc<dyn RaftStateTracker>,
    reconciled_gen: AtomicU64,
    fans: DashMap<NodeId, Sender<DataId>>,
}

impl StreamOrchestrator {
    fn new(router: Arc<dyn Router>, tracker: Arc<dyn RaftStateTracker>) -> Self {
        let start_gen = tracker.snapshot().generation.0;
        Self {
            router,
            tracker,
            reconciled_gen: AtomicU64::new(start_gen),
            fans: DashMap::new(),
        }
    }

    fn fan_senders(&self) -> Vec<(NodeId, Sender<DataId>)> {
        self.fans
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect()
    }

    fn router(&self) -> &Arc<dyn Router> {
        &self.router
    }

    fn tracker(&self) -> &Arc<dyn RaftStateTracker> {
        &self.tracker
    }

    fn should_stop(&self, node: NodeId) -> bool {
        !all_targets(&self.tracker.snapshot().replica).contains(&node)
    }

    fn remove(&self, node: &NodeId) {
        self.fans.remove(node);
    }

    fn ensure(&self, node: NodeId) -> Receiver<DataId> {
        let (ids_tx, ids_rx) = crossbeam_channel::bounded(FAN_CAP);
        self.fans.insert(node, ids_tx);
        ids_rx
    }

    fn reconcile_if_newer(&self, coordinator_weak: Weak<Coordinator>) {
        let now = self.tracker.snapshot().generation.0;
        let prev = self.reconciled_gen.load(Ordering::Relaxed);
        if now <= prev {
            return;
        }
        if self
            .reconciled_gen
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        for node in all_targets(&self.tracker.snapshot().replica) {
            if let MapEntry::Vacant(v) = self.fans.entry(node) {
                let (ids_tx, ids_rx) = crossbeam_channel::bounded(FAN_CAP);
                v.insert(ids_tx);
                let weak = coordinator_weak.clone();
                tokio::task::spawn_blocking(move || run_replica(weak, node, ids_rx));
            }
        }
    }
}

fn rpc_outcome(outcome: &WriteOutcome) -> WriteRpcOutcome {
    match outcome {
        WriteOutcome::Accepted { .. } => WriteRpcOutcome::Success,
        WriteOutcome::Rejected { .. } => WriteRpcOutcome::Rejected,
        WriteOutcome::Failed { .. } => WriteRpcOutcome::Failed,
    }
}

fn fanout_state(outcome: &WriteOutcome) -> FanoutResponseState {
    match outcome {
        WriteOutcome::Accepted { .. } => FanoutResponseState::Ack,
        WriteOutcome::Rejected { .. } => FanoutResponseState::Rejected,
        WriteOutcome::Failed { .. } => FanoutResponseState::Unreachable,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use async_trait::async_trait;
    use futures::stream::BoxStream;

    use super::*;
    use crate::api::{RaftStateTracker, Replica, ReplicaClient};
    use crate::ids::{LogIndex, RaftGeneration};
    use crate::meta::{RaftManifestState, RaftState, ReplicaMembership, ReplicaStatus};
    use crate::tests::{make_data, nid, replica_only};

    fn state(generation: u64, replicas: &[NodeId]) -> RaftState {
        RaftState {
            generation: RaftGeneration(generation),
            leader: None,
            raft_term: 0,
            replica: ReplicaMembership::single(replicas.iter().copied().collect()),
            manifest_state: RaftManifestState::default(),
            voters: BTreeSet::new(),
        }
    }

    #[derive(Clone)]
    enum Resp {
        Ack(RaftState),
        Reject(RaftState),
        Down,
        Silent,
    }

    struct FakeReplica {
        resp: Resp,
    }

    #[async_trait]
    impl Replica for FakeReplica {
        fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
            let (tx, rx) = crossbeam_channel::unbounded();
            let resp = self.resp.clone();
            // This double measures nothing, so it drops the stamp immediately.
            let mut input = input.filter_map(|s| Some(s.data));
            std::thread::spawn(move || {
                while let Ok(data) = input.recv() {
                    let data_id = data.data_id;
                    let outcome = match &resp {
                        Resp::Ack(s) => WriteOutcome::Accepted {
                            data_id,
                            raft_state: s.clone(),
                        },
                        Resp::Reject(s) => WriteOutcome::Rejected {
                            data_id,
                            error: Error::Rpc("rejected".into()),
                            raft_state: s.clone(),
                        },
                        Resp::Down => WriteOutcome::Failed {
                            data_id,
                            error: Error::Rpc("down".into()),
                        },
                        Resp::Silent => continue,
                    };
                    if tx.send(Ok(outcome)).is_err() {
                        return;
                    }
                }
            });
            let (ch, closer) = Channels::from(rx, "test replica");
            closer.close();
            ch
        }

        async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
            unimplemented!()
        }
        fn subscribe_from(&self, _: Option<LogIndex>) -> Channels<Result<(LogIndex, Data), Error>> {
            Channels::from_vec(vec![])
        }
        async fn subscribe_status(
            &self,
        ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
            unimplemented!()
        }
    }

    struct ScriptRouter {
        script: BTreeMap<NodeId, Resp>,
    }

    impl Router for ScriptRouter {
        fn register_peer(&self, _: NodeId, _: crate::ids::PeerAddr) {}
        fn remote(&self, node_id: NodeId) -> Arc<dyn ReplicaClient> {
            let resp = self.script.get(&node_id).cloned().unwrap_or(Resp::Down);
            replica_only(Arc::new(FakeReplica { resp }))
        }
        fn remote_direct(&self, _: crate::ids::PeerAddr) -> Arc<dyn ReplicaClient> {
            unimplemented!("ScriptRouter: direct dial not used")
        }
    }

    struct FixedTracker {
        state: Arc<RaftState>,
        integrated: AtomicU64,
    }

    impl crate::api::RaftStateView for FixedTracker {
        fn snapshot(&self) -> Arc<RaftState> {
            self.state.clone()
        }
        fn subscribe(&self) -> tokio::sync::watch::Receiver<Arc<RaftState>> {
            tokio::sync::watch::channel(Arc::clone(&self.state)).1
        }
    }
    impl RaftStateTracker for FixedTracker {
        fn integrate(&self, _: &RaftState) {
            self.integrated.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn tracker_of(s: &RaftState) -> Arc<FixedTracker> {
        Arc::new(FixedTracker {
            state: Arc::new(s.clone()),
            integrated: AtomicU64::new(0),
        })
    }

    fn coordinator(s: &RaftState, script: BTreeMap<NodeId, Resp>) -> Arc<Coordinator> {
        Coordinator::new(Arc::new(ScriptRouter { script }), tracker_of(s))
    }

    impl Coordinator {
        fn pending_len(&self) -> usize {
            self.pending.len()
        }
    }

    #[tokio::test]
    async fn write_succeeds_when_quorum_acks() {
        let (n1, n2, n3) = (nid(1), nid(2), nid(3));
        let s = state(1, &[n1, n2, n3]);
        let coord = coordinator(
            &s,
            BTreeMap::from([
                (n1, Resp::Ack(s.clone())),
                (n2, Resp::Ack(s.clone())),
                (n3, Resp::Down),
            ]),
        );
        coord
            .submit(make_data(1, 100, b"hi"))
            .await
            .expect("recv")
            .expect("quorum");
    }

    #[tokio::test]
    async fn write_fails_when_quorum_unreachable() {
        let (n1, n2, n3) = (nid(1), nid(2), nid(3));
        let s = state(1, &[n1, n2, n3]);
        let coord = coordinator(
            &s,
            BTreeMap::from([
                (n1, Resp::Down),
                (n2, Resp::Down),
                (n3, Resp::Ack(s.clone())),
            ]),
        );
        let err = coord
            .submit(make_data(1, 100, b"hi"))
            .await
            .expect("recv")
            .expect_err("no quorum");
        assert!(matches!(err, Error::QuorumUnreachable));
    }

    #[tokio::test]
    async fn rejected_does_not_count_toward_quorum() {
        let (n1, n2, n3) = (nid(1), nid(2), nid(3));
        let s = state(1, &[n1, n2, n3]);
        let coord = coordinator(
            &s,
            BTreeMap::from([
                (n1, Resp::Reject(s.clone())),
                (n2, Resp::Ack(s.clone())),
                (n3, Resp::Down),
            ]),
        );
        let err = coord
            .submit(make_data(1, 100, b"hi"))
            .await
            .expect("recv")
            .expect_err("1 ack of 3 is not quorum");
        assert!(matches!(err, Error::QuorumUnreachable));
    }

    #[tokio::test]
    async fn streaming_quorum_fires_before_slow_replica() {
        let (n1, n2, n3) = (nid(1), nid(2), nid(3));
        let s = state(1, &[n1, n2, n3]);
        let coord = coordinator(
            &s,
            BTreeMap::from([
                (n1, Resp::Ack(s.clone())),
                (n2, Resp::Ack(s.clone())),
                (n3, Resp::Silent),
            ]),
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            coord.submit(make_data(1, 100, b"hi")),
        )
        .await
        .expect("must not wait on the silent replica")
        .expect("recv")
        .expect("quorum from the two fast replicas");
    }

    #[tokio::test]
    async fn timed_out_write_releases_its_permit() {
        let me = nid(1);
        let s = state(1, &[me]);
        let coord = coordinator(&s, BTreeMap::from([(me, Resp::Silent)]));
        let rx = coord.submit(make_data(1, 100, b"hi"));
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(coord.pending_len(), 1, "write is in flight");
        drop(rx);
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert_eq!(
            coord.pending_len(),
            0,
            "abandoned entry reaped, permit returned"
        );
    }

    #[tokio::test]
    async fn integrates_outcome_state_into_tracker() {
        let (n1, n2, n3) = (nid(1), nid(2), nid(3));
        let s = state(1, &[n1, n2, n3]);
        let tracker = tracker_of(&s);
        let coord = Coordinator::new(
            Arc::new(ScriptRouter {
                script: BTreeMap::from([
                    (n1, Resp::Ack(s.clone())),
                    (n2, Resp::Ack(s.clone())),
                    (n3, Resp::Down),
                ]),
            }),
            tracker.clone(),
        );
        coord
            .submit(make_data(1, 100, b"hi"))
            .await
            .expect("recv")
            .expect("quorum");
        assert!(
            tracker.integrated.load(Ordering::SeqCst) > 0,
            "acked states were integrated into the tracker"
        );
    }
}
