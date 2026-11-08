//! Tests for `coordinator`.

use crossbeam_combinators::{Finite, Infinite};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use zslg_net::NetError;
use zslg_net::ReplicaClient;

use async_trait::async_trait;
use futures::stream::BoxStream;

use super::*;

use zslg_consensus::RaftGeneration;
use zslg_consensus::{RaftManifestState, RaftState, ReplicaMembership, ReplicaStatus};
use zslg_consensus_fake::nid;
use zslg_log::LogIndex;
use zslg_log::Replica;
use zslg_log_fake::make_data;
use zslg_net_fake::replica_only;

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
    /// A service instance that has been dropped but whose connection is
    /// still open: answers every write `Failed(Shutdown)` and never ends
    /// the stream.
    Shutdown,
    /// The first stream opened to it talks to a dropped instance
    /// ([`Resp::Shutdown`]); every later one reaches a live replica
    /// ([`Resp::Ack`]) — a node killed and relaunched on the same address.
    DroppedInstanceThenAck(Arc<std::sync::atomic::AtomicBool>, RaftState),
}

struct FakeReplica {
    resp: Resp,
}

#[async_trait]
impl Replica for FakeReplica {
    fn write(
        &self,
        input: Channel<StampedWrite, Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
        let run = || -> Channel<Result<WriteOutcome, Error>, Infinite> {
            let (tx, rx) = crossbeam_channel::unbounded();
            let resp = match &self.resp {
                Resp::DroppedInstanceThenAck(opened, live) => {
                    if opened.swap(true, Ordering::SeqCst) {
                        Resp::Ack(live.clone())
                    } else {
                        Resp::Shutdown
                    }
                }
                other => other.clone(),
            };
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
                            error: Error::Net(NetError::Rpc("rejected".into())).into(),
                            raft_state: s.clone(),
                        },
                        Resp::Down => WriteOutcome::Failed {
                            data_id,
                            error: Error::Net(NetError::Rpc("down".into())).into(),
                        },
                        Resp::Shutdown => WriteOutcome::Failed {
                            data_id,
                            error: Error::Lifecycle(zslg_lifecycle_types::LifecycleError::Shutdown)
                                .into(),
                        },
                        Resp::DroppedInstanceThenAck(..) => continue,
                        Resp::Silent => continue,
                    };
                    if tx.send(Ok(outcome)).is_err() {
                        return;
                    }
                }
            });
            Channel::from_infinite(rx, "test replica")
        };
        run().err_into()
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        unimplemented!()
    }
    fn subscribe_from(&self, _: Option<LogIndex>) -> zslg_log::ReplicaStream {
        Channel::aborted("test replica").ordered(None, "replica subscription")
    }
    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        unimplemented!()
    }
}

struct ScriptRouter {
    script: BTreeMap<NodeId, Resp>,
}

impl Router for ScriptRouter {
    fn register_peer(&self, _: NodeId, _: zslg_net::PeerAddr) {}
    fn remote(&self, node_id: NodeId) -> Arc<dyn ReplicaClient> {
        let resp = self.script.get(&node_id).cloned().unwrap_or(Resp::Down);
        replica_only(Arc::new(FakeReplica { resp }))
    }
    fn remote_direct(&self, _: zslg_net::PeerAddr) -> Arc<dyn ReplicaClient> {
        unimplemented!("ScriptRouter: direct dial not used")
    }
}

struct FixedTracker {
    state: Arc<RaftState>,
    integrated: AtomicU64,
}

impl zslg_consensus::RaftStateView for FixedTracker {
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

/// A node killed and relaunched on the same address leaves the writer's open
/// stream talking to the dropped service instance, which answers every write
/// `Failed(Shutdown)` and never ends the stream. The coordinator must drop
/// that stream and reopen one, or the node is refused for good — and with two
/// such nodes the writer never reaches a quorum again.
#[tokio::test]
async fn a_stream_to_a_dropped_instance_is_reopened() {
    let (n1, n2, n3) = (nid(1), nid(2), nid(3));
    let s = state(1, &[n1, n2, n3]);
    let flag = || Arc::new(std::sync::atomic::AtomicBool::new(false));
    let coord = coordinator(
        &s,
        BTreeMap::from([
            (n1, Resp::DroppedInstanceThenAck(flag(), s.clone())),
            (n2, Resp::DroppedInstanceThenAck(flag(), s.clone())),
            (n3, Resp::Ack(s.clone())),
        ]),
    );

    // Two of three answer from a dropped instance: no quorum this time.
    let first = coord
        .submit(make_data(1, 100, b"hi"))
        .await
        .expect("recv")
        .expect_err("two replicas are gone");
    assert!(matches!(
        first,
        Error::Consensus(ConsensusError::QuorumUnreachable)
    ));

    // The streams are reopened with backoff, so a later write succeeds.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut seed = 2u8;
    loop {
        let outcome = coord
            .submit(make_data(seed, 100 + u64::from(seed), b"again"))
            .await
            .expect("recv");
        if outcome.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "writes are still refused: the stream to the dropped instance was never reopened"
        );
        seed = seed.saturating_add(1);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
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
    assert!(matches!(
        err,
        Error::Consensus(ConsensusError::QuorumUnreachable)
    ));
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
    assert!(matches!(
        err,
        Error::Consensus(ConsensusError::QuorumUnreachable)
    ));
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
