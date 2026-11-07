#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use crossbeam_combinators::testing::ChannelTestExt as _;
    use tokio_util::sync::CancellationToken;

    use crate::error::Error;
    use crate::tests::{make_data, nid};
    use crossbeam_combinators::Channel;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;
    use zslg_consensus::{ConsensusError, NodeId};
    use zslg_consensus_types::RaftStateHandle;
    use zslg_consensus_types::{RaftState, ReplicaMembership};
    use zslg_hlc_clock::HlcClock;
    use zslg_hlc_gate::HlcGate;
    use zslg_log::{LogIndex, StampedWrite, WriteOutcome};
    use zslg_log::{Replica, ReplicaLog as _};
    use zslg_replica_log::{ReplicaLogConfig, ReplicaLogStore};
    use zslg_replica_service::ReplicaService;
    use zslg_time::{ClockError, Hlc};

    /// Build a ReplicaService wired to a temp dense-log store, a fixed
    /// clock, and a RaftStateHandle whose replica set is `replicas`. Returns
    /// the node, the handle (for further mutation in tests), plus keep-alive
    /// guards (tempdir, flush handle).
    /// A single flush batch must never be able to overflow a healthy
    /// subscriber's live channel — otherwise one large batch sheds every
    /// subscriber on the node regardless of how fast they are draining.
    /// Measured at capacity 256 against a 1024 batch cap: subscriptions lived
    /// 0.137 s and re-scanned 9.62 GB of replica log.
    #[test]
    fn invariant_subscribe_capacity_covers_a_full_flush_batch() {
        let cfg = ReplicaLogConfig::for_test(nid(1));
        assert!(
            cfg.subscribe_channel_capacity >= cfg.max_flush_batch,
            "default config violates the shed invariant"
        );
        assert!(cfg.validate().is_ok());
    }

    /// At 2 KiB payloads a 4,096-deep write queue is ~8 MB — on its own larger
    /// than the whole 20 MiB storage budget, and entirely invisible before this
    /// gauge existed. The 2026-08-17 harness run's 90-120 MiB RSS per node
    /// against a 20 MiB budget is mostly the pipeline, not the engine.
    #[test]
    fn queue_capacity_bytes_is_reported() {
        let mut cfg = ReplicaLogConfig::for_test(nid(1));
        cfg.write_channel_capacity = bytesize::nonzero(4096);
        cfg.subscribe_channel_capacity = bytesize::nonzero(1024);
        // (4096 + 1024) slots x 2 KiB reference payload = 10 MiB.
        assert_eq!(cfg.queue_capacity_bytes(), 10_485_760);
    }

    #[test]
    fn validate_rejects_capacity_below_the_flush_batch() {
        let mut cfg = ReplicaLogConfig::for_test(nid(1));
        cfg.subscribe_channel_capacity = std::num::NonZeroUsize::new(64).expect("nonzero");
        cfg.max_flush_batch = std::num::NonZeroUsize::new(1024).expect("nonzero");
        let err = cfg.validate().expect_err("must reject");
        assert!(
            format!("{err}").contains("subscribe_channel_capacity"),
            "error must name the offending field, got: {err}"
        );
    }

    async fn replica_with(
        self_id: NodeId,
        replicas: &[NodeId],
        clock_ms: u64,
        max_skew: Duration,
    ) -> (
        Arc<ReplicaService>,
        RaftStateHandle,
        tempfile::TempDir,
        tokio::task::JoinHandle<Result<(), zslg_replica_log::Error>>,
    ) {
        let rig = rig(self_id, replicas, clock_ms, max_skew).await;
        (rig.node, rig.raft_state, rig.dir, rig.flush)
    }

    /// Everything a test might poke at, beyond the service itself.
    struct Rig {
        node: Arc<ReplicaService>,
        raft_state: RaftStateHandle,
        dir: tempfile::TempDir,
        flush: tokio::task::JoinHandle<Result<(), zslg_replica_log::Error>>,
        store: Arc<ReplicaLogStore>,
        gate: Arc<HlcGate>,
    }

    async fn rig(self_id: NodeId, replicas: &[NodeId], clock_ms: u64, max_skew: Duration) -> Rig {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, flush) = ReplicaLogStore::open_with_flush(
            crate::database::open_for_test(&dir.path().join("replica-log")).expect("storage"),
            CancellationToken::new(),
        )
        .await
        .expect("open_with_flush");
        let clock = Arc::new(HlcClock::with_time_source(
            max_skew,
            Box::new(move || clock_ms),
        ));
        let raft_state = RaftStateHandle::new();
        raft_state.replace(RaftState {
            replica: ReplicaMembership::single(replicas.iter().copied().collect()),
            ..RaftState::empty()
        });
        let cutoff = Arc::new(HlcGate::new());
        let (_tx, rx) = watch::channel(None);
        let node = Arc::new(ReplicaService::new(
            store.clone(),
            cutoff.clone(),
            self_id,
            clock,
            Arc::new(raft_state.clone()),
            rx,
            bytesize::nonzero(2048),
            zslg_peers::static_peers(Vec::new()),
        ));
        Rig {
            node,
            raft_state,
            dir,
            flush,
            store,
            gate: cutoff,
        }
    }

    // ── `Replica::write` contract (see the trait doc) ─────────────────────

    /// Closing the input drains every write already accepted *before* the output
    /// ends — the flush loop's completion holds a sender clone, so the commit
    /// channel outlives the input.
    #[tokio::test]
    async fn invariant_write_drains_before_ending() {
        let me = nid(1);
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[me], 1_000, Duration::from_secs(10)).await;

        let (tx, rx) = crossbeam_channel::unbounded();
        let outcomes = node.write(Channel::closed(rx));
        for i in 0..8u8 {
            tx.send(StampedWrite::now(make_data(i, 1_000, b"payload")))
                .expect("send");
        }
        drop(tx); // ends the input

        let collected = tokio::task::spawn_blocking(move || outcomes.items().collect::<Vec<_>>())
            .await
            .expect("join");
        assert_eq!(collected.len(), 8, "every accepted write is answered");
        assert!(
            collected
                .iter()
                .all(|o| matches!(o, Ok(WriteOutcome::Accepted { .. }))),
            "all writes accepted: {collected:?}"
        );
    }

    /// Dropping the output tears the pipeline down: the producer's sends fail
    /// because the input receiver was dropped with it.
    #[tokio::test]
    async fn invariant_dropping_write_output_stops_the_producer() {
        let me = nid(1);
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[me], 1_000, Duration::from_secs(10)).await;

        let (tx, rx) = crossbeam_channel::unbounded();
        drop(node.write(Channel::closed(rx)));

        // The output (and with it the input receiver) is gone, so the very next
        // send must fail.
        for i in 0..=u8::MAX {
            if tx
                .send(StampedWrite::now(make_data(i, 1_000, b"payload")))
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("producer kept accepting writes after the output was dropped");
    }

    /// Per-write failures stay inside `Ok(WriteOutcome)`; a rejected write does
    /// not end the stream or stop the writes behind it.
    #[tokio::test]
    async fn invariant_write_rejection_is_not_terminal() {
        let me = nid(1);
        // Not in the replica set: every write is rejected with `NotAReplica`.
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[nid(2)], 1_000, Duration::from_secs(10)).await;

        let (tx, rx) = crossbeam_channel::unbounded();
        let outcomes = node.write(Channel::closed(rx));
        for i in 0..4u8 {
            tx.send(StampedWrite::now(make_data(i, 1_000, b"payload")))
                .expect("send");
        }
        drop(tx);

        let collected = tokio::task::spawn_blocking(move || outcomes.items().collect::<Vec<_>>())
            .await
            .expect("join");
        assert_eq!(collected.len(), 4, "a rejection does not end the stream");
        assert!(
            collected.iter().all(|o| matches!(
                o,
                Ok(WriteOutcome::Rejected {
                    error: zslg_log::Error::Consensus(ConsensusError::NotAReplica { .. }),
                    ..
                })
            )),
            "rejections ride in Ok, not Err: {collected:?}"
        );
    }

    #[tokio::test]
    async fn write_rejected_when_not_a_replica() {
        let me = nid(1);
        // Replica set contains other nodes, not `me`.
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[nid(2), nid(3)], 1_000, Duration::from_secs(10)).await;
        let err = crate::tests::common::replica_write(&*node, make_data(0, 1_000, b"payload"))
            .await
            .expect_err("must reject");
        assert!(
            matches!(err, Error::Consensus(ConsensusError::NotAReplica { node_id }) if node_id == me)
        );
    }

    #[tokio::test]
    async fn write_accepted_when_in_replica_set() {
        let me = nid(1);
        let rig = rig(me, &[me, nid(2), nid(3)], 1_000, Duration::from_secs(10)).await;
        let st = crate::tests::common::replica_write(&*rig.node, make_data(0, 1_000, b"payload"))
            .await
            .expect("write accepted");
        assert!(st.replica.current().contains(&me));
        assert_eq!(rig.store.frontier(), Some(LogIndex::ZERO));
    }

    // Skew is checked at RPC ingress against the fresh per-RPC envelope HLC, not
    // here against a write's creation HLC — that would conflate in-flight latency
    // with clock skew. See
    // `clock_skew_tests::peer_clock_diverged_beyond_drift_limit_fails_integrate`.

    /// F-3 invariant: a write at exactly the current cutoff is rejected.
    /// The covering snapshot's catch-up has already run, so an admitted
    /// boundary write would be ACKed but never reach any snapshot.
    #[tokio::test]
    async fn invariant_replica_rejects_write_at_cutoff() {
        let me = nid(1);
        let rig = rig(me, &[me], 1_000, Duration::from_secs(10)).await;
        let node = rig.node.clone();
        assert!(rig.gate.set_cutoff(Hlc(1_000)).await, "advance from None");

        let err = crate::tests::common::replica_write(&*node, make_data(0, 1_000, b"payload"))
            .await
            .expect_err("== cutoff must reject");
        assert!(matches!(
            err,
            Error::Clock(ClockError::HlcCutoff(
                zslg_time_types::CutoffError::AtOrBeforeCutoff { .. }
            ))
        ));

        // Strictly above the cutoff is accepted.
        crate::tests::common::replica_write(&*node, make_data(0, 1_001, b"payload"))
            .await
            .expect("> cutoff accepted");
    }

    #[tokio::test]
    async fn write_accepted_when_also_in_joint_next_set() {
        let me = nid(1);
        // `me` only in the next (joint) set, not the current set.
        let (node, raft_state, _dir, _flush) =
            replica_with(me, &[nid(2)], 1_000, Duration::from_secs(10)).await;
        raft_state.replace(RaftState {
            replica: ReplicaMembership::joint(
                [nid(2)].into_iter().collect(),
                [me, nid(2)].into_iter().collect(),
            ),
            ..RaftState::empty()
        });
        crate::tests::common::replica_write(&*node, make_data(0, 1_000, b"payload"))
            .await
            .expect("joint-set member may write");
    }
}
