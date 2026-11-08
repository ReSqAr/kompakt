//! What a snapshot commit refuses: an aborted window, a backwards cutoff, a stale generation.

use super::*;

/// A window read that dies mid-scan must **fail** the snapshot, not commit a
/// short one. Committing it would record a self-consistent manifest (which
/// `validate_replicated_state` accepts — it checks manifest *shape*, never
/// *coverage*) while `manifest_watermark` advanced to the full cutoff, so the
/// entries the scan never reached would fall below the next window's lower
/// bound and be lost for good.
#[tokio::test]
async fn snapshot_refuses_to_commit_an_aborted_window() {
    use crossbeam_combinators::Channel;
    use zslg_log::LocalStoreView;

    use tokio::sync::watch;

    /// Yields a prefix of the real window, then drops its `Closer` — exactly
    /// what a panicking `stream_scan` leaves behind.
    struct TruncatingLocalStore {
        inner: Arc<LocalStore>,
    }

    #[async_trait::async_trait]
    impl LocalStoreView for TruncatingLocalStore {
        fn stream_above_hlc(&self, threshold: Option<Hlc>) -> zslg_log::LocalStoreStream {
            self.inner.stream_above_hlc(threshold)
        }
        fn stream_window(&self, _above: Option<Hlc>, _upto: Hlc) -> zslg_log::LocalStoreStream {
            let (tx, rx) = crossbeam_channel::bounded(4);
            let (ch, closer) = Channel::from(rx, "truncated window");
            tx.send(Ok(make_data(1, 100, b"a"))).unwrap();
            drop(tx);
            drop(closer); // never closed: the scan died
            ch.ordered(None, "truncated window")
        }
        fn subscribe_integrated(&self) -> watch::Receiver<()> {
            self.inner.subscribe_integrated()
        }
        fn subscribe_unstable(
            &self,
        ) -> crossbeam_combinators::Channel<zslg_log::Data, crossbeam_combinators::Infinite>
        {
            self.inner.subscribe_unstable()
        }
        async fn subscription_cursor(
            &self,
            upstream: NodeId,
        ) -> Result<Option<LogIndex>, zslg_log::Error> {
            self.inner.subscription_cursor(upstream).await
        }
    }

    let h = boot_fake().await;
    append_entries(
        &h.local_store,
        &[
            make_data(1, 100, b"a"),
            make_data(2, 200, b"b"),
            make_data(3, 300, b"c"),
        ],
    )
    .await;

    let before = h.raft_state.snapshot().manifest_state.manifest_watermark;

    let truncating = TruncatingLocalStore {
        inner: h.local_store.clone(),
    };
    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: &truncating,
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };

    let err = create_snapshot_entry(&inputs, Hlc(300))
        .await
        .expect_err("an aborted window must fail the snapshot");
    assert!(
        matches!(err, zslg_leader_snapshot::Error::Log(LogError::StreamTruncated { ref what }) if what == "truncated window"),
        "expected StreamTruncated, got: {err:?}"
    );

    assert_eq!(
        h.raft_state.snapshot().manifest_state.manifest_watermark,
        before,
        "a failed snapshot must not advance the watermark"
    );
}

#[tokio::test]
async fn snapshot_rejects_backwards_cutoff() {
    let h = boot_fake().await;

    append_entries(
        &h.local_store,
        &[make_data(1, 100, b"a"), make_data(2, 200, b"b")],
    )
    .await;

    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.as_ref(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };
    let first = create_snapshot_entry(&inputs, Hlc(200))
        .await
        .expect("first");
    commit_snapshot_result_via(h.consensus.as_ref(), &h.raft_state, &first).await;

    let result = create_snapshot_entry(&inputs, Hlc(150))
        .await
        .expect("retry");
    assert_eq!(result, SnapshotResult::Skipped);
}

// ── F-24 fence: generation CAS on state writes ──────────────────────────

#[tokio::test]
async fn fenced_write_rejected_on_stale_generation() {
    use zslg_consensus::RaftGeneration;
    use zslg_raft::{FencedStateWrite, RaftResponse};

    let h = boot().await;
    let _ = h.leader_id;

    // Seed once (bumps the applied generation past ZERO).
    let seeded: std::collections::BTreeSet<NodeId> = [NodeId::new_v4()].into_iter().collect();
    zslg_raft_setup::testing::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(seeded.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed");
    let gen_after_seed = h.raft_state.snapshot().generation;
    assert!(gen_after_seed > RaftGeneration::ZERO);

    // A write fenced on the stale ZERO generation commits in the Raft log
    // but must be a no-op on the state machine.
    let response = h
        .raft
        .client_write(FencedStateWrite {
            expected_generation: RaftGeneration::ZERO,
            state: ReplicatedRaftState {
                replica: ReplicaMembership::single(std::collections::BTreeSet::new()),
                manifest: Vec::new(),
                manifest_watermark: Some(Hlc(999_999)),
                hlc_cutoff: Some(Hlc(999_999)),
            },
        })
        .await
        .expect("commit succeeds; apply is fenced");
    assert!(matches!(
        response.data,
        RaftResponse::RejectedStaleGeneration { .. }
    ));

    let snap = h.raft_state.snapshot();
    assert_eq!(
        snap.generation, gen_after_seed,
        "rejected write must not apply"
    );
    assert_eq!(snap.replica.current().clone(), seeded);
    assert_eq!(snap.manifest_state.manifest_watermark, None);
}

/// EXPERT_REVIEW F-24 invariant: a snapshot commit computed from a stale
/// state read must NOT clobber state committed concurrently (e.g. by another
/// leader during a leadership flap). The generation fence rejects it and the
/// concurrent manifest entry + watermark survive.
///
/// NOTE: Ignored post-Channel refactoring. The old async `stream_window`
/// allowed async injection (foreign manifest commit) during the window-ordered
/// read. The sync `Channel<T>` return type runs on the caller's thread — async
/// injection from a tokio worker panics (`block_on` from within a runtime).
/// Restoring this test requires bridging sync injection into the async pipeline
/// (e.g. signal + yield in `create_snapshot_entry` before the snapshot commit).
#[tokio::test]
#[ignore = "needs async injection bridge for sync Channel pipeline"]
async fn invariant_stale_snapshot_commit_is_fenced() {
    use std::sync::atomic::AtomicBool;

    use zslg_consensus::NodeId;
    use zslg_consensus::RaftGeneration;
    use zslg_leader_snapshot::create::{SnapshotInputs, build_snapshot};
    use zslg_log::LocalStoreView;
    use zslg_log::LogIndex;
    use zslg_log::Replica;
    use zslg_snapshot::SnapshotId;
    use zslg_snapshot::SnapshotMetadata;
    use zslg_time::{Hlc, HlcParts};

    use zslg_net_fake::ReadyReplica;
    use zslg_time_fake::FixedClock;

    use tokio::sync::watch;

    /// Replica stub: always drained, no end_index — quorum checks pass instantly.
    struct OnePeerRouter {
        replica: Arc<dyn Replica>,
    }

    impl Router for OnePeerRouter {
        fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

        fn remote(&self, _node_id: NodeId) -> Arc<dyn zslg_net::ReplicaClient> {
            zslg_net_fake::replica_only(self.replica.clone())
        }

        fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn zslg_net::ReplicaClient> {
            unimplemented!("OnePeerRouter: direct dial not used")
        }
    }

    /// LocalStoreView wrapper that commits a foreign manifest entry through
    /// the REAL Raft when the snapshot pipeline starts reading data — i.e.
    /// inside the window between the pipeline's state read and its final
    /// commit (simulating another leader's snapshot landing concurrently).
    struct InjectingLocalStore {
        inner: Arc<LocalStore>,
        raft: Raft,
        raft_state: RaftStateHandle,
        foreign: SnapshotMetadata,
        injected: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl LocalStoreView for InjectingLocalStore {
        fn stream_above_hlc(&self, threshold: Option<Hlc>) -> zslg_log::LocalStoreStream {
            self.inner.stream_above_hlc(threshold)
        }

        fn stream_window(&self, above: Option<Hlc>, upto: Hlc) -> zslg_log::LocalStoreStream {
            self.inner.stream_window(above, upto)
        }

        fn subscribe_integrated(&self) -> watch::Receiver<()> {
            self.inner.subscribe_integrated()
        }

        fn subscribe_unstable(
            &self,
        ) -> crossbeam_combinators::Channel<zslg_log::Data, crossbeam_combinators::Infinite>
        {
            self.inner.subscribe_unstable()
        }

        async fn subscription_cursor(
            &self,
            upstream: NodeId,
        ) -> Result<Option<LogIndex>, zslg_log::Error> {
            self.inner.subscription_cursor(upstream).await
        }
    }

    let h = boot().await;
    let _ = h.leader_id;

    // Seed a one-node replica set so the quorum fan-out has a target.
    let replica_id = NodeId::new_v4();
    zslg_raft_setup::testing::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single([replica_id].into_iter().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    // One row so the snapshot is non-empty.
    integrate_batch(
        &h.local_store,
        upstream(1),
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("integrate");

    // The "other leader's" snapshot, committed mid-window.
    let foreign = SnapshotMetadata {
        start_hlc: Hlc::from_parts(HlcParts {
            physical_ms: 4_000,
            logical: 0,
        }),
        end_hlc: Hlc::from_parts(HlcParts {
            physical_ms: 5_000,
            logical: 0,
        }),
        start_index: LogIndex(0),
        end_index: LogIndex(0),
        snapshot_id: SnapshotId::from_u128(5_000),
    };

    let injecting = Arc::new(InjectingLocalStore {
        inner: h.local_store,
        raft: h.raft.clone(),
        raft_state: h.raft_state.clone(),
        foreign: foreign.clone(),
        injected: Arc::new(AtomicBool::new(false)),
    });
    let router: Arc<dyn Router> = Arc::new(OnePeerRouter {
        replica: Arc::new(ReadyReplica {
            raft_state: h.raft_state.clone(),
        }),
    });
    let clock = FixedClock {
        now: Hlc::from_parts(HlcParts {
            physical_ms: 3_000,
            logical: 0,
        }),
    };

    let snapshot_store = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: h.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));
    let leadership: Arc<dyn zslg_consensus::RaftLeadership> = Arc::new(RaftNode::new(
        h.raft.clone(),
        zslg_consensus_fake::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    let inputs = SnapshotInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: injecting,
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*snapshot_store,
        raft_state: &h.raft_state,
        router,
        leadership,
        self_node_id: h.leader_id,
        quorum_poll_interval: Duration::from_millis(5),
        quorum_wait_timeout: Duration::from_secs(5),
        compression: CompressionLevel::fast(),
    };

    // The build's premise (the manifest end index its global indices continue
    // from) moved under it, so publishing must be refused rather than rebased.
    let build = build_snapshot(&inputs, clock.now)
        .await
        .expect("the build itself succeeds; only publishing is refused");
    let (intents, _cancel) = crate::Integrator::spawn_for_test(
        Arc::new(RaftNode::new(
            h.raft.clone(),
            zslg_consensus_fake::permissive_gate(),
            std::time::Duration::from_secs(30),
        )),
        Arc::new(h.raft_state.clone()),
    );
    let outcome = intents
        .submit(
            build
                .commit_intent()
                .expect("a committed build has an intent"),
        )
        .await
        .expect("submit");
    assert!(
        matches!(outcome, zslg_consensus::IntentOutcome::Superseded { .. }),
        "expected the stale build to be superseded, got: {outcome:?}"
    );

    // The concurrently committed state survives untouched.
    let final_state = h.raft_state.snapshot();
    assert!(
        final_state
            .manifest_state
            .manifest
            .iter()
            .any(|m| m.snapshot_id == foreign.snapshot_id),
        "foreign manifest entry was clobbered: {:?}",
        final_state.manifest_state.manifest,
    );
    assert_eq!(
        final_state.manifest_state.manifest_watermark,
        Some(foreign.end_hlc),
        "watermark must not regress below the concurrently committed value",
    );
    let _ = RaftGeneration::ZERO;
}
