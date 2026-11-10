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
