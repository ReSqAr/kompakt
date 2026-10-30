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
/// Leadership churn against an in-flight snapshot (EXPERT_REVIEW Appendix
/// B.4): a real 3-node raft cluster, a snapshot pipeline stalled between
/// its base-generation capture and its final fenced manifest commit, and a
/// real demote→re-elect cycle driven through partitions plus
/// `Raft::trigger().elect()`.
///
/// Two interleavings:
/// - the deposed leader is **re-elected** before its commit lands → the
///   write goes through raft and the apply-time generation fence (F-24)
///   rejects it with `Error::StaleStateWrite`;
/// - the deposed leader stays a follower → openraft rejects the
///   `client_write` outright (no auto-forwarding).
///
/// In both cases the manifest must stay untouched and the next snapshot at
/// the current generation must succeed and cover all the data — the
/// CLAUDE.md §12 "leader retries the next snapshot tick" story, end to end.
///
/// Note on elections: openraft 0.9 rejects vote requests while the current
/// leader's lease (= election_timeout_max) is fresh, so leadership is moved
/// by isolating the incumbent via `crate::tests::Partition`, then triggering
/// elections on the successor until the reachable nodes follow it.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::object_store::CompressionLevel;
use crossbeam_combinators::Channels;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::sync::Notify;
use tokio::sync::watch;

use crate::api::ReplicatedRaftState;
use crate::api::{LocalStoreMut, LocalStoreView, Replica, Router, StableLogSnapshot, WindowItem};
use crate::error::Error;
use crate::hlc::HlcGate;
use crate::hlc::{Hlc, HlcParts};
use crate::ids::{LogIndex, NodeId};
use crate::leader::{SnapshotInputs, build_snapshot};
use crate::learner::LocalStore;
use crate::learner::StableLog;
use crate::learner::integrate_manifest_for_test;
use crate::meta::{Data, ReplicaMembership};
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::raft::RaftNode;
use crate::tests::fast_raft_config as raft_config;
use crate::tests::make_data;
use crate::tests::{FixedClock, MapPeerRouter, ReadyReplica};
use crate::tests::{Partition, TestNode};

async fn integrate_batch(
    store: &LocalStore,
    source: NodeId,
    records: Vec<(LogIndex, Data)>,
) -> Result<(), crate::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
    for (idx, rec) in records {
        let _ = tx.send(Ok((source, idx, rec)));
    }
    let (ch, closer) = crossbeam_combinators::Channels::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await
}

/// `LocalStoreView` wrapper that pauses the snapshot pipeline inside the
/// window read — after `create_snapshot_entry` captured its base generation,
/// before the final fenced manifest commit. `entered` fires when the
/// pipeline reaches the stall; `release` lets it continue.
struct StallingLocalStore {
    inner: Arc<LocalStore>,
    entered: Arc<Notify>,
    /// Sync, not `Notify`: the stall happens on the *consumer's blocking thread*
    /// (see `stream_window_ordered` below), where awaiting is impossible.
    release: crossbeam_channel::Receiver<()>,
}

#[async_trait::async_trait]
impl LocalStoreView for StallingLocalStore {
    fn stream_above_hlc(&self, threshold: Option<Hlc>) -> Channels<Result<Data, Error>> {
        self.inner.stream_above_hlc(threshold)
    }

    /// Stall on the *first pull*, not on the call.
    ///
    /// `stream_window_ordered` is synchronous, so it cannot await here — and
    /// `block_on` inside the runtime panics (and violates CLAUDE.md §4). The
    /// returned `Channels` is drained on a blocking thread
    /// (`snapshot/create.rs` pulls it inside `spawn_blocking`), so gating the
    /// first pull is both legal and the stall point we actually want: after
    /// `create_snapshot_entry` captured its base generation, before the window
    /// is read.
    fn stream_window_ordered(
        &self,
        above: Option<Hlc>,
        upto: Hlc,
        max_entries: Option<std::num::NonZeroUsize>,
    ) -> Channels<Result<WindowItem, Error>> {
        self.entered.notify_one();
        let release = self.release.clone();
        // `filter_map` takes `Fn`, so the once-flag needs interior mutability.
        let gate = std::sync::Once::new();
        self.inner
            .stream_window_ordered(above, upto, max_entries)
            .filter_map(move |item| {
                gate.call_once(|| {
                    let _ = release.recv();
                });
                Some(item)
            })
    }

    fn subscribe_integrated(&self) -> watch::Receiver<()> {
        self.inner.subscribe_integrated()
    }

    fn subscribe_unstable(&self) -> crossbeam_combinators::Channels<crate::meta::Data> {
        self.inner.subscribe_unstable()
    }

    async fn subscription_cursor(&self, upstream: NodeId) -> Result<Option<LogIndex>, Error> {
        self.inner.subscription_cursor(upstream).await
    }
}

struct Churn {
    nodes: BTreeMap<NodeId, TestNode>,
    ids: [NodeId; 3],
    partition: Partition,
    local_store: Arc<LocalStore>,
    stable_log: Arc<StableLog>,
    object_store: Arc<dyn ObjectStore>,
    hlc_gate: Arc<HlcGate>,
    router: Arc<dyn Router>,
    _dir: tempfile::TempDir,
}

const SEEDED_ENTRIES: u8 = 10;

async fn boot_churn() -> Churn {
    let ids: [NodeId; 3] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, _, _] = ids;
    let (nodes, partition) = crate::tests::start_cluster_with_partition(&ids, raft_config())
        .await
        .expect("cluster");
    nodes
        .get(&n1)
        .expect("n1")
        .raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    crate::tests::wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;

    // Replica set = the three nodes, so the cutoff fan-out needs a 2/3 quorum.
    // Through whoever leads, not pinned to n1: this lands during bring-up, when
    // an election can still be settling.
    crate::tests::write_state_fenced_via_leader(
        &nodes,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(ids.iter().copied().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    let dir = tempfile::tempdir().expect("tempdir");
    let local_store = LocalStore::simple_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");
    let stable_log = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    for i in 0..SEEDED_ENTRIES {
        integrate_batch(
            &local_store,
            crate::tests::upstream(0),
            vec![(
                LogIndex(u64::from(i)),
                make_data(i, 100u64.checked_add(u64::from(i)).expect("hlc"), &[i]),
            )],
        )
        .await
        .expect("integrate");
    }

    let router: Arc<dyn Router> = Arc::new(MapPeerRouter {
        replicas: ids
            .iter()
            .map(|id| {
                let raft_state = nodes.get(id).expect("node").raft_state.clone();
                (
                    *id,
                    Arc::new(ReadyReplica { raft_state }) as Arc<dyn Replica>,
                )
            })
            .collect(),
    });

    // Re-establish n1 *after* the setup above, not just after `initialize`.
    // Everything between the two — two RocksDB opens and `SEEDED_ENTRIES`
    // integrate batches, each of them an fsync — is far longer than
    // `election_timeout_max` (400 ms) on a loaded machine, so n1's leadership
    // is not something bring-up can settle once and have hold here. Every
    // caller spawns a snapshot on n1 next, and `build_snapshot` abandons the
    // build outright if n1 is not leader at its post-quorum re-check, so a flap
    // in this window is the difference between the test running and the test
    // never reaching its stall at all.
    force_leader(&nodes, n1, &ids).await;

    Churn {
        nodes,
        ids,
        partition,
        local_store,
        stable_log,
        object_store,
        hlc_gate: Arc::new(HlcGate::new()),
        router,
        _dir: dir,
    }
}

fn spawn_snapshot(
    c: &Churn,
    leader: NodeId,
    store: Arc<dyn LocalStoreView>,
    physical_ms: u64,
) -> tokio::task::JoinHandle<Result<crate::leader::IntentOutcome, Error>> {
    let raft = c.nodes.get(&leader).expect("leader").raft.clone();
    let raft_state = c.nodes.get(&leader).expect("leader").raft_state.clone();
    let stable_log = c.stable_log.clone();
    let object_store = c.object_store.clone();

    let router = c.router.clone();
    tokio::spawn(async move {
        let clock = FixedClock {
            now: Hlc::from_parts(HlcParts {
                physical_ms,
                logical: 0,
            }),
        };
        let snapshot_store = RemoteSnapshotStore::new(
            SnapshotStoreConfig {
                object_store: object_store.clone(),
            },
            tokio_util::sync::CancellationToken::new(),
        );
        let leadership: Arc<dyn crate::api::RaftLeadership> = Arc::new(RaftNode::new(
            raft.clone(),
            crate::network::permissive_gate(),
            std::time::Duration::from_secs(30),
        ));
        let inputs = SnapshotInputs {
            // Large cap: these tests are not about chunking, so no truncation.
            max_snapshot_entries: std::num::NonZeroUsize::MAX,
            local_store: store,
            stable_log: stable_log.as_ref(),
            snapshot_store: &snapshot_store,
            raft_state: &raft_state,
            router,
            leadership,
            self_node_id: leader,
            quorum_poll_interval: Duration::from_millis(5),
            quorum_wait_timeout: Duration::from_secs(5),
            compression: CompressionLevel::fast(),
        };
        // The whole pipeline, which is what these tests are about: the build
        // proposes nothing, so a leadership change is caught by the
        // integrator's premise check plus the Raft generation fence.
        let build = build_snapshot(&inputs, clock.now).await?;
        let intent = build
            .commit_intent()
            .ok_or_else(|| Error::Storage("snapshot window was empty".into()))?;
        let (intents, _cancel) = crate::leader::Integrator::spawn_for_test(
            Arc::new(RaftNode::new(
                raft.clone(),
                crate::network::permissive_gate(),
                std::time::Duration::from_secs(30),
            )),
            Arc::new(raft_state.clone()),
        );
        let outcome = intents.submit(intent).await;
        drop(raft);
        outcome
    })
}

/// Multi-thread, not the `#[tokio::test]` default: each of these tests runs
/// three raft cores through several partition/election phases while a snapshot
/// pipeline sits parked on a blocking thread. On a single-threaded runtime the
/// three cores share one poller, so any stall — a RocksDB open, an fsync, a
/// machine-wide CPU contention spike from a parallel test run — freezes every
/// node's heartbeats and election timers together, and the cluster holds
/// elections it never meant to have. Worker threads service the timers
/// concurrently whenever the process gets any CPU, which is the production
/// shape and what these tests' leadership assumptions actually require.
///
/// Hard cap on one churn test. These run in ~2 s; the cap exists so a wait that
/// stops making progress ends the test instead of the run.
const CHURN_CAP: Duration = Duration::from_secs(120);

/// Wait until the stalling store reports the pipeline reached the window read —
/// or until the build ends without ever getting there.
///
/// `entered` fires from inside `StallingLocalStore::stream_window_ordered`, and
/// `build_snapshot` only reaches that call after its quorum wait *and* its
/// post-quorum leadership re-check. Three earlier exits return without touching
/// the store at all: `Error::QuorumUnreachable` on the 5 s quorum timeout,
/// `SnapshotResult::Skipped` when this node was demoted during that wait, and
/// `Skipped` when the committed watermark already covers the cutoff. Each of
/// them ends the task silently, so a bare `entered.notified().await` waits
/// forever — the test then neither passes nor fails, which is strictly worse
/// than failing: there is no assertion message, no stack, and the harness
/// reports it as still running until the whole run is killed.
///
/// Racing the notify against the join handle converts every one of those into
/// an immediate failure that names the outcome responsible. It cannot race the
/// other way: the notify is sent before the window's first pull blocks, so a
/// build that did reach the stall cannot also be complete.
async fn await_stall(
    entered: &Notify,
    task: &mut tokio::task::JoinHandle<Result<crate::leader::IntentOutcome, Error>>,
) {
    tokio::select! {
        () = entered.notified() => {}
        joined = task => panic!(
            "snapshot build ended before reaching the window stall: {joined:?}"
        ),
    }
}

/// Move leadership to `target` by isolating the incumbent first (the caller
/// does that), then triggering elections until every node in `observers`
/// follows `target`. Retries because votes are rejected while the deposed
/// leader's lease (election_timeout_max) is still fresh.
async fn force_leader(nodes: &BTreeMap<NodeId, TestNode>, target: NodeId, observers: &[NodeId]) {
    // Each `elect()` bumps the term and aborts any election already in flight,
    // so triggering faster than a full convergence (win vote → append the blank
    // entry → commit → ship any follower backlog → heartbeat) livelocks. Retry
    // well past `election_timeout_max` (400ms in `fast_raft_config`) and poll
    // the observers in between, so a win is observed rather than torn down.
    const TRIGGER_INTERVAL: Duration = Duration::from_millis(800);
    const POLL_INTERVAL: Duration = Duration::from_millis(25);

    let target_raft = &nodes.get(&target).expect("target").raft;
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(20))
        .expect("Instant + 20s does not overflow");
    loop {
        // Skip the trigger if `target` already leads — no point deposing it.
        if target_raft.current_leader().await != Some(target) {
            let _ = target_raft.trigger().elect().await;
        }
        let next_trigger = tokio::time::Instant::now()
            .checked_add(TRIGGER_INTERVAL)
            .expect("Instant + 800ms does not overflow");
        while tokio::time::Instant::now() < next_trigger {
            tokio::time::sleep(POLL_INTERVAL).await;
            let mut all_follow = true;
            for id in observers {
                if nodes.get(id).expect("observer").raft.current_leader().await != Some(target) {
                    all_follow = false;
                    break;
                }
            }
            if all_follow {
                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let mut seen = Vec::new();
            for id in observers {
                let leader = nodes.get(id).expect("observer").raft.current_leader().await;
                seen.push(format!("{id:?} follows {leader:?}"));
            }
            let term = target_raft.metrics().borrow().current_term;
            panic!(
                "could not elect {target:?} within deadline (target term {term}): {}",
                seen.join(", "),
            );
        }
    }
}

/// Apply a Normal entry through `sys` that re-writes the current state
/// verbatim — the "load": it moves the raft generation without changing
/// anything else.
async fn bump_generation(sys: &TestNode) {
    let cur = sys.raft_state.snapshot();
    crate::tests::write_state_fenced(
        &sys.raft,
        &sys.raft_state,
        ReplicatedRaftState {
            replica: cur.replica.clone(),
            manifest: cur.manifest_state.manifest.clone(),
            manifest_watermark: cur.manifest_state.manifest_watermark,
            hlc_cutoff: cur.manifest_state.hlc_cutoff,
        },
    )
    .await
    .expect("load write");
}

/// Commit a manifest entry as some other leader would, advancing both the
/// manifest end index and the watermark — i.e. moving exactly the premise a
/// stalled snapshot build depends on.
async fn commit_foreign_snapshot(sys: &TestNode) {
    let cur = sys.raft_state.snapshot();
    let start_index = cur
        .manifest_state
        .manifest
        .last()
        .map_or(0, |m| m.end_index.raw().saturating_add(1));
    let hlc = Hlc::from_parts(HlcParts {
        physical_ms: 2_500,
        logical: 0,
    });
    let mut manifest = cur.manifest_state.manifest.clone();
    manifest.push(crate::meta::SnapshotMetadata {
        start_hlc: hlc,
        end_hlc: hlc,
        start_index: LogIndex(start_index),
        end_index: LogIndex(start_index),
        snapshot_id: crate::ids::SnapshotId::open(LogIndex(start_index)),
    });
    crate::tests::write_state_fenced(
        &sys.raft,
        &sys.raft_state,
        ReplicatedRaftState {
            replica: cur.replica.clone(),
            manifest,
            manifest_watermark: Some(hlc),
            hlc_cutoff: Some(hlc),
        },
    )
    .await
    .expect("interim manifest write");
}

/// Load the committed manifest into a fresh stable log and count the entries.
async fn covered_entries(c: &Churn, via: NodeId) -> usize {
    let dir = tempfile::tempdir().expect("tempdir");
    let verify = StableLog::open_engine(dir.path().join("verify.stable"))
        .await
        .expect("open stable log");
    let manifest_state = c
        .nodes
        .get(&via)
        .expect("node")
        .raft_state
        .snapshot()
        .manifest_state
        .clone();
    let snapshot_store = RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: c.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    );
    integrate_manifest_for_test(Arc::new(snapshot_store), &verify, &manifest_state)
        .await
        .expect("load manifest");
    let entries: Vec<(LogIndex, Data)> = verify
        .range(LogIndex(0), LogIndex(1_000))
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();
    entries.len()
}

/// F-24 at cluster level, availability half: a snapshot that was stalled across
/// a leadership round-trip still publishes, **provided nothing it depends on
/// moved**.
///
/// The interim leader here churns the generation without touching the manifest,
/// the watermark, or the cutoff (`bump_generation` writes identical state), so
/// the completed snapshot is still a valid contiguous extension of exactly the
/// manifest it was built against.
///
/// The integrator checks what the build actually assumed — the watermark it
/// extends and the manifest end index its indices continue from — so a
/// bystander write does not cost a snapshot. A generation CAS would throw the
/// upload away on *any* interim write. The dangerous case is covered by
/// [`reelected_leader_snapshot_commit_is_refused_when_the_manifest_moved`].
#[tokio::test(flavor = "multi_thread")]
async fn reelected_leader_snapshot_commit_applies_when_its_premise_held() {
    crate::tests::within(
        "reelected_leader_snapshot_commit_applies_when_its_premise_held",
        CHURN_CAP,
        reelected_leader_snapshot_commit_applies_when_its_premise_held_body(),
    )
    .await;
}

/// Body of [`reelected_leader_snapshot_commit_applies_when_its_premise_held`], split out so the test
/// function is nothing but the hard cap around it.
async fn reelected_leader_snapshot_commit_applies_when_its_premise_held_body() {
    let c = boot_churn().await;
    let [n1, n2, n3] = c.ids;

    let entered = Arc::new(Notify::new());
    let (release_tx, release) = crossbeam_channel::bounded::<()>(1);
    let stalling = Arc::new(StallingLocalStore {
        inner: c.local_store.clone(),
        entered: entered.clone(),
        release,
    });

    let mut task = spawn_snapshot(&c, n1, stalling, 3_000);
    await_stall(&entered, &mut task).await;

    // Demote n1: cut it off, elect n2, apply load through n2.
    c.partition.isolate(n1, &c.ids);
    force_leader(&c.nodes, n2, &[n2, n3]).await;
    let n2sys = c.nodes.get(&n2).expect("n2");
    bump_generation(n2sys).await;
    bump_generation(n2sys).await;

    // Re-elect n1: heal, let it catch up on n2's writes, isolate n2, elect.
    c.partition.heal_all();
    let caught_up = n2sys.raft_state.snapshot().generation;
    crate::tests::wait_until(
        "n1 catches up on the interim writes",
        Duration::from_secs(5),
        || async {
            c.nodes
                .get(&n1)
                .expect("n1")
                .raft_state
                .snapshot()
                .generation
                >= caught_up
        },
    )
    .await;
    c.partition.isolate(n2, &c.ids);
    force_leader(&c.nodes, n1, &[n1, n3]).await;
    c.partition.heal_all();

    let manifest_before = c
        .nodes
        .get(&n1)
        .expect("n1")
        .raft_state
        .snapshot()
        .manifest_state
        .manifest
        .clone();

    // Release the stalled pipeline: its commit goes through real raft on the
    // re-elected n1. Its premise held, so it must land.
    let _ = release_tx.send(());
    let outcome = task.await.expect("join").expect("publish");
    assert!(
        matches!(outcome, crate::leader::IntentOutcome::Applied { .. }),
        "a snapshot whose premise still holds must publish, got: {outcome:?}",
    );

    // What actually matters: the manifest is a clean contiguous extension of what
    // it was, on every node, and no data was lost.
    for (id, sys) in &c.nodes {
        let manifest = sys.raft_state.snapshot().manifest_state.manifest.clone();
        assert!(
            manifest.starts_with(&manifest_before),
            "manifest on {id:?} must extend, never rewrite, what was committed: \
             {manifest:?} vs {manifest_before:?}",
        );
    }
    crate::tests::wait_until("manifest converges", Duration::from_secs(5), || async {
        let n1m = c
            .nodes
            .get(&n1)
            .expect("n1")
            .raft_state
            .snapshot()
            .manifest_state
            .manifest
            .clone();
        n1m.len() == manifest_before.len().saturating_add(1)
    })
    .await;
    assert_eq!(covered_entries(&c, n1).await, usize::from(SEEDED_ENTRIES));
}

/// F-24 at cluster level, safety half: when the interim leader commits a
/// snapshot of its own, the stalled build's global indices no longer continue
/// from the manifest and its window no longer starts at the watermark. Publishing
/// it would corrupt the index space, so it must be refused. The interim write
/// here moves the manifest, so this exercises the premise check rather than the
/// coarse generation fence.
#[tokio::test(flavor = "multi_thread")]
async fn reelected_leader_snapshot_commit_is_refused_when_the_manifest_moved() {
    crate::tests::within(
        "reelected_leader_snapshot_commit_is_refused_when_the_manifest_moved",
        CHURN_CAP,
        reelected_leader_snapshot_commit_is_refused_when_the_manifest_moved_body(),
    )
    .await;
}

/// Body of [`reelected_leader_snapshot_commit_is_refused_when_the_manifest_moved`], split out so the test
/// function is nothing but the hard cap around it.
async fn reelected_leader_snapshot_commit_is_refused_when_the_manifest_moved_body() {
    let c = boot_churn().await;
    let [n1, n2, n3] = c.ids;

    let entered = Arc::new(Notify::new());
    let (release_tx, release) = crossbeam_channel::bounded::<()>(1);
    let stalling = Arc::new(StallingLocalStore {
        inner: c.local_store.clone(),
        entered: entered.clone(),
        release,
    });

    let mut task = spawn_snapshot(&c, n1, stalling, 3_000);
    await_stall(&entered, &mut task).await;

    // Demote n1 and let n2 commit a *real* manifest entry, so the stalled build's
    // premise (watermark + manifest end index) genuinely moves.
    c.partition.isolate(n1, &c.ids);
    force_leader(&c.nodes, n2, &[n2, n3]).await;
    let n2sys = c.nodes.get(&n2).expect("n2");
    commit_foreign_snapshot(n2sys).await;

    // Re-elect n1 so the publish attempt runs on a leader that can actually write.
    c.partition.heal_all();
    let caught_up = n2sys.raft_state.snapshot().generation;
    crate::tests::wait_until(
        "n1 catches up on the interim manifest write",
        Duration::from_secs(5),
        || async {
            c.nodes
                .get(&n1)
                .expect("n1")
                .raft_state
                .snapshot()
                .generation
                >= caught_up
        },
    )
    .await;
    c.partition.isolate(n2, &c.ids);
    force_leader(&c.nodes, n1, &[n1, n3]).await;
    c.partition.heal_all();

    let manifest_before = c
        .nodes
        .get(&n1)
        .expect("n1")
        .raft_state
        .snapshot()
        .manifest_state
        .manifest
        .clone();

    let _ = release_tx.send(());
    let outcome = task.await.expect("join");
    assert!(
        !matches!(outcome, Ok(crate::leader::IntentOutcome::Applied { .. })),
        "a snapshot whose premise moved must never publish, got: {outcome:?}",
    );

    // Nothing torn: the interim entry survives untouched on every node.
    for (id, sys) in &c.nodes {
        assert_eq!(
            sys.raft_state.snapshot().manifest_state.manifest,
            manifest_before,
            "manifest must be untouched on {id:?}",
        );
    }

    // And the retry covers everything, so the refusal cost availability, not data.
    let result = spawn_snapshot(&c, n1, c.local_store.clone(), 4_000)
        .await
        .expect("join")
        .expect("retried snapshot must publish");
    assert!(
        matches!(result, crate::leader::IntentOutcome::Applied { .. }),
        "expected the retried snapshot to publish, got: {result:?}",
    );
}

/// The simpler interleaving: the deposed leader is never re-elected; its
/// in-flight commit is rejected by openraft (a follower cannot client_write)
/// and the new leader's own snapshot covers the data.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_leader_snapshot_commit_fails_without_corruption() {
    crate::tests::within(
        "demoted_leader_snapshot_commit_fails_without_corruption",
        CHURN_CAP,
        demoted_leader_snapshot_commit_fails_without_corruption_body(),
    )
    .await;
}

/// Body of [`demoted_leader_snapshot_commit_fails_without_corruption`], split out so the test
/// function is nothing but the hard cap around it.
async fn demoted_leader_snapshot_commit_fails_without_corruption_body() {
    let c = boot_churn().await;
    let [n1, n2, n3] = c.ids;

    let entered = Arc::new(Notify::new());
    let (release_tx, release) = crossbeam_channel::bounded::<()>(1);
    let stalling = Arc::new(StallingLocalStore {
        inner: c.local_store.clone(),
        entered: entered.clone(),
        release,
    });

    let mut task = spawn_snapshot(&c, n1, stalling, 3_000);
    await_stall(&entered, &mut task).await;

    c.partition.isolate(n1, &c.ids);
    force_leader(&c.nodes, n2, &[n2, n3]).await;
    let n2sys = c.nodes.get(&n2).expect("n2");
    bump_generation(n2sys).await;

    // Heal and wait until n1 has accepted its demotion.
    c.partition.heal_all();
    crate::tests::wait_until("n1 follows n2", Duration::from_secs(5), || async {
        c.nodes.get(&n1).expect("n1").raft.current_leader().await == Some(n2)
    })
    .await;

    let manifest_before = n2sys.raft_state.snapshot().manifest_state.manifest.clone();

    let _ = release_tx.send(());
    let result = task.await.expect("join");
    assert!(
        !matches!(result, Ok(crate::leader::IntentOutcome::Applied { .. })),
        "demoted leader's snapshot commit must not apply, got: {result:?}",
    );

    for (id, sys) in &c.nodes {
        assert_eq!(
            sys.raft_state.snapshot().manifest_state.manifest,
            manifest_before,
            "manifest must be untouched on {id:?}",
        );
    }

    // The new leader snapshots the same data successfully.
    let result = spawn_snapshot(&c, n2, c.local_store.clone(), 4_000)
        .await
        .expect("join")
        .expect("new leader's snapshot must commit");
    assert!(
        matches!(result, crate::leader::IntentOutcome::Applied { .. }),
        "expected the retried snapshot to be published, got: {result:?}",
    );
    assert_eq!(covered_entries(&c, n2).await, usize::from(SEEDED_ENTRIES));
}
