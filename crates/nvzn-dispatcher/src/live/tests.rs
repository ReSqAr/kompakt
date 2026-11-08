//! Live-view tests.

use super::coverage::Held;
use super::*;
use crate::testkit::{
    FLUSH_INTERVAL, NewTodoV1, NewUserV3, PingedV1, TestLog, Todo, TodoId, User, UserId, data_id,
    event, hlc, inmemory, payload, tenant, tenant_id,
};
use crate::{Dispatcher, Reader, Ref, Store};
use crate::{StopReason, Watermark};
use bytes::Bytes;
use crossbeam_combinators::CancelHandle;
use crossbeam_combinators::Channel;
use proptest::prelude::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use zslg::{Data, DataId, Hlc, LogIndex, StableLogEvent, UnstableEvent};

type Users = event::UserEvent<'static>;

fn new_user(id: u128, name: &str) -> event::Event {
    event::Event::NewUser(NewUserV3 {
        id: UserId(id),
        name: name.to_string(),
        address: None,
        email: None,
    })
}

fn pinged(id: u128, ts: u128) -> event::Event {
    event::Event::Pinged(PingedV1 { id: UserId(id), ts })
}

fn new_todo(id: u128, user: u128) -> event::Event {
    event::Event::NewTodo(NewTodoV1 {
        id: TodoId(id),
        user_id: UserId(user),
        title: "t".to_string(),
    })
}

fn blob(events: &[event::Event]) -> Bytes {
    payload(tenant_id(), events)
}

/// A reader over `User` and `Todo` with no fold running: F moves only when
/// a test commits a watermark ([`set_f`]).
async fn rig() -> (Arc<dyn Store>, TestLog, Reader<event::Event>) {
    let store = inmemory();
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    manager.register::<Todo>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    (store, log, reader)
}

/// The store's watermark, as if the fold had committed through `did`.
fn set_f(store: &Arc<dyn Store>, did: DataId) {
    store
        .write()
        .unwrap()
        .commit(Watermark {
            index: None,
            data_id: Some(did),
            hlc: Some(did.hlc()),
        })
        .unwrap();
}

async fn user(live: &Live<event::Event>, id: u128) -> Option<User> {
    let live = live.clone();
    tokio::task::spawn_blocking(move || {
        live.snapshot()
            .unwrap()
            .get(Ref::<User>::new(UserId(id).into()))
            .unwrap()
    })
    .await
    .unwrap()
}

async fn todo(live: &Live<event::Event>, id: u128) -> Option<Todo> {
    let live = live.clone();
    tokio::task::spawn_blocking(move || {
        live.snapshot()
            .unwrap()
            .get(Ref::<Todo>::new(TodoId(id).into()))
            .unwrap()
    })
    .await
    .unwrap()
}

fn held(live: &Live<event::Event>) -> usize {
    live.inner.lock().coverage.held().0
}

fn coverage(live: &Live<event::Event>) -> &'static str {
    live.inner.lock().coverage.name()
}

/// Waits until the integrator has taken in what the test pushed.
async fn settle(live: &Live<event::Event>, pred: impl Fn(&Live<event::Event>) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pred(live) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the integrator caught up");
}

const SHORT: Duration = Duration::from_millis(50);
const LONG: Duration = Duration::from_secs(5);

#[tokio::test]
async fn wait_for_sees_an_entry_once_held() {
    let (_store, mut log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    let d = log.push_unstable(blob(&[new_user(1, "holden")]));
    assert_eq!(live.wait_for(d, LONG).await, Wait::Seen);
    assert!(
        user(&live, 1).await.is_some(),
        "Seen: a snapshot now shows it"
    );
}

/// An entry that reached this node already stable is never held: only an
/// F advance shows it, and the retirement wakes the wait.
#[tokio::test]
async fn wait_for_wakes_when_f_passes_the_entry() {
    let (store, _log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    let d = data_id(9, 9);
    let waiting = tokio::spawn({
        let live = live.clone();
        async move { live.wait_for(d, LONG).await }
    });
    tokio::time::sleep(SHORT).await;
    assert!(!waiting.is_finished());
    set_f(&store, d);
    // The retirer, as the fold's watermark watch would run it.
    let inner = Arc::clone(&live.inner);
    tokio::task::spawn_blocking(move || inner.retire_now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.await.unwrap(), Wait::Seen);
}

#[tokio::test]
async fn wait_for_times_out_on_an_entry_that_never_arrives() {
    let (_store, _log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    assert_eq!(live.wait_for(data_id(5, 5), SHORT).await, Wait::TimedOut);
}

/// Overflowed, the view serves store@F alone: an entry it dropped is not
/// served, and a wait runs to its deadline.
#[tokio::test]
async fn wait_for_times_out_while_overflowed() {
    let (_store, mut log, reader) = rig().await;
    let live = reader
        .live_started::<Users>(LiveLimits {
            max_held_events: 1,
            max_held_bytes: usize::MAX,
        })
        .unwrap();
    log.push_unstable(blob(&[new_user(1, "a")]));
    log.push_unstable(blob(&[new_user(2, "b")]));
    settle(&live, |l| coverage(l) == "overflowed").await;
    assert_eq!(
        live.wait_for(data_id(5, 5), Duration::from_millis(50))
            .await,
        Wait::TimedOut
    );
}

/// Overflowed, an entry the fold has passed is in store@F, which is what
/// a snapshot serves: the wait answers at once, though the view has not
/// recovered.
#[tokio::test(flavor = "multi_thread")]
async fn wait_for_sees_a_folded_entry_while_overflowed() {
    let (store, log, reader) = rig().await;
    let live = reader
        .live_started::<Users>(LiveLimits {
            max_held_events: 2,
            max_held_bytes: usize::MAX,
        })
        .unwrap();
    for (i, ms) in [10u64, 11, 12].into_iter().enumerate() {
        let id = u128::try_from(i).unwrap();
        log.push_unstable_at(data_id(ms, ms), blob(&[new_user(id, "x")]));
    }
    settle(&live, |l| coverage(l) == "overflowed").await;
    let d = data_id(10, 10);
    set_f(&store, d);
    let inner = Arc::clone(&live.inner);
    tokio::task::spawn_blocking(move || inner.retire_now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(coverage(&live), "overflowed", "F is below `until`");
    assert_eq!(live.wait_for(d, SHORT).await, Wait::Seen);
}

/// Not started yet: the stable snapshot is served, and a wait that ends
/// before the start times out.
#[tokio::test]
async fn not_started_serves_the_stable_snapshot_and_a_wait_times_out() {
    let (store, mut log, reader) = rig().await;
    let live = reader.not_started::<Users>(LiveLimits::default()).unwrap();
    let holden = User {
        id: UserId(1),
        name: "holden".into(),
        address: None,
        email: None,
        last_ping: None,
    };
    crate::testkit::commit_row(&store, tenant_id(), Ref::new(UserId(1).into()), &holden);
    let d = log.push_unstable(blob(&[new_user(2, "naomi")]));
    assert_eq!(
        live.wait_for(d, Duration::from_millis(50)).await,
        Wait::TimedOut
    );
    assert!(user(&live, 1).await.is_some(), "the stable row");
    assert!(
        user(&live, 2).await.is_none(),
        "nothing held before the start"
    );
}

/// A sync that fails at startup is retried: the view starts once one
/// succeeds, rather than serving the stable snapshot for good.
#[tokio::test]
async fn the_start_retries_a_failed_sync() {
    let store = inmemory();
    let log = TestLog::new();
    let failing: Arc<dyn zslg::Reader> =
        Arc::new(crate::testkit::FailingSyncReader::new(log.reader(), 2));
    let mut dispatcher = Dispatcher::from_db(
        store,
        log.writer(),
        failing,
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (writer, reader) = manager.finalise().unwrap();
    let live = reader.live::<Users>().unwrap();
    assert_eq!(coverage(&live), "not_started");
    // The fold runs: the start waits for it to pass the write below.
    let _fold = dispatcher.finalise().await.unwrap();
    // A write before the start: its wait waits for the start too.
    let d = writer.write(&[new_user(3, "amos")]).await.unwrap();
    assert_eq!(live.wait_for(d, LONG).await, Wait::Seen);
    assert_ne!(
        coverage(&live),
        "not_started",
        "started after two failed syncs"
    );
}

#[test]
fn liveness_follows_the_events() {
    assert!(is_live::<User, Users>());
    assert!(!is_live::<Todo, Users>());
    assert!(
        is_live::<Todo, event::Event>(),
        "the whole set holds everything"
    );
    crate::assert_live!(event::UserEvent<'static>, [User]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_s_event_is_live_at_once_and_folds_only_into_live_projections() {
    let (_store, mut log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    let changed = live.changed();
    log.push_unstable(blob(&[new_user(1, "ada"), new_todo(7, 1)]));
    settle(&live, |l| held(l) == 1).await;

    assert_eq!(user(&live, 1).await.unwrap().name, "ada");
    assert!(
        todo(&live, 7).await.is_none(),
        "Todo folds NewTodo, outside S: it reads at F"
    );
    assert!(
        changed.has_changed().unwrap(),
        "an S arrival moves `changed`"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_without_s_events_is_not_held() {
    let (_store, mut log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    log.push_unstable(blob(&[new_todo(7, 1)]));
    log.push_unstable(blob(&[new_user(1, "ada")]));
    settle(&live, |l| held(l) == 1).await;
    assert_eq!(held(&live), 1, "only the write with an S event is held");
}

#[tokio::test(flavor = "multi_thread")]
async fn held_events_retire_when_f_covers_them_not_at_the_watermark() {
    let (store, log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    let did = data_id(10, 1);
    log.push_unstable_at(did, blob(&[new_user(1, "ada")]));
    settle(&live, |l| held(l) == 1).await;

    log.watermark(20);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        user(&live, 1).await.is_some(),
        "zslg's watermark passing it does not drop it: the store does not have it"
    );

    // The fold passes it without storing it: it never converged.
    set_f(&store, did);
    assert!(user(&live, 1).await.is_none(), "F covers it: gone");
    assert_eq!(held(&live), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn at_an_hlc_tie_only_the_data_id_decides() {
    let (store, log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    let (first, second) = (data_id(10, 1), data_id(10, 2));
    log.push_unstable_at(first, blob(&[new_user(1, "ada")]));
    log.push_unstable_at(second, blob(&[new_user(2, "grace")]));
    settle(&live, |l| held(l) == 2).await;

    set_f(&store, first);
    assert!(user(&live, 1).await.is_none());
    assert!(
        user(&live, 2).await.is_some(),
        "same hlc as F, larger DataId: not covered"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn held_events_fold_in_data_id_order_whatever_their_arrival() {
    let (_store, log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    log.push_unstable_at(data_id(20, 2), blob(&[pinged(1, 99)]));
    log.push_unstable_at(data_id(10, 1), blob(&[new_user(1, "ada")]));
    settle(&live, |l| held(l) == 2).await;
    assert_eq!(user(&live, 1).await.unwrap().last_ping, Some(99));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redelivery_is_held_once_and_announced_once() {
    let (_store, log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    let did = data_id(10, 1);
    log.push_unstable_at(did, blob(&[new_user(1, "ada")]));
    settle(&live, |l| held(l) == 1).await;
    let mut changed = live.changed();
    changed.mark_unchanged();
    assert!(!live.inner.arrive(Data {
        data_id: did,
        blob: blob(&[new_user(1, "ada")]),
    }));
    assert_eq!(held(&live), 1);
    assert!(!changed.has_changed().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_f_advance_does_not_move_changed() {
    let (store, log, reader) = rig().await;
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    let did = data_id(10, 1);
    log.push_unstable_at(did, blob(&[new_user(1, "ada")]));
    settle(&live, |l| held(l) == 1).await;
    let mut changed = live.changed();
    changed.mark_unchanged();
    set_f(&store, did);
    let _ = user(&live, 1).await;
    assert!(!changed.has_changed().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn overflow_serves_f_freezes_until_and_recovers_with_what_it_collected() {
    let (store, log, reader) = rig().await;
    let live = reader
        .live_started::<Users>(LiveLimits {
            max_held_events: 2,
            max_held_bytes: usize::MAX,
        })
        .unwrap();
    for (i, ms) in [10u64, 11, 12].into_iter().enumerate() {
        let id = u128::try_from(i).unwrap();
        log.push_unstable_at(data_id(ms, ms), blob(&[new_user(id, "x")]));
    }
    settle(&live, |l| coverage(l) == "overflowed").await;
    assert!(user(&live, 0).await.is_none(), "overflowed: store@F alone");
    let until = data_id(12, 12);

    // Below `until`: ignored. Above: held, not served.
    assert!(!live.inner.arrive(Data {
        data_id: data_id(9, 9),
        blob: blob(&[new_user(9, "old")]),
    }));
    log.push_unstable_at(data_id(13, 13), blob(&[new_user(13, "new")]));
    settle(&live, |l| held(l) == 1).await;
    assert_eq!(held(&live), 1);
    assert!(user(&live, 13).await.is_none());

    // F passes `until`: complete again, serving what it collected.
    let mut changed = live.changed();
    changed.mark_unchanged();
    set_f(&store, until);
    assert_eq!(user(&live, 13).await.unwrap().name, "new");
    assert_eq!(coverage(&live), "complete");
    assert!(changed.has_changed().unwrap(), "recovering moves `changed`");
}

#[tokio::test(flavor = "multi_thread")]
async fn hitting_the_cap_again_moves_until() {
    let (store, log, reader) = rig().await;
    let live = reader
        .live_started::<Users>(LiveLimits {
            max_held_events: 1,
            max_held_bytes: usize::MAX,
        })
        .unwrap();
    for ms in [10u64, 11] {
        log.push_unstable_at(data_id(ms, ms), blob(&[new_user(1, "x")]));
    }
    settle(&live, |l| coverage(l) == "overflowed").await;
    for ms in [12u64, 13] {
        log.push_unstable_at(data_id(ms, ms), blob(&[new_user(2, "y")]));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    set_f(&store, data_id(11, 11));
    assert!(user(&live, 2).await.is_none());
    assert_eq!(coverage(&live), "overflowed", "until moved to 13");
    set_f(&store, data_id(13, 13));
    let _ = user(&live, 2).await;
    assert_eq!(coverage(&live), "complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cold_start_scan_holds_what_is_stable_but_not_folded() {
    let (_store, mut log, reader) = rig().await;
    // Stable before the view starts, F still at nothing: neither the tail
    // nor the store has it.
    log.append(blob(&[new_user(1, "ada")]));
    log.append(blob(&[new_todo(7, 1)]));
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    assert_eq!(user(&live, 1).await.unwrap().name, "ada");
    assert_eq!(coverage(&live), "complete");
    assert_eq!(held(&live), 1, "only the S event is kept");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overflow_during_the_scan_releases_the_waiting_snapshots() {
    let (_store, mut log, reader) = rig().await;
    for id in 0..5 {
        log.append(blob(&[new_user(id, "x")]));
    }
    let live = reader
        .live_started::<Users>(LiveLimits {
            max_held_events: 2,
            max_held_bytes: usize::MAX,
        })
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), user(&live, 0))
        .await
        .expect("the snapshot does not hang on an overflowed scan");
    assert!(got.is_none(), "overflowed: store@F alone");

    // The scan read on to its end: the third dropped is `until`, the
    // last two held; nothing it read is lost.
    settle(&live, |l| {
        matches!(
            l.inner.lock().coverage,
            Coverage::Overflowed { scan: false, .. }
        )
    })
    .await;
    assert_eq!(held(&live), 2);
}

/// An overflow while the scan runs does not recover before the scan has
/// read to its end, however far F gets: what it has not read yet is
/// neither held nor folded.
#[tokio::test(flavor = "multi_thread")]
async fn an_overflow_during_the_scan_recovers_only_after_it() {
    let (store, _log, reader) = rig().await;
    let live = reader
        .live_started::<Users>(LiveLimits {
            max_held_events: 1,
            max_held_bytes: usize::MAX,
        })
        .unwrap();
    // As if the scan were running.
    live.inner.lock().coverage = Coverage::Scanning {
        held: Held::new(),
        bytes: 0,
    };
    for ms in [10u64, 11] {
        live.inner.arrive(Data {
            data_id: data_id(ms, ms),
            blob: blob(&[new_user(u128::from(ms), "x")]),
        });
    }
    assert_eq!(coverage(&live), "overflowed");

    set_f(&store, data_id(11, 11));
    live.inner.retire_now().unwrap();
    assert_eq!(coverage(&live), "overflowed", "the scan has not ended");

    live.inner.finish_scan();
    assert_eq!(coverage(&live), "complete", "ended, and F covers `until`");
}

/// A reader whose stable and unstable subscriptions stay quiet: the scan
/// cannot finish, so a snapshot waits on `scanned`.
struct StalledReader {
    stable: Mutex<Option<crossbeam_channel::Sender<Result<StableLogEvent, zslg::LogApiError>>>>,
    tail: Mutex<Option<crossbeam_channel::Sender<Result<UnstableEvent, zslg::LogApiError>>>>,
    watermark: Hlc,
}

impl StalledReader {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stable: Mutex::new(None),
            tail: Mutex::new(None),
            watermark: hlc(1_000),
        })
    }
}

#[async_trait::async_trait]
impl zslg::Reader for StalledReader {
    async fn sync(&self) -> Result<(), zslg::LogApiError> {
        Ok(())
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        Some(self.watermark)
    }

    fn hlc_now(&self) -> Hlc {
        self.watermark
    }

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> zslg::StableStream {
        let (tx, rx) = crossbeam_channel::bounded(1);
        *self.stable.lock().expect("test mutex") = Some(tx);
        zslg::StableStream::new(Channel::from_infinite(rx, "stalled stable"), after)
    }

    fn unstable_tail(
        &self,
    ) -> Channel<Result<Data, zslg::LogApiError>, crossbeam_combinators::Finite> {
        Channel::from_vec(Vec::new())
    }

    fn subscribe_unstable_tail(
        &self,
    ) -> Channel<Result<UnstableEvent, zslg::LogApiError>, crossbeam_combinators::Infinite> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        *self.tail.lock().expect("test mutex") = Some(tx);
        Channel::from_infinite(rx, "stalled tail")
    }
}

/// Cancelling while the cold-start scan is still running must wake a
/// `snapshot()` parked on `scanned`; the `run` guard calls `finish_scan`
/// on every exit, including `AbortReason::Cancelled`.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_mid_scan_wakes_a_waiting_snapshot() {
    let store = inmemory();
    let log = TestLog::new();
    let stalled = StalledReader::new();
    let stalled_reader: Arc<dyn zslg::Reader> = stalled.clone();
    let mut dispatcher = Dispatcher::from_db(
        store,
        log.writer(),
        stalled_reader.clone(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    let live = reader.not_started::<Users>(LiveLimits::default()).unwrap();
    let (handle, cancellation) = CancelHandle::new();
    let (_fold_tx, folded) = tokio::sync::watch::channel(None);
    begin(&live, &stalled_reader, &cancellation, folded).unwrap();
    assert_eq!(coverage(&live), "scanning");

    let waiting = {
        let live = live.clone();
        tokio::task::spawn_blocking(move || live.snapshot().map(|_| ()))
    };
    tokio::time::sleep(SHORT).await;
    assert!(!waiting.is_finished(), "the snapshot waits for the scan");

    handle.cancel();
    tokio::time::timeout(LONG, waiting)
        .await
        .expect("the cancel wakes the waiting snapshot")
        .expect("the snapshot task")
        .expect("the snapshot");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ended_tail_stops_the_dispatcher() {
    let store = inmemory();
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    let fold = dispatcher.finalise().await.unwrap();
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();

    log.hang_up_unstable();
    let ended = tokio::time::timeout(Duration::from_secs(5), fold)
        .await
        .expect("the dispatcher stops")
        .unwrap();
    assert!(
        matches!(ended, Err(Error::FoldStopped(StopReason::LiveStreamEnded))),
        "{ended:?}"
    );
    let live2 = live.clone();
    let read = tokio::task::spawn_blocking(move || live2.snapshot().map(|_| ()))
        .await
        .unwrap();
    assert!(matches!(
        read,
        Err(Error::FoldStopped(StopReason::LiveStreamEnded))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn no_snapshot_waits_on_a_stalled_fold() {
    let (_store, mut log, reader) = rig().await;
    // No fold runs at all in this rig: F never moves.
    let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
    log.push_unstable(blob(&[new_user(1, "ada")]));
    settle(&live, |l| held(l) == 1).await;
    let got = tokio::time::timeout(Duration::from_secs(1), user(&live, 1))
        .await
        .expect("served without the fold");
    assert!(got.is_some());
}

/// One event of the property test: a user's creation or ping, where it
/// arrives (`tail`: through the unstable tail; otherwise only through
/// the fold), and its position.
#[derive(Debug, Clone)]
struct Step {
    user: u128,
    ping: Option<u128>,
    tail: bool,
}

fn steps() -> impl Strategy<Value = (Vec<Step>, usize)> {
    prop::collection::vec(
        (0u128..3, prop::option::of(0u128..100), any::<bool>())
            .prop_map(|(user, ping, tail)| Step { user, ping, tail }),
        1..12,
    )
    .prop_flat_map(|steps| {
        let n = steps.len();
        (Just(steps), 0..=n)
    })
}

fn step_event(step: &Step) -> event::Event {
    match step.ping {
        Some(ts) => pinged(step.user, ts),
        None => new_user(step.user, &format!("u{}", step.user)),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// A snapshot equals a full fold of: everything up to F, plus what
    /// reached the tail above it, in DataId order. Events above F that
    /// never reached the tail are missing (the floor's), in any
    /// interleaving of arrivals.
    #[test]
    fn a_snapshot_is_the_fold_of_f_plus_what_reached_the_tail((steps, cut) in steps()) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let (store, log, reader) = rig().await;
            let ids: Vec<DataId> = (0..steps.len())
                .map(|i| {
                    let i = u64::try_from(i).unwrap();
                    data_id(i.saturating_add(10), i)
                })
                .collect();

            // What the store holds at F: the fold of the first `cut`.
            let mut manager = crate::ProjectorManager::<event::Event>::new(tenant());
            manager.register::<User>().unwrap();
            let mut txn = store.write().unwrap();
            for step in &steps[..cut] {
                fold_event(&manager.projectors, tenant(), &step_event(step), &mut txn).unwrap();
            }
            let f = cut.checked_sub(1).map(|i| ids[i]);
            txn.commit(Watermark {
                index: None,
                data_id: f,
                hlc: f.map(DataId::hlc),
            })
            .unwrap();

            let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
            // Arrivals in reverse order, above and below F alike.
            let mut expected_held: usize = 0;
            for (i, step) in steps.iter().enumerate().rev() {
                if step.tail {
                    log.push_unstable_at(ids[i], blob(&[step_event(step)]));
                    if i >= cut {
                        expected_held = expected_held.saturating_add(1);
                    }
                }
            }
            settle(&live, |l| held(l) == expected_held).await;

            // Expected: a fresh fold of the first `cut` and the tail ones above.
            let expect_store = inmemory();
            let mut txn = expect_store.write().unwrap();
            for (i, step) in steps.iter().enumerate() {
                if i < cut || step.tail {
                    fold_event(&manager.projectors, tenant(), &step_event(step), &mut txn).unwrap();
                }
            }
            txn.commit(Watermark::default()).unwrap();

            for id in 0..3u128 {
                let want: Option<User> = crate::ReadTransactionExt::get::<User>(
                    &*expect_store.read().unwrap(),
                    tenant_id(),
                    Ref::new(UserId(id).into()),
                )
                .unwrap();
                let got = user(&live, id).await;
                assert_eq!(got, want, "user {id}");
            }
            drop(log);
        });
    }
}
