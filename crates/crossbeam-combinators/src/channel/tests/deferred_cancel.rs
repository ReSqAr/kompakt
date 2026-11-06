//! `defer_async`, `until` and `Cancellation`.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;

// ── defer_async ────────────────────────────────────────────────────

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "multi_thread")]
async fn defer_async_starts_on_first_read_and_delegates_the_verdict() {
    let opened = Arc::new(AtomicUsize::new(0));
    let handle = tokio::runtime::Handle::current();
    let o = Arc::clone(&opened);
    let ch = Channel::defer_async(&handle, move || async move {
        o.fetch_add(1, Ordering::SeqCst);
        Channel::from_vec(vec![1u32, 2])
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        opened.load(Ordering::SeqCst),
        0,
        "nothing runs at construction"
    );
    let got = tokio::task::spawn_blocking(move || {
        let mut ch = ch;
        let items: Vec<u32> = std::iter::from_fn(|| ch.recv().ok()).collect();
        (items, ch.end_reason())
    })
    .await
    .unwrap();
    assert_eq!(got, (vec![1, 2], End::Complete));
    assert_eq!(opened.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "multi_thread")]
async fn deferred_phases_in_a_chain_open_one_at_a_time() {
    let open = Arc::new(AtomicUsize::new(0));
    let max_open = Arc::new(AtomicUsize::new(0));
    let handle = tokio::runtime::Handle::current();
    let phases: Vec<Channel<u32, Finite>> = (0..3u32)
        .map(|i| {
            let (open, max_open) = (Arc::clone(&open), Arc::clone(&max_open));
            Channel::defer_async(&handle, move || async move {
                let now = open.fetch_add(1, Ordering::SeqCst) + 1;
                max_open.fetch_max(now, Ordering::SeqCst);
                let ch = Channel::from_vec(vec![i]);
                open.fetch_sub(1, Ordering::SeqCst);
                ch
            })
        })
        .collect();
    let got = tokio::task::spawn_blocking(move || {
        let mut ch = Channel::chain_all(phases);
        std::iter::from_fn(|| ch.recv().ok()).collect::<Vec<_>>()
    })
    .await
    .unwrap();
    assert_eq!(got, vec![0, 1, 2]);
    assert_eq!(
        max_open.load(Ordering::SeqCst),
        1,
        "never two openers at once"
    );
}

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_opener_that_dies_aborts_the_phase() {
    let handle = tokio::runtime::Handle::current();
    let ch: Channel<u32, Finite> =
        Channel::defer_async(&handle, || async { panic!("open blew up") });
    let end = tokio::task::spawn_blocking(move || {
        let mut ch = ch;
        ch.recv()
    })
    .await
    .unwrap();
    assert_eq!(
        end,
        Err(End::Aborted {
            reason: AbortReason::ProducerGone {
                what: "defer_async"
            }
        })
    );
}

// ── until / Cancellation ───────────────────────────────────────────

/// The point of `until`: a consumer parked in `recv` on a stream that has
/// gone quiet — sender alive, nothing arriving — is woken by the cancel.
#[test]
fn until_wakes_a_blocked_recv_with_cancelled() {
    let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
    let (handle, cancellation) = CancelHandle::new();
    let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(50));
            handle.cancel();
        });
        assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
    });
    assert!(cancellation.is_cancelled());
    assert_eq!(ch.end_reason(), AbortReason::Cancelled);
}

/// A race, not a merge: a finite stream that closes ends the bound stream
/// at once instead of waiting on the cancellation, and a cancel that comes
/// later does not rewrite a declared completion.
#[test]
fn until_keeps_a_clean_close_after_a_later_cancel() {
    let (tx, rx) = crossbeam_channel::bounded(2);
    let (handle, cancellation) = CancelHandle::new();
    let mut ch = closed(rx).until(&cancellation);
    tx.send(1).unwrap();
    drop(tx);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Err(End::Complete));
    handle.cancel();
    assert_eq!(ch.end_reason(), End::Complete);
}

/// A handle that goes away without `cancel` ends the stream at once, even
/// with its source still alive — that source may be what whoever holds the
/// stream keeps alive — but it is not a cancel.
#[test]
fn until_a_handle_dropped_unfired_ends_aborted() {
    let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
    let (handle, cancellation) = CancelHandle::new();
    let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
    drop(handle);
    assert_eq!(ch.recv(), Err(AbortReason::CancelHandleDropped));
    assert!(!cancellation.is_cancelled());
}

/// The verdict follows the flag even when the select wakes on the inner
/// side first: a cancel whose teardown disconnects the source is still a
/// cancel, never that source's abort.
#[test]
fn until_a_cancel_that_tears_down_the_source_is_still_a_cancel() {
    for _ in 0..64 {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = Channel::from_infinite(rx, "source").until(&cancellation);
        handle.cancel();
        drop(tx);
        assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
    }
}

/// A stream bound after the cancel has already fired ends immediately.
#[test]
fn until_an_already_fired_cancellation_ends_at_once() {
    let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
    let (handle, cancellation) = CancelHandle::new();
    handle.cancel();
    handle.cancel(); // idempotent
    let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
    assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
}

/// `Cancelled` condemns an enclosing merge like `Aborted` does: the
/// surviving branch is not drained as though the stream were whole.
#[test]
fn a_cancelled_branch_condemns_a_merge() {
    let (_tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
    let (_tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
    let (handle, cancellation) = CancelHandle::new();
    let mut ch = Channel::from_infinite(rx_a, "a")
        .until(&cancellation)
        .merge(Channel::from_infinite(rx_b, "b"));
    handle.cancel();
    assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
}

/// `never` binds nothing: the stream and its verdict are untouched.
#[test]
fn never_leaves_the_stream_untouched() {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let mut ch = closed(rx).until(&Cancellation::never());
    tx.send(7).unwrap();
    drop(tx);
    assert_eq!(ch.recv(), Ok(7));
    assert_eq!(ch.recv(), Err(End::Complete));
}

/// `from_token` is the bridge from a tokio token to a blocked crossbeam
/// `recv` on a blocking thread.
#[cfg(feature = "tokio")]
#[tokio::test(flavor = "multi_thread")]
async fn from_token_wakes_a_blocking_recv() {
    let token = tokio_util::sync::CancellationToken::new();
    let cancellation = Cancellation::from_token(&token);
    let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
    let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
    let blocked = tokio::task::spawn_blocking(move || ch.recv());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!blocked.is_finished(), "recv returned before the cancel");
    token.cancel();
    let got = tokio::time::timeout(std::time::Duration::from_secs(5), blocked)
        .await
        .unwrap() // the cancel must wake recv within the bound
        .unwrap();
    assert_eq!(got, Err(AbortReason::Cancelled));
    drop(tx);
}
