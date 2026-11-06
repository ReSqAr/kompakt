//! Shutdown, drop, drain and the producer-side errors.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;
use crate::channel::AbortReason;

#[test]
fn shutdown_ends_subscribers_as_producer_gone() {
    let (fanout, registry) = broadcast::<u32>("test", config(4, 4, 4), OverflowPolicy::Shed);
    let mut sub = registry.subscribe().unwrap();

    fanout.shutdown();

    assert!(matches!(
        sub.recv(),
        Err(AbortReason::ProducerGone { what: "test" })
    ));
}

#[test]
fn dropping_the_fanout_ends_subscribers_after_delivered_items() {
    let (fanout, registry) = broadcast("test", config(4, 4, 4), OverflowPolicy::Shed);
    let mut sub = registry.subscribe().unwrap();
    fanout.publish(7u32).unwrap();
    assert_eq!(sub.recv().unwrap(), 7);
    drop(fanout);
    assert!(matches!(
        sub.recv(),
        Err(AbortReason::ProducerGone { what: "test" })
    ));
}

/// `drain` delivers what `publish` already returned for; `shutdown` and
/// `Drop` may discard it. The subscriber reads slowly through a queue of
/// one, so most of the elements are still queued when the drain starts.
#[test]
fn drain_delivers_the_queue_then_ends_the_subscribers() {
    let (fanout, registry) = broadcast::<u32>("drain", config(64, 4, 1), OverflowPolicy::Block);
    let mut sub = registry.subscribe().unwrap();
    let reader = std::thread::spawn(move || {
        let mut got = Vec::new();
        let end = loop {
            match sub.recv() {
                Ok(v) => {
                    got.push(v);
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                Err(end) => break end,
            }
        };
        (got, end)
    });
    for v in 0..30 {
        fanout.publish(v).unwrap();
    }
    fanout.drain(std::time::Duration::from_secs(10));
    let (got, end) = reader.join().unwrap();
    assert_eq!(got, (0..30).collect::<Vec<_>>(), "nothing queued was lost");
    assert_eq!(end, AbortReason::ProducerGone { what: "drain" });
}

/// A subscriber that never reads cannot hold a drain past its grace.
#[test]
fn drain_gives_up_on_a_stuck_subscriber_after_the_grace() {
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    std::thread::spawn(move || {
        let (fanout, registry) = broadcast::<u32>("stuck", config(8, 4, 1), OverflowPolicy::Block);
        let _stuck = registry.subscribe().unwrap();
        for v in 0..4 {
            fanout.publish(v).unwrap();
        }
        let started = std::time::Instant::now();
        fanout.drain(std::time::Duration::from_millis(200));
        let _ = done_tx.send(started.elapsed());
    });
    let took = done_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("drain must return once the grace is spent");
    assert!(took >= std::time::Duration::from_millis(150), "{took:?}");
}

#[test]
fn publish_after_shutdown_hands_the_item_back() {
    let (fanout, registry) = broadcast::<u32>("test", config(4, 4, 4), OverflowPolicy::Shed);
    let probe = registry.subscribe().unwrap();
    let tx = fanout.tx.clone();
    fanout.shutdown();
    drop(probe);
    assert!(tx.send((0, 1)).is_err(), "the thread dropped its receiver");
}

#[test]
fn try_publish_reports_full_and_stopped() {
    let (fanout, registry) = broadcast::<u32>("test", config(1, 4, 1), OverflowPolicy::Block);
    let _slow = registry.subscribe().unwrap();
    // The thread takes 1 and blocks delivering 2 to the full subscriber;
    // 3 fills the data queue; 4 finds it full.
    let mut full = None;
    for v in 1..=50u32 {
        if let Err(e) = fanout.try_publish(v) {
            full = Some(e);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert!(matches!(full, Some(TryPublishError::Full(_))));
    let tx = fanout.tx.clone();
    fanout.shutdown();
    assert!(tx.try_send((0, 9)).is_err());
}
