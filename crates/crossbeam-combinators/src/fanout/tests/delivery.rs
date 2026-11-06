//! Who receives what, and in which order.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;
use crate::channel::AbortReason;

#[test]
fn broadcast_delivers_to_every_subscriber() {
    let (fanout, registry) = broadcast("test", config(4, 4, 4), OverflowPolicy::Shed);
    let mut a = registry.subscribe().unwrap();
    let mut b = registry.subscribe().unwrap();

    fanout.publish(1u32).unwrap();
    fanout.publish(2u32).unwrap();

    assert_eq!(a.recv().unwrap(), 1);
    assert_eq!(a.recv().unwrap(), 2);
    assert_eq!(b.recv().unwrap(), 1);
    assert_eq!(b.recv().unwrap(), 2);
    fanout.shutdown();
}

#[test]
fn keyed_routes_only_matching_subscribers() {
    let (fanout, registry) = keyed(
        "test",
        |n: &u32| Some(n % 2),
        config(4, 4, 4),
        OverflowPolicy::Shed,
    );
    let mut even = registry.subscribe(0).unwrap();
    let mut odd = registry.subscribe(1).unwrap();

    fanout.publish(2u32).unwrap();
    fanout.publish(3u32).unwrap();

    assert_eq!(even.recv().unwrap(), 2);
    assert_eq!(odd.recv().unwrap(), 3);
    fanout.shutdown();
}

#[test]
fn shed_removes_a_full_subscriber() {
    let (fanout, registry) = broadcast("test", config(4, 4, 1), OverflowPolicy::Shed);
    let mut sub = registry.subscribe().unwrap();

    // The data queue holds 4, so by the time the last of these publishes
    // returns the thread has dequeued element 2 and shed the subscriber (whose
    // queue of 1 nothing has drained): the read below cannot race the shed.
    for v in 1u32..=100 {
        fanout.publish(v).unwrap();
    }

    assert_eq!(sub.recv().unwrap(), 1);
    assert!(matches!(
        sub.recv(),
        Err(AbortReason::ProducerGone { what: "test" })
    ));
    fanout.shutdown();
}

#[test]
fn block_waits_for_the_slowest_subscriber() {
    let (fanout, registry) = broadcast("test", config(4, 4, 1), OverflowPolicy::Block);
    let mut sub = registry.subscribe().unwrap();

    fanout.publish(1u32).unwrap();
    fanout.publish(2u32).unwrap();

    assert_eq!(sub.recv().unwrap(), 1);
    assert_eq!(sub.recv().unwrap(), 2);
    fanout.shutdown();
}

#[test]
fn subscribe_infinite_aborts_instead_of_completing() {
    let (fanout, registry) = broadcast::<u32>("infinite", config(4, 4, 4), OverflowPolicy::Shed);
    fanout.shutdown();

    let mut sub = registry.subscribe_infinite();
    assert!(matches!(
        sub.recv(),
        Err(AbortReason::ProducerGone { what: "infinite" })
    ));
}

#[test]
fn keyed_subscribe_infinite_uses_the_fanout_label() {
    let (fanout, registry) = keyed(
        "keyed infinite",
        |n: &u32| Some(n % 2),
        config(4, 4, 4),
        OverflowPolicy::Shed,
    );
    fanout.shutdown();

    let mut sub = registry.subscribe_infinite(0);
    assert!(matches!(
        sub.recv(),
        Err(AbortReason::ProducerGone {
            what: "keyed infinite"
        })
    ));
}

/// `subscribe` after `publish` returned must not see that element, however
/// far behind the distribution thread is. The registration is preferred
/// over queued data, so without the publish count a lagging thread would
/// hand the newcomer elements that predate it.
#[test]
fn a_subscriber_never_sees_what_was_published_before_it_subscribed() {
    // Regressed, this deadlocks (the newcomer's unread queue blocks the
    // thread, which blocks `slow`), so the body runs behind a watchdog.
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    std::thread::spawn(move || {
        let config = FanoutConfig {
            data_cap: NonZeroUsize::new(8).unwrap(),
            register_cap: NonZeroUsize::new(8).unwrap(),
            sub_cap: NonZeroUsize::new(1).unwrap(),
        };
        let (fanout, registry) = broadcast::<u32>("order", config, OverflowPolicy::Block);
        let mut slow = registry.subscribe().unwrap();

        // 1 fills the slow subscriber's queue; the thread then blocks
        // sending 2 to it, so 3 stays queued behind it: the thread is
        // provably behind when `late` subscribes.
        fanout.publish(1).unwrap();
        fanout.publish(2).unwrap();
        fanout.publish(3).unwrap();
        let mut late = registry.subscribe().unwrap();

        // Release the thread, then publish after the subscription.
        assert_eq!(slow.recv(), Ok(1));
        assert_eq!(slow.recv(), Ok(2));
        assert_eq!(slow.recv(), Ok(3));
        fanout.publish(4).unwrap();
        assert_eq!(late.recv(), Ok(4), "only what followed the subscription");
        assert_eq!(slow.recv(), Ok(4));
        fanout.shutdown();
        let _ = done_tx.send(());
    });
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .is_ok(),
        "a subscriber was handed elements published before it subscribed"
    );
}
