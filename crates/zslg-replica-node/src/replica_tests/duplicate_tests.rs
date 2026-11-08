#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
//! A duplicated write reaches the replica twice and is stored once.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use zslg_consensus_fake::nid;
use zslg_log::{DataId, Replica};
use zslg_log_fake::replica_write;
use zslg_net_fake::DuplicateReplica;
use zslg_time_fake::SharedClockController;

use super::harness::{ReplicaServiceHarness, SkewedClock};

#[tokio::test]
async fn duplicate_write_is_idempotent() {
    let me = nid(1);
    let max_skew = Duration::from_secs(10);
    let ctl = SharedClockController::new();
    let clock = SkewedClock::new(max_skew, ctl);

    let harness = ReplicaServiceHarness::new(me, &[me], clock.hlc_clock())
        .await
        .expect("harness");

    let (dup_replica, dup_handle) =
        DuplicateReplica::new(harness.replica.clone() as Arc<dyn zslg_log::Replica>);

    // Next write is sent twice, concurrently.
    dup_handle.duplicate_next(1, 2);

    let data = zslg_log::Data {
        data_id: DataId::from_parts(clock.hlc_clock().now(), me, 0),
        blob: Bytes::from_static(b"duplicate-write"),
    };
    // The wrapper returns the first Ok; the duplicate's DataIdReused is discarded.
    let state = replica_write(&*dup_replica, data)
        .await
        .expect("duplicate write succeeds");
    assert!(state.replica.current().contains(&me));

    // Data is stored exactly once (the single write got index 0).
    let mut ch = harness
        .replica
        .subscribe_from(Some(zslg_log::LogIndex::ZERO));
    let first = ch.recv();
    assert!(
        first.is_ok_and(|r| r.is_ok()),
        "data must be present after write"
    );
}
