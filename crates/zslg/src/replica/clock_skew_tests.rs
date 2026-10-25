#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
//! Clock-skew rejection lives at the RPC *ingress*, not in `ReplicaService::write`.
//! Every frame carries a fresh `RpcEnvelope.hlc` (stamped at send) which the
//! receiver feeds to `Clock::integrate`; a sender whose clock has drifted past
//! `max_clock_skew` is rejected with `HlcDriftTooLarge` before the write is ever
//! dispatched. (The former `ReplicaService::write` data-creation-timestamp skew
//! check was removed: it conflated in-flight queueing latency with clock skew and
//! collapsed throughput under load — see designdoc "Error propagation".)

use std::time::Duration;

use crate::api::Clock;
use crate::error::Error;
use crate::hlc::{Hlc, HlcParts};
use crate::tests::{SharedClockController, SkewedClock};

#[tokio::test]
async fn peer_clock_diverged_beyond_drift_limit_fails_integrate() {
    let max_skew = Duration::from_secs(1);

    let ctl = SharedClockController::new();
    let clock = SkewedClock::new(max_skew, ctl);

    // A peer 5 seconds ahead — far beyond the 1s drift limit.
    let diverged_peer = Hlc::from_parts(HlcParts {
        physical_ms: clock.hlc_clock().now().parts().physical_ms + 5_000,
        logical: 0,
    });
    let err = clock
        .hlc_clock()
        .integrate(diverged_peer)
        .expect_err("wildly diverged peer must be rejected by integrate");
    assert!(
        matches!(err, Error::HlcDriftTooLarge { .. }),
        "expected HlcDriftTooLarge, got {err:?}",
    );
}
