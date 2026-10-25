//! Scheduler tests: shared constants here; one file per concern.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]

mod breaker;
mod fetch;
mod hedge;
mod prefetch;
mod tuning;

use super::*;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};

const MS: Duration = Duration::from_millis(1);
const T: Kind = Kind {
    name: "t",
    hedge: false,
};
const THUMB: Kind = Kind {
    name: "thumbnail",
    hedge: true,
};
const VIDEO: Kind = Kind {
    name: "video",
    hedge: false,
};
