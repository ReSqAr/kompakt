#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;
use crate::Channel;
use crate::channel::{AbortReason, End};
use crate::kind::Finite;

impl Key<u64> for u64 {
    fn key(item: &u64) -> Option<Self> {
        Some(*item)
    }
}

impl Key<Option<u64>> for u64 {
    fn key(item: &Option<u64>) -> Option<Self> {
        *item
    }
}

impl Successor for u64 {
    fn next_after(prev: Option<Self>) -> Self {
        prev.map_or(0, |p| {
            p.checked_add(1)
                .unwrap_or_else(|| panic!("test key exhausted"))
        })
    }
}

pub(super) fn strict(
    items: Vec<u64>,
    after: Option<u64>,
) -> OrderedChannel<u64, StrictlyIncreasing<u64>, Finite> {
    OrderedChannel::new(Channel::from_vec(items), after)
}

pub(super) fn contiguous(
    items: Vec<u64>,
    after: Option<u64>,
) -> OrderedChannel<u64, Contiguous<u64>, Finite> {
    Channel::from_vec(items).ordered(after, "test window")
}

pub(super) fn drain<P: OrderPolicy>(mut ch: OrderedChannel<u64, P, Finite>) -> (Vec<u64>, End)
where
    P::Key: Key<u64>,
{
    let mut got = Vec::new();
    let end = loop {
        match ch.recv() {
            Ok(v) => got.push(v),
            Err(end) => break end,
        }
    };
    (got, end)
}

mod covers;
mod policy;
mod stitch;
