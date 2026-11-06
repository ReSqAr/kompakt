#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use std::num::NonZeroUsize;

use super::*;

pub(super) fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

pub(super) fn config(data: usize, register: usize, sub: usize) -> FanoutConfig {
    FanoutConfig {
        data_cap: cap(data),
        register_cap: cap(register),
        sub_cap: cap(sub),
    }
}

mod delivery;
mod lifecycle;
