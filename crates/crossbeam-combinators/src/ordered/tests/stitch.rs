//! `stitch`.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;

#[test]
fn stitch_fills_a_leading_gap() {
    let mut ch = strict(vec![3, 4], Some(0)).stitch(|gap| {
        assert_eq!(
            gap,
            Gap {
                after: Some(0),
                got: 3
            }
        );
        Channel::from_vec(vec![1, 2])
    });
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Ok(3));
    assert_eq!(ch.recv(), Ok(4));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn stitch_fills_the_gap_and_returns_gapless() {
    let mut ch = strict(vec![1, 4], Some(0)).stitch(|gap| {
        assert_eq!(
            gap,
            Gap {
                after: Some(1),
                got: 4
            }
        );
        Channel::from_vec(vec![2, 3])
    });
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Ok(3));
    assert_eq!(ch.recv(), Ok(4));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn stitch_drains_through_ready_batch() {
    let mut ch = strict(vec![1, 4, 5], Some(0)).stitch(|_| Channel::from_vec(vec![2, 3]));
    let batch: Vec<u64> = ch.ready_batch(10).unwrap().collect();
    assert_eq!(batch, vec![1, 2, 3, 4, 5]);
    assert_eq!(ch.end_reason(), End::Complete);
}

#[test]
fn stitch_passes_no_key_fallback_items() {
    let ordered = OrderedChannel::<_, StrictlyIncreasing<u64>, Finite>::new(
        Channel::from_vec(vec![Some(1u64), Some(4)]),
        Some(0),
    );
    let mut ch = ordered.stitch(|_| Channel::from_vec(vec![Some(2), None, Some(3)]));
    assert_eq!(ch.recv(), Ok(Some(1)));
    assert_eq!(ch.recv(), Ok(Some(2)));
    assert_eq!(ch.recv(), Ok(None));
    assert_eq!(ch.recv(), Ok(Some(3)));
    assert_eq!(ch.recv(), Ok(Some(4)));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn stitch_with_empty_fallback_aborts_instead_of_completing() {
    let mut ch = strict(vec![1, 4, 5], Some(0))
        .stitch(|_| Channel::chain_all(Vec::<Channel<u64, Finite>>::new()));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::OrderViolation {
                what: "stitch fallback"
            }
        })
    );
}

#[test]
fn stitch_rejects_a_short_fallback() {
    let mut ch = strict(vec![1, 4], Some(0)).stitch(|_| Channel::from_vec(vec![2]));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::OrderViolation {
                what: "stitch fallback"
            }
        })
    );
}

#[test]
fn stitch_rejects_an_overlong_fallback() {
    let mut ch = strict(vec![1, 4], Some(0)).stitch(|_| Channel::from_vec(vec![2, 3, 4]));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Ok(3));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::OrderViolation {
                what: "stitch fallback"
            }
        })
    );
}

#[test]
fn stitch_propagates_a_fallback_abort() {
    let mut ch =
        strict(vec![1, 4], Some(0)).stitch(|_| crate::channel::aborted_of(vec![2, 3], "fallback"));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Ok(3));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "fallback" }
        })
    );
}
