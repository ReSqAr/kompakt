//! Order policies, `also` and exclusive starts.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;

#[test]
fn strictly_monotone_accepts_forward_gaps() {
    let mut ch = strict(vec![1, 3, 4], Some(0));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(3));
    assert_eq!(ch.recv(), Ok(4));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn strictly_monotone_rejects_duplicate() {
    let mut ch = strict(vec![1, 2, 2], Some(0));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::OrderViolation {
                what: "strictly increasing"
            }
        })
    );
}

#[test]
fn strictly_monotone_rejects_leading_regression() {
    let mut ch = strict(vec![0, 1], Some(0));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::OrderViolation {
                what: "strictly increasing"
            }
        })
    );
}

#[test]
fn gapless_rejects_a_gap() {
    let mut ch =
        OrderedChannel::<_, Contiguous<u64>, Finite>::new(Channel::from_vec(vec![1, 3]), Some(0));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::OrderViolation { what: "contiguous" }
        })
    );
}

#[test]
fn non_decreasing_allows_duplicates() {
    let mut ch = OrderedChannel::<_, NonDecreasing<u64>, Finite>::new(
        Channel::from_vec(vec![1, 1, 2]),
        Some(0),
    );
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn no_key_items_pass_without_advancing() {
    let mut ch = OrderedChannel::<_, Contiguous<u64>, Finite>::new(
        Channel::from_vec(vec![Some(1), None, Some(2)]),
        Some(0),
    );
    assert_eq!(ch.recv(), Ok(Some(1)));
    assert_eq!(ch.recv(), Ok(None));
    assert_eq!(ch.recv(), Ok(Some(2)));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn batch_and_projection_keep_the_order_source() {
    let ordered = strict(vec![1, 2, 3], Some(0));
    let mut ch = ordered.into_channel();
    let batch: Vec<u64> = ch.ready_batch(2).unwrap().collect();
    assert_eq!(batch, vec![1, 2]);
    let rest: Vec<u64> = std::iter::from_fn(|| ch.recv().ok()).collect();
    assert_eq!(rest, vec![3]);
    assert_eq!(ch.end_reason(), End::Complete);
}

#[test]
fn contiguous_from_none_starts_at_the_first_key() {
    assert_eq!(
        drain(contiguous(vec![0, 1], None)),
        (vec![0, 1], End::Complete)
    );
    let (_, end) = drain(contiguous(vec![1], None));
    assert!(matches!(end, End::Aborted { .. }), "0 was skipped");
}

#[test]
fn also_guards_a_second_order_without_changing_the_type() {
    // Index order holds, the secondary (descending is a violation) trips.
    let ch: OrderedChannel<u64, StrictlyIncreasing<u64>, Finite> =
        strict(vec![1, 5, 3], None).also::<NonDecreasing<u64>>(None, "secondary");
    let (got, end) = drain(ch);
    assert_eq!(got, vec![1, 5]);
    assert!(matches!(
        end,
        End::Aborted {
            reason: AbortReason::OrderViolation { .. }
        }
    ));
}

#[test]
fn non_decreasing_first_key_is_strictly_after() {
    let ch = Channel::from_vec(vec![2u64, 2, 3]).ordered::<NonDecreasing<u64>>(Some(2), "nd");
    assert!(
        matches!(drain(ch).1, End::Aborted { .. }),
        "2 is not after 2"
    );
    let ch = Channel::from_vec(vec![3u64, 3, 4]).ordered::<NonDecreasing<u64>>(Some(2), "nd");
    assert_eq!(drain(ch), (vec![3, 3, 4], End::Complete));
}
