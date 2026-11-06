//! `covers`.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;

#[test]
fn covers_completes_only_at_the_bound() {
    let reached = contiguous(vec![3, 4], Some(2)).covers(..5, "window");
    assert_eq!(drain(reached), (vec![3, 4], End::Complete));

    let short = contiguous(vec![3], Some(2)).covers(..5, "window");
    assert_eq!(
        drain(short),
        (
            vec![3],
            End::Aborted {
                reason: AbortReason::OrderViolation { what: "window" }
            }
        )
    );
}

#[test]
fn covers_judges_an_empty_window_by_its_start() {
    let empty = contiguous(vec![], Some(4)).covers(..5, "window");
    assert_eq!(drain(empty).1, End::Complete, "(4, 5) holds nothing");
    let missing = contiguous(vec![], Some(2)).covers(..5, "window");
    assert!(
        matches!(drain(missing).1, End::Aborted { .. }),
        "(2, 5) was owed"
    );
}

#[test]
fn covers_keeps_a_source_abort() {
    let ch = crate::channel::aborted_of(vec![1u64], "src")
        .ordered::<Contiguous<u64>>(Some(0), "w")
        .covers(..5, "window");
    assert_eq!(
        drain(ch).1,
        End::Aborted {
            reason: AbortReason::ProducerGone { what: "src" }
        }
    );
}

#[test]
fn covers_through_ready_batch() {
    let mut ch = contiguous(vec![1, 2], Some(0)).covers(..4, "window");
    let mut got = Vec::new();
    let end = loop {
        match ch.ready_batch(8) {
            Ok(batch) => got.extend(batch),
            Err(end) => break end,
        }
    };
    assert_eq!(got, vec![1, 2]);
    assert!(matches!(end, End::Aborted { .. }), "3 was owed");
}
