//! What counts as a quorum, and the two ways of collecting one.
//!
//! ```text
//!   is_quorum(replica, acked) = majority(current) ∧ majority(next)?
//!
//!   majority(S) = |S ∩ acked| >= |S| / 2 + 1
//! ```
//!
//! A joint config demands a majority of *both* sets, which is what makes
//! consecutive configurations overlap; a node in both is counted once against
//! each. `JointDraining` is joint for this purpose — its armed exit bound says
//! when the outgoing set may be dropped, not that it has stopped counting
//! (design doc §"Quorum Detection", CLAUDE.md §8).
//!
//! Two collectors, differing only in who drives them:
//!
//! | [`Coordinator`] | writes | long-lived streams, quorum decided as acks arrive |
//! | [`fan_out_quorum`] | control ops | one task per peer, awaited to quorum |
//!
//! Both re-evaluate against the *highest generation seen so far* rather than
//! the set they started from (CLAUDE.md §9), so a config that moves mid-flight
//! widens the target set instead of invalidating the attempt.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use crate::ids::NodeId;
use crate::meta::ReplicaMembership;

mod coordinator;
mod fanout;

pub(crate) use coordinator::Coordinator;
pub(crate) use fanout::{FanOutResponse, fan_out_quorum};

#[must_use]
#[allow(
    clippy::expect_used,
    reason = "checked_add(1) on a set size cannot overflow usize"
)]
fn quorum_threshold(n: NonZeroUsize) -> usize {
    (n.get() / 2)
        .checked_add(1)
        .expect("set size / 2 + 1 cannot overflow usize")
}

#[must_use]
fn is_quorum_of(set: &BTreeSet<NodeId>, successful: &BTreeSet<NodeId>) -> bool {
    let Some(n) = NonZeroUsize::new(set.len()) else {
        debug_assert!(false, "is_quorum_of called with an empty set");
        return false;
    };
    set.intersection(successful).count() >= quorum_threshold(n)
}

#[must_use]
pub(crate) fn is_quorum(replica: &ReplicaMembership, successful: &BTreeSet<NodeId>) -> bool {
    let current = is_quorum_of(replica.current(), successful);
    match replica.next() {
        None => current,
        Some(next) => current && is_quorum_of(next, successful),
    }
}

#[must_use]
pub(crate) fn all_targets(replica: &ReplicaMembership) -> BTreeSet<NodeId> {
    let mut out = replica.current().clone();
    if let Some(next) = replica.next() {
        out.extend(next.iter().copied());
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::tests::nid;

    fn ids_from(seeds: &[u8]) -> BTreeSet<NodeId> {
        seeds.iter().map(|&b| nid(b)).collect()
    }

    fn single(ids: &[u8]) -> ReplicaMembership {
        ReplicaMembership::single(ids_from(ids))
    }

    fn joint(old: &[u8], new: &[u8]) -> ReplicaMembership {
        ReplicaMembership::joint(ids_from(old), ids_from(new))
    }

    fn draining(old: &[u8], new: &[u8]) -> ReplicaMembership {
        ReplicaMembership::JointDraining {
            old: ids_from(old),
            next: ids_from(new),
            exit: crate::meta::Exit::WatermarkAbove(crate::hlc::Hlc(1)),
        }
    }

    // ── quorum_threshold ────────────────────────────────────────────────

    #[test]
    fn quorum_threshold_values() {
        let n = |v: usize| NonZeroUsize::new(v).expect("non-zero");
        assert_eq!(quorum_threshold(n(1)), 1);
        assert_eq!(quorum_threshold(n(2)), 2);
        assert_eq!(quorum_threshold(n(3)), 2);
        assert_eq!(quorum_threshold(n(4)), 3);
        assert_eq!(quorum_threshold(n(5)), 3);
        assert_eq!(quorum_threshold(n(100)), 51);
    }

    // ── is_quorum_of ────────────────────────────────────────────────────

    #[test]
    fn is_quorum_of_simple_majority() {
        // 3-node set: 2 acks is a quorum, 1 is not.
        let s = ids_from(&[1, 2, 3]);
        assert!(is_quorum_of(&s, &ids_from(&[1, 2])));
        assert!(!is_quorum_of(&s, &ids_from(&[1])));
    }

    #[test]
    fn is_quorum_of_exact_boundary() {
        // 5-node set: exactly 3 is the threshold.
        let s = ids_from(&[1, 2, 3, 4, 5]);
        assert!(!is_quorum_of(&s, &ids_from(&[1, 2])));
        assert!(is_quorum_of(&s, &ids_from(&[1, 2, 3])));
    }

    #[test]
    fn is_quorum_of_ignores_acks_from_outside_the_set() {
        // Acks from nodes not in the set must not count.
        let s = ids_from(&[1, 2, 3]);
        assert!(!is_quorum_of(&s, &ids_from(&[4, 5])));
        assert!(is_quorum_of(&s, &ids_from(&[1, 4, 2])));
    }

    // ── is_quorum: single config ────────────────────────────────────────

    #[test]
    fn is_quorum_single_config() {
        let r = single(&[1, 2, 3]);
        assert!(is_quorum(&r, &ids_from(&[1, 2])));
        assert!(!is_quorum(&r, &ids_from(&[1])));
    }

    // ── is_quorum: joint config ─────────────────────────────────────────

    #[test]
    fn is_quorum_joint_requires_both() {
        // old = {1,2,3}, new = {3,4,5}. Need a quorum in BOTH.
        let r = joint(&[1, 2, 3], &[3, 4, 5]);
        // Quorum in old only → not enough.
        assert!(!is_quorum(&r, &ids_from(&[1, 2])));
        // Quorum in new only → not enough.
        assert!(!is_quorum(&r, &ids_from(&[4, 5])));
        // Quorum in both (3 is shared) → success.
        assert!(is_quorum(&r, &ids_from(&[2, 3, 4])));
    }

    #[test]
    fn is_quorum_draining_still_requires_both() {
        // An armed exit bound does not relax the quorum requirement: until the
        // config actually collapses, both sets are still load-bearing.
        let r = draining(&[1, 2, 3], &[3, 4, 5]);
        assert!(!is_quorum(&r, &ids_from(&[1, 2])));
        assert!(!is_quorum(&r, &ids_from(&[4, 5])));
        assert!(is_quorum(&r, &ids_from(&[2, 3, 4])));
    }

    #[test]
    fn all_targets_draining_is_union() {
        assert_eq!(
            all_targets(&draining(&[1, 2, 3], &[3, 4, 5])),
            ids_from(&[1, 2, 3, 4, 5])
        );
    }

    #[test]
    fn is_quorum_joint_shared_node_counts_for_both() {
        // old = {1,2,3}, new = {1,2,3} (same set). Acks {1,2} give quorum in both.
        let r = joint(&[1, 2, 3], &[1, 2, 3]);
        assert!(is_quorum(&r, &ids_from(&[1, 2])));
    }

    // ── all_targets ─────────────────────────────────────────────────────

    #[test]
    fn all_targets_single_config() {
        assert_eq!(all_targets(&single(&[1, 2, 3])), ids_from(&[1, 2, 3]));
    }

    #[test]
    fn all_targets_joint_is_union() {
        assert_eq!(
            all_targets(&joint(&[1, 2, 3], &[3, 4, 5])),
            ids_from(&[1, 2, 3, 4, 5])
        );
    }

    // ── the intersection law ────────────────────────────────────────────

    // `clippy::wildcard_imports` exempts prelude globs, and every other
    // proptest in this crate imports the prelude this way.
    use proptest::prelude::*;

    /// Node ids are drawn from a small pool so that generated sets actually
    /// overlap — with a wide pool almost every pair would be disjoint and the
    /// property would pass vacuously.
    const POOL: std::ops::Range<u8> = 0..12;

    fn id_set(max: usize) -> impl Strategy<Value = BTreeSet<NodeId>> {
        proptest::collection::btree_set(POOL, 1..=max)
            .prop_map(|seeds| seeds.iter().map(|&b| nid(b)).collect())
    }

    fn ack_set() -> impl Strategy<Value = BTreeSet<NodeId>> {
        proptest::collection::btree_set(POOL, 0..12)
            .prop_map(|seeds| seeds.iter().map(|&b| nid(b)).collect())
    }

    fn membership() -> impl Strategy<Value = ReplicaMembership> {
        prop_oneof![
            id_set(7).prop_map(ReplicaMembership::single),
            (id_set(7), id_set(7)).prop_map(|(old, next)| ReplicaMembership::joint(old, next)),
            (id_set(7), id_set(7)).prop_map(|(old, next)| ReplicaMembership::JointDraining {
                old,
                next,
                exit: crate::meta::Exit::WatermarkAbove(crate::hlc::Hlc(1)),
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// **The safety property the whole scheme rests on.** Two acknowledgement
        /// sets that each constitute a quorum must share at least one node that
        /// actually counts — otherwise two disjoint groups could both believe
        /// they committed, and a write acknowledged by one would be invisible to
        /// the other.
        ///
        /// It is a statement about *pairs* of runs, which is why the
        /// example-based tests beside it cannot express it: they would have to
        /// pick pairs by hand and would prove nothing about the rest.
        #[test]
        fn invariant_any_two_quorums_intersect(
            m in membership(),
            a in ack_set(),
            b in ack_set(),
        ) {
            if is_quorum(&m, &a) && is_quorum(&m, &b) {
                if let Some(next) = m.next() {
                    // Joint config: quorum requires a majority of `old` and a
                    // majority of `next` independently, so the two quorums
                    // must share a node inside each set, not merely somewhere
                    // in their union.
                    let old = m.current();
                    let shared_old = a.intersection(&b).filter(|id| old.contains(*id)).count();
                    prop_assert!(
                        shared_old > 0,
                        "two quorums of {m:?} were disjoint in `old`: {a:?} and {b:?}"
                    );
                    let shared_next = a.intersection(&b).filter(|id| next.contains(*id)).count();
                    prop_assert!(
                        shared_next > 0,
                        "two quorums of {m:?} were disjoint in `next`: {a:?} and {b:?}"
                    );
                } else {
                    let targets = all_targets(&m);
                    let shared = a
                        .intersection(&b)
                        .filter(|id| targets.contains(*id))
                        .count();
                    prop_assert!(
                        shared > 0,
                        "two quorums of {m:?} were disjoint: {a:?} and {b:?}"
                    );
                }
            }
        }
    }
}
