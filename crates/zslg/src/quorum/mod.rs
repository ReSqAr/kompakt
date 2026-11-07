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

mod coordinator;

pub(crate) use coordinator::Coordinator;
