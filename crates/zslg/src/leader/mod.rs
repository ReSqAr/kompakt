//! Leader-only background work is assembled by `zslg-leader`; what stays here
//! are the end-to-end tests that need a real Raft instance next to it.

#[cfg(test)]
mod compaction_tests;

#[cfg(test)]
mod jobs_tests;

#[cfg(test)]
mod membership_change_tests;

#[cfg(test)]
mod snapshot_build_tests;

#[cfg(test)]
mod snapshot_lifecycle_tests;

#[cfg(test)]
mod snapshot_member_lifecycle_tests;

#[cfg(test)]
mod leadership_churn_tests;

#[cfg(test)]
pub(in crate::leader) use zslg_leader::Integrator;
pub(super) use zslg_leader::{LeaderLoopConfig, LivenessConfig, SnapshotConfig, spawn_leader_loop};
#[cfg(test)]
pub(crate) use zslg_leader_snapshot::create::SnapshotResult;
#[cfg(test)]
pub(in crate::leader) use zslg_leader_snapshot::create::{SnapshotInputs, build_snapshot};
