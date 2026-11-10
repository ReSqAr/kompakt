//! The leader's background jobs against a real Raft instance: membership
//! changes, snapshot build and lifecycle, compaction, leadership churn.

mod compaction_tests;
mod jobs_tests;
mod leadership_churn_tests;
mod membership_change;
mod snapshot_build_tests;
mod snapshot_harness;
mod snapshot_lifecycle;
mod snapshot_member_lifecycle_tests;
