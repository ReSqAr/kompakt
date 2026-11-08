//! The replica composition lives in `zslg-replica-node`; these are the tests of
//! the replica service that need the real clock, gate and log stack together.

#[cfg(test)]
mod clock_skew_tests;
#[cfg(test)]
mod service_tests;
#[cfg(test)]
mod subscribe_status_tests;

#[cfg(test)]
pub(crate) use zslg_replica_log::{REPLICA_LOG_CF, ReplicaLogStore};
#[cfg(test)]
pub(crate) use zslg_replica_service::ReplicaService;
