//! Re-exports the rest of the crate's test code names.

#[cfg(test)]
pub(crate) use zslg_replica_log::{REPLICA_LOG_CF, ReplicaLogStore};
#[cfg(test)]
pub(crate) use zslg_replica_service::ReplicaService;
