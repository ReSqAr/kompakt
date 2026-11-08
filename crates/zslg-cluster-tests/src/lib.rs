//! End-to-end tests: real multi-node clusters over TCP, with fault injection and
//! the cluster-wide invariants checked after each scenario.
//!
//! Single-node behaviour, Raft on its own, the leader's jobs and the replica
//! service are tested in the crates that own them; what is left here needs the
//! whole stack of several nodes at once.
#![cfg(test)]

// Dev-dependency that exists to switch on the `testing` hooks of the node.
use zslg_replica_node as _;

mod common;
pub(crate) use common::{
    Cluster, ClusterConfig, CutoffObserver, NodePlacement, assert_cluster_invariants,
    assert_cluster_invariants_with_acks, assert_durability_chain_on, default_fast_config,
    init_logging, nid, reserve_port, wait_until_sync,
};

mod cluster_bringup;
mod cluster_proptest;
mod decommission;
mod eviction;
mod full_restart;
mod leader_failover;
mod read_paths;
mod rolling_replacement;
mod stable_read;
