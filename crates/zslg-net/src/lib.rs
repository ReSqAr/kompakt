//! The net group's ports: how a node reaches its peers. The one ports crate
//! that bundles other groups' ports, because `ReplicaClient` is everything one
//! node can ask another. Re-exports the group's types.

use error_union::error_union;
use zslg_consensus_types::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log_types::LogError;
use zslg_snapshot_types::SnapshotError;
use zslg_time_types::ClockError;

mod liveness_probe;
mod peers;
mod replica_client;
mod router;

pub use liveness_probe::LivenessProbe;
pub use peers::PeersWatcher;
pub use replica_client::ReplicaClient;
pub use router::Router;
pub use zslg_net_types::{HostPort, NetError, PeerAddr, ServerName, ToRpc};

error_union! {
    /// What the ports of this crate can fail with. All seven leaves: the
    /// implementations of these ports raise every one of them (a transport
    /// fails with `Net`, the object store with `Log`, a shut-down node with
    /// `Lifecycle`, ...), and a port's error type must hold what any of them
    /// returns. Narrowing needs per-method error types; see
    /// `docs/logs/2026-11-07_post_crate_split.md`.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(LifecycleError),
        Config(ConfigError),
        Clock(ClockError),
        Log(LogError),
        Snapshot(SnapshotError),
        Net(NetError),
        Consensus(ConsensusError),
    }
    bridge zslg_error::Error
}
