//! The RPC wire: the messages two nodes exchange, the frame codec that carries
//! them, and TCP socket setup. Shared by the client, the server and the
//! router; implements no port.

use error_union::error_union;
use serde::{Deserialize, Serialize};
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

mod frame;
pub mod messages;
pub mod metrics;
pub mod tcp;

pub use frame::{
    AsyncFrameSink, AsyncFrameStream, FrameSink, FrameStream, TcpFrameSink, TcpFrameStream,
    adopt_blocking,
};

error_union! {
    /// What the wire can fail with, and what an RPC error frame carries:
    /// `Serialize`/`Deserialize` rebuild the error exactly on the caller's side.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub Error {
        Lifecycle(LifecycleError),
        Config(ConfigError),
        Clock(ClockError),
        Log(LogError),
        Snapshot(SnapshotError),
        Net(NetError),
        Consensus(ConsensusError),
    }
    from zslg_time::Error { Clock }
    bridge zslg_log::Error
    bridge zslg_snapshot::Error
    bridge zslg_consensus::Error
    bridge zslg_net::Error
    bridge zslg_error::Error
}
