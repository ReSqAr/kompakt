//! Every byte that leaves or enters this node. Plain TCP on a trusted network —
//! no TLS, peer identity is the `node_id` carried in every envelope (design doc
//! §10, CLAUDE.md §10).
//!
//! ```text
//!   messages   the request/response vocabulary — one enum per plane
//!   framing    length | postcard envelope | crc32, async and blocking
//!   tcp        dial / listen and the socket tuning both ends share
//!   client     outbound: unary pool, subscribe streams, the write pool
//!   server     inbound: accept, dispatch, and the two streaming handlers
//!   router     NodeId -> client, with the in-process short-circuit for self
//!   transport  the per-peer client that presents `zslg_net::ReplicaClient`
//!   peers      the configured member set, and the join admission gate
//! ```
//!
//! One connection per logical stream: a subscribe, a write pool, and a unary
//! exchange never share a socket. Multiplexing them would couple their failure
//! modes — a torn write connection would take a subscription with it — and each
//! shape wants a different lifetime anyway.
//!
//! The `RpcClient` is deliberately unexported: [`setup_router`] captures it and
//! hands back a [`Router`], so callers name peers by `NodeId` and cannot reach
//! the wire directly.

use std::sync::Arc;
use zslg_net::{ReplicaClient, Router};

use tokio_util::sync::CancellationToken;

use shutdown::Shutdown;
pub use zslg_net_client::RemoteRouter;
use zslg_net_client::RpcClient;
pub use zslg_net_client::RpcClientConfig;
pub use zslg_net_router::{LocalReplicaClient, PeerRouter};
use zslg_net_server::DispatchHandler;
use zslg_net_server::RpcServer;
pub use zslg_net_server::RpcServerConfig;
pub use zslg_net_wire::tcp::SocketOpts;
use zslg_time::Clock;

/// Build an outbound [`Router`] from the client tuning config. The `RpcClient`
/// is captured inside and never handed back, so it stays confined to `network`.
///
/// `cancel` is the node's shutdown token, and it is here for the same reason
/// [`bind_server`] takes one: a connection adopted onto blocking threads can
/// only be stopped by closing its socket, and nothing inside a parked
/// `read(2)` can notice a token on its own. Outbound streams were the half
/// that had no way to be told.
pub fn setup_router(
    clock: Arc<dyn Clock>,
    config: RpcClientConfig,
    cancel: CancellationToken,
) -> Result<Arc<dyn Router>, Error> {
    let rpc_client = RpcClient::new(clock, config, cancel);
    Ok(Arc::new(RemoteRouter::new(rpc_client)))
}

pub fn bind_server<E>(
    client: Arc<dyn ReplicaClient>,
    hlc_clock: Arc<dyn Clock>,
    server_config: RpcServerConfig,
    shutdown: &mut Shutdown<E>,
) -> Result<std::net::SocketAddr, Error>
where
    E: From<zslg_net_server::Error> + From<shutdown::Stopped> + std::fmt::Display + Send + 'static,
{
    let handler = Arc::new(DispatchHandler::new(client));
    let server = RpcServer::bind(handler, hlc_clock, server_config, shutdown.token())?;

    let server_addr = server.local_addr();
    shutdown.spawn(
        "rpc_server",
        async move { server.run().await.map_err(E::from) },
    );

    Ok(server_addr)
}

error_union::error_union! {
    /// What the network assembly can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_time::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_server::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_client::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}
