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
//! hands back a [`Router`], so the rest of the crate names peers by `NodeId` and
//! cannot reach the wire directly.

use std::sync::Arc;
use zslg_net::{ReplicaClient, Router};

use tokio_util::sync::CancellationToken;

use crate::Error;

use shutdown::Shutdown;
pub(crate) use zslg_net_client::RemoteRouter;
use zslg_net_client::RpcClient;
pub(crate) use zslg_net_client::RpcClientConfig;
pub(crate) use zslg_net_router::{LocalReplicaClient, PeerRouter};
use zslg_net_server::DispatchHandler;
use zslg_net_server::RpcServer;
pub(crate) use zslg_net_server::RpcServerConfig;
pub(crate) use zslg_net_wire::tcp::SocketOpts;
use zslg_time::Clock;

/// Build an outbound [`Router`] from the client tuning config. The `RpcClient`
/// is captured inside and never handed back, so it stays confined to `network`.
///
/// `cancel` is the node's shutdown token, and it is here for the same reason
/// [`bind_server`] takes one: a connection adopted onto blocking threads can
/// only be stopped by closing its socket, and nothing inside a parked
/// `read(2)` can notice a token on its own. Outbound streams were the half
/// that had no way to be told.
pub(crate) fn setup_router(
    clock: Arc<dyn Clock>,
    config: RpcClientConfig,
    cancel: CancellationToken,
) -> Result<Arc<dyn Router>, Error> {
    let rpc_client = RpcClient::new(clock, config, cancel);
    Ok(Arc::new(RemoteRouter::new(rpc_client)))
}

pub(crate) fn bind_server(
    client: Arc<dyn ReplicaClient>,
    hlc_clock: Arc<dyn Clock>,
    server_config: RpcServerConfig,
    shutdown: &mut Shutdown<Error>,
) -> Result<std::net::SocketAddr, Error> {
    let handler = Arc::new(DispatchHandler::new(client));
    let server = RpcServer::bind(handler, hlc_clock, server_config, shutdown.token())?;

    let server_addr = server.local_addr();
    shutdown.spawn("rpc_server", async move {
        server.run().await.map_err(Error::from)
    });

    Ok(server_addr)
}
