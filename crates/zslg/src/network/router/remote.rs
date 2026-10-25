//! Every node dialed over TCP, including this one. The self fast-path is
//! [`PeerRouter`](crate::network::PeerRouter)'s job, layered on top.

use std::sync::Arc;

use crate::api::{ReplicaClient, Router};
use crate::ids::{NodeId, PeerAddr};
use crate::network::client::RpcClient;
use crate::network::transport::TcpPeerClient;

/// The single owner of the peer-connection [`RpcClient`]. Built inside
/// [`setup_router`](crate::network::setup_router), which keeps the raw client
/// from ever escaping the `network` module.
pub(crate) struct RemoteRouter {
    rpc_client: Arc<RpcClient>,
}

impl RemoteRouter {
    #[must_use]
    pub(in crate::network) fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }

    /// Test-only: build the plain-TCP client side with default socket options, so
    /// out-of-module integration tests never name the raw [`RpcClient`]. Wrap the
    /// result in an `Arc` and register peers through the [`Router`] trait.
    ///
    /// The shutdown token is a fresh one nobody holds: these tests drive
    /// teardown by dropping the router, never by cancelling. A test that wants
    /// to exercise cancellation calls the stream opener directly.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_test(
        clock: Arc<dyn crate::api::Clock>,
        unary_timeout: std::time::Duration,
    ) -> Self {
        let sopts = crate::network::tcp::SocketOpts::for_test();
        Self::new(RpcClient::new(
            clock,
            crate::network::client::RpcClientConfig {
                tcp_keepalive_interval: sopts.keepalive_interval,
                #[cfg(target_os = "linux")]
                tcp_user_timeout: sopts.user_timeout,
                tcp_keepalive_retries: sopts.keepalive_retries,
                socket_buffer_size: sopts.buffer_size,
                unary_timeout,
                connect_timeout: std::time::Duration::from_secs(10),
                max_unary_conns_per_peer: 32,
                max_idle_unary_conns_per_peer: 8,
                pool_channel_capacity: 4096,
                subscribe_channel_capacity: crate::unit::nonzero(1024),
            },
            tokio_util::sync::CancellationToken::new(),
        ))
    }

    /// Test-only: build the client side from an [`RpcClientConfig`] (from the
    /// node config's `router_config()` producer).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_test_from_client(
        config: crate::network::RpcClientConfig,
        clock: Arc<dyn crate::api::Clock>,
    ) -> Self {
        Self::new(RpcClient::new(
            clock,
            config,
            tokio_util::sync::CancellationToken::new(),
        ))
    }
}

impl Router for RemoteRouter {
    fn register_peer(&self, node_id: NodeId, peer_addr: PeerAddr) {
        self.rpc_client.register_peer(node_id, peer_addr);
    }

    fn remote(&self, node_id: NodeId) -> Arc<dyn ReplicaClient> {
        Arc::new(TcpPeerClient::new(self.rpc_client.clone(), node_id))
    }

    fn remote_direct(&self, peer_addr: PeerAddr) -> Arc<dyn ReplicaClient> {
        Arc::new(TcpPeerClient::direct(self.rpc_client.clone(), peer_addr))
    }
}
