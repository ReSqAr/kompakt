//! The outbound side of the wire: everything this node does to reach another.
//!
//! Three connection shapes, because three lifetimes:
//!
//! ```text
//!   unary        dial ─> [req/resp] ─> checkin        pooled, reused
//!                  raft rpcs, status, echo            one exchange at a time
//!
//!   server       dial ─> req ─> drop(sink) ─> items…  one-shot, long-lived
//!   stream         subscribe, subscribe-status        never writes again
//!
//!   write pool   dial ─> handshake ─> adopt blocking ─> many req ⇄ many resp
//!                  writer → replica                   out of order, by DataId
//! ```
//!
//! **Unary connections are pooled but never pipelined.** The server dispatches
//! sequentially per connection, so a second concurrent request on one connection
//! would deadlock; concurrency comes from more connections, capped by a
//! semaphore so a slow peer applies backpressure instead of growing connections
//! without bound. A reused connection that fails is retried once on a fresh dial
//! — every unary RPC here is idempotent, so a peer that dropped an idle socket
//! (a restarted pod) costs a retry rather than a failure.
//!
//! A pool is keyed by `(addr, Option<NodeId>)`, not by address alone. A
//! `Registered` connection verifies the peer's identity on every frame and an
//! anonymous `Direct` dial cannot; sharing a socket between them would let an
//! unverified exchange ride on a verified connection. `Direct` exists because
//! during join the target's `NodeId` is not yet known.
//!
//! The write path leaves this module almost immediately: `write_stream` adopts
//! the connection onto dedicated blocking threads, since it is the only hot
//! path. Subscribes stay async.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use zslg_lifecycle_types::ConfigError;
use zslg_net::NetError;

use tokio_util::sync::CancellationToken;

use crate::Error;
use bytesize::ByteSize;
use crossbeam_combinators::Channel;
use zslg_consensus::NodeId;
use zslg_log::LogIndex;
use zslg_log::{StampedWrite, WriteOutcome};
use zslg_net::PeerAddr;
use zslg_net_wire::messages::{
    RpcFrame, RpcRaftFrame, RpcRaftRequest, RpcReplicaFrame, RpcReplicaRequest, RpcRequest,
};
use zslg_net_wire::tcp::SocketOpts;
use zslg_net_wire::{AsyncFrameSink, AsyncFrameStream};
use zslg_time::Clock;

mod write_stream;

mod subscribe_stream;

mod dial;
mod stream;
mod unary;

pub(crate) use self::dial::{AddrBook, Dialer};
pub(crate) use self::stream::StreamOpener;
pub(crate) use self::unary::UnaryPoolStore;

/// How an outgoing RPC reaches its peer.
#[derive(Debug, Clone)]
pub enum Target {
    /// A peer already in the routing table: pooled connection, responses are
    /// `NodeId`-verified.
    Registered(NodeId),
    /// An anonymous peer dialed directly by address: no `NodeId` verification.
    /// Used during cluster join before the target's identity is known.
    Direct(PeerAddr),
}

/// Knobs for [`RpcClient::new`], grouped so the constructor stays under the
/// argument limit. TCP socket tuning and write-pool knobs are inlined (flat).
pub struct RpcClientConfig {
    pub tcp_keepalive_interval: Duration,
    #[cfg(target_os = "linux")]
    pub tcp_user_timeout: Duration,
    pub tcp_keepalive_retries: u32,
    pub socket_buffer_size: ByteSize,
    pub unary_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_unary_conns_per_peer: usize,
    pub max_idle_unary_conns_per_peer: usize,
    pub pool_channel_capacity: usize,
    /// Receive-buffer depth for a replica-log subscription. Defaults to the
    /// replica's live-channel depth: a smaller value here would just move the
    /// bottleneck one hop downstream. Unlike the replica's fan-out this
    /// **blocks** (see `CONSUMER_STALL_TIMEOUT`) rather than shedding, so an
    /// undersized buffer stalls the stream instead of collapsing it.
    pub subscribe_channel_capacity: NonZeroUsize,
}

impl RpcClientConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.tcp_keepalive_retries == 0 {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "tcp_keepalive_retries must be >= 1".to_owned(),
            )));
        }
        if self.max_unary_conns_per_peer == 0 {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "max_unary_connections_per_peer must be >= 1".to_owned(),
            )));
        }
        if self.max_idle_unary_conns_per_peer == 0 {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "max_idle_unary_connections_per_peer must be >= 1".to_owned(),
            )));
        }
        if self.socket_buffer_size.as_u64() == 0 {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "socket_buffer_size must be > 0".to_owned(),
            )));
        }
        if self.connect_timeout > self.unary_timeout {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "connect_timeout must be <= unary_timeout".to_owned(),
            )));
        }
        #[cfg(target_os = "linux")]
        if self.tcp_user_timeout <= self.tcp_keepalive_interval {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "tcp_user_timeout must be > tcp_keepalive_interval".to_owned(),
            )));
        }
        Ok(())
    }
}

pub struct RpcClient {
    addrs: AddrBook,
    rpc: Arc<UnaryPoolStore>,
    streams: StreamOpener,
}

impl RpcClient {
    pub fn new(
        clock: Arc<dyn Clock>,
        opts: RpcClientConfig,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let dialer = Arc::new(Dialer::new(
            clock,
            SocketOpts {
                keepalive_interval: opts.tcp_keepalive_interval,
                #[cfg(target_os = "linux")]
                user_timeout: opts.tcp_user_timeout,
                keepalive_retries: opts.tcp_keepalive_retries,
                buffer_size: opts.socket_buffer_size,
            },
            opts.connect_timeout,
        ));

        Arc::new_cyclic(|self_weak| {
            let rpc = Arc::new(UnaryPoolStore::new(
                dialer.clone(),
                opts.max_unary_conns_per_peer,
                opts.max_idle_unary_conns_per_peer,
                opts.unary_timeout,
            ));
            Self {
                addrs: AddrBook::new(),
                rpc,
                streams: StreamOpener::new(
                    self_weak.clone(),
                    dialer,
                    opts.pool_channel_capacity,
                    opts.subscribe_channel_capacity,
                    cancel,
                ),
            }
        })
    }

    /// Make `node_id` addressable, so a `Target::Registered` can resolve. Fed
    /// lazily from Raft membership as peers are learned; a node registers itself
    /// too, since its own `RpcClient` may route back to it.
    pub fn register_peer(&self, node_id: NodeId, peer_addr: PeerAddr) {
        self.addrs.register(node_id, peer_addr);
    }

    /// Send one request and receive exactly one response frame, bounded by the
    /// per-RPC deadline. Pooled, with the one retry described in the header.
    pub async fn send_unary(
        &self,
        target: &Target,
        request: RpcRequest,
    ) -> Result<RpcFrame, Error> {
        let (addr, expected) = self.addrs.resolve(target)?;
        tokio::time::timeout(
            self.rpc.unary_timeout(),
            self.rpc.send_unary(addr, expected, request),
        )
        .await
        .map_err(|_| Error::Net(NetError::Rpc(format!("unary rpc to {target:?} timed out"))))?
    }

    /// Send one replica request and receive the response as `RpcReplicaFrame`.
    pub async fn call_replica(
        &self,
        target: &Target,
        request: RpcReplicaRequest,
    ) -> Result<RpcReplicaFrame, Error> {
        match self
            .send_unary(target, RpcRequest::Replica(request))
            .await?
        {
            RpcFrame::Replica(frame) => Ok(frame),
            RpcFrame::Error(e) => Err(e.into()),
            other => Err(Error::Net(NetError::Rpc(format!(
                "unexpected frame in replica response from {target:?}: {other:?}"
            )))),
        }
    }

    /// Send one Raft request and receive the response as `RpcRaftFrame`.
    /// Converts `RpcFrame::Error` into `Err` automatically.
    pub async fn call_raft(
        &self,
        target: &Target,
        request: RpcRaftRequest,
    ) -> Result<RpcRaftFrame, Error> {
        match self
            .send_unary(target, RpcRequest::Raft(Box::new(request)))
            .await?
        {
            RpcFrame::Raft(frame) => Ok(frame),
            RpcFrame::Error(e) => Err(e.into()),
            other => Err(Error::Net(NetError::Rpc(format!(
                "unexpected frame in raft response from {target:?}: {other:?}"
            )))),
        }
    }

    /// Ping `peer` with an echo and confirm the reply arrives.
    pub async fn echo(&self, peer: NodeId) -> Result<(), Error> {
        match self
            .send_unary(&Target::Registered(peer), RpcRequest::Echo)
            .await?
        {
            RpcFrame::Echo => Ok(()),
            RpcFrame::Error(e) => Err(e.into()),
            other => Err(Error::Net(NetError::Rpc(format!(
                "echo: unexpected frame {other:?}"
            )))),
        }
    }

    /// Open a server-streaming RPC (`Subscribe` / `SubscribeStatus`). The write
    /// half is dropped immediately: the subscription never writes again, so
    /// half-closing it is clean. The connection lives until the returned stream
    /// is dropped.
    pub async fn open(
        &self,
        target: &Target,
        request: RpcRequest,
    ) -> Result<AsyncFrameStream, Error> {
        let (addr, expected) = self.addrs.resolve(target)?;
        self.streams.open(addr, expected, request).await
    }

    /// Dial and send the `ReplicaWrite` handshake, handing both halves to the
    /// pool worker.
    pub async fn dial_write_stream(
        &self,
        target: &Target,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (addr, expected) = self.addrs.resolve(target)?;
        self.streams.dial_write_stream(addr, expected).await
    }

    /// Open a write connection to `node_id`, driving writes off `input`. One TCP
    /// connection per call — the caller (the writer coordinator, one loop per peer)
    /// owns reopen, so there is no cross-caller pool to arbitrate. Items carry
    /// their enqueue timestamp (the write→replica queue-residency probes).
    #[must_use]
    pub fn write(
        &self,
        node_id: NodeId,
        input: Channel<StampedWrite, crossbeam_combinators::Finite>,
    ) -> Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> {
        self.streams.write(node_id, input)
    }

    /// Open a fresh connection for a subscribe stream: dial, send the
    /// `ReplicaSubscribe { from }` handshake.
    pub async fn dial_subscribe_stream(
        &self,
        target: &Target,
        from: Option<LogIndex>,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (addr, expected) = self.addrs.resolve(target)?;
        self.streams
            .dial_subscribe_stream(addr, expected, from)
            .await
    }

    #[must_use]
    pub fn subscribe_from(
        &self,
        node_id: NodeId,
        from: Option<LogIndex>,
    ) -> zslg_log::ReplicaStream {
        self.streams.subscribe_from(node_id, from)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests;
