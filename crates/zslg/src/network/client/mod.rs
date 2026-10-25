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
use std::sync::{Arc, Weak};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::ByteSize;
use crate::api::{Clock, StampedWrite, WriteOutcome};
use crate::channel::Channels;
use crate::error::Error;
use crate::ids::{LogIndex, NodeId, PeerAddr};
use crate::meta::Data;
use crate::metrics::OpState;
use crate::network::framing::{AsyncFrameSink, AsyncFrameStream};
use crate::network::messages::{
    RpcFrame, RpcRaftFrame, RpcRaftRequest, RpcReplicaFrame, RpcReplicaRequest, RpcRequest,
    RpcStreamKind,
};
use crate::network::tcp::{self, SocketOpts};

mod write_stream;
pub(super) use write_stream::open_write;

mod subscribe_stream;
pub(super) use subscribe_stream::open_subscribe;

/// Identifies a per-peer unary connection pool. See the header for why the
/// `Option<NodeId>` is part of the key.
type UnaryKey = (PeerAddr, Option<NodeId>);

/// How an outgoing RPC reaches its peer.
#[derive(Debug, Clone)]
pub(in crate::network) enum Target {
    /// A peer already in the routing table: pooled connection, responses are
    /// `NodeId`-verified.
    Registered(NodeId),
    /// An anonymous peer dialed directly by address: no `NodeId` verification.
    /// Used during cluster join before the target's identity is known.
    Direct(PeerAddr),
}

/// Knobs for [`RpcClient::new`], grouped so the constructor stays under the
/// argument limit. TCP socket tuning and write-pool knobs are inlined (flat).
pub(crate) struct RpcClientConfig {
    pub(crate) tcp_keepalive_interval: Duration,
    #[cfg(target_os = "linux")]
    pub(crate) tcp_user_timeout: Duration,
    pub(crate) tcp_keepalive_retries: u32,
    pub(crate) socket_buffer_size: ByteSize,
    pub(crate) unary_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) max_unary_conns_per_peer: usize,
    pub(crate) max_idle_unary_conns_per_peer: usize,
    pub(crate) pool_channel_capacity: usize,
    /// Receive-buffer depth for a replica-log subscription. Defaults to the
    /// replica's live-channel depth: a smaller value here would just move the
    /// bottleneck one hop downstream. Unlike the replica's fan-out this
    /// **blocks** (see `CONSUMER_STALL_TIMEOUT`) rather than shedding, so an
    /// undersized buffer stalls the stream instead of collapsing it.
    pub(crate) subscribe_channel_capacity: NonZeroUsize,
}

impl RpcClientConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.tcp_keepalive_retries == 0 {
            return Err(Error::InvalidConfiguration(
                "tcp_keepalive_retries must be >= 1".to_owned(),
            ));
        }
        if self.max_unary_conns_per_peer == 0 {
            return Err(Error::InvalidConfiguration(
                "max_unary_connections_per_peer must be >= 1".to_owned(),
            ));
        }
        if self.max_idle_unary_conns_per_peer == 0 {
            return Err(Error::InvalidConfiguration(
                "max_idle_unary_connections_per_peer must be >= 1".to_owned(),
            ));
        }
        if self.socket_buffer_size.as_u64() == 0 {
            return Err(Error::InvalidConfiguration(
                "socket_buffer_size must be > 0".to_owned(),
            ));
        }
        if self.connect_timeout > self.unary_timeout {
            return Err(Error::InvalidConfiguration(
                "connect_timeout must be <= unary_timeout".to_owned(),
            ));
        }
        #[cfg(target_os = "linux")]
        if self.tcp_user_timeout <= self.tcp_keepalive_interval {
            return Err(Error::InvalidConfiguration(
                "tcp_user_timeout must be > tcp_keepalive_interval".to_owned(),
            ));
        }
        Ok(())
    }
}

struct AddrBook {
    addrs: DashMap<NodeId, PeerAddr>,
}

impl AddrBook {
    fn new() -> Self {
        Self {
            addrs: DashMap::new(),
        }
    }

    fn register(&self, node_id: NodeId, addr: PeerAddr) {
        self.addrs.insert(node_id, addr);
    }

    fn resolve(&self, target: &Target) -> Result<(PeerAddr, Option<NodeId>), Error> {
        match target {
            Target::Registered(peer) => {
                let addr = self
                    .addrs
                    .get(peer)
                    .map(|e| e.clone())
                    .ok_or(Error::UnknownPeer(*peer))?;
                Ok((addr, Some(*peer)))
            }
            Target::Direct(addr) => Ok((addr.clone(), None)),
        }
    }
}

struct Dialer {
    clock: Arc<dyn Clock>,
    socket_opts: SocketOpts,
    connect_timeout: Duration,
}

impl Dialer {
    fn new(clock: Arc<dyn Clock>, socket_opts: SocketOpts, connect_timeout: Duration) -> Self {
        Self {
            clock,
            socket_opts,
            connect_timeout,
        }
    }

    async fn dial(
        &self,
        peer_addr: &PeerAddr,
        expected: Option<NodeId>,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (write, read) = tcp::dial(peer_addr, self.connect_timeout, &self.socket_opts).await?;
        let sink = AsyncFrameSink::new(write, self.clock.clone(), expected);
        let stream = AsyncFrameStream::new(read, self.clock.clone(), expected);
        Ok((sink, stream))
    }
}

/// A pooled, keep-alive unary connection: one framed TCP connection over which a
/// single request/response exchange happens at a time. Returned to its
/// [`UnaryPool`] after each successful exchange for reuse.
struct UnaryConn {
    sink: AsyncFrameSink,
    stream: AsyncFrameStream,
}

impl UnaryConn {
    /// Send the request and read exactly one response frame. Any error leaves
    /// the connection unusable (the caller drops it).
    async fn exchange(&mut self, request: RpcRequest) -> Result<RpcFrame, Error> {
        self.sink.send(request).await?;
        match self.stream.next_frame::<RpcFrame>().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(e)) => Err(e),
            None => Err(Error::Rpc("stream closed without response".into())),
        }
    }
}

/// Per-peer pool of idle unary connections plus the concurrency gate. Invariant:
/// at most one request in flight on a checked-out connection (see the header).
struct UnaryPool {
    idle: Mutex<Vec<UnaryConn>>,
    permits: Semaphore,
    max_idle: usize,
}

impl UnaryPool {
    /// Take an idle connection for reuse, if any.
    fn checkout(&self) -> Option<UnaryConn> {
        self.idle.lock().pop()
    }

    /// Return a connection for reuse, or drop it (closing the socket) if the
    /// idle list is already at capacity.
    fn checkin(&self, conn: UnaryConn) {
        let mut idle = self.idle.lock();
        if idle.len() < self.max_idle {
            idle.push(conn);
        }
    }
}

struct UnaryPoolStore {
    pools: DashMap<UnaryKey, Arc<UnaryPool>>,
    dialer: Arc<Dialer>,
    max_unary_conns: usize,
    max_idle: usize,
    unary_timeout: Duration,
}

impl UnaryPoolStore {
    fn new(
        dialer: Arc<Dialer>,
        max_unary_conns: usize,
        max_idle: usize,
        unary_timeout: Duration,
    ) -> Self {
        Self {
            pools: DashMap::new(),
            dialer,
            max_unary_conns,
            max_idle,
            unary_timeout,
        }
    }

    fn unary_timeout(&self) -> Duration {
        self.unary_timeout
    }

    async fn send_unary(
        &self,
        addr: PeerAddr,
        expected: Option<NodeId>,
        request: RpcRequest,
    ) -> Result<RpcFrame, Error> {
        let key: UnaryKey = (addr.clone(), expected);
        let pool = self
            .pools
            .entry(key)
            .or_insert_with(|| {
                Arc::new(UnaryPool {
                    idle: Mutex::new(Vec::new()),
                    permits: Semaphore::new(self.max_unary_conns),
                    max_idle: self.max_idle,
                })
            })
            .clone();

        let roundtrip = crate::metrics::rpc_roundtrip_probe();
        let _permit = pool
            .permits
            .acquire()
            .await
            .map_err(|_| Error::Rpc("unary pool closed".into()))?;

        let frame = self
            .exchange_with_retry(&pool, &addr, expected, request)
            .await?;
        roundtrip.finish(OpState::Success);
        Ok(frame)
    }

    async fn exchange_with_retry(
        &self,
        pool: &UnaryPool,
        peer_addr: &PeerAddr,
        expected: Option<NodeId>,
        request: RpcRequest,
    ) -> Result<RpcFrame, Error> {
        if let Some(mut conn) = pool.checkout() {
            crate::metrics::rpc_conn_reuse(true);
            match conn.exchange(request.clone()).await {
                Ok(frame) => {
                    pool.checkin(conn);
                    return Ok(frame);
                }
                Err(e) => {
                    debug!(error = %e, "reused unary connection failed; retrying fresh dial");
                }
            }
        } else {
            crate::metrics::rpc_conn_reuse(false);
        }

        let checkout = crate::metrics::rpc_conn_checkout_probe();
        let (sink, stream) = self.dialer.dial(peer_addr, expected).await?;
        checkout.record();
        let mut conn = UnaryConn { sink, stream };
        let frame = conn.exchange(request).await?;
        pool.checkin(conn);
        Ok(frame)
    }
}

struct StreamOpener {
    rpc_weak: Weak<RpcClient>,
    dialer: Arc<Dialer>,
    pool_channel_cap: usize,
    subscribe_channel_cap: NonZeroUsize,
    /// The node's shutdown token — see [`open_subscribe`] for what it is for.
    cancel: CancellationToken,
}

impl StreamOpener {
    fn new(
        rpc_weak: Weak<RpcClient>,
        dialer: Arc<Dialer>,
        pool_channel_cap: usize,
        subscribe_channel_cap: NonZeroUsize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            rpc_weak,
            dialer,
            pool_channel_cap,
            subscribe_channel_cap,
            cancel,
        }
    }

    async fn open(
        &self,
        addr: PeerAddr,
        expected: Option<NodeId>,
        request: RpcRequest,
    ) -> Result<AsyncFrameStream, Error> {
        let probe = crate::metrics::rpc_open_stream_probe();
        let (mut sink, stream) = self.dialer.dial(&addr, expected).await?;
        sink.send(request).await?;
        drop(sink);
        probe.finish(OpState::Success);
        Ok(stream)
    }

    async fn dial_write_stream(
        &self,
        addr: PeerAddr,
        expected: Option<NodeId>,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (mut sink, stream) = self.dialer.dial(&addr, expected).await?;
        sink.send(RpcRequest::Stream(RpcStreamKind::ReplicaWrite))
            .await?;
        Ok((sink, stream))
    }

    fn write(
        &self,
        node_id: NodeId,
        input: Channels<StampedWrite>,
    ) -> Channels<Result<WriteOutcome, Error>> {
        open_write(
            self.rpc_weak.clone(),
            Target::Registered(node_id),
            input,
            self.pool_channel_cap,
        )
    }

    async fn dial_subscribe_stream(
        &self,
        addr: PeerAddr,
        expected: Option<NodeId>,
        from: Option<LogIndex>,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (mut sink, stream) = self.dialer.dial(&addr, expected).await?;
        sink.send(RpcRequest::Stream(RpcStreamKind::ReplicaSubscribe { from }))
            .await?;
        Ok((sink, stream))
    }

    fn subscribe_from(
        &self,
        node_id: NodeId,
        from: Option<LogIndex>,
    ) -> Channels<Result<(LogIndex, Data), Error>> {
        open_subscribe(
            self.rpc_weak.clone(),
            Target::Registered(node_id),
            from,
            self.subscribe_channel_cap,
            self.cancel.clone(),
        )
    }
}

pub(in crate::network) struct RpcClient {
    addrs: AddrBook,
    rpc: Arc<UnaryPoolStore>,
    streams: StreamOpener,
}

impl RpcClient {
    pub(in crate::network) fn new(
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

    /// Make `node_id` addressable, so a [`Target::Registered`] can resolve. Fed
    /// lazily from Raft membership as peers are learned; a node registers itself
    /// too, since its own `RpcClient` may route back to it.
    pub(in crate::network) fn register_peer(&self, node_id: NodeId, peer_addr: PeerAddr) {
        self.addrs.register(node_id, peer_addr);
    }

    /// Send one request and receive exactly one response frame, bounded by the
    /// per-RPC deadline. Pooled, with the one retry described in the header.
    pub(in crate::network) async fn send_unary(
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
        .map_err(|_| Error::Rpc(format!("unary rpc to {target:?} timed out")))?
    }

    /// Send one replica request and receive the response as `RpcReplicaFrame`.
    pub(in crate::network) async fn call_replica(
        &self,
        target: &Target,
        request: RpcReplicaRequest,
    ) -> Result<RpcReplicaFrame, Error> {
        match self
            .send_unary(target, RpcRequest::Replica(request))
            .await?
        {
            RpcFrame::Replica(frame) => Ok(frame),
            RpcFrame::Error(e) => Err(e),
            other => Err(Error::Rpc(format!(
                "unexpected frame in replica response from {target:?}: {other:?}"
            ))),
        }
    }

    /// Send one Raft request and receive the response as `RpcRaftFrame`.
    /// Converts `RpcFrame::Error` into `Err` automatically.
    pub(in crate::network) async fn call_raft(
        &self,
        target: &Target,
        request: RpcRaftRequest,
    ) -> Result<RpcRaftFrame, Error> {
        match self
            .send_unary(target, RpcRequest::Raft(Box::new(request)))
            .await?
        {
            RpcFrame::Raft(frame) => Ok(frame),
            RpcFrame::Error(e) => Err(e),
            other => Err(Error::Rpc(format!(
                "unexpected frame in raft response from {target:?}: {other:?}"
            ))),
        }
    }

    /// Ping `peer` with an echo and confirm the reply arrives.
    pub(in crate::network) async fn echo(&self, peer: NodeId) -> Result<(), Error> {
        match self
            .send_unary(&Target::Registered(peer), RpcRequest::Echo)
            .await?
        {
            RpcFrame::Echo => Ok(()),
            RpcFrame::Error(e) => Err(e),
            other => Err(Error::Rpc(format!("echo: unexpected frame {other:?}"))),
        }
    }

    /// Open a server-streaming RPC (`Subscribe` / `SubscribeStatus`). The write
    /// half is dropped immediately: the subscription never writes again, so
    /// half-closing it is clean. The connection lives until the returned stream
    /// is dropped.
    pub(in crate::network) async fn open(
        &self,
        target: &Target,
        request: RpcRequest,
    ) -> Result<AsyncFrameStream, Error> {
        let (addr, expected) = self.addrs.resolve(target)?;
        self.streams.open(addr, expected, request).await
    }

    /// Dial and send the `ReplicaWrite` handshake, handing both halves to the
    /// pool worker.
    pub(in crate::network) async fn dial_write_stream(
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
    pub(in crate::network) fn write(
        &self,
        node_id: NodeId,
        input: Channels<StampedWrite>,
    ) -> Channels<Result<WriteOutcome, Error>> {
        self.streams.write(node_id, input)
    }

    /// Open a fresh connection for a subscribe stream: dial, send the
    /// `ReplicaSubscribe { from }` handshake.
    pub(in crate::network) async fn dial_subscribe_stream(
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
    pub(in crate::network) fn subscribe_from(
        &self,
        node_id: NodeId,
        from: Option<LogIndex>,
    ) -> Channels<Result<(LogIndex, Data), Error>> {
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
mod tests {
    use std::time::Duration;

    use tokio::net::TcpListener;

    use super::*;
    use crate::hlc::HlcClock;

    fn test_opts(unary_timeout: Duration) -> RpcClientConfig {
        RpcClientConfig {
            tcp_keepalive_interval: Duration::from_secs(5),
            #[cfg(target_os = "linux")]
            tcp_user_timeout: Duration::from_secs(15),
            tcp_keepalive_retries: 3,
            socket_buffer_size: ByteSize::from_mib(2),
            unary_timeout,
            connect_timeout: Duration::from_secs(10),
            max_unary_conns_per_peer: 32,
            max_idle_unary_conns_per_peer: 8,
            pool_channel_capacity: 4096,
            subscribe_channel_capacity: crate::unit::nonzero(1024),
        }
    }

    // ── RpcClientConfig::validate ──────────────────────────────────────────

    fn ok_client_config() -> RpcClientConfig {
        test_opts(Duration::from_secs(10))
    }

    /// The client buffer must default to the same depth as the replica's live
    /// channel. A smaller one just relocates the bottleneck one hop: unlike the
    /// replica side this blocks (30 s CONSUMER_STALL_TIMEOUT) rather than
    /// shedding, so it would stall the stream instead of collapsing it.
    #[test]
    fn subscribe_buffer_defaults_to_the_replica_live_channel_depth() {
        assert_eq!(ok_client_config().subscribe_channel_capacity.get(), 1024);
    }

    #[test]
    fn rejects_zero_keepalive_retries() {
        let mut c = ok_client_config();
        c.tcp_keepalive_retries = 0;
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    #[test]
    fn rejects_connect_timeout_above_unary_timeout() {
        let mut c = ok_client_config();
        c.connect_timeout = Duration::from_secs(11);
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    #[test]
    fn rejects_zero_unary_connection_caps() {
        let mut c = ok_client_config();
        c.max_unary_conns_per_peer = 0;
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
        let mut c = ok_client_config();
        c.max_idle_unary_conns_per_peer = 0;
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    /// F-29: a peer whose handler is wedged (connection alive, request read, no
    /// response ever sent) must not hang the caller — the unary deadline converts
    /// the stall into a typed error.
    #[tokio::test]
    async fn unary_rpc_times_out_against_wedged_handler() {
        // Silent server: accepts the TCP connection, then holds it open forever
        // without ever writing a response frame.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let _hold_open = stream;
                    std::future::pending::<()>().await;
                });
            }
        });

        let clock = Arc::new(HlcClock::new(Duration::from_secs(60)));
        let client = RpcClient::new(
            clock,
            test_opts(Duration::from_millis(300)),
            CancellationToken::new(),
        );
        let peer = NodeId::new_v4();
        client.register_peer(
            peer,
            PeerAddr {
                addr: server_addr.into(),
                server_name: "localhost".into(),
            },
        );

        let start = tokio::time::Instant::now();
        let err = client.echo(peer).await.expect_err("must time out");
        assert!(
            err.to_string().contains("timed out"),
            "unexpected error: {err}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "timeout must fire promptly, not hang"
        );
    }
}
