//! Unary RPC connections: one pooled connection per peer, and the store that hands them out.

use super::Dialer;
use crate::Error;
use dashmap::DashMap;
use metrics_kit::OpState;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::debug;
use zslg_consensus::NodeId;
use zslg_net::NetError;
use zslg_net::PeerAddr;
use zslg_net_wire::messages::{RpcFrame, RpcRequest};
use zslg_net_wire::{AsyncFrameSink, AsyncFrameStream};

/// Identifies a per-peer unary connection pool. See the header for why the
/// `Option<NodeId>` is part of the key.
pub(crate) type UnaryKey = (PeerAddr, Option<NodeId>);

/// A pooled, keep-alive unary connection: one framed TCP connection over which a
/// single request/response exchange happens at a time. Returned to its
/// [`UnaryPool`] after each successful exchange for reuse.
pub(crate) struct UnaryConn {
    sink: AsyncFrameSink,
    stream: AsyncFrameStream,
}

impl UnaryConn {
    /// Send the request and read exactly one response frame. Any error leaves
    /// the connection unusable (the caller drops it).
    pub(crate) async fn exchange(&mut self, request: RpcRequest) -> Result<RpcFrame, Error> {
        self.sink.send(request).await?;
        match self.stream.next_frame::<RpcFrame>().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(e)) => Err(e.into()),
            None => Err(Error::Net(NetError::Rpc(
                "stream closed without response".into(),
            ))),
        }
    }
}

/// Per-peer pool of idle unary connections plus the concurrency gate. Invariant:
/// at most one request in flight on a checked-out connection (see the header).
pub(crate) struct UnaryPool {
    idle: Mutex<Vec<UnaryConn>>,
    permits: Semaphore,
    max_idle: usize,
}

impl UnaryPool {
    /// Take an idle connection for reuse, if any.
    pub(crate) fn checkout(&self) -> Option<UnaryConn> {
        self.idle.lock().pop()
    }

    /// Return a connection for reuse, or drop it (closing the socket) if the
    /// idle list is already at capacity.
    pub(crate) fn checkin(&self, conn: UnaryConn) {
        let mut idle = self.idle.lock();
        if idle.len() < self.max_idle {
            idle.push(conn);
        }
    }
}

pub(crate) struct UnaryPoolStore {
    pools: DashMap<UnaryKey, Arc<UnaryPool>>,
    dialer: Arc<Dialer>,
    max_unary_conns: usize,
    max_idle: usize,
    unary_timeout: Duration,
}

impl UnaryPoolStore {
    pub(crate) fn new(
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

    pub(crate) fn unary_timeout(&self) -> Duration {
        self.unary_timeout
    }

    pub(crate) async fn send_unary(
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
            .map_err(|_| Error::Net(NetError::Rpc("unary pool closed".into())))?;

        let frame = self
            .exchange_with_retry(&pool, &addr, expected, request)
            .await?;
        roundtrip.finish(OpState::Success);
        Ok(frame)
    }

    pub(crate) async fn exchange_with_retry(
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
