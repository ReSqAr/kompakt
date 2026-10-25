//! The inbound side: accept connections and decide, per connection, what kind it
//! is. The first frame settles that for good.
//!
//! ```text
//!   accept ─> handle_connection ─> first frame
//!                                    │
//!            ┌── Stream(kind) ───────┴── anything else ──┐
//!            │  hand the whole          dispatch, reply, │
//!            │  connection to that      loop for the next│
//!            │  handler; never returns  request (keep-alive)
//! ```
//!
//! **One request at a time per connection**, and the client guarantees it (see
//! `client`'s no-pipelining invariant), so sequential dispatch here never blocks
//! a request that could have proceeded. A server-streaming request just parks in
//! `dispatch` until the subscription ends; the client has already half-closed
//! its write side, so the next read returns EOF and the loop exits on its own.
//!
//! Each connection is its own task, reaped as it finishes so the `JoinSet` does
//! not retain storage for the server's lifetime. Shutdown aborts them rather
//! than draining: a subscription can be arbitrarily long-lived, and the peer
//! reconnects.
//!
//! Rejections that are the *peer's* fault — HLC drift, a wrong `node_id` — are
//! answered with an `Error` frame before closing, so the caller learns why
//! instead of seeing a bare disconnect.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::api::Clock;
use crate::error::{Error, ToRpc};
use crate::ids::NodeId;
use crate::metrics::{self};
use crate::network::framing::{AsyncFrameSink, AsyncFrameStream};
use crate::network::messages::{RpcFrame, RpcRequest};
use crate::network::tcp::{self, SocketOpts};

mod handlers;
mod stream_handler;

pub(in crate::network) use handlers::{DispatchHandler, RpcHandlers};
pub(in crate::network) use stream_handler::{run_subscribe, run_write};

pub(crate) struct RpcServerConfig {
    pub(crate) listen_addr: SocketAddr,
    pub(crate) socket_opts: SocketOpts,
    pub(crate) node_id: NodeId,
}

pub(in crate::network) struct RpcServer {
    listener: TcpListener,
    handlers: Arc<dyn RpcHandlers>,
    clock: Arc<dyn Clock>,
    cancel: CancellationToken,
    local_addr: SocketAddr,
    node_id: NodeId,
    socket_opts: SocketOpts,
    connection_count: Arc<AtomicU64>,
}

impl RpcServer {
    pub(in crate::network) fn bind(
        handlers: Arc<dyn RpcHandlers>,
        clock: Arc<dyn Clock>,
        RpcServerConfig {
            listen_addr,
            socket_opts,
            node_id,
        }: RpcServerConfig,
        cancel: CancellationToken,
    ) -> Result<Self, Error> {
        // Bind synchronously (std), then adopt into the ambient tokio runtime —
        // keeps `bind_server` non-async while still yielding an async listener.
        let std_listener = std::net::TcpListener::bind(listen_addr).to_rpc("tcp server bind")?;
        std_listener
            .set_nonblocking(true)
            .to_rpc("listener set_nonblocking")?;
        let listener = TcpListener::from_std(std_listener).to_rpc("listener from_std")?;
        let local_addr = listener.local_addr().to_rpc("local_addr")?;
        Ok(Self {
            listener,
            handlers,
            clock,
            cancel,
            local_addr,
            node_id,
            socket_opts,
            connection_count: Arc::new(AtomicU64::new(0)),
        })
    }

    #[must_use]
    pub(in crate::network) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(in crate::network) async fn run(self) -> Result<(), Error> {
        info!(addr = %self.local_addr, node_id = ?self.node_id, "rpc server: starting");

        let mut conn_tasks = JoinSet::new();
        loop {
            let accepted = tokio::select! {
                () = self.cancel.cancelled() => break,
                // Reap finished connection tasks so the JoinSet doesn't retain
                // their storage for the server's lifetime.
                Some(_joined) = conn_tasks.join_next() => continue,
                r = self.listener.accept() => r,
            };
            let (stream, remote) = match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    let reason = metrics::ConnectFailure::classify(&e);
                    metrics::rpc_accept_failed(reason);
                    // NOTE: this `continue` retries immediately. For a transient
                    // error that is right; for a persistent one (fd exhaustion)
                    // it is a hot spin that also refuses every new connection
                    // while the node still looks alive. The counter is what
                    // distinguishes the two — see zslg_rpc_accept_failed_total.
                    warn!(error = %e, ?reason, "tcp accept failed");
                    continue;
                }
            };
            if let Err(e) = tcp::apply_socket_opts(&stream, &self.socket_opts) {
                warn!(error = %e, ?remote, "failed to apply socket options; dropping connection");
                continue;
            }
            let (read, write) = stream.into_split();
            let handlers = self.handlers.clone();
            let clock = self.clock.clone();
            let cancel = self.cancel.clone();
            let node_id = self.node_id;
            let connection_count = self.connection_count.clone();
            conn_tasks.spawn(async move {
                connection_count.fetch_add(1, Ordering::Relaxed);
                metrics::rpc_connections_active(connection_count.load(Ordering::Relaxed));
                handle_connection(write, read, handlers, clock, cancel, node_id).await;
                connection_count.fetch_sub(1, Ordering::Relaxed);
                metrics::rpc_connections_active(connection_count.load(Ordering::Relaxed));
            });
        }
        info!(addr = %self.local_addr, node_id = ?self.node_id, "rpc server: shutdown");
        conn_tasks.abort_all();
        while conn_tasks.join_next().await.is_some() {}
        Ok(())
    }
}

/// Serve one TCP connection to close. See the header for the two modes.
async fn handle_connection(
    write: OwnedWriteHalf,
    read: OwnedReadHalf,
    handlers: Arc<dyn RpcHandlers>,
    clock: Arc<dyn Clock>,
    cancel: CancellationToken,
    node_id: NodeId,
) {
    trace!("accepted connection");
    let mut frame_recv = AsyncFrameStream::new(read, clock.clone(), Some(node_id));
    let mut sink = AsyncFrameSink::new(write, clock.clone(), Some(node_id));

    loop {
        let mut guard = metrics::RpcServerGuard::new();
        let request: RpcRequest = match tokio::select! {
            biased;
            () = cancel.cancelled() => {
                guard.disarm();
                return;
            }
            r = frame_recv.next_frame() => r,
        } {
            Some(Ok(r)) => r,
            Some(Err(e @ Error::HlcDriftTooLarge { .. })) => {
                debug!(error = %e, "request rejected: HLC drift");
                let _ = sink.send(RpcFrame::Error(e)).await;
                return;
            }
            Some(Err(e @ Error::UnexpectedNodeId { .. })) => {
                debug!(error = %e, "request rejected: unexpected node_id");
                let _ = sink.send(RpcFrame::Error(e)).await;
                return;
            }
            Some(Err(e)) => {
                warn!(error = %e, "request decode failed");
                return;
            }
            // Clean EOF: the client closed (or half-closed) the connection.
            None => return,
        };

        match request {
            // The server stays out of the stream protocol — it only routes.
            RpcRequest::Stream(kind) => {
                guard.disarm();
                handlers
                    .dispatch_stream(kind, frame_recv, sink, cancel)
                    .await;
                return;
            }
            request => {
                let kind = request.kind();
                guard.set_method(kind);
                let result = tokio::select! {
                    r = handlers.dispatch(request, &mut sink) => r,
                    () = cancel.cancelled() => {
                        guard.disarm();
                        return;
                    }
                };
                guard.finish(result.is_ok());
                if let Err(e) = result {
                    debug!(error = %e, kind, "handler error → emitting Error frame");
                    if let Err(e2) = sink.send(RpcFrame::Error(e)).await {
                        warn!(error = %e2, kind, "failed to send error frame");
                        return;
                    }
                }
                // Keep-alive: loop and read the next request on this connection.
            }
        }
    }
}
