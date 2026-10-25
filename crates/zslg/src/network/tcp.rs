//! Plain-TCP connection layer: dial a peer, apply socket options, and split
//! the stream into the owned read/write halves the framing layer wraps.
//!
//! There is no TLS: the cluster runs on a trusted network and peer identity is
//! established by the `node_id` carried in every envelope, verified in
//! [`FrameStream`](crate::network::framing::FrameStream).
//!
//! Dead-peer detection — the reason this transport exists — is handled by the
//! kernel: `TCP_NODELAY` so frames leave immediately, TCP keepalive so an idle
//! half-open connection is probed, and `TCP_USER_TIMEOUT` so a connection with
//! unacked in-flight data (a blackholed peer, e.g. a restarted k8s pod that kept
//! its DNS name but changed IP) is torn down in seconds rather than the ~15 min
//! kernel retransmit default. `SO_RCVBUF`/`SO_SNDBUF` bound how much data can age
//! in the socket buffers, which matters because buffer latency shows up as HLC
//! drift: bounding them turns a pile-up into backpressure instead of a tripped
//! skew guard.

use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::ByteSize;
use crate::error::{Error, ToRpc};
use crate::ids::PeerAddr;
use crate::metrics::ConnectFailure;

/// TCP socket tuning applied to every connection on both the dialing and
/// accepting side. Derived from flat `network_*` node-config fields.
/// `pub(crate)` because it crosses the `network` boundary via `NetworkSetup` and
/// `bind_server`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SocketOpts {
    /// TCP keepalive probe interval (and idle time before the first probe).
    pub(crate) keepalive_interval: Duration,
    /// `TCP_USER_TIMEOUT`: how long transmitted data may go unacknowledged
    /// before the connection is declared dead. Bounds blackhole detection.
    #[cfg(target_os = "linux")]
    pub(crate) user_timeout: Duration,
    /// Number of keepalive probes sent before an *idle* half-open connection is
    /// dropped (Linux `TCP_KEEPCNT`). With `keepalive_interval` this bounds idle
    /// detection (`interval × retries`); `user_timeout` covers the busy case.
    pub(crate) keepalive_retries: u32,
    /// `SO_RCVBUF` / `SO_SNDBUF` size.
    pub(crate) buffer_size: ByteSize,
}

impl SocketOpts {
    /// Default socket tuning for tests (mirrors the flat node-config defaults).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_test() -> Self {
        Self {
            keepalive_interval: Duration::from_secs(5),
            #[cfg(target_os = "linux")]
            user_timeout: Duration::from_secs(15),
            keepalive_retries: 3,
            buffer_size: ByteSize::from_mib(2),
        }
    }
}

/// Apply `TCP_NODELAY` + keepalive + `TCP_USER_TIMEOUT` + socket buffers to an
/// established stream. Used by both `dial` (client) and the server accept loop.
pub(in crate::network) fn apply_socket_opts(
    stream: &TcpStream,
    opts: &SocketOpts,
) -> Result<(), Error> {
    stream.set_nodelay(true).to_rpc("set_nodelay")?;

    let sock = SockRef::from(stream);
    let keepalive = TcpKeepalive::new()
        .with_time(opts.keepalive_interval)
        .with_interval(opts.keepalive_interval)
        .with_retries(opts.keepalive_retries);
    sock.set_tcp_keepalive(&keepalive)
        .to_rpc("set_tcp_keepalive")?;
    #[cfg(target_os = "linux")]
    sock.set_tcp_user_timeout(Some(opts.user_timeout))
        .to_rpc("set_tcp_user_timeout")?;
    let buffer_size = usize_from_u32(opts.buffer_size.as_u32()?);
    sock.set_recv_buffer_size(buffer_size)
        .to_rpc("set_recv_buffer_size")?;
    sock.set_send_buffer_size(buffer_size)
        .to_rpc("set_send_buffer_size")?;
    Ok(())
}

/// Resolve `peer_addr`, connect (bounded by `connect_timeout`), tune the socket,
/// and return the owned halves for the framing layer. DNS is resolved on **every**
/// call so a peer that changed IP behind a stable name is reached on reconnect.
pub(in crate::network) async fn dial(
    peer_addr: &PeerAddr,
    connect_timeout: Duration,
    opts: &SocketOpts,
) -> Result<(OwnedWriteHalf, OwnedReadHalf), Error> {
    let socket_addr = {
        let _probe = crate::metrics::rpc_dns_resolve_probe();
        peer_addr.addr.resolve().await.inspect_err(|_e| {
            // Counted here too: a resolve failure never reaches the connect
            // below, so without this the dial simply vanishes from the counter.
            crate::metrics::rpc_connect_failed(ConnectFailure::Dns);
        })?
    };
    let connecting = TcpStream::connect(socket_addr);
    let stream = tokio::time::timeout(connect_timeout, connecting)
        .await
        .map_err(|_| {
            crate::metrics::rpc_connect_failed(ConnectFailure::Timeout);
            Error::Rpc(format!(
                "connect to {} timed out after {connect_timeout:?}",
                peer_addr.addr
            ))
        })?
        .inspect_err(|e| {
            crate::metrics::rpc_connect_failed(ConnectFailure::classify(e));
        })
        .to_rpc("tcp connect")?;
    apply_socket_opts(&stream, opts)?;
    let (read, write) = stream.into_split();
    Ok((write, read))
}

/// Infallible `u32 -> usize` widening (all supported targets are ≥ 32-bit).
#[must_use]
fn usize_from_u32(v: u32) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn test_opts() -> SocketOpts {
        SocketOpts {
            keepalive_interval: Duration::from_secs(5),
            #[cfg(target_os = "linux")]
            user_timeout: Duration::from_secs(15),
            keepalive_retries: 3,
            buffer_size: ByteSize::from_mib(2),
        }
    }

    #[tokio::test]
    async fn dial_reaches_bound_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = PeerAddr {
            addr: format!("127.0.0.1:{}", addr.port()).as_str().into(),
            server_name: "test".to_owned().into(),
        };
        let accept = tokio::spawn(async move { listener.accept().await.map(|(s, _)| s) });
        let dialed = dial(&peer, Duration::from_secs(5), &test_opts()).await;
        assert!(dialed.is_ok(), "dial failed: {:?}", dialed.err());
        assert!(accept.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn connection_refused_is_fast_typed_error() {
        // Bind then drop to obtain a port nothing is listening on.
        let addr = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap()
        };
        let peer = PeerAddr {
            addr: format!("127.0.0.1:{}", addr.port()).as_str().into(),
            server_name: "test".to_owned().into(),
        };
        let start = std::time::Instant::now();
        let res = dial(&peer, Duration::from_secs(5), &test_opts()).await;
        assert!(
            matches!(res, Err(Error::Rpc(_))),
            "expected Rpc err: {res:?}"
        );
        // RST comes back well under the connect timeout.
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn socket_opts_are_applied() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await.map(|(s, _)| s) });
        let stream = TcpStream::connect(addr).await.unwrap();
        apply_socket_opts(&stream, &test_opts()).unwrap();
        assert!(stream.nodelay().unwrap());
        let sock = SockRef::from(&stream);
        // Keepalive is enabled after applying the options.
        assert!(sock.keepalive().unwrap());
        let _ = accept.await.unwrap();
    }
}
