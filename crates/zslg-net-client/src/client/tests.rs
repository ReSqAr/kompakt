use std::time::Duration;

use tokio::net::TcpListener;

use super::*;
use zslg_time_fake::CountingClock;

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
        subscribe_channel_capacity: bytesize::nonzero(1024),
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
    assert!(matches!(
        c.validate(),
        Err(Error::Config(ConfigError::InvalidConfiguration(_)))
    ));
}

#[test]
fn rejects_connect_timeout_above_unary_timeout() {
    let mut c = ok_client_config();
    c.connect_timeout = Duration::from_secs(11);
    assert!(matches!(
        c.validate(),
        Err(Error::Config(ConfigError::InvalidConfiguration(_)))
    ));
}

#[test]
fn rejects_zero_unary_connection_caps() {
    let mut c = ok_client_config();
    c.max_unary_conns_per_peer = 0;
    assert!(matches!(
        c.validate(),
        Err(Error::Config(ConfigError::InvalidConfiguration(_)))
    ));
    let mut c = ok_client_config();
    c.max_idle_unary_conns_per_peer = 0;
    assert!(matches!(
        c.validate(),
        Err(Error::Config(ConfigError::InvalidConfiguration(_)))
    ));
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

    let clock = Arc::new(CountingClock::new());
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
