//! A free-standing writer node.
//!
//! `WriterNode` is the write half of [`ReplicaNode`](crate::replica_node::ReplicaNode)
//! with none of the storage or consensus: it joins no Raft, owns no replica log
//! or local store, and persists nothing. It tracks the cluster's `RaftState` by
//! streaming from the configured peers over RPC (see [`crate::raft`]) and
//! fans writes out to a quorum of replicas via `WriterService`.
//!
//! Its identity is ephemeral (a fresh [`NodeId`] each start) — a writer has no
//! durable state to protect, so there is nothing to recover and no reason to
//! persist an id.

use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use std::num::NonZeroUsize;

use crate::ByteSize;
use crate::Error;
use crate::api;
use crate::hlc::{HlcClock, run_clock_gauges};
use crate::ids::NodeId;
use crate::ids::PeerAddr;
use crate::network::setup_router;
use crate::raft::{RaftTracker, RaftTrackerConfig, spawn_raft_tracker_streamer};
use crate::shutdown::Shutdown;
use crate::writer::WriterService;

pub struct WriterNode {
    config: WriterNodeConfig,
    node_id: NodeId,
    hlc_clock: Arc<HlcClock>,
    raft_tracker: Arc<RaftTracker>,
    router: Arc<dyn api::Router>,
    writer: Arc<WriterService>,
    shutdown: Shutdown,
}

impl WriterNode {
    /// Start a writer node: build the TCP client, start the per-peer
    /// raft-tracker streamers, wire up the write path, and block until a quorum
    /// of replicas have been discovered. Returns an error if the
    /// [`WriterNodeConfig::startup_timeout`] elapses first. Once this returns,
    /// the writer is ready for writes.
    pub async fn start(config: WriterNodeConfig) -> Result<Self, Error> {
        config.validate()?;

        // Ephemeral identity: a writer has no durable state, so a fresh id each
        // start is correct — nothing to recover, nothing to collide with.
        let node_id = NodeId::new_v4();
        info!(node_id = ?node_id, "writer node starting");

        let mut shutdown = Shutdown::new();

        let hlc_clock = Arc::new(HlcClock::new(config.hlc_max_clock_skew));
        shutdown.spawn(
            "clock_gauges",
            run_clock_gauges(hlc_clock.clone(), shutdown.token()),
        );
        shutdown.spawn(
            "process_gauges",
            // No durable state and none of the pipeline queues: a writer
            // publishes neither reading rather than a pair of zeroes that would
            // read as "measured, and empty".
            crate::observability::run_process_gauges(None, None, shutdown.token()),
        );

        // Outbound-only: keep the router, discard the server config (a writer
        // never binds a listener).
        let router = setup_router(hlc_clock.clone(), config.router_config(), shutdown.token())?;

        let raft_tracker = spawn_raft_tracker_streamer(
            router.clone(),
            config.raft_tracker_config(),
            &mut shutdown,
        )
        .await?;

        let writer = Arc::new(WriterService::new(
            router.clone(),
            raft_tracker.clone(),
            hlc_clock.clone(),
            config.writer_config(node_id),
        ));

        info!(node_id = ?node_id, "writer node ready");

        Ok(Self {
            config,
            node_id,
            hlc_clock,
            raft_tracker,
            router,
            writer,
            shutdown,
        })
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn writer(&self) -> Arc<dyn api::Writer> {
        self.writer.clone()
    }

    #[must_use]
    pub fn config(&self) -> &WriterNodeConfig {
        &self.config
    }

    /// Clone of the global [`CancellationToken`]; cancelling it triggers a
    /// graceful shutdown of every background task (mirrors
    /// [`ReplicaNode::shutdown_token`](crate::replica_node::ReplicaNode::shutdown_token)).
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// Run the node until shutdown is requested or a background task fails.
    /// Consumes `self`; clone any needed handles ([`writer`](Self::writer))
    /// before calling.
    pub async fn run(self) -> Result<(), Error> {
        let drain_timeout = self.config.drain_timeout;
        let Self {
            config,
            node_id,
            hlc_clock,
            raft_tracker,
            router,
            writer,
            shutdown,
        } = self;

        let result = shutdown.run(drain_timeout).await;

        drop((config, node_id, hlc_clock, raft_tracker, router, writer));
        result
    }
}

/// Configuration for a free-standing writer node.
///
/// A writer holds no durable state — it joins no Raft, owns no replica log, and
/// stores no data — so this carries only what is needed to *talk to* a replica
/// cluster: the network config (plus the `peers` to poll for Raft state) and
/// the clock. There is deliberately no storage config — a writer persists
/// nothing.
/// Flat, prefix-grouped configuration. Every field carries the prefix of the
/// subsystem it feeds (`hlc_*`, `raft_tracker_*`, `network_*`); the `*_config()`
/// producers reassemble the subsystem configs, and `new()` seeds every field
/// from the subsystem defaults. Set any field directly after `new()`.
#[derive(Debug, Clone)]
pub struct WriterNodeConfig {
    pub hlc_max_clock_skew: Duration,
    pub raft_tracker_min_backoff: Duration,
    pub raft_tracker_max_backoff: Duration,
    pub peers: Vec<PeerAddr>,
    pub keepalive_interval: Duration,
    pub user_timeout: Duration,
    pub keepalive_retries: u32,
    pub max_unary_conns_per_peer: usize,
    pub max_idle_unary_conns_per_peer: usize,
    pub socket_buffer: ByteSize,
    pub unary_timeout: Duration,
    pub connect_timeout: Duration,
    pub pool_channel_capacity: usize,
    /// Receive-buffer depth for a replica-log subscription. Matches the
    /// replica's live-channel depth by default; a smaller value would just
    /// move the bottleneck one hop downstream.
    pub subscribe_channel_capacity: NonZeroUsize,
    pub write_timeout: Duration,
    /// Maximum time to wait for a quorum of replica addresses to be discovered
    /// during [`WriterNode::start`](WriterNode::start). If the deadline
    /// elapses before discovery, start returns an error.
    pub startup_timeout: Duration,
    /// Maximum time to wait for background tasks to drain during shutdown
    /// before forcefully aborting them.
    pub drain_timeout: Duration,
}

impl WriterNodeConfig {
    /// Create a writer config with defaults, customising only the required
    /// fields: the replica `peers` to write to / poll.
    #[must_use]
    pub fn new(peers: Vec<PeerAddr>) -> Self {
        Self {
            hlc_max_clock_skew: Duration::from_millis(200),
            raft_tracker_min_backoff: Duration::from_millis(200),
            raft_tracker_max_backoff: Duration::from_secs(5),
            peers,
            keepalive_interval: Duration::from_secs(5),
            user_timeout: Duration::from_secs(15),
            keepalive_retries: 3,
            max_unary_conns_per_peer: 32,
            max_idle_unary_conns_per_peer: 8,
            socket_buffer: ByteSize::from_mib(2),
            unary_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            pool_channel_capacity: 4096,
            subscribe_channel_capacity: crate::unit::nonzero(1024),
            write_timeout: Duration::from_secs(10),
            startup_timeout: Duration::from_secs(15),
            drain_timeout: Duration::from_secs(30),
        }
    }

    /// Produce the raft-tracker subsystem config.
    #[must_use]
    pub(crate) fn raft_tracker_config(&self) -> RaftTrackerConfig {
        RaftTrackerConfig {
            peers: self.peers_watcher(),
            min_backoff: self.raft_tracker_min_backoff,
            max_backoff: self.raft_tracker_max_backoff,
            startup_timeout: self.startup_timeout,
        }
    }

    /// Produce the outbound router's client config. The TCP + client + write-pool
    /// fields from the flat `network_*` prefix feed directly into the flat
    /// [`RpcClientConfig`].
    #[must_use]
    pub(crate) fn router_config(&self) -> crate::network::RpcClientConfig {
        crate::network::RpcClientConfig {
            tcp_keepalive_interval: self.keepalive_interval,
            #[cfg(target_os = "linux")]
            tcp_user_timeout: self.user_timeout,
            tcp_keepalive_retries: self.keepalive_retries,
            socket_buffer_size: self.socket_buffer,
            unary_timeout: self.unary_timeout,
            connect_timeout: self.connect_timeout,
            max_unary_conns_per_peer: self.max_unary_conns_per_peer,
            max_idle_unary_conns_per_peer: self.max_idle_unary_conns_per_peer,
            pool_channel_capacity: self.pool_channel_capacity,
            subscribe_channel_capacity: self.subscribe_channel_capacity,
        }
    }

    /// Produce the persistent peers watcher from the flat peer list.
    #[must_use]
    pub(crate) fn peers_watcher(&self) -> Arc<dyn crate::api::PeersWatcher> {
        crate::network::static_peers(self.peers.clone())
    }

    /// Produce the writer constructor config.
    #[must_use]
    pub(crate) fn writer_config(&self, node_id: NodeId) -> crate::writer::WriterConfig {
        crate::writer::WriterConfig {
            node_id,
            write_timeout: self.write_timeout,
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.router_config().validate()?;
        if self.peers.is_empty() {
            return Err(Error::InvalidConfiguration(
                "peers must not be empty".to_owned(),
            ));
        }
        // Dummy identity — validation doesn't need a real node_id, just tunings.
        self.writer_config(NodeId::new_v4()).validate()?;
        self.raft_tracker_config().validate()?;
        if self.drain_timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "drain_timeout must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }
}
