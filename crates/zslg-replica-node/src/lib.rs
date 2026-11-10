//! The full node: consensus, replication, reads and writes.
//!
//! `ReplicaNode` is the only node type that carries every role, and the other
//! three are proper subsets of it:
//!
//! | [`ReplicaNode`] | raft voter + replica log + writer + reader |
//! | `FollowerNode` | writer + reader, no raft, no replica log |
//! | `LearnerNode` | reader only |
//! | `WriterNode` | writer only, no durable state |
//!
//! This file is composition, not logic: it validates config (`config.rs`), opens the stores in
//! dependency order, and hands every long-lived task to [`Shutdown`] so that any
//! one of them failing stops the node. Behaviour lives in the modules it wires
//! together.
//!
//! Leader-only work is not started here. Every leader job is spawned
//! unconditionally and gates on leadership itself (`zslg-leader`), so
//! promotion and demotion move no tasks.

// Dev-dependency that exists to switch on its `testing` hooks.
#[cfg(test)]
use {zslg_net_client as _, zslg_net_wire as _, zslg_raft_node as _};

use futures::FutureExt as _;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;
use zslg_consensus::RaftStateView;
pub use zslg_error::Error;
use zslg_lifecycle_types::LifecycleError;
use zslg_snapshot::SnapshotStore;

mod config;
mod replica;

pub use config::ReplicaNodeConfig;

// Depended on only to switch on the `testing` hooks of `RaftNode`.
#[cfg(feature = "testing")]
use zslg_raft_node as _;

use crate::replica::spawn_replica;
use rocksdb_kit::Storage;
use shutdown::Shutdown;
use zslg_consensus::NodeId;
use zslg_consensus::{EvictionStatus, RaftStateHandle};
use zslg_hlc_clock::{HlcClock, run_clock_gauges};
use zslg_hlc_gate::HlcGate;
use zslg_leader::spawn_leader_loop;
use zslg_learner::{LearnerService, spawn_learner, spawn_stable_store};
use zslg_net_setup as network;
use zslg_net_setup::{LocalReplicaClient, PeerRouter, setup_router};
use zslg_object_store::RemoteSnapshotStore;
use zslg_peers::PeersMembershipGate;
use zslg_raft_setup::{RaftNode, run_raft_startup, setup_raft};
use zslg_raft_tracker::NoOpRaftViewer;
use zslg_replica_service::ReplicaService;
use zslg_writer::WriterService;

pub struct ReplicaNode {
    config: ReplicaNodeConfig,
    node_id: NodeId,
    hlc_clock: Arc<HlcClock>,
    raft_state: RaftStateHandle,
    server_addr: std::net::SocketAddr,
    writer: Arc<WriterService>,
    replica: Arc<ReplicaService>,
    learner: Arc<LearnerService>,
    raft: Arc<RaftNode>,
    /// Published by `raft::startup::rejoin::RejoinMonitor`; consulted by
    /// [`Self::can_serve_clients`] and [`Self::reader`]'s [`GatedReader`](zslg_learner::GatedReader).
    /// Cheap to clone (one `Arc<AtomicBool>`) — held here rather than
    /// reconstructed per call.
    eviction: EvictionStatus,
    _self_client: Arc<dyn zslg_net::ReplicaClient>,
    /// Held for shutdown only: its background retire compactions must be joined
    /// before the node's `Storage` handle drops.
    stable_log: Arc<zslg_learner::StableLog>,
    /// The node's one RocksDB. Held so [`run`](Self::run) can join the
    /// background retire compactions before the handle drops — a detached one
    /// keeps the DB, and its LOCK, alive past this node.
    storage: Arc<Storage>,
    shutdown: Shutdown<Error>,
}

impl std::fmt::Debug for ReplicaNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaNode")
            .field("node_id", &self.node_id)
            .field("server_addr", &self.server_addr)
            .finish_non_exhaustive()
    }
}

/// A [`ReplicaNode`] that is running and serving its peers but has not yet
/// caught up to a quorum.
///
/// This is a real state, not a wrapper: during it the node has no learner, no
/// writer and no leader loop, because those are constructed only after the
/// freshness gate. There is therefore nothing to read from and nothing to write
/// to, which is why this type exposes neither `reader()` nor `writer()` — the
/// services do not exist.
///
/// Serving peers while not yet ready is what lets a whole cluster cold-start:
/// every node answers `fetch_status` for the others while none of them is
/// caught up, so a quorum can form.
pub struct InitialisingReplicaNode {
    config: ReplicaNodeConfig,
    node_id: NodeId,
    hlc_clock: Arc<HlcClock>,
    raft_state: RaftStateHandle,
    raft_state_view: Arc<dyn RaftStateView>,
    server_addr: std::net::SocketAddr,
    replica: Arc<ReplicaService>,
    raft: Arc<RaftNode>,
    snapshot_store: Arc<dyn SnapshotStore>,
    stable_log: Arc<zslg_learner::StableLog>,
    storage: Arc<Storage>,
    router: Arc<PeerRouter>,
    has_local_state: bool,
    _self_client: Arc<dyn zslg_net::ReplicaClient>,
    shutdown: Shutdown<Error>,
}

impl std::fmt::Debug for InitialisingReplicaNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitialisingReplicaNode")
            .field("node_id", &self.node_id)
            .field("server_addr", &self.server_addr)
            .finish_non_exhaustive()
    }
}

impl InitialisingReplicaNode {
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The node's raft state view. Watching it during catch-up is exactly what
    /// an operator wants while a cluster is forming.
    #[must_use]
    pub fn state_handle(&self) -> &RaftStateHandle {
        &self.raft_state
    }

    /// Cancel this to stop a node that will never become ready.
    ///
    /// Not optional: the freshness gate retries indefinitely by design, so
    /// without it a node waiting for a quorum that never returns could not be
    /// stopped. Capture it **before** calling [`Self::wait_until_ready`], which
    /// consumes `self`.
    ///
    /// Cancelling makes a pending [`Self::wait_until_ready`] return
    /// `Error::StartupCancelled` promptly — a distinct variant precisely so an
    /// operator-requested stop is not mistaken for a startup fault — after
    /// shutting the node down gracefully. Cancelling it before or after that
    /// call is equally fine; a node that is only ever [`Self::run`] sees the same
    /// token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// True from construction: the server is bound and peers are being served.
    ///
    /// This is the signal an orchestrator's readiness probe should use. It goes
    /// green without needing a quorum, which is what lets a cold start converge
    /// under any pod-management policy.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        true
    }

    /// Run to completion (i.e. until the shutdown token is cancelled).
    ///
    /// The counterpart to [`ReplicaNode::run`] for a node that never became
    /// ready: same drain, same Raft shutdown, so a cold-start abort leaves the
    /// same state behind as a graceful stop of a ready node.
    pub async fn run(self) -> Result<(), Error> {
        let drain_timeout = self.config.drain_timeout;
        let raft = self.raft.clone();
        let result = self.shutdown.run(drain_timeout).await;
        // All background tasks have drained — shut down Raft, as `ReplicaNode::run`
        // does.
        let _ = raft.shutdown().await;
        result
    }

    /// Wait until this node's state is at least as fresh as a responding
    /// quorum, then finish construction.
    ///
    /// Consumes `self`. On failure the node is shut down gracefully here rather
    /// than handed back — there is nothing a caller could do with a node whose
    /// startup failed except stop it.
    ///
    /// Returns `Error::StartupCancelled` if [`Self::shutdown_token`] fires
    /// while the gate is still waiting. That is a clean stop, not a fault: it is
    /// the documented way to abandon a node whose quorum never returns, and it
    /// is a separate variant so a caller can log it as such instead of alerting.
    ///
    /// **Cancel via [`Self::shutdown_token`], not by dropping this future.**
    /// Dropping it drops the node, which aborts its `JoinSet` mid-flight — the
    /// hard-kill path, not a graceful stop.
    pub async fn wait_until_ready(self: Box<Self>) -> Result<ReplicaNode, Error> {
        match self.into_ready().boxed().await {
            Ok(node) => Ok(node),
            Err(boxed) => {
                let (shutdown, drain_timeout, raft, e) = *boxed;
                shutdown.token().cancel();
                // Best-effort: report the startup failure, not a shutdown error
                // that would mask it.
                let _ = shutdown.run(drain_timeout).await;
                let _ = raft.shutdown().await;
                Err(e)
            }
        }
    }

    /// The second half of construction. The error arm carries the pieces
    /// [`Self::wait_until_ready`] needs to stop the node it can no longer
    /// return; it is internal only and deliberately not exposed.
    #[allow(
        clippy::type_complexity,
        reason = "the tuple is boxed for clippy::result_large_err, not exposed"
    )]
    async fn into_ready(
        self,
    ) -> Result<ReplicaNode, Box<(Shutdown<Error>, Duration, Arc<RaftNode>, Error)>> {
        let Self {
            config,
            node_id,
            hlc_clock,
            raft_state,
            raft_state_view,
            server_addr,
            replica,
            raft,
            snapshot_store,
            stable_log,
            storage,
            router,
            has_local_state,
            _self_client,
            mut shutdown,
        } = self;
        let drain_timeout = config.drain_timeout;

        // `?` would drop `shutdown` on the error path, aborting every task where
        // it stands; each step therefore hands the shutdown handle back via an
        // explicit match instead of `?`.
        let startup_config = config.raft_startup_config(
            node_id,
            has_local_state,
            config.peers_watcher(),
            server_addr,
        );
        // The freshness gate retries indefinitely by design: every phase inside
        // `run_raft_startup` (fence read, join, quorum generation, catch-up) loops
        // on transient failure and only escalates its logging. That is right for
        // an unattended cold start and wrong for an operator who wants the node
        // to stop, so cancellation is imposed *here*, at the one place that owns
        // the token, rather than threaded through every retry loop in
        // `raft::startup::bootstrap`.
        //
        // Dropping the startup future at an await point is the intended effect —
        // the node is being stopped, and `wait_until_ready` shuts it down (drain
        // + `raft.shutdown()`) on this error exactly as on any other.
        let cancelled = shutdown.token();
        let startup = tokio::select! {
            res = run_raft_startup(
                raft.clone(),
                raft_state_view.clone(),
                snapshot_store.clone(),
                router.clone(),
                startup_config,
                &mut shutdown,
            ).boxed() => res.map_err(Error::from),
            () = cancelled.cancelled() => {
                info!("raft startup cancelled — node stopped before it became ready");
                Err(Error::Lifecycle(LifecycleError::StartupCancelled))
            }
        };
        let eviction = match startup {
            Ok(v) => v,
            Err(e) => return Err(Box::new((shutdown, drain_timeout, raft, e))),
        };
        info!("raft startup complete");

        let learner = match spawn_learner(
            hlc_clock.clone(),
            stable_log.clone(),
            storage.clone(),
            snapshot_store.clone(),
            raft_state_view.clone(),
            router.clone(),
            &config.learner_config(),
            &mut shutdown,
        )
        .boxed()
        .await
        {
            Ok(v) => v,
            Err(e) => return Err(Box::new((shutdown, drain_timeout, raft, e.into()))),
        };
        info!("learner spawned");

        spawn_leader_loop(
            &mut shutdown,
            hlc_clock.clone(),
            learner.local_store(),
            stable_log.clone(),
            snapshot_store.clone(),
            raft.clone(),
            raft.clone(),
            raft_state_view.clone(),
            router.clone(),
            config.leader_loop_config(node_id),
        );
        info!("leader loop spawned");

        let writer = Arc::new(WriterService::new(
            router.clone(),
            Arc::new(NoOpRaftViewer::new(raft_state_view.clone())),
            hlc_clock.clone(),
            config.writer_config(node_id),
        ));

        Ok(ReplicaNode {
            config,
            node_id,
            hlc_clock,
            raft_state,
            server_addr,
            writer,
            replica,
            learner,
            raft,
            eviction,
            _self_client,
            stable_log,
            storage,
            shutdown,
        })
    }
}

impl ReplicaNode {
    /// Start the node's local services and bind its RPC server, returning as
    /// soon as it is serving its peers.
    ///
    /// It deliberately does **not** wait for the node to catch up to a quorum:
    /// that is [`InitialisingReplicaNode::wait_until_ready`]. Splitting the two
    /// is what makes a whole-cluster cold start expressible — every node can be
    /// started (and so answer its peers) before any of them is awaited.
    pub async fn start(config: ReplicaNodeConfig) -> Result<Box<InitialisingReplicaNode>, Error> {
        config.validate()?;

        let mut shutdown = Shutdown::new();
        rocksdb_kit::create_storage_root(&config.db_path)
            .boxed()
            .await?;

        // The node's ONE RocksDB: one root, one block cache, one column family
        // per store. The budget is an argument rather than a process-wide
        // static because it is a *node's* property — the loadgen runs six nodes
        // in one process.
        let storage = Storage::open(
            &config.db_path,
            config.storage_config(),
            &config.column_families(),
        )?;

        let node_id = zslg_node_id::load_or_create_node_id(&storage)?;
        info!(node_id = ?node_id, "node_id loaded or created");

        let snapshot_store: Arc<dyn SnapshotStore> = Arc::new(RemoteSnapshotStore::new(
            config.snapshot_store_config(),
            shutdown.token(),
        ));

        let hlc_clock = Arc::new(HlcClock::new(config.hlc_max_clock_skew));
        shutdown.spawn(
            "clock_gauges",
            run_clock_gauges(hlc_clock.clone(), shutdown.token()),
        );
        let hlc_gate = Arc::new(HlcGate::new());

        let router = setup_router(hlc_clock.clone(), config.router_config(), shutdown.token())?;

        let raft_state = RaftStateHandle::new();
        let raft_state_view: Arc<dyn RaftStateView> = Arc::new(raft_state.clone());
        let membership_gate = Arc::new(PeersMembershipGate::new(config.peers_watcher()));
        // Spawned here rather than at the top of `start`: the storage-memory
        // gauge reads this node's `Storage`, so the tick has to know it.
        shutdown.spawn(
            "process_gauges",
            zslg_observability::run_process_gauges(
                Some(storage.clone()),
                // A replica node runs both pipelines: the replica-side write
                // queue and live-tail rings, and the learner-side integrate
                // channel and dedup ring. saturating: two independently-set
                // operator knobs, and an overflowed sum must read as huge.
                Some(
                    config
                        .replica_log_config(node_id)
                        .queue_capacity_bytes()
                        .saturating_add(config.learner_config().queue_capacity_bytes()),
                ),
                shutdown.token(),
            ),
        );
        let (raft, has_local_state) = setup_raft(
            router.clone(),
            raft_state.clone(),
            storage.clone(),
            &config.raft_config(node_id),
            membership_gate,
            &mut shutdown,
        )
        .boxed()
        .await?;
        info!(?has_local_state, "raft setup complete");

        let stable_log = spawn_stable_store(&config.stable_log_config(), storage.clone())
            .boxed()
            .await?;
        info!("stable log initialized");

        let replica = spawn_replica(
            raft_state_view.clone(),
            &hlc_clock,
            &hlc_gate,
            storage.clone(),
            &config.replica_log_config(node_id),
            &mut shutdown,
            config.peers_watcher(),
        )
        .boxed()
        .await?;
        info!("replica node spawned");

        // Self/loopback client: created first so it can be shared with bind_server
        // and PeerRouter. The manager holds a `Weak` to it, so it must be
        // kept alive for the node's lifetime (stored in `ReplicaNode`).
        let self_client: Arc<dyn zslg_net::ReplicaClient> = Arc::new(LocalReplicaClient::new(
            replica.clone(),
            raft.clone(),
            raft_state_view.clone(),
        ));

        let server_addr = network::bind_server(
            self_client.clone(),
            hlc_clock.clone(),
            config.rpc_server_config(node_id),
            &mut shutdown,
        )?;
        info!(addr = %server_addr, "server bound");

        let router = Arc::new(PeerRouter::new(
            node_id,
            Arc::downgrade(&self_client),
            router,
        ));

        Ok(Box::new(InitialisingReplicaNode {
            config,
            node_id,
            hlc_clock,
            raft_state,
            raft_state_view,
            server_addr,
            replica,
            raft,
            snapshot_store,
            stable_log,
            storage,
            router,
            has_local_state,
            _self_client: self_client,
            shutdown,
        }))
    }

    /// True unless a reachable peer has affirmatively denied this node's
    /// rejoin attempt (removed it from `config.peers`) — i.e. whether client
    /// reads will succeed. Distinct from `is_serving`.
    ///
    /// NOT the openraft voter set: under a `retain: false` eviction (what
    /// production eviction actually performs, `leader::jobs::voters`) the
    /// leader stops replicating to this node entirely, so its own view of
    /// `voters` would freeze and never reflect the removal — the same reason
    /// the gate is "has a peer told us we're out", not "do we still see
    /// ourselves as in". See `raft::startup::rejoin::EvictionStatus`.
    #[must_use]
    pub fn can_serve_clients(&self) -> bool {
        zslg_learner::can_serve_clients(&self.eviction)
    }

    /// Remove `victim` from this node's openraft voter set. Test-only:
    /// exercises group removal (CLAUDE.md: membership is the group's
    /// decision) without standing up a full `leader:voters` reconciliation
    /// cycle.
    ///
    /// Demotes to learner (`retain: true`) rather than dropping the node
    /// outright (production's `RaftLeadership::remove_member` uses
    /// `retain: false`, matching `leader::jobs::voters`'s permanent eviction).
    /// The distinction matters for *this test's observability*, not for
    /// production correctness: `retain: false` removes the node from
    /// openraft's membership map entirely, so the leader stops replicating to
    /// it and the victim's own view of `voters` can legitimately lag forever
    /// (`eviction` hits exactly this and asserts on the
    /// *remaining* nodes instead). `retain: true` keeps it as a learner, which
    /// still receives `AppendEntries` — so its own metrics promptly reflect
    /// the demotion, which is what this test needs to assert on.
    #[cfg(any(test, feature = "testing"))]
    pub async fn remove_member_for_test(&self, victim: NodeId) -> Result<(), Error> {
        self.raft
            .demote_to_learner_for_test(victim)
            .await
            .map_err(Error::from)
    }

    /// True from construction: a `ReplicaNode` only exists after its server is
    /// bound. The mirror of [`InitialisingReplicaNode::is_serving`], so a
    /// readiness probe can be written once against either state.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        true
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn writer(&self) -> Arc<dyn zslg_log::Writer> {
        self.writer.clone()
    }

    /// Reads are gated on voter-set membership (see [`Self::can_serve_clients`]):
    /// a node removed from the group stays up but must not serve a snapshot of
    /// replicated state that has stopped advancing. Writes are not gated here —
    /// `Writer::write` only coordinates a fan-out over the router and writes
    /// nothing locally, so this node's own membership does not bear on it (see
    /// `writer.rs`).
    #[must_use]
    pub fn reader(&self) -> Arc<dyn zslg_log::Reader> {
        Arc::new(zslg_learner::GatedReader::new(
            self.learner.reader(),
            self.eviction.clone(),
            self.node_id,
        ))
    }

    /// Returns a clone of the global [`CancellationToken`] that all background
    /// tasks check. Cancelling it triggers a graceful shutdown of every
    /// subsystem. Useful for wiring the node into an external signal handler
    /// (e.g. SIGTERM) when using [`ReplicaNode::run`].
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// Run the node until shutdown is requested or a background task fails.
    ///
    /// On the first unexpected task exit (error, panic, or clean completion
    /// without prior cancellation), cancels all remaining tasks, drains them
    /// with [`ReplicaNodeConfig::drain_timeout`], shuts down Raft, and returns
    /// the error.
    ///
    /// Returns `Ok(())` when the [`shutdown_token`](Self::shutdown_token) was
    /// cancelled externally **before** the first task exited — i.e. the
    /// embedder requested a graceful shutdown.
    ///
    /// Consumes `self`. Clone any needed handles ([`writer`](Self::writer),
    /// [`reader`](Self::reader), etc.) **before** calling this method.
    pub async fn run(self) -> Result<(), Error> {
        let drain_timeout = self.config.drain_timeout;
        let Self {
            config,
            node_id,
            hlc_clock,
            raft_state,
            server_addr,
            writer,
            replica,
            learner,
            raft,
            eviction,
            _self_client,
            stable_log,
            storage,
            shutdown,
        } = self;

        // Wait for first unexpected task exit, cancel + drain the rest.
        let result = shutdown.run(drain_timeout).await;

        // All background tasks have drained — shut down Raft.
        let _ = raft.shutdown().await;

        drop((
            config,
            node_id,
            hlc_clock,
            raft_state,
            server_addr,
            writer,
            replica,
            learner,
            eviction,
            _self_client,
            stable_log,
            storage,
        ));

        result
    }

    #[must_use]
    #[cfg(any(test, feature = "testing"))]
    pub fn replica(&self) -> Arc<dyn zslg_log::Replica> {
        self.replica.clone()
    }

    #[must_use]
    #[cfg(any(test, feature = "testing"))]
    pub fn server_addr(&self) -> std::net::SocketAddr {
        self.server_addr
    }

    /// This node's committed-and-derived Raft state view.
    ///
    /// Read the current state with [`RaftStateHandle::snapshot`] and follow
    /// changes with [`RaftStateHandle::subscribe`]; both are also available
    /// through the [`RaftStateView`] trait.
    ///
    /// Note which fields are replicated and which are local: `generation`,
    /// `replica` and `manifest_state` are the state machine's applied output
    /// and agree across nodes at the same generation, while `leader`,
    /// `raft_term` and `voters` are blended from *this* node's openraft
    /// metrics. In particular `voters` (Raft's membership) and `replica` (the
    /// set that serves writes) are different sets, and a membership job's
    /// whole task is moving one toward the other.
    ///
    /// This doc example is also the crate's check that the surface really is
    /// reachable from outside: a doc test compiles as a separate crate, so it
    /// fails if any of these items is `pub`.
    ///
    /// ```no_run
    /// use zslg_consensus::{RaftStateView, ReplicaMembership};
    /// use zslg_replica_node::ReplicaNode;
    ///
    /// fn inspect(node: &ReplicaNode) {
    ///     // Inherent methods, no trait import needed…
    ///     let state = node.state_handle().snapshot();
    ///     println!(
    ///         "leader={:?} term={} generation={} voters={:?}",
    ///         state.leader,
    ///         state.raft_term,
    ///         state.generation.raw(),
    ///         state.voters,
    ///     );
    ///     if let ReplicaMembership::Joint { old, next } = &state.replica {
    ///         println!("membership change in flight: {old:?} -> {next:?}");
    ///     }
    ///     for snapshot in &state.manifest_state.manifest {
    ///         println!("{:?} covers {:?}", snapshot.snapshot_id, snapshot.end_hlc);
    ///     }
    ///
    ///     // …and through the trait, which is what a generic consumer takes.
    ///     let _rx = RaftStateView::subscribe(node.state_handle());
    /// }
    /// ```
    #[must_use]
    pub fn state_handle(&self) -> &RaftStateHandle {
        &self.raft_state
    }
}

#[cfg(test)]
mod node_tests;
#[cfg(test)]
mod replica_tests;
