//! Replica cluster test harness.
//!
//! Provides [`Cluster`] — a builder-pattern helper that starts interconnected
//! [`ReplicaNode`]s over real plain-TCP connections, with shared state (object
//! store), and lifecycle methods (`launch`, `remove`, `replace`, `shutdown`).
//!
//! # Two modes
//!
//! | `distinct_names: false` | All nodes share the `"localhost"` server name |
//! | `distinct_names: true`  | Nodes get distinct `"node-0"`, `"node-1"`, … server names |
//!
//! When `distinct_names` is true, node 0 bootstraps the cluster; others join
//! via the shared object-store fence.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments,
    clippy::result_large_err,
    reason = "test code"
)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;

use crate::error::Error;
use crate::ids::{NodeId, PeerAddr, ServerName};
use crate::replica_node::{ReplicaNode, ReplicaNodeConfig};

use super::reserve_port;

const BOOTSTRAP_NAME: &str = "node-0";
const LOCALHOST: &str = "localhost";

// ── ClusterConfig ─────────────────────────────────────────────────────

pub(crate) struct ClusterConfig {
    /// Number of nodes to start initially.
    pub(crate) count: usize,
    /// If true, nodes get distinct server names ("node-0", "node-1", …).
    /// Node 0 bootstraps; the rest join. If false, all nodes share "localhost".
    pub(crate) distinct_names: bool,
    /// Shared object store (fence must be visible to joiners).
    pub(crate) object_store: Arc<dyn ObjectStore>,
    /// Optional base config template. If `None`, fast test defaults are used
    /// (250ms heartbeat, 1-2s election, 500ms snapshot).
    pub(crate) base_config: Option<ReplicaNodeConfig>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            count: 3,
            distinct_names: false,
            object_store: Arc::new(InMemory::new()),
            base_config: None,
        }
    }
}

// ── NodePlacement ─────────────────────────────────────────────────────

/// Records how a node was placed so `replace`/`restart_all_staggered` can
/// recreate it on the same address AND db-path (the latter is what lets a
/// restart resume the node's persisted Raft state).
#[derive(Clone)]
pub(crate) struct NodePlacement {
    pub(crate) server_name: ServerName,
    pub(crate) listen_addr: SocketAddr,
    pub(crate) db_path: PathBuf,
}

// ── Cluster ───────────────────────────────────────────────────────────

pub(crate) struct Cluster {
    nodes: BTreeMap<NodeId, ReplicaNode>,
    placements: BTreeMap<NodeId, NodePlacement>,
    tempdirs: Vec<TempDir>,
    config: ClusterConfig,
    next_node_index: usize,
}

impl Cluster {
    /// Start a new cluster according to `config`.
    pub(crate) async fn new(config: ClusterConfig) -> Result<Self, Error> {
        let mut cluster = Self {
            nodes: BTreeMap::new(),
            placements: BTreeMap::new(),
            tempdirs: Vec::new(),
            next_node_index: 0,
            config,
        };
        cluster.launch(cluster.config.count).await?;
        Ok(cluster)
    }

    // ── lifecycle ─────────────────────────────────────────────────

    /// Add `count` new nodes with auto-assigned ports, server names, and
    /// tempdirs. Returns the new [`NodeId`]s in launch order.
    ///
    /// All ports and server names are pre-computed so every node starts
    /// with the full peer list — necessary for `static_peers` membership
    /// gating on the bootstrap leader.
    pub(crate) async fn launch(&mut self, count: usize) -> Result<Vec<NodeId>, Error> {
        // Pre-reserve all addresses and server names so every node sees
        // the complete peer list from the start.
        let mut specs: Vec<(ServerName, SocketAddr)> = Vec::new();
        for i in 0..count {
            let idx = self.next_node_index.saturating_add(i);
            let srv = self.server_name_for(idx);
            let addr = reserve_port();
            specs.push((srv, addr));
        }

        let full_peers: Vec<PeerAddr> = self
            .placements
            .values()
            .map(|p| PeerAddr {
                addr: p.listen_addr.into(),
                server_name: p.server_name.clone(),
            })
            .chain(specs.iter().map(|(srv, addr)| PeerAddr {
                addr: (*addr).into(),
                server_name: srv.clone(),
            }))
            .collect();

        let mut launched = Vec::new();
        for (srv, addr) in specs {
            let node_id = self
                .launch_one(Some(srv), Some(addr), &full_peers, None)
                .await?;
            launched.push(node_id);
        }
        Ok(launched)
    }

    /// Relaunch a previously-[`remove`]d node at its ORIGINAL server name,
    /// address, AND db-path, so it resumes its persisted Raft state (the
    /// `Rejoin` route) into the still-running cluster. Used to test that a node
    /// that was merely down (never removed from `config.peers`) rejoins on its
    /// own when it comes back.
    pub(crate) async fn relaunch(&mut self, placement: NodePlacement) -> Result<NodeId, Error> {
        let full_peers: Vec<PeerAddr> = self
            .placements
            .values()
            .map(|p| PeerAddr {
                addr: p.listen_addr.into(),
                server_name: p.server_name.clone(),
            })
            .chain(std::iter::once(PeerAddr {
                addr: placement.listen_addr.into(),
                server_name: placement.server_name.clone(),
            }))
            .collect();
        self.launch_one(
            Some(placement.server_name),
            Some(placement.listen_addr),
            &full_peers,
            Some(placement.db_path),
        )
        .await
    }

    /// Relaunch a node at its ORIGINAL server name, address, AND db-path (the
    /// `Rejoin` route), but with a custom `config.peers`.
    ///
    /// The leader's `config.peers` is the eviction job's authority
    /// (`leader:voters`), so a node whose server name is absent from the
    /// leader's peer list is evicted from the openraft voter set after
    /// `forced_node_removal_timeout` — which the reconciliation in
    /// `leader::jobs::membership::change::maybe_initiate` then turns into a
    /// replica-set shrink.
    ///
    /// **The node must already be stopped.** This binds
    /// `placement.listen_addr`, so callers pass a placement returned by
    /// [`kill`](Self::kill) or [`remove`](Self::remove) — both wait for the
    /// port to free. Calling it for a node that is still running fails to bind.
    pub(crate) async fn relaunch_with_peers(
        &mut self,
        placement: NodePlacement,
        peers: &[PeerAddr],
    ) -> Result<NodeId, Error> {
        self.launch_one(
            Some(placement.server_name),
            Some(placement.listen_addr),
            peers,
            Some(placement.db_path),
        )
        .await
    }

    /// Relaunch several killed nodes, every one of them carrying the full peer
    /// list — including the peers being relaunched alongside it — and **all of
    /// them serving before any of them is awaited for readiness**.
    ///
    /// Both properties are required, and [`relaunch`](Self::relaunch) has
    /// neither, because it was built for the one-node-down case where the rest
    /// of the replica set is still live.
    ///
    /// *Start-all-then-await*, because the freshness gate fans out to a quorum
    /// of the replica set the node **persisted** — which, when the whole set was
    /// killed, is exactly the set being relaunched. Splitting
    /// `ReplicaNode::start` from `wait_until_ready` removed the `tokio::spawn`
    /// this used to need, but not the helper: a loop of `relaunch` (start *and*
    /// wait, per node) still deadlocks, because the first node's gate cannot
    /// close until its siblings are at least serving, and they are not launched
    /// until it returns. Measured: such a loop times out at 120 s with the two
    /// unlaunched siblings logged as unreachable.
    ///
    /// *Full peer list*, because `relaunch` derives peers from
    /// `self.placements`, and `kill` removes a killed node's placement — so
    /// with three nodes down each would come back knowing five of seven
    /// addresses, missing precisely the siblings whose quorum it is waiting
    /// for.
    pub(crate) async fn relaunch_many(
        &mut self,
        placements: Vec<NodePlacement>,
    ) -> Result<Vec<NodeId>, Error> {
        let full_peers: Vec<PeerAddr> = self
            .placements
            .values()
            .chain(placements.iter())
            .map(|p| PeerAddr {
                addr: p.listen_addr.into(),
                server_name: p.server_name.clone(),
            })
            .collect();

        // Phase 1: bind every node. Each is serving its peers when `start`
        // returns, so by the end of the loop the quorum they need exists.
        let mut initialising = Vec::new();
        for placement in placements {
            eprintln!(
                "  [cluster] relaunching node ({} at {}) with {} peers (serving first)",
                placement.server_name,
                placement.listen_addr,
                full_peers.len()
            );
            let cfg = self.build_config(
                &placement.server_name,
                placement.listen_addr,
                placement.db_path.clone(),
                &full_peers,
            );
            let node = tokio::time::timeout(Duration::from_secs(120), ReplicaNode::start(cfg))
                .await
                .expect("ReplicaNode::start timed out")?;
            initialising.push((placement, node));
        }

        // Phase 2: only now await readiness, concurrently — they satisfy each
        // other's quorum.
        let started =
            futures::future::try_join_all(initialising.into_iter().map(|(p, node)| async move {
                let node = tokio::time::timeout(Duration::from_secs(120), node.wait_until_ready())
                    .await
                    .expect("wait_until_ready timed out")?;
                Ok::<_, Error>((p, node))
            }))
            .await?;

        let mut ids = Vec::new();
        for (placement, node) in started {
            let id = node.node_id();
            self.nodes.insert(id, node);
            self.placements.insert(id, placement);
            ids.push(id);
        }
        Ok(ids)
    }

    /// Replace the base config applied to *subsequently* launched nodes.
    ///
    /// Nodes already running keep the config they started with, which is the
    /// point: this is how a test gives one node different timings from the
    /// rest (see `tests::reconciliation`, which relaunches every node but one
    /// with a faster election timeout so the odd one out never wins a vote).
    pub(crate) fn set_base_config(&mut self, cfg: ReplicaNodeConfig) {
        self.config.base_config = Some(cfg);
    }

    /// The placement of a running node (panics if unknown).
    pub(crate) fn placement_of(&self, node_id: NodeId) -> NodePlacement {
        self.placements
            .get(&node_id)
            .expect("node not in cluster")
            .clone()
    }

    /// Add one node at a specific server name and listen address.
    /// Used after [`remove`] to place a replacement on the same address.
    pub(crate) async fn launch_with_addr(
        &mut self,
        server_name: ServerName,
        addr: SocketAddr,
    ) -> Result<NodeId, Error> {
        let full_peers: Vec<PeerAddr> = self
            .placements
            .values()
            .map(|p| PeerAddr {
                addr: p.listen_addr.into(),
                server_name: p.server_name.clone(),
            })
            .chain(std::iter::once(PeerAddr {
                addr: addr.into(),
                server_name: server_name.clone(),
            }))
            .collect();
        self.launch_one(Some(server_name), Some(addr), &full_peers, None)
            .await
    }

    /// Gracefully shut down `node_id`, wait for its port to be released,
    /// and return its placement (so the caller can reuse the address).
    pub(crate) async fn remove(&mut self, node_id: NodeId) -> Result<NodePlacement, Error> {
        let placement = self
            .placements
            .remove(&node_id)
            .expect("node not in cluster");
        let node = self.nodes.remove(&node_id).expect("node not in cluster");
        eprintln!(
            "  [cluster] removing node {node_id:?} at {}",
            placement.listen_addr
        );
        node.shutdown_token().cancel();
        node.run().await?;

        // Wait for the TCP port to be released (the RPC server binds TCP).
        let addr = placement.listen_addr;
        super::wait_until_sync("port free", Duration::from_secs(15), || {
            std::net::TcpListener::bind(addr).is_ok()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        eprintln!("  [cluster] port {addr} released");

        Ok(placement)
    }

    /// Drop a node without draining it: no cancellation, no `run()`, so its
    /// `JoinSet` is dropped and its tasks are aborted where they stand. The
    /// closest analogue to a crash available at this tier, and the only way to
    /// leave un-fsynced state behind — `remove` flushes everything, so the
    /// state that makes crash recovery interesting never exists after it.
    ///
    /// Like [`remove`](Self::remove) it waits, bounded, for the OS to release
    /// the listen port before returning, so the placement it hands back is one
    /// [`relaunch`](Self::relaunch) can immediately bind. That wait flushes
    /// nothing: the un-fsynced state the kill left behind stays exactly as it
    /// is, and every caller needs it, so it belongs here rather than duplicated
    /// at each call site.
    pub(crate) async fn kill(&mut self, node_id: NodeId) -> NodePlacement {
        let placement = self
            .placements
            .remove(&node_id)
            .expect("node not in cluster");
        let node = self.nodes.remove(&node_id).expect("node not in cluster");
        eprintln!(
            "  [cluster] killing node {node_id:?} at {}",
            placement.listen_addr
        );
        // Signal the background tasks to stop before dropping the node. This is
        // not a graceful drain (no `run().await`) — the `JoinSet` is still
        // dropped, which aborts tasks where they stand. But the cancellation
        // signal lets tasks that are already at an await point exit and drop
        // their `Arc<Storage>` clones before `relaunch` reopens the same DB
        // path. Without this, tokio's asynchronous task abort can leave the
        // RocksDB LOCK held past `drop(node)`, and the relaunch fails with
        // "lock hold by current process". In a real crash the OS releases the
        // lock; in-process, this cancellation is the closest analogue.
        node.shutdown_token().cancel();
        drop(node);

        let addr = placement.listen_addr;
        super::wait_until_sync(
            "killed node's port frees",
            Duration::from_secs(15),
            move || std::net::TcpListener::bind(addr).is_ok(),
        )
        .await;
        // Yield to let the runtime finish dropping the aborted tasks' futures
        // — their `Arc<Storage>` clones are released here, not at `drop(node)`.
        tokio::task::yield_now().await;
        placement
    }

    /// Extract `node_id` from the cluster's supervision, **without**
    /// cancelling or draining it: unlike [`remove`](Self::remove), the node's
    /// background tasks (already running — `shutdown.spawn` fires at
    /// construction, not at `run()`) are left exactly as they are.
    ///
    /// For a caller that needs to observe the node's OWN supervision tree —
    /// i.e. whether its `Shutdown::run()` eventually reacts to a background
    /// task's fatal exit — `Cluster` cannot answer that: `node_ids()` is a
    /// static map untouched by a node's internal task failures, and nothing
    /// in `Cluster` ever calls a kept node's `run()` (only `remove`/`kill`/
    /// `shutdown` do, and only as teardown). This hands the node back so the
    /// caller can spawn `node.run()` itself and hold the `JoinHandle` — same
    /// shape as `tests::full_restart::cancelling_the_shutdown_token_stops_a_node_that_never_becomes_ready`.
    pub(crate) fn take_running(&mut self, node_id: NodeId) -> ReplicaNode {
        self.placements
            .remove(&node_id)
            .expect("node not in cluster");
        self.nodes.remove(&node_id).expect("node not in cluster")
    }

    /// Convenience: remove `node_id`, then launch a fresh node on the same
    /// address. Returns the new node's ID.
    pub(crate) async fn replace(&mut self, node_id: NodeId) -> Result<NodeId, Error> {
        let placement = self.remove(node_id).await?;
        self.launch_with_addr(placement.server_name, placement.listen_addr)
            .await
    }

    /// Shut down all nodes.
    pub(crate) async fn shutdown(self) -> Result<(), Error> {
        for (_, node) in self.nodes {
            node.shutdown_token().cancel();
            node.run().await?;
        }
        Ok(())
    }

    /// Remove `victim` from the openraft voter set via `leader` (demotes to
    /// learner — see `ReplicaNode::remove_member_for_test`). Test-only:
    /// production reaches voter removal through `leader:voters`, which drops
    /// the node outright rather than demoting it.
    pub(crate) async fn remove_voter(&self, leader: NodeId, victim: NodeId) -> Result<(), Error> {
        self.node(leader).remove_member_for_test(victim).await
    }

    // ── accessors ─────────────────────────────────────────────────

    /// Look up a node by ID (panics if not found).
    pub(crate) fn node(&self, id: NodeId) -> &ReplicaNode {
        self.nodes.get(&id).expect("node not in cluster")
    }

    /// All active node IDs.
    pub(crate) fn node_ids(&self) -> BTreeSet<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Iterate over all nodes.
    pub(crate) fn nodes(&self) -> impl Iterator<Item = &ReplicaNode> {
        self.nodes.values()
    }

    /// Peer addresses for all nodes (for WriterNode / external-client config).
    pub(crate) fn peer_addrs(&self) -> Vec<PeerAddr> {
        self.placements
            .values()
            .map(|p| PeerAddr {
                addr: p.listen_addr.into(),
                server_name: p.server_name.clone(),
            })
            .collect()
    }

    // ── wait helpers ──────────────────────────────────────────────

    /// Poll until any node sees `leader.is_some()`.
    pub(crate) async fn wait_for_leader(&self, deadline: Duration) {
        super::wait_until_sync("cluster has leader", deadline, || {
            self.nodes
                .values()
                .any(|n| n.state_handle().snapshot().leader.is_some())
        })
        .await;
    }

    /// Poll until all active nodes observe a common non-joint replica set
    /// containing exactly `expected` IDs.
    pub(crate) async fn wait_for_convergence(&self, deadline: Duration) {
        super::wait_until_sync("replica set converges", deadline, || self.all_converged()).await;
    }

    fn all_converged(&self) -> bool {
        if self.nodes.is_empty() {
            return false;
        }
        let mut replica_sets = self.nodes.values().map(|n| {
            let s = n.state_handle().snapshot();
            let mut set = s.replica.current().clone();
            if let Some(next) = &s.replica.next() {
                set.extend(next.iter().copied());
            }
            (set, s.replica.is_joint())
        });
        let Some((first, is_joint)) = replica_sets.next() else {
            return false;
        };
        if is_joint {
            return false;
        }
        // Require at least 2 members (not just self-bootstrap)
        if first.len() < 2 && self.nodes.len() >= 2 {
            return false;
        }
        replica_sets.all(|(s, j)| s == first && !j)
    }

    // ── internals ─────────────────────────────────────────────────

    async fn launch_one(
        &mut self,
        server_name: Option<ServerName>,
        listen_addr: Option<SocketAddr>,
        full_peers: &[PeerAddr],
        reuse_db_path: Option<PathBuf>,
    ) -> Result<NodeId, Error> {
        let _idx = self.next_node_index;
        self.next_node_index = self.next_node_index.saturating_add(1);

        let (srv, addr) = match (server_name, listen_addr) {
            (Some(name), Some(addr)) => (name, addr),
            _ => unreachable!("launch_one always receives both name and addr"),
        };

        // `Some` reuses a prior node's db-path (a restart that resumes persisted
        // Raft state → Rejoin); `None` mints a fresh tempdir (a brand-new node).
        let db_path = match reuse_db_path {
            Some(path) => path,
            None => {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("node.db");
                self.tempdirs.push(dir);
                path
            }
        };
        let cfg = self.build_config(&srv, addr, db_path.clone(), full_peers);

        eprintln!(
            "  [cluster] launching node ({} at {}) with {} peers",
            srv,
            addr,
            full_peers.len()
        );
        // The timeout covers the *pair*: binding plus catching up to a quorum is
        // what a caller of `launch_one` is waiting for, and either half hanging
        // should surface here rather than block the test forever.
        let node = tokio::time::timeout(Duration::from_secs(120), async {
            ReplicaNode::start(cfg).await?.wait_until_ready().await
        })
        .await
        .expect("ReplicaNode::start timed out")?;

        let node_id = node.node_id();
        self.nodes.insert(node_id, node);
        self.placements.insert(
            node_id,
            NodePlacement {
                server_name: srv,
                listen_addr: addr,
                db_path,
            },
        );
        Ok(node_id)
    }

    /// Gracefully stop every node (keeping placements + tempdirs + object store),
    /// wait for their TCP ports to free, then return the rebuilt configs for a
    /// relaunch that reuses each node's address AND db-path (so each resumes its
    /// persisted Raft state — the `Rejoin` route).
    async fn stop_all_and_rebuild_configs(&mut self) -> Result<Vec<ReplicaNodeConfig>, Error> {
        let nodes = std::mem::take(&mut self.nodes);
        let addrs: Vec<SocketAddr> = self.placements.values().map(|p| p.listen_addr).collect();
        for (_, node) in nodes {
            node.shutdown_token().cancel();
            node.run().await?;
        }
        for addr in &addrs {
            let addr = *addr;
            super::wait_until_sync("port free", Duration::from_secs(15), move || {
                std::net::TcpListener::bind(addr).is_ok()
            })
            .await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        let placements: Vec<NodePlacement> = self.placements.values().cloned().collect();
        let full_peers: Vec<PeerAddr> = placements
            .iter()
            .map(|p| PeerAddr {
                addr: p.listen_addr.into(),
                server_name: p.server_name.clone(),
            })
            .collect();
        Ok(placements
            .iter()
            .map(|p| {
                self.build_config(
                    &p.server_name,
                    p.listen_addr,
                    p.db_path.clone(),
                    &full_peers,
                )
            })
            .collect())
    }

    /// Spawn one node's `start()` as a task, bounded (60s) only so an unexpected
    /// hang surfaces as a clear error rather than blocking the test forever — the
    /// production startup imposes no such bound.
    fn spawn_start(cfg: ReplicaNodeConfig) -> tokio::task::JoinHandle<Result<ReplicaNode, Error>> {
        tokio::spawn(async move {
            let start = async { ReplicaNode::start(cfg).await?.wait_until_ready().await };
            match tokio::time::timeout(Duration::from_secs(60), start).await {
                Ok(res) => res,
                Err(_) => Err(Error::Raft(
                    "restart: node start timed out (60s)".to_owned(),
                )),
            }
        })
    }

    /// Await every relaunched node's `start()` and re-register it (same NodeId,
    /// since the db-path was preserved).
    async fn await_and_register(
        &mut self,
        handles: Vec<tokio::task::JoinHandle<Result<ReplicaNode, Error>>>,
    ) -> Result<(), Error> {
        for h in handles {
            let node = h.await.expect("restart start task panicked")?;
            self.nodes.insert(node.node_id(), node);
        }
        Ok(())
    }

    /// Full-cluster outage + SIMULTANEOUS restart: stop every node, then bring
    /// them ALL back at once. Every node resumes its persisted Raft state
    /// (Rejoin) and they must find each other and recover a leader with no
    /// operator action. Concurrent because a Rejoin node's `start()` blocks until
    /// it can reach a quorum, so a sequential relaunch would deadlock.
    pub(crate) async fn restart_all(&mut self) -> Result<(), Error> {
        let configs = self.stop_all_and_rebuild_configs().await?;
        let handles: Vec<tokio::task::JoinHandle<Result<ReplicaNode, Error>>> =
            configs.into_iter().map(Self::spawn_start).collect();
        self.await_and_register(handles).await
    }

    /// Full-cluster outage + STAGGERED restart: the first node is brought up
    /// alone, then after `stagger` (during which it cannot reach a quorum) the
    /// rest follow. With a small `startup_phase_timeout` this proves a resuming
    /// node WAITS through a quorum outage instead of aborting — the regression
    /// guard for the cold-start crash-loop.
    pub(crate) async fn restart_all_staggered(&mut self, stagger: Duration) -> Result<(), Error> {
        let mut configs = self.stop_all_and_rebuild_configs().await?;
        let mut handles: Vec<tokio::task::JoinHandle<Result<ReplicaNode, Error>>> = Vec::new();
        let first = configs.remove(0);
        handles.push(Self::spawn_start(first));
        tokio::time::sleep(stagger).await;
        for cfg in configs {
            handles.push(Self::spawn_start(cfg));
        }
        self.await_and_register(handles).await
    }

    fn server_name_for(&self, idx: usize) -> ServerName {
        if self.config.distinct_names {
            ServerName::from(format!("node-{idx}"))
        } else {
            ServerName::from(LOCALHOST)
        }
    }

    fn build_config(
        &self,
        server_name: &ServerName,
        addr: SocketAddr,
        db_path: PathBuf,
        full_peers: &[PeerAddr],
    ) -> ReplicaNodeConfig {
        let bootstrap_name: ServerName = if self.config.distinct_names {
            ServerName::from(BOOTSTRAP_NAME)
        } else {
            ServerName::from(LOCALHOST)
        };

        let base = self
            .config
            .base_config
            .clone()
            .unwrap_or_else(default_fast_config);

        let mut cfg = base;
        cfg.network_server_name = server_name.clone();
        cfg.peers = full_peers.to_vec();
        cfg.network_bootstrap_server_name = bootstrap_name;
        cfg.network_listen_addr = addr;
        // `write_timeout` is deliberately *not* set here. It used to be pinned
        // to 30 s at this point, which silently clobbered whatever a caller put
        // in `base_config` — the field looked tunable and was not. It now lives
        // in `default_fast_config` (and in `fast_rolling_config`, the one base
        // config built from scratch), so every existing caller sees the same
        // 30 s it always did, and a caller that wants to scale the client's
        // hard deadline with its own timescales can.
        cfg.db_path = db_path;
        cfg.object_store = self.config.object_store.clone();
        cfg
    }
}

// ── fast default config ───────────────────────────────────────────────

pub(crate) fn default_fast_config() -> ReplicaNodeConfig {
    let snapshot_interval = Duration::from_millis(500);
    let tier0: crate::leader::CompactionTier = snapshot_interval.into();
    // Placeholder identity/storage — every real node replaces these in
    // build_config().
    let mut cfg = ReplicaNodeConfig::new(
        "placeholder".into(),
        vec![],
        LOCALHOST.into(),
        "127.0.0.1:0".parse().expect("addr"),
        PathBuf::from("placeholder"),
        Arc::new(InMemory::new()),
    )
    .expect("fast config builds");
    cfg.raft_heartbeat_interval = Duration::from_millis(250);
    cfg.raft_election_timeout_min = Duration::from_millis(1000);
    cfg.raft_election_timeout_max = Duration::from_millis(2000);
    cfg.raft_startup_phase_timeout = Duration::from_secs(30);
    cfg.snapshot_interval = snapshot_interval;
    cfg.snapshot_cutoff_timeout = Duration::from_millis(10);
    cfg.snapshot_compaction_tiers = vec![
        tier0,
        tier0 + Duration::from_secs(3600),
        tier0 + Duration::from_secs(86_400),
        tier0 + Duration::from_secs(7 * 86_400),
        tier0 + Duration::from_secs(30 * 86_400),
        tier0 + Duration::from_secs(365 * 86_400),
    ];
    cfg.membership_poll_interval = Duration::from_millis(500);
    cfg.liveness_probe_interval = Duration::from_millis(300);
    cfg.drain_timeout = Duration::from_secs(30);
    // Was applied in `build_config` and moved here so `base_config` can
    // override it; the value is unchanged.
    cfg.write_timeout = Duration::from_secs(30);
    cfg
}
