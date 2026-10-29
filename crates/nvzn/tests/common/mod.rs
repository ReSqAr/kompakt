//! Shared rig for the cluster-tier tests: a projection to fold, and the two
//! node shapes an application builds a dispatcher over.
//!
//! ```text
//!   Cluster::start()  ──> ReplicaNode ──┬─> Deployment::over(replica)
//!                             ^         │
//!   cluster.follower() ──> FollowerNode ┴─> Deployment::over(follower)
//!                                             └─ EventWriter + Reader + fold
//! ```
//!
//! A `Deployment` is one process's view of the cluster: its own nvzn store
//! folding a node's stable log. Tests build several against one `Cluster` —
//! sequentially, to stand in for successive CLI runs, or side by side for a
//! server and a CLI.
//!
//! Ports are fixed per test binary rather than ephemeral, because a follower's
//! peer list has to name the replica's address before the replica is up.

// Each test binary compiles this module and uses a different part of it, so
// the unused half is not dead code in any meaningful sense.
#![allow(dead_code, unreachable_pub, reason = "shared test rig")]

use futures::FutureExt as _;
use nvzn::{
    Dispatcher, Error, EventWriter, Projection, ProjectionId, ProjectionTypeId, Ref, StoreConfig,
    Tenant, View, Write,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

/// Every sync in these tests is bounded well below the cadences that would
/// otherwise hide a stall: the leader's cutoff interval (5s) and the learner's
/// quorum-read timeout (10s). A sync that waits on either fails the test
/// rather than showing up as latency in an application.
pub const SYNC_BOUND: Duration = Duration::from_secs(5);

/// How long the live view may take to show what the node has integrated: one
/// integrate batch and a fan-out hop, neither on a timer.
pub const SEE_BOUND: Duration = Duration::from_secs(2);

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct UserId(pub u128);

impl From<UserId> for ProjectionId {
    fn from(v: UserId) -> Self {
        ProjectionId::from_u128(v.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NewUserV1 {
    pub id: UserId,
    pub name: String,
}

nvzn::define_events! {
    event::Event = {
        event 1 := NewUser { version 1 => NewUserV1 }
    } subsets {
        UserEvent := NewUser,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub id: UserId,
    pub name: String,
}

impl Projection for User {
    type Event<'a> = event::UserEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("User", 0);

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::UserEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        let event::UserEvent::NewUser(n) = event;
        Write::insert(view, Ref::new(n.id.into()), || User {
            id: n.id,
            name: n.name.clone(),
        })
    }
}

/// A one-node replica cluster plus the object store its followers read
/// snapshots from. Outlives the deployments built against it.
pub struct Cluster {
    replica: Arc<dyn zslg::Reader>,
    replica_writer: Arc<dyn zslg::Writer>,
    peers: Vec<zslg::PeerAddr>,
    shutdown: CancellationToken,
    root: TempDir,
}

impl Cluster {
    /// Bring up the cluster on `port` and leave it running on its own task.
    #[allow(
        clippy::large_stack_frames,
        reason = "one-shot startup; boxing these would churn for no runtime benefit (CLAUDE.md §2)"
    )]
    pub async fn start(port: u16) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        // LocalFs refuses a root that does not exist yet.
        std::fs::create_dir_all(root.path().join("objstore")).expect("object store root");

        let listen: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("valid addr");
        let peers = vec![zslg::PeerAddr {
            addr: listen.into(),
            server_name: "localhost".into(),
        }];

        let mut config = zslg::ReplicaNodeConfig::new(
            "localhost".into(),
            peers.clone(),
            "localhost".into(),
            listen,
            root.path().join("replica"),
            object_store(root.path()),
        )
        .expect("replica config");
        config.membership_replica_node_count = 1;

        let replica_node = zslg::ReplicaNode::start(config)
            .boxed()
            .await
            .expect("replica start")
            .wait_until_ready()
            .boxed()
            .await
            .expect("replica ready");
        let shutdown = replica_node.shutdown_token();
        let replica = replica_node.reader();
        let replica_writer = replica_node.writer();
        tokio::spawn(async move {
            let outcome = replica_node.run().boxed().await;
            tracing::info!(?outcome, "replica node stopped");
        });

        Self {
            replica,
            replica_writer,
            peers,
            shutdown,
            root,
        }
    }

    /// A deployment over the replica itself — the shape of a server process.
    pub async fn server(&self, tag: &str) -> Deployment {
        Deployment::build(
            self.root.path(),
            tag,
            self.replica_writer.clone(),
            self.replica.clone(),
            None,
        )
        .await
    }

    /// A deployment over a brand-new follower — the shape of a CLI process.
    /// Nothing is shared with any earlier one but the cluster and the object
    /// store.
    #[allow(
        clippy::large_stack_frames,
        reason = "one-shot startup; boxing these would churn for no runtime benefit (CLAUDE.md §2)"
    )]
    pub async fn follower(&self, tag: &str) -> Deployment {
        let config = zslg::FollowerNodeConfig::new(
            self.peers.clone(),
            self.root.path().join(format!("{tag}-zslg")),
            object_store(self.root.path()),
        )
        .expect("follower config");
        let node = zslg::FollowerNode::start(config)
            .boxed()
            .await
            .expect("follower start");
        let writer = node.writer();
        let reader = node.reader();
        let shutdown = node.shutdown_token();
        tokio::spawn(async move {
            let outcome = node.run().boxed().await;
            tracing::info!(?outcome, "follower node stopped");
        });

        Deployment::build(self.root.path(), tag, writer, reader, Some(shutdown)).await
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

fn object_store(root: &Path) -> zslg::ObjectStoreConfig {
    zslg::ObjectStoreConfig::LocalFs {
        root: root.join("objstore"),
    }
}

/// One process's view: an nvzn store folding a node's stable log.
pub struct Deployment {
    pub writer: EventWriter<event::Event>,
    pub reader: nvzn::Reader<event::Event>,
    /// Every projection live: what a person's reads would go through.
    pub live: nvzn::Live<event::Event>,
    pub fold: tokio::task::JoinHandle<Result<(), nvzn::Error>>,
    node_shutdown: Option<CancellationToken>,
}

impl Deployment {
    async fn build(
        root: &Path,
        tag: &str,
        zslg_writer: Arc<dyn zslg::Writer>,
        zslg_reader: Arc<dyn zslg::Reader>,
        node_shutdown: Option<CancellationToken>,
    ) -> Self {
        let store: PathBuf = root.join(format!("{tag}-nvzn"));
        // The node's shutdown, or (no node: an in-process log) a root of its own.
        let shutdown = node_shutdown.clone().unwrap_or_default();
        let mut dispatcher = Dispatcher::new(
            &StoreConfig::new(store),
            zslg_writer,
            zslg_reader,
            &shutdown,
        )
        .await
        .expect("dispatcher");
        let mut manager = dispatcher.new_handler::<event::Event>(Tenant::new("todo", 0));
        manager.register::<User>().expect("register");
        let (writer, reader) = manager.finalise().expect("finalise handler");
        let fold = dispatcher.finalise().await.expect("finalise dispatcher");
        let live = reader.live::<event::Event>().expect("live view");

        Self {
            writer,
            reader,
            live,
            fold,
            node_shutdown,
        }
    }

    /// Sync, requiring it to return inside [`SYNC_BOUND`].
    pub async fn sync(&self, what: &str) {
        let started = Instant::now();
        tokio::time::timeout(SYNC_BOUND, self.reader.sync())
            .await
            .unwrap_or_else(|_| panic!("{what}: sync did not return within {SYNC_BOUND:?}"))
            .expect("sync");
        let elapsed = started.elapsed();
        assert!(elapsed < SYNC_BOUND, "{what}: sync took {elapsed:?}");
    }

    pub async fn write(&self, id: u128, name: &str) -> zslg::DataId {
        self.writer
            .write(&[NewUserV1 {
                id: UserId(id),
                name: name.to_owned(),
            }
            .into()])
            .await
            .expect("write")
    }

    /// Whether the live view shows user `id` within [`SEE_BOUND`]: what a
    /// caller reads behind [`sync`](Self::sync). Polled, because the view takes
    /// in what the node integrated on its own task.
    pub async fn sees(&self, id: u128) -> bool {
        let started = Instant::now();
        loop {
            let live = self.live.clone();
            let seen = tokio::task::spawn_blocking(move || {
                live.snapshot()
                    .expect("live snapshot")
                    .get(Ref::<User>::new(UserId(id).into()))
                    .expect("get")
                    .is_some()
            })
            .await
            .expect("snapshot task");
            if seen {
                return true;
            }
            if started.elapsed() > SEE_BOUND {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The folded state alone, without the unstable tail on top.
    pub fn sees_folded(&self, id: u128) -> bool {
        self.reader
            .snapshot()
            .expect("snapshot")
            .get(Ref::<User>::new(UserId(id).into()))
            .expect("get")
            .is_some()
    }

    /// Shut this deployment down the way a process exits cleanly.
    pub async fn stop(self) {
        if let Some(token) = self.node_shutdown {
            token.cancel();
        }
        self.fold.abort();
    }
}

/// Silent unless `RUST_LOG` asks for output.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}
