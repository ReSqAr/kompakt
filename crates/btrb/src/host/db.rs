//! Bringing up the two stores this process owns, from the config.
//!
//! ```text
//!   config::Settings ──────────┬─> zslg::ReplicaNode ───> Writer + Reader ──┐
//!                              │                                           ├─> Dispatcher
//!                              └─> nvzn::StoreConfig ──────────────────────┘
//!
//!   config::WorkerSettings ────┬─> zslg::FollowerNode ──> Writer + Reader ──┐
//!                              │                                           ├─> Dispatcher
//!                              └─> nvzn::StoreConfig ──────────────────────┘
//! ```
//!
//! The order is forced: the dispatcher's projections are derived from the zslg
//! log, so the node has to be serving before the dispatcher can be built on its
//! `Writer`/`Reader`.
//!
//! Both setups borrow their node config rather than consuming it, because a
//! `:temp:` store's `TempDir` guard lives in the settings value: `main` holds
//! the [`Settings`](crate::Settings) or the
//! [`WorkerSettings`](crate::WorkerSettings), so the directories outlive the
//! databases inside them.

use futures::FutureExt as _;
use std::error::Error;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
pub(crate) struct NodeShutdown {
    token: CancellationToken,
    run: JoinHandle<()>,
}

impl NodeShutdown {
    /// The token that stops the node, for code that needs to know a shutdown
    /// is under way (the fold supervisor).
    #[must_use]
    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub(crate) async fn shutdown(self) {
        self.token.cancel();
        if let Err(e) = self.run.await {
            tracing::error!(?e, "the replica node task did not finish");
        }
    }
}

pub(crate) async fn setup_db(
    zslg: &zslg::ReplicaNodeConfig,
    nvzn: &nvzn::StoreConfig,
) -> Result<(nvzn::Dispatcher, NodeShutdown), Box<dyn Error>> {
    let (writer, reader, shutdown) = setup_zslg_db(zslg.clone()).boxed().await?;
    let dispatcher = nvzn::Dispatcher::new(nvzn, writer, reader, &shutdown.token())
        .boxed()
        .await?;

    Ok((dispatcher, shutdown))
}

pub(crate) async fn setup_follower_db(
    zslg: &zslg::FollowerNodeConfig,
    nvzn: &nvzn::StoreConfig,
) -> Result<(nvzn::Dispatcher, CancellationToken), Box<dyn Error>> {
    let (writer, reader, token) = setup_follower_zslg_db(zslg.clone()).boxed().await?;
    let dispatcher = nvzn::Dispatcher::new(nvzn, writer, reader, &token)
        .boxed()
        .await?;

    Ok((dispatcher, token))
}

async fn setup_zslg_db(
    config: zslg::ReplicaNodeConfig,
) -> Result<(Arc<dyn zslg::Writer>, Arc<dyn zslg::Reader>, NodeShutdown), Box<dyn Error>> {
    let node = zslg::ReplicaNode::start(config)
        .boxed()
        .await?
        .wait_until_ready()
        .boxed()
        .await?;

    let writer = node.writer();
    let reader = node.reader();
    let shutdown = NodeShutdown {
        token: node.shutdown_token(),
        run: tokio::spawn(async move {
            if let Err(e) = node.run().boxed().await {
                tracing::error!(?e, "Error running zslg replica node");
            }
        }),
    };

    Ok((writer, reader, shutdown))
}

async fn setup_follower_zslg_db(
    config: zslg::FollowerNodeConfig,
) -> Result<
    (
        Arc<dyn zslg::Writer>,
        Arc<dyn zslg::Reader>,
        CancellationToken,
    ),
    Box<dyn Error>,
> {
    let node = zslg::FollowerNode::start(config).boxed().await?;

    let writer = node.writer();
    let reader = node.reader();
    let cancellation_token = node.shutdown_token();

    tokio::spawn(async move {
        let e = node.run().boxed().await;
        tracing::error!(?e, "Error running zslg follower node");
    });

    Ok((writer, reader, cancellation_token))
}
