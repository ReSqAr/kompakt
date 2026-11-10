//! Composition of one replica: its log (with the flush loop), the cutoff applier,
//! the purge worker and the service over them.

use std::sync::Arc;

use tokio::sync::watch;
use tracing::info;

use crate::Error;
use shutdown::Shutdown;
use zslg_consensus::RaftStateView;
use zslg_hlc_clock::HlcClock;
use zslg_hlc_gate::{CutoffApplier, HlcGate};
use zslg_net::PeersWatcher;
use zslg_replica_log::flush_loop;
use zslg_replica_service::run_purge_worker;
use zslg_time::Hlc;

use zslg_replica_log::{ReplicaLogConfig, ReplicaLogStore};
use zslg_replica_service::ReplicaService;

#[allow(
    clippy::too_many_arguments,
    reason = "spawn_replica requires all dependencies at creation time"
)]
pub(crate) async fn spawn_replica<E>(
    raft_state: Arc<dyn RaftStateView>,
    hlc_clock: &Arc<HlcClock>,
    hlc_cutoff: &Arc<HlcGate>,
    storage: Arc<rocksdb_kit::Storage>,
    config: &ReplicaLogConfig,
    shutdown: &mut Shutdown<E>,
    peers: Arc<dyn PeersWatcher>,
) -> Result<Arc<ReplicaService>, Error>
where
    E: From<zslg_replica_log::Error>
        + From<zslg_replica_service::Error>
        + From<zslg_hlc_gate::Error>
        + From<shutdown::Stopped>
        + std::fmt::Display
        + Send
        + 'static,
{
    info!("spawning replica log store");
    // One appender behind a bounded queue. RocksDB's group commit merges
    // concurrent writers into one WAL write, but it cannot batch the *publish*
    // step, and per-writer publishing has to serialize on a lock to stay
    // ordered — measured at roughly half the throughput of this shape. The
    // queue lets one thread turn N writes into one append, one fsync and one
    // pass over the subscribers.
    let (write_tx, write_rx) = crossbeam_channel::bounded(config.write_channel_capacity.get());
    let replica_log_store = ReplicaLogStore::open(storage, write_tx, config).await?;
    let replica_log = Arc::new(replica_log_store);
    let log_port: Arc<dyn zslg_log::ReplicaLog> = replica_log.clone();

    let (effective_cutoff_tx, effective_cutoff_rx) = watch::channel::<Option<Hlc>>(None);

    let applier = CutoffApplier::new(
        raft_state.clone(),
        hlc_cutoff.clone(),
        effective_cutoff_tx,
        shutdown.token(),
    );
    shutdown.spawn("replica:applier", async move { applier.run::<E>().await });

    let purge = run_purge_worker(
        raft_state.clone(),
        log_port.clone(),
        config.purge_grace_period,
        shutdown.token(),
    );
    shutdown.spawn("replica:purge", async move { purge.await.map_err(E::from) });

    let replica = Arc::new(ReplicaService::new(
        log_port,
        hlc_cutoff.clone(),
        config.effective_node_id,
        hlc_clock.clone(),
        raft_state.clone(),
        effective_cutoff_rx.clone(),
        config.write_out_capacity,
        peers,
    ));

    let cancel = shutdown.token();
    let weak = Arc::downgrade(&replica_log);
    shutdown.spawn("replica:log_flush", async move {
        flush_loop(weak, write_rx, cancel).await.map_err(E::from)
    });

    Ok(replica)
}
