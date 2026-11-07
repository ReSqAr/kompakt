//! The replica RPC service — the node-local end of a writer's write stream.
//!
//! ```text
//!   input ──> gate ──┬─> reject ──────────────────> out    (inline, in order)
//!    (pull)          └─> submit ──> flush loop ───> out    (after fsync)
//! ```
//!
//! The gate is synchronous — membership plus HLC cutoff, no `.await` anywhere —
//! and runs at pull time on a blocking driver, so the writer's hot path never
//! touches the reactor. A rejection is answered on the spot; an accepted write is
//! handed to the log and answered later by the flush loop, once it is durable.
//!
//! **Answers are therefore out of input order**, and that is not a defect to
//! paper over: a rejection costs nothing while an acceptance waits for an fsync,
//! so forcing them back into order would make every cheap answer wait for an
//! expensive one. Callers correlate by `data_id`.
//!
//! The stream never ends in an error. A local replica has no connection to lose,
//! so even a shut-down log surfaces per-write as [`WriteOutcome::Failed`]; the
//! output closes only once `input` is exhausted *and* every accepted write has
//! committed, because the flush loop holds a clone of the completion sender.

pub mod metrics;
mod purge;

pub use purge::run_purge_worker;
use zslg_consensus::ConsensusError;
use zslg_consensus::RaftStateView;
use zslg_lifecycle_types::LifecycleError;
use zslg_net::PeersWatcher;
use zslg_time::ClockError;

use futures::StreamExt as _;
use futures::stream::BoxStream;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::watch;

use tracing::warn;

use crate::metrics::ReplicaWriteState;
use crossbeam_combinators::Channel;
use warn_throttle::WarnThrottle;
use zslg_consensus::NodeId;
use zslg_consensus::ReplicaStatus;
use zslg_log::Data;
use zslg_log::{Admission, ReplicaLog};
use zslg_log::{DataId, LogIndex};
use zslg_log::{Replica, StampedWrite, WriteOutcome};
use zslg_time::Hlc;
use zslg_time::{Clock, WriteGate};

/// Minimum spacing between replica-write-rejection `warn!`s. Rejections recur at
/// the write rate (tens/sec) under a persistent failure, so they are throttled
/// to keep the reason (and its diagnostic context) visible without flooding Loki.
const REJECT_WARN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
pub struct ReplicaService {
    replica_log: Arc<dyn ReplicaLog>,
    hlc_gate: Arc<dyn WriteGate>,
    node_id: NodeId,
    clock: Arc<dyn Clock>,
    raft_state: Arc<dyn RaftStateView>,
    effective_cutoff_rx: watch::Receiver<Option<Hlc>>,
    reject_warn: Arc<WarnThrottle>,
    /// See [`ReplicaLogConfig::write_out_capacity`].
    write_out_capacity: NonZeroUsize,
    peers: Arc<dyn PeersWatcher>,
}

impl Drop for ReplicaService {
    fn drop(&mut self) {
        tracing::info!(node_id = ?self.node_id, "ReplicaService dropped");
    }
}

impl ReplicaService {
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "ReplicaService::new gathers all replica subsystem dependencies"
    )]
    pub fn new(
        replica_log: Arc<dyn ReplicaLog>,
        hlc_gate: Arc<dyn WriteGate>,
        node_id: NodeId,
        clock: Arc<dyn Clock>,
        raft_state: Arc<dyn RaftStateView>,
        effective_cutoff_rx: watch::Receiver<Option<Hlc>>,
        write_out_capacity: NonZeroUsize,
        peers: Arc<dyn PeersWatcher>,
    ) -> Self {
        Self {
            replica_log: replica_log.clone(),
            hlc_gate,
            node_id,
            clock,
            raft_state,
            effective_cutoff_rx,
            reject_warn: Arc::new(WarnThrottle::new(REJECT_WARN_INTERVAL)),
            write_out_capacity,
            peers,
        }
    }
}

#[async_trait::async_trait]
impl Replica for ReplicaService {
    /// Wall-clock skew is not checked here: it is enforced at ingress, against
    /// the fresh per-RPC envelope HLC rather than the write's own.
    fn write(
        &self,
        input: Channel<StampedWrite, crossbeam_combinators::Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, crossbeam_combinators::Infinite> {
        let run = || -> Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> {
            // No thread: the gate is a `filter_map` stage that runs when the *consumer*
            // pulls the returned channel. A rejection short-circuits onto the output; an
            // accepted write is submitted (durably, inline) and the stage yields
            // `None`, its outcome arriving later on the merged commit channel. The
            // `Weak<dyn ReplicaLog>` inside the submitter means a parked consumer never
            // pins the node's RocksDB.
            let submitter = WriteSubmitter {
                replica_log: Arc::downgrade(&self.replica_log),
                hlc_gate: self.hlc_gate.clone(),
                node_id: self.node_id,
                clock: self.clock.clone(),
                raft_state: self.raft_state.clone(),
                reject_warn: self.reject_warn.clone(),
                write_out_capacity: self.write_out_capacity,
            };
            // The stamp is read here and nowhere else: this is the server→raft
            // dispatch wait, measured at the moment the write leaves the queue.
            // Same pull-time stage as the gate, so it costs no thread either.
            submitter.write(input.filter_map(|s| {
                metrics::server_dispatch_wait(s.enqueued.elapsed());
                Some(s.data)
            }))
        };
        let result: Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> = run();
        result.err_into()
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        self.replica_log.clone().subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        let result: Result<ReplicaStatus, Error> = async {
            Ok(status_snapshot(
                self.node_id,
                self.replica_log.as_ref(),
                self.raft_state.as_ref(),
                &self.effective_cutoff_rx,
                self.peers.as_ref(),
            ))
        }
        .await;
        result.map_err(Into::into)
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        let result: Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> = async {
            let replica_log = self.replica_log.clone();
            let node_id = self.node_id;
            let raft_state = self.raft_state.clone();
            let cutoff_rx = self.effective_cutoff_rx.clone();
            let peers = self.peers.clone();

            let stream = futures::stream::unfold(
                (raft_state.subscribe(), cutoff_rx, false),
                move |(mut rx, mut cutoff_rx, baseline_sent)| {
                    let replica_log = replica_log.clone();
                    let raft_state = raft_state.clone();
                    let peers = peers.clone();
                    async move {
                        if !baseline_sent {
                            let status = status_snapshot(
                                node_id,
                                replica_log.as_ref(),
                                raft_state.as_ref(),
                                &cutoff_rx,
                                peers.as_ref(),
                            );
                            return Some((Ok(status), (rx, cutoff_rx, true)));
                        }

                        let alive = tokio::select! {
                            raft = rx.changed() => raft.is_ok(),
                            changed = cutoff_rx.changed() => changed.is_ok(),
                        };
                        if !alive {
                            return None;
                        }
                        let status = status_snapshot(
                            node_id,
                            replica_log.as_ref(),
                            raft_state.as_ref(),
                            &cutoff_rx,
                            peers.as_ref(),
                        );
                        Some((Ok(status), (rx, cutoff_rx, true)))
                    }
                },
            );

            Ok(stream.boxed())
        }
        .await;
        result
            .map(|s| {
                futures::StreamExt::boxed(futures::StreamExt::map(s, |r| r.map_err(Into::into)))
            })
            .map_err(Into::into)
    }
}

/// The slice of [`ReplicaService`] the write path needs, cloned out so a write
/// stream's blocking driver thread owns it without pinning the whole service.
struct WriteSubmitter {
    /// **Weak on purpose.** This thread spends its life parked on the input
    /// channel, and a strong ref would keep the node's RocksDB open long after
    /// the node dropped its `ReplicaService` — a fast relaunch then hits "dir
    /// already open". Upgraded per write; a failed upgrade means shutdown.
    replica_log: std::sync::Weak<dyn ReplicaLog>,
    hlc_gate: Arc<dyn WriteGate>,
    node_id: NodeId,
    clock: Arc<dyn Clock>,
    raft_state: Arc<dyn RaftStateView>,
    reject_warn: Arc<WarnThrottle>,
    /// See [`ReplicaLogConfig::write_out_capacity`].
    write_out_capacity: NonZeroUsize,
}

/// Outcome of the synchronous write gate: either the write is answered before it
/// reaches storage (rejected, or the node is shutting down), or it passes and
/// carries its [`Admission`] to [`WriteSubmitter::submit`].
enum Gated {
    /// Boxed: a `WriteOutcome` carries a whole `RaftState` (so the writer can
    /// re-evaluate quorum from the reply) and is several times the size of
    /// `Pass`. This is the cold rejection/shutdown path, so the allocation is
    /// off the accepted-write path entirely.
    Answer(Box<WriteOutcome>),
    Pass(Data, Admission),
}
impl WriteSubmitter {
    /// Validate one write (membership + HLC cutoff, both synchronous). Runs as a
    /// `filter_map` stage on the consumer's pull, so it must not block or `.await`.
    fn gate(&self, data: Data) -> Gated {
        let data_id = data.data_id;
        // Liveness probe: if the log is gone the node is shutting down. Drop the
        // upgraded `Arc` immediately — a parked consumer must never pin the node's
        // RocksDB (fast relaunch → LOCK still held); `submit` re-upgrades to write.
        if self.replica_log.upgrade().is_none() {
            metrics::replica_write_request(ReplicaWriteState::FailStorage);
            return Gated::Answer(Box::new(WriteOutcome::Failed {
                data_id,
                error: zslg_log::Error::Lifecycle(LifecycleError::Shutdown),
            }));
        }
        let state = self.raft_state.snapshot();
        let in_current = state.replica.current().contains(&self.node_id);
        let in_joint = state
            .replica
            .next()
            .is_some_and(|s| s.contains(&self.node_id));
        if !in_current && !in_joint {
            metrics::replica_write_request(ReplicaWriteState::FailNotReplica);
            if self.reject_warn.should_warn() {
                warn!(
                    node_id = ?self.node_id,
                    replica_node_ids = ?state.replica.current(),
                    next_replica_node_ids = ?state.replica.next(),
                    "replica write rejected: this node is not in the current replica set \
                     (NotAReplica) — restored replica-set state may be stale/missing this node"
                );
            }
            return Gated::Answer(Box::new(WriteOutcome::Rejected {
                data_id,
                error: (Error::Consensus(ConsensusError::NotAReplica {
                    node_id: self.node_id,
                }))
                .into(),
                raft_state: (*state).clone(),
            }));
        }

        // Wall-clock skew is enforced *at ingress*, on a fresh per-RPC timestamp:
        // every frame carries an `RpcEnvelope.hlc` stamped at send time, and
        // `FrameStream::next_frame` rejects it (`HlcDriftTooLarge`) if the
        // sender's clock has drifted past `max_clock_skew`. A second check here
        // against `data.data_id.hlc()` (the *creation* time) is not just
        // redundant — it conflates in-flight queueing latency with clock skew:
        // under load a perfectly-clocked write that waited >max_clock_skew in
        // flight would be falsely rejected, collapsing throughput. So the only
        // gate left here is the HLC cutoff (logical ordering / staleness).
        let guard = match self.hlc_gate.check_hlc(data.data_id.hlc()) {
            Ok(g) => g,
            Err(e) => {
                metrics::replica_write_request(ReplicaWriteState::FailHlcCutoff);
                if self.reject_warn.should_warn() {
                    // The `error` already carries the rejected hlc + the cutoff;
                    // logging `clock_now` completes the triple: if clock_now is
                    // *behind* the cutoff, this node's HLC reset to wall-time on
                    // restart and will reject every local write until it catches
                    // up (see the HLC-cutoff-vs-reset-clock failure mode).
                    warn!(
                        node_id = ?self.node_id,
                        error = %e,
                        clock_now = ?self.clock.now(),
                        "replica write rejected: HLC at or below cutoff"
                    );
                }
                return Gated::Answer(Box::new(WriteOutcome::Rejected {
                    data_id,
                    error: Error::Clock(ClockError::HlcCutoff(e)).into(),
                    raft_state: (*state).clone(),
                }));
            }
        };
        let _ = self.clock.integrate(data.data_id.hlc());
        Gated::Pass(data, guard)
    }

    fn write(
        self,
        input: Channel<Data, crossbeam_combinators::Finite>,
    ) -> Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> {
        // Bounded: the flush loop sends here on the shared flush thread and must
        // never block it, so cap ≥ the in-flight window the writer's permits
        // enforce (a Full here is a sizing bug, not a steady state).
        let (cmt_tx, cmt_rx) = crossbeam_channel::bounded::<(
            DataId,
            Result<LogIndex, zslg_log::Error>,
        )>(self.write_out_capacity.get());
        let tx = Arc::new(cmt_tx);

        // Map raw log results → WriteOutcome at pull time (consumer-driven, no
        // thread). The data_id is carried through by the flush loop; raft_state is
        // read fresh once per outcome — the coordinator's tracker-integrate is
        // downstream of this and will see it anyway.
        let raft_state = self.raft_state.clone();
        let reject_warn = self.reject_warn.clone();
        let node_id = self.node_id;
        // Lives as long as the connection: it ends by refcount when the gate
        // branch and every in-flight write are gone, which no producer can
        // declare — so any end is an error the writer retries.
        let cmt_ch = Channel::from_infinite(cmt_rx, "replica commit outcomes");
        let outcomes = cmt_ch.filter_map(move |(data_id, res)| match res {
            Ok(_idx) => {
                metrics::replica_write_request(ReplicaWriteState::Success);
                Some(Ok(WriteOutcome::Accepted {
                    data_id,
                    raft_state: (*raft_state.snapshot()).clone(),
                }))
            }
            Err(e) => {
                metrics::replica_write_request(ReplicaWriteState::FailStorage);
                if reject_warn.should_warn() {
                    warn!(
                        node_id = ?node_id,
                        error = %e,
                        "replica write rejected: storage append failed \
                         (flush loop may be stalled)"
                    );
                }
                Some(Ok(WriteOutcome::Failed { data_id, error: e }))
            }
        });

        // Gate short-circuits inline; passes → submit (→ flush loop). Both
        // sources (gate answers + flush-loop outcomes) are merged.
        input
            .filter_map(move |data| match self.gate(data) {
                Gated::Answer(outcome) => Some(Ok(*outcome)),
                Gated::Pass(data, guard) => match self.replica_log.upgrade() {
                    Some(replica_log) => {
                        replica_log.submit(data, guard, tx.clone());
                        None
                    }
                    None => {
                        metrics::replica_write_request(ReplicaWriteState::FailStorage);
                        Some(Ok(WriteOutcome::Failed {
                            data_id: data.data_id,
                            error: zslg_log::Error::Lifecycle(LifecycleError::Shutdown),
                        }))
                    }
                },
            })
            .merge(outcomes)
    }
}

/// Build a [`ReplicaStatus`] from the live replica state. Shared by
/// `fetch_status` and the `subscribe_status` baseline / per-change items.
///
/// The effective (fully-drained) cutoff comes from the watch channel, which is
/// updated only after `set_cutoff` has drained all in-flight writes;
/// `HlcGate::current()` would return the new value before drain and could race
/// ahead.
fn status_snapshot(
    node_id: NodeId,
    replica_log: &dyn ReplicaLog,
    raft_state: &dyn RaftStateView,
    cutoff_rx: &watch::Receiver<Option<Hlc>>,
    peers: &dyn PeersWatcher,
) -> ReplicaStatus {
    ReplicaStatus {
        node_id,
        frontier: replica_log.frontier(),
        raft_state: (*raft_state.snapshot()).clone(),
        effective_cutoff: *cutoff_rx.borrow(),
        max_hlc: replica_log.max_hlc(),
        peer_names: peers.current().into_iter().map(|p| p.server_name).collect(),
    }
}

error_union::error_union! {
    /// What this crate can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_time::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_union::error_bridge!(Error => zslg_time::Error, zslg_log::Error, zslg_snapshot::Error, zslg_consensus::Error, zslg_raft::Error, zslg_net::Error, zslg_quorum_read::Error; Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);
error_union::error_home!(Error);
