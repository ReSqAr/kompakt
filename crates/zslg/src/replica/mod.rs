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

mod log;
mod purge;

#[cfg(test)]
mod clock_skew_tests;
#[cfg(test)]
mod subscribe_status_tests;

pub(crate) use log::ReplicaLogStore;
use purge::run_purge_worker;

use futures::StreamExt as _;
use futures::stream::BoxStream;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use tracing::{info, warn};

use crate::api::{Clock as _, PeersWatcher, RaftStateView, Replica, StampedWrite, WriteOutcome};
use crate::error::Error;
use crate::hlc::{CutoffApplier, Hlc, HlcClock, HlcGate, HlcGuard};
use crate::ids::{DataId, LogIndex, NodeId};
use crate::meta::{Data, ReplicaStatus};
use crate::metrics::{self, ReplicaWriteState};
use crate::replica::log::flush_loop;
use crate::shutdown::Shutdown;
use crate::warn_throttle::WarnThrottle;
use crossbeam_combinators::Channels;

/// Minimum spacing between replica-write-rejection `warn!`s. Rejections recur at
/// the write rate (tens/sec) under a persistent failure, so they are throttled
/// to keep the reason (and its diagnostic context) visible without flooding Loki.
const REJECT_WARN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Reference record size used to turn a queue's *depth* into bytes.
///
/// The pipeline queues are sized in slots, never in bytes, so any byte figure
/// for them is depth times an assumed record size — there is no measurement to
/// read instead, because a slot's cost is whatever payload happens to sit in
/// it. 2 KiB is the midpoint of the harness workload (`examples/loadgen.rs`
/// defaults `--payload-min 64 --payload-max 4096`), which is what the
/// 2026-08-17 memory sweep ran, so the gauge is directly comparable with that
/// run's RSS. It is a planning figure, not a measurement, and the metric's help
/// string says so.
pub(crate) const QUEUE_REFERENCE_PAYLOAD_BYTES: u64 = 2 * 1024;

#[derive(Clone)]
pub(crate) struct ReplicaService {
    replica_log: Arc<ReplicaLogStore>,
    hlc_gate: Arc<HlcGate>,
    node_id: NodeId,
    clock: Arc<HlcClock>,
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
    pub(crate) fn new(
        replica_log: Arc<ReplicaLogStore>,
        hlc_gate: Arc<HlcGate>,
        node_id: NodeId,
        clock: Arc<HlcClock>,
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
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        // No thread: the gate is a `filter_map` stage that runs when the *consumer*
        // pulls the returned channel. A rejection short-circuits onto the output; an
        // accepted write is submitted (durably, inline) and the stage yields
        // `None`, its outcome arriving later on the merged commit channel. The
        // `Weak<ReplicaLogStore>` inside the submitter means a parked consumer never
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
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> Channels<Result<(LogIndex, Data), Error>> {
        self.replica_log.subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
        Ok(status_snapshot(
            self.node_id,
            &self.replica_log,
            self.raft_state.as_ref(),
            &self.effective_cutoff_rx,
            self.peers.as_ref(),
        ))
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
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
                            &replica_log,
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
                        &replica_log,
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
}

/// The slice of [`ReplicaService`] the write path needs, cloned out so a write
/// stream's blocking driver thread owns it without pinning the whole service.
struct WriteSubmitter {
    /// **Weak on purpose.** This thread spends its life parked on the input
    /// channel, and a strong ref would keep the node's RocksDB open long after
    /// the node dropped its `ReplicaService` — a fast relaunch then hits "dir
    /// already open". Upgraded per write; a failed upgrade means shutdown.
    replica_log: std::sync::Weak<ReplicaLogStore>,
    hlc_gate: Arc<HlcGate>,
    node_id: NodeId,
    clock: Arc<HlcClock>,
    raft_state: Arc<dyn RaftStateView>,
    reject_warn: Arc<WarnThrottle>,
    /// See [`ReplicaLogConfig::write_out_capacity`].
    write_out_capacity: NonZeroUsize,
}

/// Outcome of the synchronous write gate: either the write is answered before it
/// reaches storage (rejected, or the node is shutting down), or it passes and
/// carries its [`HlcGuard`] to [`WriteSubmitter::submit`].
enum Gated {
    /// Boxed: a `WriteOutcome` carries a whole `RaftState` (so the writer can
    /// re-evaluate quorum from the reply) and is several times the size of
    /// `Pass`. This is the cold rejection/shutdown path, so the allocation is
    /// off the accepted-write path entirely.
    Answer(Box<WriteOutcome>),
    Pass(Data, HlcGuard),
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
                error: Error::Shutdown,
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
                error: Error::NotAReplica {
                    node_id: self.node_id,
                },
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
                    error: Error::HlcCutoff(e),
                    raft_state: (*state).clone(),
                }));
            }
        };
        let _ = self.clock.integrate(data.data_id.hlc());
        Gated::Pass(data, guard)
    }

    fn write(self, input: Channels<Data>) -> Channels<Result<WriteOutcome, Error>> {
        // Bounded: the flush loop sends here on the shared flush thread and must
        // never block it, so cap ≥ the in-flight window the writer's permits
        // enforce (a Full here is a sizing bug, not a steady state).
        let (cmt_tx, cmt_rx) = crossbeam_channel::bounded::<(DataId, Result<LogIndex, Error>)>(
            self.write_out_capacity.get(),
        );
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
        let cmt_ch = Channels::from_infinite(cmt_rx, "replica commit outcomes");
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
                            error: Error::Shutdown,
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
    replica_log: &ReplicaLogStore,
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

#[allow(
    clippy::too_many_arguments,
    reason = "spawn_replica requires all dependencies at creation time"
)]
pub(crate) async fn spawn_replica(
    raft_state: Arc<dyn RaftStateView>,
    hlc_clock: &Arc<HlcClock>,
    hlc_cutoff: &Arc<HlcGate>,
    storage: Arc<crate::database::storage::Storage>,
    config: &ReplicaLogConfig,
    shutdown: &mut Shutdown,
    peers: Arc<dyn PeersWatcher>,
) -> Result<Arc<ReplicaService>, Error> {
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

    let (effective_cutoff_tx, effective_cutoff_rx) = watch::channel::<Option<Hlc>>(None);

    let applier = CutoffApplier::new(
        raft_state.clone(),
        hlc_cutoff.clone(),
        effective_cutoff_tx,
        shutdown.token(),
    );
    shutdown.spawn("replica:applier", applier.run());

    shutdown.spawn(
        "replica:purge",
        run_purge_worker(
            raft_state.clone(),
            replica_log.clone(),
            config.purge_grace_period,
            shutdown.token(),
        ),
    );

    let replica = Arc::new(ReplicaService::new(
        replica_log.clone(),
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
    shutdown.spawn("replica:log_flush", flush_loop(weak, write_rx, cancel));

    Ok(replica)
}

/// Replica log persistence and purge configuration.
///
/// The store itself is a column family of the node's one
/// [`Storage`](crate::database::storage::Storage), which [`spawn_replica`] is
/// handed; what is left here is the queue sizing and the purge policy.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaLogConfig {
    pub(crate) purge_grace_period: Duration,
    pub(crate) fetch_sweep_interval: Duration,
    /// Live-tail channel depth per subscriber. Must be >= `max_flush_batch`
    /// (enforced in [`Self::validate`]): the flush loop `try_send`s a whole
    /// batch and evicts any subscriber whose channel is full, so a capacity
    /// below the batch cap sheds even subscribers that are keeping up. The
    /// evicted subscriber then reconnects and re-scans from its cursor, which
    /// burns the cores that caused the shed in the first place.
    ///
    /// Entries are `(LogIndex, Data)` where `Data.blob` is a `Bytes` handle,
    /// so this holds references, not payload copies — but the *slots* are
    /// allocated up front whether or not anything is queued, and an idle heap
    /// profile found the subscribe rings to be the single largest consumer in
    /// the process (36% of live heap across 9 rings per replica node). Default
    /// 1024 rather than 8192 because 1024-vs-8192 measured as noise on
    /// throughput (-0.1%, 2 of 4 reps) while the drop is worth ~6 MB/node.
    pub(crate) subscribe_channel_capacity: NonZeroUsize,
    /// Depth of the queue between `submit` (any RPC handler thread) and the
    /// single flush loop. Holds `Data` — payload — so it is a memory knob as
    /// well as a backpressure one: a full queue blocks the submitter, which is
    /// the intended behaviour, and the slots are allocated whether or not
    /// anything is queued.
    pub(crate) write_channel_capacity: NonZeroUsize,
    /// Depth of the per-connection outcome queue the flush loop `try_send`s
    /// into. Must stay at or above the in-flight window a caller drives
    /// (`quorum::coordinator::MAX_INFLIGHT`) or a healthy connection sheds:
    /// the flush loop counts a drop rather than blocking, so undersizing costs
    /// the writer an idempotent resend on reconnect. Tuning, not safety.
    pub(crate) write_out_capacity: NonZeroUsize,
    /// Max records drained into one append + fsync.
    pub(crate) max_flush_batch: NonZeroUsize,
    /// The owning node's durable identity — the one `replica_log_config()` was
    /// called with. Identity, not a tunable: filled by the producer.
    pub(crate) effective_node_id: NodeId,
}

impl ReplicaLogConfig {
    /// Default sizing/tunables. Test-only — production builds the config from
    /// the owning node config's flat fields. The store's location and
    /// compression are no longer here: it is a column family of the node's one
    /// DB, named in that node type's `COLUMN_FAMILIES`.
    #[cfg(test)]
    #[must_use]
    pub(in crate::replica) fn for_test(effective_node_id: NodeId) -> Self {
        Self {
            purge_grace_period: Duration::from_secs(5 * 60),
            fetch_sweep_interval: Duration::from_secs(2),
            subscribe_channel_capacity: crate::unit::nonzero(1024),
            write_channel_capacity: crate::unit::nonzero(4096),
            write_out_capacity: crate::unit::nonzero(2048),
            max_flush_batch: crate::unit::nonzero(1024),
            effective_node_id,
        }
    }

    /// Worst-case bytes this node's replica-side pipeline queues can hold, at
    /// full depth and the [`QUEUE_REFERENCE_PAYLOAD_BYTES`] reference record.
    ///
    /// **This is deliberately not charged against `storage_memory`.** That
    /// budget bounds RocksDB and nothing else: the two are different sizing
    /// decisions with different failure modes — the engine stalls on its
    /// `WriteBufferManager`, the pipeline blocks or sheds at submit time — and
    /// folding them into one number makes both harder to set. The queues get
    /// their own reported number, beside it.
    ///
    /// Two payload-carrying queues are counted, and they are distinct
    /// instances rather than two views of one: `write_channel_capacity` holds
    /// submitted-but-not-yet-flushed writes, a subscriber's live-tail ring
    /// holds flushed-but-not-yet-delivered ones. The ring is counted **once**
    /// — the depth is per subscriber, and the subscriber count is dynamic, so
    /// a node serving N learners holds up to N times that term.
    /// `write_out_capacity` is not counted: it carries outcomes, not payloads.
    #[must_use]
    pub(crate) fn queue_capacity_bytes(&self) -> u64 {
        // saturating throughout: these are usize config knobs that an operator
        // sets, so a nonsense value must report a huge number rather than wrap
        // to a small one — the gauge exists to make oversizing visible.
        let slots = u64::try_from(self.write_channel_capacity.get())
            .unwrap_or(u64::MAX)
            .saturating_add(
                u64::try_from(self.subscribe_channel_capacity.get()).unwrap_or(u64::MAX),
            );
        slots.saturating_mul(QUEUE_REFERENCE_PAYLOAD_BYTES)
    }

    pub(crate) fn validate(&self) -> Result<(), crate::error::Error> {
        if self.subscribe_channel_capacity < self.max_flush_batch {
            return Err(crate::error::Error::InvalidConfiguration(format!(
                "replica_log_subscribe_channel_capacity ({}) must be >= \
                 replica_log_max_flush_batch ({}): one flush batch would shed \
                 every subscriber on the node",
                self.subscribe_channel_capacity, self.max_flush_batch,
            )));
        }
        if self.fetch_sweep_interval.is_zero() {
            return Err(crate::error::Error::InvalidConfiguration(
                "fetch_sweep_interval must be > 0".to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::meta::{RaftState, ReplicaMembership};
    use crate::raft::RaftStateHandle;
    use crate::tests::{make_data, nid};

    /// Build a ReplicaService wired to a temp dense-log store, a fixed
    /// clock, and a RaftStateHandle whose replica set is `replicas`. Returns
    /// the node, the handle (for further mutation in tests), plus keep-alive
    /// guards (tempdir, flush handle).
    /// A single flush batch must never be able to overflow a healthy
    /// subscriber's live channel — otherwise one large batch sheds every
    /// subscriber on the node regardless of how fast they are draining.
    /// Measured at capacity 256 against a 1024 batch cap: subscriptions lived
    /// 0.137 s and re-scanned 9.62 GB of replica log.
    #[test]
    fn invariant_subscribe_capacity_covers_a_full_flush_batch() {
        let cfg = ReplicaLogConfig::for_test(nid(1));
        assert!(
            cfg.subscribe_channel_capacity >= cfg.max_flush_batch,
            "default config violates the shed invariant"
        );
        assert!(cfg.validate().is_ok());
    }

    /// At 2 KiB payloads a 4,096-deep write queue is ~8 MB — on its own larger
    /// than the whole 20 MiB storage budget, and entirely invisible before this
    /// gauge existed. The 2026-08-17 harness run's 90-120 MiB RSS per node
    /// against a 20 MiB budget is mostly the pipeline, not the engine.
    #[test]
    fn queue_capacity_bytes_is_reported() {
        let mut cfg = ReplicaLogConfig::for_test(nid(1));
        cfg.write_channel_capacity = crate::unit::nonzero(4096);
        cfg.subscribe_channel_capacity = crate::unit::nonzero(1024);
        // (4096 + 1024) slots x 2 KiB reference payload = 10 MiB.
        assert_eq!(cfg.queue_capacity_bytes(), 10_485_760);
    }

    #[test]
    fn validate_rejects_capacity_below_the_flush_batch() {
        let mut cfg = ReplicaLogConfig::for_test(nid(1));
        cfg.subscribe_channel_capacity = std::num::NonZeroUsize::new(64).expect("nonzero");
        cfg.max_flush_batch = std::num::NonZeroUsize::new(1024).expect("nonzero");
        let err = cfg.validate().expect_err("must reject");
        assert!(
            format!("{err}").contains("subscribe_channel_capacity"),
            "error must name the offending field, got: {err}"
        );
    }

    async fn replica_with(
        self_id: NodeId,
        replicas: &[NodeId],
        clock_ms: u64,
        max_skew: Duration,
    ) -> (
        Arc<ReplicaService>,
        RaftStateHandle,
        tempfile::TempDir,
        tokio::task::JoinHandle<Result<(), Error>>,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, flush) = ReplicaLogStore::open_with_flush(
            dir.path().join("replica-log"),
            CancellationToken::new(),
        )
        .await
        .expect("open_with_flush");
        let clock = Arc::new(HlcClock::with_time_source(
            max_skew,
            Box::new(move || clock_ms),
        ));
        let raft_state = RaftStateHandle::new();
        raft_state.replace(RaftState {
            replica: ReplicaMembership::single(replicas.iter().copied().collect()),
            ..RaftState::empty()
        });
        let cutoff = Arc::new(HlcGate::new());
        let (_tx, rx) = watch::channel(None);
        let node = Arc::new(ReplicaService::new(
            store,
            cutoff,
            self_id,
            clock,
            Arc::new(raft_state.clone()),
            rx,
            crate::unit::nonzero(2048),
            crate::network::static_peers(Vec::new()),
        ));
        (node, raft_state, dir, flush)
    }

    // ── `Replica::write` contract (see the trait doc) ─────────────────────

    /// Closing the input drains every write already accepted *before* the output
    /// ends — the flush loop's completion holds a sender clone, so the commit
    /// channel outlives the input.
    #[tokio::test]
    async fn invariant_write_drains_before_ending() {
        let me = nid(1);
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[me], 1_000, Duration::from_secs(10)).await;

        let (tx, rx) = crossbeam_channel::unbounded();
        let outcomes = node.write(Channels::closed(rx));
        for i in 0..8u8 {
            tx.send(StampedWrite::now(make_data(i, 1_000, b"payload")))
                .expect("send");
        }
        drop(tx); // ends the input

        let collected =
            tokio::task::spawn_blocking(move || outcomes.into_iter().collect::<Vec<_>>())
                .await
                .expect("join");
        assert_eq!(collected.len(), 8, "every accepted write is answered");
        assert!(
            collected
                .iter()
                .all(|o| matches!(o, Ok(WriteOutcome::Accepted { .. }))),
            "all writes accepted: {collected:?}"
        );
    }

    /// Dropping the output tears the pipeline down: the producer's sends fail
    /// because the input receiver was dropped with it.
    #[tokio::test]
    async fn invariant_dropping_write_output_stops_the_producer() {
        let me = nid(1);
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[me], 1_000, Duration::from_secs(10)).await;

        let (tx, rx) = crossbeam_channel::unbounded();
        drop(node.write(Channels::closed(rx)));

        // The output (and with it the input receiver) is gone, so the very next
        // send must fail.
        for i in 0..=u8::MAX {
            if tx
                .send(StampedWrite::now(make_data(i, 1_000, b"payload")))
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("producer kept accepting writes after the output was dropped");
    }

    /// Per-write failures stay inside `Ok(WriteOutcome)`; a rejected write does
    /// not end the stream or stop the writes behind it.
    #[tokio::test]
    async fn invariant_write_rejection_is_not_terminal() {
        let me = nid(1);
        // Not in the replica set: every write is rejected with `NotAReplica`.
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[nid(2)], 1_000, Duration::from_secs(10)).await;

        let (tx, rx) = crossbeam_channel::unbounded();
        let outcomes = node.write(Channels::closed(rx));
        for i in 0..4u8 {
            tx.send(StampedWrite::now(make_data(i, 1_000, b"payload")))
                .expect("send");
        }
        drop(tx);

        let collected =
            tokio::task::spawn_blocking(move || outcomes.into_iter().collect::<Vec<_>>())
                .await
                .expect("join");
        assert_eq!(collected.len(), 4, "a rejection does not end the stream");
        assert!(
            collected.iter().all(|o| matches!(
                o,
                Ok(WriteOutcome::Rejected {
                    error: Error::NotAReplica { .. },
                    ..
                })
            )),
            "rejections ride in Ok, not Err: {collected:?}"
        );
    }

    #[tokio::test]
    async fn write_rejected_when_not_a_replica() {
        let me = nid(1);
        // Replica set contains other nodes, not `me`.
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[nid(2), nid(3)], 1_000, Duration::from_secs(10)).await;
        let err = crate::tests::common::replica_write(&*node, make_data(0, 1_000, b"payload"))
            .await
            .expect_err("must reject");
        assert!(matches!(err, Error::NotAReplica { node_id } if node_id == me));
    }

    #[tokio::test]
    async fn write_accepted_when_in_replica_set() {
        let me = nid(1);
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[me, nid(2), nid(3)], 1_000, Duration::from_secs(10)).await;
        let st = crate::tests::common::replica_write(&*node, make_data(0, 1_000, b"payload"))
            .await
            .expect("write accepted");
        assert!(st.replica.current().contains(&me));
        assert_eq!(node.replica_log.frontier(), Some(LogIndex::ZERO));
    }

    // Skew is checked at RPC ingress against the fresh per-RPC envelope HLC, not
    // here against a write's creation HLC — that would conflate in-flight latency
    // with clock skew. See
    // `clock_skew_tests::peer_clock_diverged_beyond_drift_limit_fails_integrate`.

    /// F-3 invariant: a write at exactly the current cutoff is rejected.
    /// The covering snapshot's catch-up has already run, so an admitted
    /// boundary write would be ACKed but never reach any snapshot.
    #[tokio::test]
    async fn invariant_replica_rejects_write_at_cutoff() {
        let me = nid(1);
        let (node, _raft_state, _dir, _flush) =
            replica_with(me, &[me], 1_000, Duration::from_secs(10)).await;
        assert!(
            node.hlc_gate.set_cutoff(Hlc(1_000)).await,
            "advance from None"
        );

        let err = crate::tests::common::replica_write(&*node, make_data(0, 1_000, b"payload"))
            .await
            .expect_err("== cutoff must reject");
        assert!(matches!(
            err,
            Error::HlcCutoff(crate::hlc::CutoffError::AtOrBeforeCutoff { .. })
        ));

        // Strictly above the cutoff is accepted.
        crate::tests::common::replica_write(&*node, make_data(0, 1_001, b"payload"))
            .await
            .expect("> cutoff accepted");
    }

    #[tokio::test]
    async fn write_accepted_when_also_in_joint_next_set() {
        let me = nid(1);
        // `me` only in the next (joint) set, not the current set.
        let (node, raft_state, _dir, _flush) =
            replica_with(me, &[nid(2)], 1_000, Duration::from_secs(10)).await;
        raft_state.replace(RaftState {
            replica: ReplicaMembership::joint(
                [nid(2)].into_iter().collect(),
                [me, nid(2)].into_iter().collect(),
            ),
            ..RaftState::empty()
        });
        crate::tests::common::replica_write(&*node, make_data(0, 1_000, b"payload"))
            .await
            .expect("joint-set member may write");
    }
}
