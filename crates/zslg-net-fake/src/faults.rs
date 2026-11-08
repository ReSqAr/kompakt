//! Fault-injection decorators at the `Replica` seam and a static peer router.
//!
//! Two fault modes: `fail_next(n)` (the next n calls error like a dropped RPC)
//! and `wedge()`/`unwedge()` (calls hang until released — a slow replica).
//! `DuplicateReplica` sends each call several times, concurrently.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use crossbeam_combinators::{Channel, Finite, Infinite};
use futures::stream::BoxStream;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::watch;
use zslg_consensus::{NodeId, ReplicaStatus};
use zslg_log::{DataId, Error, LogIndex, Replica, StampedWrite, WriteOutcome};
use zslg_net::{NetError, PeerAddr, Router};

use crate::replica_only;

/// Control handle for a [`FaultyReplica`]. Clone freely; clones share state.
#[derive(Clone)]
pub struct FaultHandle {
    fail_next: Arc<AtomicU32>,
    wedged: watch::Sender<bool>,
}

impl Default for FaultHandle {
    fn default() -> Self {
        let (wedged, _) = watch::channel(false);
        Self {
            fail_next: Arc::new(AtomicU32::new(0)),
            wedged,
        }
    }
}

impl FaultHandle {
    /// Make the next `n` calls fail with an injected RPC error.
    pub fn fail_next(&self, n: u32) {
        self.fail_next.store(n, Ordering::SeqCst);
    }

    /// Wedge the replica: calls block until [`Self::unwedge`].
    pub fn wedge(&self) {
        self.wedged.send_replace(true);
    }

    /// Release wedged calls.
    pub fn unwedge(&self) {
        self.wedged.send_replace(false);
    }
}

/// Wraps any [`Replica`], applying the faults of its [`FaultHandle`] before
/// delegating.
pub struct FaultyReplica {
    inner: Arc<dyn Replica>,
    faults: FaultHandle,
}

impl FaultyReplica {
    pub fn new(inner: Arc<dyn Replica>) -> (Arc<Self>, FaultHandle) {
        let faults = FaultHandle::default();
        let replica = Arc::new(Self {
            inner,
            faults: faults.clone(),
        });
        (replica, faults)
    }

    async fn gate(&self) -> Result<(), Error> {
        let mut wedged = self.faults.wedged.subscribe();
        wedged
            .wait_for(|w| !*w)
            .await
            .map_err(|_| Error::Net(NetError::Rpc("fault handle dropped while wedged".into())))?;
        let injected = self
            .faults
            .fail_next
            // decrement one failure budget; Err means it was already 0
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if injected {
            return Err(Error::Net(NetError::Rpc("injected fault".into())));
        }
        Ok(())
    }
}

#[async_trait]
impl Replica for FaultyReplica {
    /// Splits the input: writes that hit an injected fault are answered directly,
    /// the rest are forwarded to the inner replica's own stream. Both outcome
    /// sources are merged onto one channel by a forwarder thread, so a wedged or
    /// failing write never blocks the ones behind it.
    fn write(
        &self,
        input: Channel<StampedWrite, Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
        let run = || -> Channel<Result<WriteOutcome, Error>, Infinite> {
            let (out_tx, out_rx) = crossbeam_channel::unbounded::<Result<WriteOutcome, Error>>();

            let faults = self.faults.clone();
            let inner = self.inner.clone();
            // Fault injection is about outcomes, not timing: drop the stamp here
            // and re-stamp when handing on to the inner replica.
            let mut input = input.filter_map(|s| Some(s.data));
            std::thread::spawn(move || {
                // Opened on the first write that actually reaches the inner replica —
                // some stubs (`ReadyReplica`) serve no writes at all, so a stream
                // opened eagerly would panic on a run where every write is faulted.
                let mut inner_tx: Option<crossbeam_channel::Sender<StampedWrite>> = None;
                let mut open_inner = || {
                    inner_tx
                        .get_or_insert_with(|| {
                            let (tx, rx) = crossbeam_channel::unbounded::<StampedWrite>();
                            let mut outcomes = inner.write(Channel::closed(rx));
                            let fwd_tx = out_tx.clone();
                            std::thread::spawn(move || {
                                while let Ok(outcome) = outcomes.recv() {
                                    if fwd_tx.send(outcome).is_err() {
                                        return;
                                    }
                                }
                            });
                            tx
                        })
                        .clone()
                };
                while let Ok(data) = input.recv() {
                    let data_id = data.data_id;
                    // `wedged` is a watch channel (async); this thread is not on a
                    // reactor, so block on the change notification directly.
                    let mut wedged = faults.wedged.subscribe();
                    let waited = futures::executor::block_on(wedged.wait_for(|w| !*w));
                    if waited.is_err() {
                        let _ = out_tx.send(Ok(WriteOutcome::Failed {
                            data_id,
                            error: Error::Net(NetError::Rpc(
                                "fault handle dropped while wedged".into(),
                            )),
                        }));
                        continue;
                    }
                    let injected = faults
                        .fail_next
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                    let sent = if injected {
                        out_tx
                            .send(Ok(WriteOutcome::Failed {
                                data_id,
                                error: Error::Net(NetError::Rpc("injected fault".into())),
                            }))
                            .is_ok()
                    } else {
                        open_inner().send(StampedWrite::now(data)).is_ok()
                    };
                    if !sent {
                        return;
                    }
                }
            });

            Channel::from_infinite(out_rx, "faulty replica")
        };
        let result: Channel<Result<WriteOutcome, Error>, Infinite> = run();
        result.err_into()
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        self.inner.subscribe_from(from)
    }
    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        self.gate().await?;
        self.inner.fetch_status().await
    }
    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        self.gate().await?;
        self.inner.subscribe_status().await
    }
}

/// Static per-node replica routing for tests; panics on unknown peers.
pub struct MapPeerRouter {
    pub replicas: BTreeMap<NodeId, Arc<dyn Replica>>,
}

impl Router for MapPeerRouter {
    fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

    fn remote(&self, node_id: NodeId) -> Arc<dyn zslg_net::ReplicaClient> {
        replica_only(
            self.replicas
                .get(&node_id)
                .unwrap_or_else(|| panic!("no replica registered for {node_id:?}"))
                .clone(),
        )
    }

    fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn zslg_net::ReplicaClient> {
        unimplemented!("MapPeerRouter: direct dial not used")
    }
}

impl MapPeerRouter {
    /// Wrap every replica in the map with a [`FaultyReplica`], returning the
    /// wrapped router and per-node [`FaultHandle`]s.
    #[must_use]
    pub fn wrap_all_faulty(
        replicas: BTreeMap<NodeId, Arc<dyn Replica>>,
    ) -> (Self, BTreeMap<NodeId, FaultHandle>) {
        let mut wrapped = BTreeMap::new();
        let mut handles = BTreeMap::new();
        for (id, replica) in replicas {
            let (faulty, handle) = FaultyReplica::new(replica);
            wrapped.insert(id, faulty as Arc<dyn Replica>);
            handles.insert(id, handle);
        }
        (Self { replicas: wrapped }, handles)
    }
}

// ── Duplicated RPC injection ───────────────────────────────────────────

struct DuplicationState {
    /// How many more calls to duplicate.
    remaining: u32,
    /// How many times to send each duplicated call (concurrently).
    multiplier: u32,
    /// When true, ALL calls are duplicated (overrides remaining/multiplier).
    duplicate_all: bool,
}

/// Cloneable handle for a [`DuplicateReplica`].
#[derive(Clone)]
pub struct DuplicateHandle {
    state: Arc<SyncMutex<DuplicationState>>,
}

impl DuplicateHandle {
    /// The next `count` calls are sent `multiplier` times each.
    pub fn duplicate_next(&self, count: u32, multiplier: u32) {
        let mut s = self.state.lock();
        s.remaining = count;
        s.multiplier = multiplier;
    }

    /// When `enabled`, every call is duplicated (overrides `duplicate_next`).
    pub fn set_duplicate_all(&self, enabled: bool) {
        self.state.lock().duplicate_all = enabled;
    }

    fn should_duplicate(&self) -> Option<u32> {
        let mut s = self.state.lock();
        if s.duplicate_all {
            return Some(2);
        }
        if s.remaining > 0 {
            s.remaining = s.remaining.saturating_sub(1);
            let m = s.multiplier;
            if m > 1 {
                return Some(m);
            }
        }
        None
    }
}

impl Default for DuplicateHandle {
    fn default() -> Self {
        Self {
            state: Arc::new(SyncMutex::new(DuplicationState {
                remaining: 0,
                multiplier: 0,
                duplicate_all: false,
            })),
        }
    }
}

/// Wraps a [`Replica`] and duplicates calls according to its
/// [`DuplicateHandle`]. Duplicated calls happen concurrently; the first
/// `Ok` result is returned (others are discarded).
pub struct DuplicateReplica {
    inner: Arc<dyn Replica>,
    dup: DuplicateHandle,
}

impl DuplicateReplica {
    pub fn new(inner: Arc<dyn Replica>) -> (Arc<Self>, DuplicateHandle) {
        let dup = DuplicateHandle::default();
        let replica = Arc::new(Self {
            inner,
            dup: dup.clone(),
        });
        (replica, dup)
    }
}

/// Per-`data_id` state while a duplicated write's N outcomes come back: how many
/// are still outstanding, and the best one seen so far.
type DupExpectations = Arc<SyncMutex<BTreeMap<DataId, (u32, Option<WriteOutcome>)>>>;

#[async_trait]
impl Replica for DuplicateReplica {
    /// Feeds N copies of each write (same `data_id`) into the inner replica —
    /// exercising the resend path — then collapses the N outcomes back to one: the
    /// `Accepted` if there is one, else the first. (With dedup removed each copy now
    /// simply appends and is `Accepted`; the collapse still yields one.)
    fn write(
        &self,
        input: Channel<StampedWrite, Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
        let run = || -> Channel<Result<WriteOutcome, Error>, Infinite> {
            let (out_tx, out_rx) = crossbeam_channel::unbounded::<Result<WriteOutcome, Error>>();
            let (inner_tx, inner_rx) = crossbeam_channel::unbounded::<StampedWrite>();
            let mut outcomes = self.inner.write(Channel::closed(inner_rx));

            // How many outcomes are still expected per data_id, and the best one seen
            // so far. Only shared between this thread and the feeder below.
            let expected: DupExpectations = Arc::new(SyncMutex::new(BTreeMap::new()));

            let dup = self.dup.clone();
            let expected_feed = expected.clone();
            let mut input = input;
            std::thread::spawn(move || {
                while let Ok(stamped) = input.recv() {
                    let n = dup.should_duplicate().unwrap_or(1);
                    if n > 1 {
                        expected_feed.lock().insert(stamped.data.data_id, (n, None));
                    }
                    for _ in 0..n {
                        // Each copy is stamped afresh: a duplicate is a distinct
                        // arrival, so sharing one enqueue time would misreport the
                        // residency of every copy after the first.
                        if inner_tx
                            .send(StampedWrite::now(stamped.data.clone()))
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            });

            std::thread::spawn(move || {
                while let Ok(outcome) = outcomes.recv() {
                    let Ok(outcome) = outcome else {
                        let _ = out_tx.send(outcome);
                        return; // terminal
                    };
                    let mut guard = expected.lock();
                    let Some((remaining, best)) = guard.get_mut(&outcome.data_id()) else {
                        // Not duplicated — pass straight through.
                        drop(guard);
                        if out_tx.send(Ok(outcome)).is_err() {
                            return;
                        }
                        continue;
                    };
                    *remaining = remaining.saturating_sub(1);
                    let accepted = matches!(outcome, WriteOutcome::Accepted { .. });
                    if accepted || best.is_none() {
                        *best = Some(outcome);
                    }
                    if *remaining > 0 {
                        continue;
                    }
                    let data_id = best.as_ref().map(WriteOutcome::data_id);
                    let chosen = data_id.and_then(|id| guard.remove(&id).and_then(|(_, b)| b));
                    drop(guard);
                    if let Some(chosen) = chosen
                        && out_tx.send(Ok(chosen)).is_err()
                    {
                        return;
                    }
                }
            });

            Channel::from_infinite(out_rx, "faulty replica")
        };
        let result: Channel<Result<WriteOutcome, Error>, Infinite> = run();
        result.err_into()
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        self.inner.subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        let result: Result<ReplicaStatus, Error> = self.inner.fetch_status().await;
        result
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        self.inner.subscribe_status().await
    }
}
