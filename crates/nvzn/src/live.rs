//! Live views: the folded state plus the few `S` events this node has received
//! above it, readable at once, without waiting for the fold.
//!
//! ```text
//!   NotStarted ──(the fold has caught up: Syncer::sync, retried)──▶ begin
//!   unstable tail ──▶ integrator: keep the S events ──▶ Coverage { held }
//!   stable log (once, at start) ──▶ scan ──────────────────┘     │
//!   fold advanced ──▶ retire: drop what F covers ────────────────┤
//!   snapshot(): lock, store @F, the uncovered held events ───────┘
//!               folded through the live projections into a scratch transaction
//! ```
//!
//! `S` is a sub-event set (a subset enum of `define_events!`). A projection is
//! *live* under it when every event it folds is in `S` ([`is_live`]); a live
//! view folds exactly the registered projections that are, and a snapshot
//! reads them as store@F plus the held events, and every other projection at
//! F. The unstable tail is neither complete nor ordered, so a live view never
//! "catches up": its floor is the stable snapshot at the same F, and what it
//! never received shows when F passes it. Held events are retired when F
//! covers them, not at zslg's watermark: the fold stores an entry later than
//! zslg calls it stable.
//!
//! A view starts only once the fold has replayed the log: the dispatcher's
//! finalise may reset the store and fold again, and a view begun before that
//! would hold events above a watermark the store no longer has. Until then it
//! serves the stable snapshot. [`Live::wait_for`] is the read-after-write
//! question: does a snapshot taken now serve this entry?
//!
//! Spec: nlnr `docs/logs/2026-10-26_spec_nvzn_live_views.md`; the deferred
//! start and `wait_for`: nlnr `docs/logs/2026-10-28_design_read_after_write.md`.

use crate::dispatcher::FoldState;
use crate::events::{contains, subset};
use crate::ids::APP_ID_LEN;
use crate::metrics::{self, ReadKind, StopReason};
use crate::projection::{BoxedProjectorLike, fold_event};
use crate::{
    ArcStore, Error, EventSet, Projection, ProjectionTypeId, Snapshot, Tenant, TenantId, Watermark,
    WithWireFormat, WriteTransactionImpl,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{error, info, warn};
use zslg::{ChannelEnd, Channels, Data, DataId, Hlc, LogIndex, StableLogEvent, UnstableEvent};

/// Whether `P` is live under `S`: every event `P` folds is in `S`. A `const
/// fn`, so it answers at run time and in a `const` assertion
/// ([`assert_live!`](crate::assert_live)).
#[must_use]
pub const fn is_live<P: Projection, S: EventSet>() -> bool {
    subset(P::EVENTS, S::EVENTS)
}

/// Pins the projections an app relies on being live under a sub-event set:
/// a build error naming the first that is not, because one of its events is
/// outside the set. The regression it catches is a live projection silently
/// turning stale when someone adds a bulk event to its subset.
///
/// ```
/// # use serde::{Deserialize, Serialize};
/// # use nvzn::{Error, Projection, ProjectionTypeId, Ref, View, Write};
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct NoteSetV1 { id: u128 }
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct FileSeenV1 { id: u128 }
/// nvzn::define_events! {
///     event::Event = {
///         event 1 := NoteSet { version 1 => NoteSetV1 }
///         event 2 := FileSeen { version 1 => FileSeenV1 }
///     } subsets {
///         LiveEvent := NoteSet,
///         NoteEvent := NoteSet,
///         MixedEvent := NoteSet | FileSeen,
///     }
/// }
/// # macro_rules! projection { ($name:ident, $ev:ident, $n:literal) => {
/// #     #[derive(Debug, Serialize, Deserialize, Clone)] struct $name { id: u128 }
/// #     impl Projection for $name {
/// #         type Event<'a> = event::$ev<'a>;
/// #         const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash(stringify!($name), $n);
/// #         fn id(&self) -> Ref<Self> { Ref::new(nvzn::ProjectionId::from_u128(self.id)) }
/// #         fn next(_: &event::$ev, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> { Ok(vec![]) }
/// #     }
/// # } }
/// // `Note` folds `NoteSet` only; `Mixed` also folds `FileSeen`.
/// # projection!(Note, NoteEvent, 0);
/// # projection!(Mixed, MixedEvent, 0);
/// nvzn::assert_live!(event::LiveEvent<'static>, [Note]);
/// # fn main() {}
/// ```
///
/// ```compile_fail
/// # use serde::{Deserialize, Serialize};
/// # use nvzn::{Error, Projection, ProjectionTypeId, Ref, View, Write};
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct NoteSetV1 { id: u128 }
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct FileSeenV1 { id: u128 }
/// nvzn::define_events! {
///     event::Event = {
///         event 1 := NoteSet { version 1 => NoteSetV1 }
///         event 2 := FileSeen { version 1 => FileSeenV1 }
///     } subsets {
///         LiveEvent := NoteSet,
///         NoteEvent := NoteSet,
///         MixedEvent := NoteSet | FileSeen,
///     }
/// }
/// # macro_rules! projection { ($name:ident, $ev:ident, $n:literal) => {
/// #     #[derive(Debug, Serialize, Deserialize, Clone)] struct $name { id: u128 }
/// #     impl Projection for $name {
/// #         type Event<'a> = event::$ev<'a>;
/// #         const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash(stringify!($name), $n);
/// #         fn id(&self) -> Ref<Self> { Ref::new(nvzn::ProjectionId::from_u128(self.id)) }
/// #         fn next(_: &event::$ev, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> { Ok(vec![]) }
/// #     }
/// # } }
/// // `Note` folds `NoteSet` only; `Mixed` also folds `FileSeen`.
/// # projection!(Note, NoteEvent, 0);
/// # projection!(Mixed, MixedEvent, 0);
/// nvzn::assert_live!(event::LiveEvent<'static>, [Note, Mixed]);
/// # fn main() {}
/// ```
#[macro_export]
macro_rules! assert_live {
    ($s:ty, [$($p:ty),* $(,)?]) => {
        $(
            const _: () = ::core::assert!(
                $crate::is_live::<$p, $s>(),
                ::core::concat!(
                    ::core::stringify!($p),
                    " is not live under ",
                    ::core::stringify!($s),
                    ": it folds an event outside it"
                )
            );
        )*
    };
}

/// What a live view may hold before it overflows (spec §4).
#[derive(Debug, Clone, Copy)]
pub struct LiveLimits {
    pub max_held_events: usize,
    pub max_held_bytes: usize,
}

impl Default for LiveLimits {
    /// Far above what people's actions produce in one fold lag; reached only
    /// by a flood or a fold stalled for long.
    fn default() -> Self {
        Self {
            max_held_events: 10_000,
            max_held_bytes: 16 * 1024 * 1024,
        }
    }
}

/// A live view over one tenant: see the module docs. Cheap to clone; one per
/// process, handed to whoever reads.
pub struct Live<Ev> {
    inner: Arc<Inner<Ev>>,
}

impl<Ev> Clone for Live<Ev> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<Ev> {
    tenant: Tenant,
    store: ArcStore,
    fold_state: FoldState,
    /// The live projections' own projectors (the fold keeps its own).
    projectors: BTreeMap<ProjectionTypeId, BoxedProjectorLike<Ev>>,
    /// `S`'s event numbers.
    events: &'static [u32],
    limits: LiveLimits,
    state: Mutex<State<Ev>>,
    /// Signalled when the cold-start scan ends (or overflows), for the
    /// snapshots waiting on it.
    scanned: Condvar,
    changed: watch::Sender<u64>,
    /// Moves after every retirement (F reached the state): what
    /// [`Live::wait_for`] wakes on for an entry only F shows.
    retired: watch::Sender<u64>,
}

struct State<Ev> {
    coverage: Coverage<Ev>,
    /// The store's watermark at the last retirement: an arrival it covers is
    /// already in the store.
    folded: Watermark,
}

/// One held write: its `S` events, decoded once, and its payload size.
struct Entry<Ev> {
    events: Arc<Vec<Ev>>,
    bytes: usize,
}

/// Keyed by `DataId`: that is the fold's order, whatever order they arrived in.
type Held<Ev> = BTreeMap<DataId, Entry<Ev>>;

enum Coverage<Ev> {
    /// Waiting for the fold to replay the log (`Reader::live` defers the
    /// start): nothing held, snapshot = the stable snapshot.
    NotStarted,
    /// Cold start: the stable-log scan fills `held` up to the stable
    /// watermark at start. Snapshots wait for it.
    Scanning { held: Held<Ev>, bytes: usize },
    /// Snapshot = store@F ⊕ held.
    Complete { held: Held<Ev>, bytes: usize },
    /// The cap was hit. `until` is the newest `DataId` dropped, frozen: it moves
    /// only if the cap is hit again. `held` collects arrivals above it, not
    /// served; snapshot = store@F only. Once F covers `until` — and the
    /// cold-start scan, if it was running, has read to its end — `Complete`
    /// with what is held: everything older is then folded, held, or dropped
    /// below `until`, so nothing is served ahead of an event it depends on.
    Overflowed {
        until: DataId,
        held: Held<Ev>,
        bytes: usize,
        /// The scan was running at the overflow and has not finished: what it
        /// has not read yet is neither held nor folded, so no recovery yet.
        scan: bool,
    },
}

impl<Ev> Coverage<Ev> {
    fn name(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Scanning { .. } => "scanning",
            Self::Complete { .. } => "complete",
            Self::Overflowed { .. } => "overflowed",
        }
    }

    /// Held entries and their bytes.
    fn held(&self) -> (usize, usize) {
        match self {
            Self::NotStarted => (0, 0),
            Self::Scanning { held, bytes }
            | Self::Complete { held, bytes }
            | Self::Overflowed { held, bytes, .. } => (held.len(), *bytes),
        }
    }
}

fn add(bytes: usize, more: usize) -> usize {
    // Saturating: a byte estimate held against a cap; past usize::MAX it is
    // over any cap, which is all the number is for.
    bytes.saturating_add(more)
}

/// Drop what `f` covers from `held`, and the bytes with it.
fn retain_uncovered<Ev>(held: &mut Held<Ev>, bytes: &mut usize, f: &Watermark) {
    held.retain(|id, entry| {
        let keep = !f.covers(*id);
        if !keep {
            // Saturating: the entries' sizes sum to `bytes`, so this never
            // goes below zero; saturating keeps it that way if they drift.
            *bytes = bytes.saturating_sub(entry.bytes);
        }
        keep
    });
}

impl<Ev: WithWireFormat> Inner<Ev> {
    fn lock(&self) -> MutexGuard<'_, State<Ev>> {
        // A panic under the lock leaves a map half-retained at worst: still a
        // set of held writes, so poisoning is ignored.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn over_cap(&self, events: usize, bytes: usize) -> bool {
        events > self.limits.max_held_events || bytes > self.limits.max_held_bytes
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn publish(&self, state: &State<Ev>) {
        let (held, bytes) = state.coverage.held();
        metrics::live_held(self.tenant, held, bytes);
        metrics::live_coverage(self.tenant, state.coverage.name());
    }

    /// One write from the tail or the scan: keep its `S` events, if any.
    /// Whether what a snapshot serves moved (an `S` event that will be shown).
    fn arrive(&self, data: Data) -> bool {
        let Data { data_id, blob } = data;
        let Some((tenant_id, payload)) = blob.split_first_chunk::<APP_ID_LEN>() else {
            metrics::live_undecodable(self.tenant);
            warn!(data_id = ?data_id, "live view: a payload too short for a tenant id; skipped");
            return false;
        };
        if TenantId::from_slice(*tenant_id) != self.tenant.id() {
            return false;
        }
        let events = match Ev::decode(payload) {
            Ok(events) => events,
            Err(e) => {
                metrics::live_undecodable(self.tenant);
                warn!(data_id = ?data_id, error = %e, "live view: an undecodable payload; skipped");
                return false;
            }
        };
        let kept: Vec<Ev> = events
            .into_iter()
            .filter(|e| contains(self.events, e.number()))
            .collect();
        if kept.is_empty() {
            return false;
        }
        let entry = Entry {
            events: Arc::new(kept),
            bytes: blob.len(),
        };

        let mut state = self.lock();
        let served = self.insert(&mut state, data_id, entry);
        self.publish(&state);
        drop(state);
        if served {
            self.bump();
        }
        served
    }

    fn insert(&self, state: &mut State<Ev>, id: DataId, entry: Entry<Ev>) -> bool {
        if state.folded.covers(id) {
            return false;
        }
        let over = match &mut state.coverage {
            // The tail is opened at the start: nothing arrives before it.
            Coverage::NotStarted => return false,
            Coverage::Scanning { held, bytes } | Coverage::Complete { held, bytes } => {
                if held.contains_key(&id) {
                    return false;
                }
                *bytes = add(*bytes, entry.bytes);
                held.insert(id, entry);
                if !self.over_cap(held.len(), *bytes) {
                    return true;
                }
                held.last_key_value().map(|(newest, _)| *newest)
            }
            Coverage::Overflowed {
                until, held, bytes, ..
            } => {
                if id <= *until || held.contains_key(&id) {
                    return false;
                }
                *bytes = add(*bytes, entry.bytes);
                held.insert(id, entry);
                if self.over_cap(held.len(), *bytes)
                    && let Some((newest, _)) = held.last_key_value()
                {
                    *until = *newest;
                    held.clear();
                    *bytes = 0;
                    warn!(until = ?until, "live view: over the cap again; still serving the fold's state alone");
                }
                return false;
            }
        };
        if let Some(until) = over {
            let was = state.coverage.name();
            state.coverage = Coverage::Overflowed {
                until,
                held: Held::new(),
                bytes: 0,
                scan: matches!(state.coverage, Coverage::Scanning { .. }),
            };
            warn!(
                tenant = self.tenant.name(),
                was,
                until = ?until,
                max_held_events = self.limits.max_held_events,
                max_held_bytes = self.limits.max_held_bytes,
                "live view overflowed: serving the fold's state alone until it passes what was dropped"
            );
            // The scan reads on to its end; its waiters go on now, served
            // the fold's state alone.
            self.scanned.notify_all();
        }
        false
    }

    /// The fold reached `f`: drop what it covers. Whether what a snapshot
    /// serves moved (an overflow ended).
    fn retire(&self, state: &mut State<Ev>, f: Watermark) -> bool {
        let recovered = match &mut state.coverage {
            Coverage::NotStarted => false,
            Coverage::Scanning { held, bytes } | Coverage::Complete { held, bytes } => {
                retain_uncovered(held, bytes, &f);
                false
            }
            Coverage::Overflowed {
                until,
                held,
                bytes,
                scan,
            } => {
                retain_uncovered(held, bytes, &f);
                !*scan && f.covers(*until)
            }
        };
        if recovered
            && let Coverage::Overflowed { held, bytes, .. } = std::mem::replace(
                &mut state.coverage,
                Coverage::Complete {
                    held: Held::new(),
                    bytes: 0,
                },
            )
        {
            info!(
                tenant = self.tenant.name(),
                held = held.len(),
                "live view recovered from the overflow"
            );
            state.coverage = Coverage::Complete { held, bytes };
        }
        state.folded = f;
        recovered
    }

    fn retire_now(&self) -> Result<(), Error> {
        let mut state = self.lock();
        let f = self.store.read()?.watermark()?;
        let recovered = self.retire(&mut state, f);
        self.publish(&state);
        drop(state);
        self.retired.send_modify(|v| *v = v.wrapping_add(1));
        if recovered {
            self.bump();
        }
        Ok(())
    }

    /// The scan is over: `Scanning` becomes `Complete`; an overflow during it
    /// may now recover, once F covers `until`.
    fn finish_scan(&self) {
        let mut state = self.lock();
        match std::mem::replace(
            &mut state.coverage,
            Coverage::Complete {
                held: Held::new(),
                bytes: 0,
            },
        ) {
            Coverage::Scanning { held, bytes } => {
                state.coverage = Coverage::Complete { held, bytes };
            }
            Coverage::Overflowed {
                until, held, bytes, ..
            } => {
                state.coverage = Coverage::Overflowed {
                    until,
                    held,
                    bytes,
                    scan: false,
                };
                let f = state.folded.clone();
                self.retire(&mut state, f);
            }
            other @ (Coverage::NotStarted | Coverage::Complete { .. }) => state.coverage = other,
        }
        self.publish(&state);
        drop(state);
        self.scanned.notify_all();
        self.bump();
    }
}

/// The integrator: the unstable tail, for as long as the process runs.
fn integrate<Ev: WithWireFormat>(
    inner: &Inner<Ev>,
    mut tail: Channels<Result<UnstableEvent, zslg::Error>>,
) {
    loop {
        match tail.recv() {
            Ok(Ok(UnstableEvent::Appended(data))) => {
                inner.arrive(data);
            }
            // Not a drop signal: an entry at or below it is stable, but the
            // fold may not have stored it yet. Retirement follows F.
            Ok(Ok(UnstableEvent::Watermark(_))) => {}
            Ok(Err(e)) => {
                error!(
                    tenant = inner.tenant.name(),
                    error = %e,
                    "live view: the unstable tail sent an error; stopping the dispatcher"
                );
                inner.fold_state.halt(StopReason::UnstableTailEnded);
                inner.scanned.notify_all();
                return;
            }
            Err(ChannelEnd::Cancelled) => return,
            Err(end) => {
                // zslg resyncs a slow consumer inside the stream, so an end is
                // the node's reader breaking; a live view that stopped
                // integrating would serve a stale world as fresh.
                error!(
                    tenant = inner.tenant.name(),
                    ?end,
                    "live view: the unstable tail ended; stopping the dispatcher"
                );
                inner.fold_state.halt(StopReason::UnstableTailEnded);
                inner.scanned.notify_all();
                return;
            }
        }
    }
}

/// The cold-start scan: the stable log from the store's watermark up to
/// `until`, the stable watermark when the view started. What it holds is
/// stable but not folded yet, and never comes through the tail.
fn scan<Ev: WithWireFormat>(
    inner: &Inner<Ev>,
    until: Hlc,
    mut log: Channels<Result<StableLogEvent, zslg::Error>>,
) {
    let started = Instant::now();
    let mut entries: u64 = 0;
    let ended_early = loop {
        match log.recv() {
            Ok(Ok(StableLogEvent::Appended { data, .. })) => {
                // The stable log is HLC-ordered: past `until`, done.
                if data.data_id.hlc() > until {
                    break false;
                }
                entries = entries.saturating_add(1);
                // Overflowed or not, the scan reads to its end: until it
                // has, an overflow cannot recover (`Coverage::Overflowed`).
                inner.arrive(data);
            }
            // Sent right after the replay (zslg `learner/read.rs`), so the
            // scan never waits for the next watermark advance.
            Ok(Ok(StableLogEvent::Watermark(h))) => {
                if h >= until {
                    break false;
                }
            }
            Ok(Err(e)) => {
                error!(error = %e, "live view: the cold-start scan read an error");
                break true;
            }
            Err(ChannelEnd::Cancelled) => break false,
            Err(end) => {
                error!(?end, "live view: the cold-start scan ended early");
                break true;
            }
        }
    };
    if ended_early {
        inner.fold_state.halt(StopReason::LiveScanEnded);
    }
    metrics::live_scan(entries, started.elapsed());
    info!(
        tenant = inner.tenant.name(),
        entries,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "live view: cold-start scan done"
    );
    inner.finish_scan();
}

/// A live view not started yet: see [`Coverage::NotStarted`]. [`begin`]
/// starts it.
pub(crate) fn not_started<Ev: WithWireFormat>(
    tenant: Tenant,
    store: ArcStore,
    fold_state: FoldState,
    projectors: BTreeMap<ProjectionTypeId, BoxedProjectorLike<Ev>>,
    events: &'static [u32],
    limits: LiveLimits,
) -> Result<Live<Ev>, Error> {
    let f = store.read()?.watermark()?;
    let (changed, _) = watch::channel(0);
    let (retired, _) = watch::channel(0);
    let inner = Arc::new(Inner {
        tenant,
        store,
        fold_state,
        projectors,
        events,
        limits,
        state: Mutex::new(State {
            coverage: Coverage::NotStarted,
            folded: f,
        }),
        scanned: Condvar::new(),
        changed,
        retired,
    });
    inner.publish(&inner.lock());
    Ok(Live { inner })
}

/// Start `live`: open the unstable tail, scan the stable log from the store's
/// watermark up to the stable one, and integrate from then on.
pub(crate) fn begin<Ev: WithWireFormat>(
    live: &Live<Ev>,
    reader: &Arc<dyn zslg::Reader>,
    cancelled: &zslg::Cancellation,
    mut folded: watch::Receiver<Option<Hlc>>,
) -> Result<(), Error> {
    let inner = &live.inner;
    // Tail first, then the stable watermark: nothing falls between them.
    let tail = reader.subscribe_unstable_tail().until(cancelled);
    let until = reader.stable_watermark();
    let f = inner.store.read()?.watermark()?;
    let from = LogIndex::next_after(f.index);
    {
        let mut state = inner.lock();
        state.coverage = match until {
            Some(_) => Coverage::Scanning {
                held: Held::new(),
                bytes: 0,
            },
            None => Coverage::Complete {
                held: Held::new(),
                bytes: 0,
            },
        };
        state.folded = f;
        inner.publish(&state);
    }
    info!(
        tenant = inner.tenant.name(),
        live_projections = inner.projectors.len(),
        scan_until = ?until,
        "live view started"
    );

    let integrator = Arc::clone(inner);
    tokio::task::spawn_blocking(move || integrate(&integrator, tail));

    if let Some(until) = until {
        let log = reader.subscribe_stable_log(from).until(cancelled);
        let scanner = Arc::clone(inner);
        tokio::task::spawn_blocking(move || scan(&scanner, until, log));
    }

    let retirer = Arc::clone(inner);
    tokio::spawn(async move {
        // Ends with the fold (its sender goes) or the runtime.
        while folded.changed().await.is_ok() {
            let retirer = Arc::clone(&retirer);
            match tokio::task::spawn_blocking(move || retirer.retire_now()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(error = %e, "live view: reading the store's watermark failed"),
                Err(e) => warn!(error = %e, "live view: the retirement task failed"),
            }
        }
    });

    // Started: a reader of the stable state until now looks again.
    inner.bump();
    Ok(())
}

/// The first wait between two attempts to start, doubling up to [`START_BACKOFF_MAX`].
const START_BACKOFF: Duration = Duration::from_millis(250);
const START_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Start `live` once the fold has caught up (`Syncer::sync`): the dispatcher's
/// finalise may reset the store and fold the log again, and a view started
/// before that would hold events above a watermark the store no longer has.
/// A failed sync is retried with backoff; the view stays `NotStarted` (and
/// serves the stable snapshot) only if the fold stops.
pub(crate) async fn begin_when_caught_up<Ev: WithWireFormat>(
    live: Live<Ev>,
    syncer: crate::Syncer,
    reader: Arc<dyn zslg::Reader>,
    cancelled: zslg::Cancellation,
) {
    let tenant = live.inner.tenant;
    let mut backoff = START_BACKOFF;
    loop {
        let attempt = match syncer.sync().await {
            Ok(()) => begin(&live, &reader, &cancelled, syncer.watermark()),
            Err(e) => Err(e),
        };
        let Err(e) = attempt else { return };
        if live.inner.fold_state.check().is_err() {
            warn!(tenant = tenant.name(), error = %e, "live view: not started, the fold stopped");
            return;
        }
        warn!(
            tenant = tenant.name(),
            error = %e,
            retry_in_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
            "live view: not started yet, the fold has not caught up"
        );
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2).min(START_BACKOFF_MAX);
    }
}

/// What [`Live::wait_for`] found: the entry, or the deadline. Why a view could
/// not serve it in time is the view's business; the caller goes on either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Wait {
    /// A snapshot taken now serves the entry: held, or folded.
    Seen,
    /// The deadline passed first: go on without it.
    TimedOut,
}

impl Wait {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Seen => "seen",
            Self::TimedOut => "timed_out",
        }
    }
}

impl<Ev: WithWireFormat> Live<Ev> {
    /// The live state: the store at the fold's watermark F, with the held `S`
    /// events F does not cover folded through the live projections. A live
    /// projection reads live; every other one reads at F, as the stable
    /// snapshot at that F would.
    ///
    /// Never waits on the fold. The one wait is the cold-start scan, once per
    /// view; overflowed, it serves store@F alone. Blocking (the scan wait, the
    /// fold of the held events): call it from blocking code.
    pub fn snapshot(&self) -> Result<Snapshot<'_>, Error> {
        let inner = &*self.inner;
        inner.fold_state.check()?;
        metrics::read(inner.tenant, ReadKind::Live);
        let started = Instant::now();

        let mut state = inner.lock();
        if matches!(state.coverage, Coverage::NotStarted) {
            drop(state);
            return Ok(Snapshot::from_txn(inner.tenant.id(), inner.store.read()?));
        }
        while matches!(state.coverage, Coverage::Scanning { .. }) {
            inner.fold_state.check()?;
            state = inner
                .scanned
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        inner.fold_state.check()?;
        // The store is opened under the lock: every retirement so far used an
        // F at or below this one, so `held` has everything this F does not
        // cover. Opened first, a retirement in between could drop entries the
        // older F still needs.
        let txn = inner.store.read()?;
        let f = txn.watermark()?;
        let recovered = inner.retire(&mut state, f);
        let pending: Vec<Arc<Vec<Ev>>> = match &state.coverage {
            Coverage::Complete { held, .. } => held
                .values()
                .map(|entry| Arc::clone(&entry.events))
                .collect(),
            Coverage::NotStarted | Coverage::Scanning { .. } | Coverage::Overflowed { .. } => {
                Vec::new()
            }
        };
        inner.publish(&state);
        drop(state);
        if recovered {
            inner.bump();
        }

        let mut scratch = WriteTransactionImpl::new(txn, &*inner.store);
        for events in &pending {
            for event in events.iter() {
                fold_event(&inner.projectors, inner.tenant, event, &mut scratch)?;
            }
        }
        metrics::live_materialise(pending.len(), started.elapsed());
        Ok(Snapshot::from_txn(inner.tenant.id(), Arc::new(scratch)))
    }

    /// Moves when an `S` event arrives (and when an overflow ends): the
    /// trigger for "a person did something". What the fold brings in is the
    /// stable change streams' business. A level, not a stream of edges: a
    /// consumer re-reads and re-tests, so a missed bump costs nothing.
    #[must_use]
    pub fn changed(&self) -> watch::Receiver<u64> {
        self.inner.changed.subscribe()
    }

    /// Wait until a snapshot taken now serves the entry `d`, for at most
    /// `timeout`. Whatever keeps the view from serving it meanwhile (not
    /// started yet, overflowed, the entry still on its way) is waited out
    /// alike: each ends with a bump of [`Self::changed`] or a retirement.
    ///
    /// ```text
    ///   F covers d, or held has d (scanning or complete) → Seen
    ///   else wake on an S arrival, a retirement, the start or a recovery;
    ///   at the deadline → TimedOut
    /// ```
    ///
    /// The retirement wake is for an entry that reached this node already
    /// stable: the local store does not publish it on the tail, so only F
    /// shows it.
    pub async fn wait_for(&self, d: DataId, timeout: Duration) -> Wait {
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        let mut arrived = self.inner.changed.subscribe();
        let mut retired = self.inner.retired.subscribe();
        loop {
            arrived.mark_unchanged();
            retired.mark_unchanged();
            if self.inner.serves(d) {
                return Wait::Seen;
            }
            tokio::select! {
                () = &mut deadline => {
                    return if self.inner.serves(d) { Wait::Seen } else { Wait::TimedOut };
                }
                r = arrived.changed() => if r.is_err() { return Wait::TimedOut },
                r = retired.changed() => if r.is_err() { return Wait::TimedOut },
            }
        }
    }
}

impl<Ev> Inner<Ev> {
    /// Whether a snapshot taken now serves the entry `d`.
    fn serves(&self, d: DataId) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match &state.coverage {
            Coverage::NotStarted | Coverage::Overflowed { .. } => false,
            _ if state.folded.covers(d) => true,
            Coverage::Scanning { held, .. } | Coverage::Complete { held, .. } => {
                held.contains_key(&d)
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::testsupport::{
        FLUSH_INTERVAL, NewTodoV1, NewUserV3, PingedV1, TestLog, Todo, TodoId, User, UserId,
        data_id, event, inmemory, payload, tenant, tenant_id,
    };
    use crate::{Dispatcher, Reader, Ref, Store};
    use bytes::Bytes;
    use proptest::prelude::*;
    use std::time::Duration;

    type Users = event::UserEvent<'static>;

    fn new_user(id: u128, name: &str) -> event::Event {
        event::Event::NewUser(NewUserV3 {
            id: UserId(id),
            name: name.to_string(),
            address: None,
            email: None,
        })
    }

    fn pinged(id: u128, ts: u128) -> event::Event {
        event::Event::Pinged(PingedV1 { id: UserId(id), ts })
    }

    fn new_todo(id: u128, user: u128) -> event::Event {
        event::Event::NewTodo(NewTodoV1 {
            id: TodoId(id),
            user_id: UserId(user),
            title: "t".to_string(),
        })
    }

    fn blob(events: &[event::Event]) -> Bytes {
        payload(tenant_id(), events)
    }

    /// A reader over `User` and `Todo` with no fold running: F moves only when
    /// a test commits a watermark ([`set_f`]).
    async fn rig() -> (Arc<dyn Store>, TestLog, Reader<event::Event>) {
        let store = inmemory();
        let log = TestLog::new();
        let mut dispatcher = Dispatcher::from_db(
            store.clone(),
            log.writer(),
            log.reader(),
            FLUSH_INTERVAL,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        let mut manager = dispatcher.new_handler::<event::Event>(tenant());
        manager.register::<User>().unwrap();
        manager.register::<Todo>().unwrap();
        let (_writer, reader) = manager.finalise().unwrap();
        (store, log, reader)
    }

    /// The store's watermark, as if the fold had committed through `did`.
    fn set_f(store: &Arc<dyn Store>, did: DataId) {
        store
            .write()
            .unwrap()
            .commit(Watermark {
                index: None,
                data_id: Some(did),
                hlc: Some(did.hlc()),
            })
            .unwrap();
    }

    async fn user(live: &Live<event::Event>, id: u128) -> Option<User> {
        let live = live.clone();
        tokio::task::spawn_blocking(move || {
            live.snapshot()
                .unwrap()
                .get(Ref::<User>::new(UserId(id).into()))
                .unwrap()
        })
        .await
        .unwrap()
    }

    async fn todo(live: &Live<event::Event>, id: u128) -> Option<Todo> {
        let live = live.clone();
        tokio::task::spawn_blocking(move || {
            live.snapshot()
                .unwrap()
                .get(Ref::<Todo>::new(TodoId(id).into()))
                .unwrap()
        })
        .await
        .unwrap()
    }

    fn held(live: &Live<event::Event>) -> usize {
        live.inner.lock().coverage.held().0
    }

    fn coverage(live: &Live<event::Event>) -> &'static str {
        live.inner.lock().coverage.name()
    }

    /// Waits until the integrator has taken in what the test pushed.
    async fn settle(live: &Live<event::Event>, pred: impl Fn(&Live<event::Event>) -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !pred(live) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the integrator caught up");
    }

    const SHORT: Duration = Duration::from_millis(50);
    const LONG: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn wait_for_sees_an_entry_once_held() {
        let (_store, mut log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        let d = log.push_unstable(blob(&[new_user(1, "holden")]));
        assert_eq!(live.wait_for(d, LONG).await, Wait::Seen);
        assert!(
            user(&live, 1).await.is_some(),
            "Seen: a snapshot now shows it"
        );
    }

    /// An entry that reached this node already stable is never held: only an
    /// F advance shows it, and the retirement wakes the wait.
    #[tokio::test]
    async fn wait_for_wakes_when_f_passes_the_entry() {
        let (store, _log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        let d = data_id(9, 9);
        let waiting = tokio::spawn({
            let live = live.clone();
            async move { live.wait_for(d, LONG).await }
        });
        tokio::time::sleep(SHORT).await;
        assert!(!waiting.is_finished());
        set_f(&store, d);
        // The retirer, as the fold's watermark watch would run it.
        let inner = Arc::clone(&live.inner);
        tokio::task::spawn_blocking(move || inner.retire_now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(waiting.await.unwrap(), Wait::Seen);
    }

    #[tokio::test]
    async fn wait_for_times_out_on_an_entry_that_never_arrives() {
        let (_store, _log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        assert_eq!(live.wait_for(data_id(5, 5), SHORT).await, Wait::TimedOut);
    }

    /// Overflowed, the view serves store@F alone: an entry it dropped is not
    /// served, and a wait runs to its deadline.
    #[tokio::test]
    async fn wait_for_times_out_while_overflowed() {
        let (_store, mut log, reader) = rig().await;
        let live = reader
            .live_started::<Users>(LiveLimits {
                max_held_events: 1,
                max_held_bytes: usize::MAX,
            })
            .unwrap();
        log.push_unstable(blob(&[new_user(1, "a")]));
        log.push_unstable(blob(&[new_user(2, "b")]));
        settle(&live, |l| coverage(l) == "overflowed").await;
        assert_eq!(
            live.wait_for(data_id(5, 5), Duration::from_millis(50))
                .await,
            Wait::TimedOut
        );
    }

    /// Not started yet: the stable snapshot is served, and a wait that ends
    /// before the start times out.
    #[tokio::test]
    async fn not_started_serves_the_stable_snapshot_and_a_wait_times_out() {
        let (store, mut log, reader) = rig().await;
        let live = reader.not_started::<Users>(LiveLimits::default()).unwrap();
        let holden = User {
            id: UserId(1),
            name: "holden".into(),
            address: None,
            email: None,
            last_ping: None,
        };
        crate::testsupport::commit_row(&store, tenant_id(), Ref::new(UserId(1).into()), &holden);
        let d = log.push_unstable(blob(&[new_user(2, "naomi")]));
        assert_eq!(
            live.wait_for(d, Duration::from_millis(50)).await,
            Wait::TimedOut
        );
        assert!(user(&live, 1).await.is_some(), "the stable row");
        assert!(
            user(&live, 2).await.is_none(),
            "nothing held before the start"
        );
    }

    /// A sync that fails at startup is retried: the view starts once one
    /// succeeds, rather than serving the stable snapshot for good.
    #[tokio::test]
    async fn the_start_retries_a_failed_sync() {
        let store = inmemory();
        let log = TestLog::new();
        let failing: Arc<dyn zslg::Reader> =
            Arc::new(crate::testsupport::FailingSyncReader::new(log.reader(), 2));
        let mut dispatcher = Dispatcher::from_db(
            store,
            log.writer(),
            failing,
            FLUSH_INTERVAL,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        let mut manager = dispatcher.new_handler::<event::Event>(tenant());
        manager.register::<User>().unwrap();
        let (writer, reader) = manager.finalise().unwrap();
        let live = reader.live::<Users>().unwrap();
        assert_eq!(coverage(&live), "not_started");
        // The fold runs: the start waits for it to pass the write below.
        let _fold = dispatcher.finalise().await.unwrap();
        // A write before the start: its wait waits for the start too.
        let d = writer.write(&[new_user(3, "amos")]).await.unwrap();
        assert_eq!(live.wait_for(d, LONG).await, Wait::Seen);
        assert_ne!(
            coverage(&live),
            "not_started",
            "started after two failed syncs"
        );
    }

    #[test]
    fn liveness_follows_the_events() {
        assert!(is_live::<User, Users>());
        assert!(!is_live::<Todo, Users>());
        assert!(
            is_live::<Todo, event::Event>(),
            "the whole set holds everything"
        );
        crate::assert_live!(event::UserEvent<'static>, [User]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_s_event_is_live_at_once_and_folds_only_into_live_projections() {
        let (_store, mut log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        let changed = live.changed();
        log.push_unstable(blob(&[new_user(1, "ada"), new_todo(7, 1)]));
        settle(&live, |l| held(l) == 1).await;

        assert_eq!(user(&live, 1).await.unwrap().name, "ada");
        assert!(
            todo(&live, 7).await.is_none(),
            "Todo folds NewTodo, outside S: it reads at F"
        );
        assert!(
            changed.has_changed().unwrap(),
            "an S arrival moves `changed`"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_without_s_events_is_not_held() {
        let (_store, mut log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        log.push_unstable(blob(&[new_todo(7, 1)]));
        log.push_unstable(blob(&[new_user(1, "ada")]));
        settle(&live, |l| held(l) == 1).await;
        assert_eq!(held(&live), 1, "only the write with an S event is held");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn held_events_retire_when_f_covers_them_not_at_the_watermark() {
        let (store, log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        let did = data_id(10, 1);
        log.push_unstable_at(did, blob(&[new_user(1, "ada")]));
        settle(&live, |l| held(l) == 1).await;

        log.watermark(20);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            user(&live, 1).await.is_some(),
            "zslg's watermark passing it does not drop it: the store does not have it"
        );

        // The fold passes it without storing it: it never converged.
        set_f(&store, did);
        assert!(user(&live, 1).await.is_none(), "F covers it: gone");
        assert_eq!(held(&live), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn at_an_hlc_tie_only_the_data_id_decides() {
        let (store, log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        let (first, second) = (data_id(10, 1), data_id(10, 2));
        log.push_unstable_at(first, blob(&[new_user(1, "ada")]));
        log.push_unstable_at(second, blob(&[new_user(2, "grace")]));
        settle(&live, |l| held(l) == 2).await;

        set_f(&store, first);
        assert!(user(&live, 1).await.is_none());
        assert!(
            user(&live, 2).await.is_some(),
            "same hlc as F, larger DataId: not covered"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn held_events_fold_in_data_id_order_whatever_their_arrival() {
        let (_store, log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        log.push_unstable_at(data_id(20, 2), blob(&[pinged(1, 99)]));
        log.push_unstable_at(data_id(10, 1), blob(&[new_user(1, "ada")]));
        settle(&live, |l| held(l) == 2).await;
        assert_eq!(user(&live, 1).await.unwrap().last_ping, Some(99));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_redelivery_is_held_once_and_announced_once() {
        let (_store, log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        let did = data_id(10, 1);
        log.push_unstable_at(did, blob(&[new_user(1, "ada")]));
        settle(&live, |l| held(l) == 1).await;
        let mut changed = live.changed();
        changed.mark_unchanged();
        assert!(!live.inner.arrive(Data {
            data_id: did,
            blob: blob(&[new_user(1, "ada")]),
        }));
        assert_eq!(held(&live), 1);
        assert!(!changed.has_changed().unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_f_advance_does_not_move_changed() {
        let (store, log, reader) = rig().await;
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        let did = data_id(10, 1);
        log.push_unstable_at(did, blob(&[new_user(1, "ada")]));
        settle(&live, |l| held(l) == 1).await;
        let mut changed = live.changed();
        changed.mark_unchanged();
        set_f(&store, did);
        let _ = user(&live, 1).await;
        assert!(!changed.has_changed().unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn overflow_serves_f_freezes_until_and_recovers_with_what_it_collected() {
        let (store, log, reader) = rig().await;
        let live = reader
            .live_started::<Users>(LiveLimits {
                max_held_events: 2,
                max_held_bytes: usize::MAX,
            })
            .unwrap();
        for (i, ms) in [10u64, 11, 12].into_iter().enumerate() {
            let id = u128::try_from(i).unwrap();
            log.push_unstable_at(data_id(ms, ms), blob(&[new_user(id, "x")]));
        }
        settle(&live, |l| coverage(l) == "overflowed").await;
        assert!(user(&live, 0).await.is_none(), "overflowed: store@F alone");
        let until = data_id(12, 12);

        // Below `until`: ignored. Above: held, not served.
        assert!(!live.inner.arrive(Data {
            data_id: data_id(9, 9),
            blob: blob(&[new_user(9, "old")]),
        }));
        log.push_unstable_at(data_id(13, 13), blob(&[new_user(13, "new")]));
        settle(&live, |l| held(l) == 1).await;
        assert_eq!(held(&live), 1);
        assert!(user(&live, 13).await.is_none());

        // F passes `until`: complete again, serving what it collected.
        let mut changed = live.changed();
        changed.mark_unchanged();
        set_f(&store, until);
        assert_eq!(user(&live, 13).await.unwrap().name, "new");
        assert_eq!(coverage(&live), "complete");
        assert!(changed.has_changed().unwrap(), "recovering moves `changed`");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hitting_the_cap_again_moves_until() {
        let (store, log, reader) = rig().await;
        let live = reader
            .live_started::<Users>(LiveLimits {
                max_held_events: 1,
                max_held_bytes: usize::MAX,
            })
            .unwrap();
        for ms in [10u64, 11] {
            log.push_unstable_at(data_id(ms, ms), blob(&[new_user(1, "x")]));
        }
        settle(&live, |l| coverage(l) == "overflowed").await;
        for ms in [12u64, 13] {
            log.push_unstable_at(data_id(ms, ms), blob(&[new_user(2, "y")]));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        set_f(&store, data_id(11, 11));
        assert!(user(&live, 2).await.is_none());
        assert_eq!(coverage(&live), "overflowed", "until moved to 13");
        set_f(&store, data_id(13, 13));
        let _ = user(&live, 2).await;
        assert_eq!(coverage(&live), "complete");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_cold_start_scan_holds_what_is_stable_but_not_folded() {
        let (_store, mut log, reader) = rig().await;
        // Stable before the view starts, F still at nothing: neither the tail
        // nor the store has it.
        log.append(blob(&[new_user(1, "ada")]));
        log.append(blob(&[new_todo(7, 1)]));
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        assert_eq!(user(&live, 1).await.unwrap().name, "ada");
        assert_eq!(coverage(&live), "complete");
        assert_eq!(held(&live), 1, "only the S event is kept");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_overflow_during_the_scan_releases_the_waiting_snapshots() {
        let (_store, mut log, reader) = rig().await;
        for id in 0..5 {
            log.append(blob(&[new_user(id, "x")]));
        }
        let live = reader
            .live_started::<Users>(LiveLimits {
                max_held_events: 2,
                max_held_bytes: usize::MAX,
            })
            .unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), user(&live, 0))
            .await
            .expect("the snapshot does not hang on an overflowed scan");
        assert!(got.is_none(), "overflowed: store@F alone");

        // The scan read on to its end: the third dropped is `until`, the
        // last two held; nothing it read is lost.
        settle(&live, |l| {
            matches!(
                l.inner.lock().coverage,
                Coverage::Overflowed { scan: false, .. }
            )
        })
        .await;
        assert_eq!(held(&live), 2);
    }

    /// An overflow while the scan runs does not recover before the scan has
    /// read to its end, however far F gets: what it has not read yet is
    /// neither held nor folded.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_overflow_during_the_scan_recovers_only_after_it() {
        let (store, _log, reader) = rig().await;
        let live = reader
            .live_started::<Users>(LiveLimits {
                max_held_events: 1,
                max_held_bytes: usize::MAX,
            })
            .unwrap();
        // As if the scan were running.
        live.inner.lock().coverage = Coverage::Scanning {
            held: Held::new(),
            bytes: 0,
        };
        for ms in [10u64, 11] {
            live.inner.arrive(Data {
                data_id: data_id(ms, ms),
                blob: blob(&[new_user(u128::from(ms), "x")]),
            });
        }
        assert_eq!(coverage(&live), "overflowed");

        set_f(&store, data_id(11, 11));
        live.inner.retire_now().unwrap();
        assert_eq!(coverage(&live), "overflowed", "the scan has not ended");

        live.inner.finish_scan();
        assert_eq!(coverage(&live), "complete", "ended, and F covers `until`");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_ended_tail_stops_the_dispatcher() {
        let store = inmemory();
        let log = TestLog::new();
        let mut dispatcher = Dispatcher::from_db(
            store.clone(),
            log.writer(),
            log.reader(),
            FLUSH_INTERVAL,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        let mut manager = dispatcher.new_handler::<event::Event>(tenant());
        manager.register::<User>().unwrap();
        let (_writer, reader) = manager.finalise().unwrap();
        let fold = dispatcher.finalise().await.unwrap();
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();

        log.hang_up_unstable();
        let ended = tokio::time::timeout(Duration::from_secs(5), fold)
            .await
            .expect("the dispatcher stops")
            .unwrap();
        assert!(
            matches!(
                ended,
                Err(Error::FoldStopped(StopReason::UnstableTailEnded))
            ),
            "{ended:?}"
        );
        let live2 = live.clone();
        let read = tokio::task::spawn_blocking(move || live2.snapshot().map(|_| ()))
            .await
            .unwrap();
        assert!(matches!(
            read,
            Err(Error::FoldStopped(StopReason::UnstableTailEnded))
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_snapshot_waits_on_a_stalled_fold() {
        let (_store, mut log, reader) = rig().await;
        // No fold runs at all in this rig: F never moves.
        let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
        log.push_unstable(blob(&[new_user(1, "ada")]));
        settle(&live, |l| held(l) == 1).await;
        let got = tokio::time::timeout(Duration::from_secs(1), user(&live, 1))
            .await
            .expect("served without the fold");
        assert!(got.is_some());
    }

    /// One event of the property test: a user's creation or ping, where it
    /// arrives (`tail`: through the unstable tail; otherwise only through
    /// the fold), and its position.
    #[derive(Debug, Clone)]
    struct Step {
        user: u128,
        ping: Option<u128>,
        tail: bool,
    }

    fn steps() -> impl Strategy<Value = (Vec<Step>, usize)> {
        prop::collection::vec(
            (0u128..3, prop::option::of(0u128..100), any::<bool>())
                .prop_map(|(user, ping, tail)| Step { user, ping, tail }),
            1..12,
        )
        .prop_flat_map(|steps| {
            let n = steps.len();
            (Just(steps), 0..=n)
        })
    }

    fn step_event(step: &Step) -> event::Event {
        match step.ping {
            Some(ts) => pinged(step.user, ts),
            None => new_user(step.user, &format!("u{}", step.user)),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        /// A snapshot equals a full fold of: everything up to F, plus what
        /// reached the tail above it, in DataId order. Events above F that
        /// never reached the tail are missing (the floor's), in any
        /// interleaving of arrivals.
        #[test]
        fn a_snapshot_is_the_fold_of_f_plus_what_reached_the_tail((steps, cut) in steps()) {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let (store, log, reader) = rig().await;
                let ids: Vec<DataId> = (0..steps.len())
                    .map(|i| {
                        let i = u64::try_from(i).unwrap();
                        data_id(i.saturating_add(10), i)
                    })
                    .collect();

                // What the store holds at F: the fold of the first `cut`.
                let mut manager = crate::ProjectorManager::<event::Event>::new(tenant());
                manager.register::<User>().unwrap();
                let mut txn = store.write().unwrap();
                for step in &steps[..cut] {
                    fold_event(&manager.projectors, tenant(), &step_event(step), &mut txn).unwrap();
                }
                let f = cut.checked_sub(1).map(|i| ids[i]);
                txn.commit(Watermark {
                    index: None,
                    data_id: f,
                    hlc: f.map(DataId::hlc),
                })
                .unwrap();

                let live = reader.live_started::<Users>(LiveLimits::default()).unwrap();
                // Arrivals in reverse order, above and below F alike.
                let mut expected_held: usize = 0;
                for (i, step) in steps.iter().enumerate().rev() {
                    if step.tail {
                        log.push_unstable_at(ids[i], blob(&[step_event(step)]));
                        if i >= cut {
                            expected_held = expected_held.saturating_add(1);
                        }
                    }
                }
                settle(&live, |l| held(l) == expected_held).await;

                // Expected: a fresh fold of the first `cut` and the tail ones above.
                let expect_store = inmemory();
                let mut txn = expect_store.write().unwrap();
                for (i, step) in steps.iter().enumerate() {
                    if i < cut || step.tail {
                        fold_event(&manager.projectors, tenant(), &step_event(step), &mut txn).unwrap();
                    }
                }
                txn.commit(Watermark::default()).unwrap();

                for id in 0..3u128 {
                    let want: Option<User> = crate::ReadTransactionExt::get::<User>(
                        &*expect_store.read().unwrap(),
                        tenant_id(),
                        Ref::new(UserId(id).into()),
                    )
                    .unwrap();
                    let got = user(&live, id).await;
                    assert_eq!(got, want, "user {id}");
                }
                drop(log);
            });
        }
    }
}
