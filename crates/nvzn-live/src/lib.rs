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

use metrics::ReadKind;
use nvzn_projector::fold_event;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use zslg::DataId;

// Only the `assert_live!` doctests use `serde`.
#[cfg(test)]
use serde as _;

mod consumer;
mod coverage;
pub mod metrics;
mod start;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests;

use coverage::{Coverage, Inner};
pub use start::{begin, begin_when_caught_up, not_started};

// The names the modules reach for as `crate::X`.
pub(crate) use nvzn_projection::{Projection, Watermark};
pub(crate) use nvzn_resultset::Snapshot;
pub(crate) use nvzn_store::{ArcStore, WriteTransactionImpl};
pub(crate) use nvzn_types::{
    APP_ID_LEN, Error, EventSet, FoldState, ProjectionTypeId, StopReason, Tenant, TenantId,
    WithWireFormat, subset,
};

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
/// # use nvzn_projection::{Projection, Ref, View, Write};
/// # use nvzn_types::{Error, ProjectionTypeId};
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct NoteSetV1 { id: u128 }
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct FileSeenV1 { id: u128 }
/// nvzn_types::define_events! {
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
/// #         fn id(&self) -> Ref<Self> { Ref::new(nvzn_types::ProjectionId::from_u128(self.id)) }
/// #         fn next(_: &event::$ev, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> { Ok(vec![]) }
/// #     }
/// # } }
/// // `Note` folds `NoteSet` only; `Mixed` also folds `FileSeen`.
/// # projection!(Note, NoteEvent, 0);
/// # projection!(Mixed, MixedEvent, 0);
/// nvzn_live::assert_live!(event::LiveEvent<'static>, [Note]);
/// # fn main() {}
/// ```
///
/// ```compile_fail
/// # use serde::{Deserialize, Serialize};
/// # use nvzn_projection::{Projection, Ref, View, Write};
/// # use nvzn_types::{Error, ProjectionTypeId};
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct NoteSetV1 { id: u128 }
/// # #[derive(Debug, Serialize, Deserialize, Clone)] struct FileSeenV1 { id: u128 }
/// nvzn_types::define_events! {
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
/// #         fn id(&self) -> Ref<Self> { Ref::new(nvzn_types::ProjectionId::from_u128(self.id)) }
/// #         fn next(_: &event::$ev, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> { Ok(vec![]) }
/// #     }
/// # } }
/// // `Note` folds `NoteSet` only; `Mixed` also folds `FileSeen`.
/// # projection!(Note, NoteEvent, 0);
/// # projection!(Mixed, MixedEvent, 0);
/// nvzn_live::assert_live!(event::LiveEvent<'static>, [Note, Mixed]);
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
