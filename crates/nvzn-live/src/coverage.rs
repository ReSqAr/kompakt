//! The live view's held state: what it holds above the fold's watermark, how
//! arrivals are admitted, retired and capped.

use super::LiveLimits;
use crate::APP_ID_LEN;
use crate::FoldState;
use crate::metrics::{self};
use crate::{ArcStore, Error, ProjectionTypeId, Tenant, TenantId, Watermark, WithWireFormat};
use nvzn_projector::BoxedProjectorLike;
use nvzn_types::contains;
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use tokio::sync::watch;
use tracing::{info, warn};
use zslg::{Data, DataId};

pub(super) struct Inner<Ev> {
    pub(super) tenant: Tenant,
    pub(super) store: ArcStore,
    pub(super) fold_state: FoldState,
    /// The live projections' own projectors (the fold keeps its own).
    pub(super) projectors: BTreeMap<ProjectionTypeId, BoxedProjectorLike<Ev>>,
    /// `S`'s event numbers.
    pub(super) events: &'static [u32],
    pub(super) limits: LiveLimits,
    pub(super) state: Mutex<State<Ev>>,
    /// Signalled when the cold-start scan ends (or overflows), for the
    /// snapshots waiting on it.
    pub(super) scanned: Condvar,
    pub(super) changed: watch::Sender<u64>,
    /// Moves after every retirement (F reached the state): what
    /// [`Live::wait_for`](super::Live::wait_for) wakes on for an entry only F shows.
    pub(super) retired: watch::Sender<u64>,
}

pub(super) struct State<Ev> {
    pub(super) coverage: Coverage<Ev>,
    /// The store's watermark at the last retirement: an arrival it covers is
    /// already in the store.
    pub(super) folded: Watermark,
}

/// One held write: its `S` events, decoded once, and its payload size.
pub(super) struct Entry<Ev> {
    pub(super) events: Arc<Vec<Ev>>,
    pub(super) bytes: usize,
}

/// Keyed by `DataId`: that is the fold's order, whatever order they arrived in.
pub(super) type Held<Ev> = BTreeMap<DataId, Entry<Ev>>;

pub(super) enum Coverage<Ev> {
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
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Scanning { .. } => "scanning",
            Self::Complete { .. } => "complete",
            Self::Overflowed { .. } => "overflowed",
        }
    }

    /// Held entries and their bytes.
    pub(super) fn held(&self) -> (usize, usize) {
        match self {
            Self::NotStarted => (0, 0),
            Self::Scanning { held, bytes }
            | Self::Complete { held, bytes }
            | Self::Overflowed { held, bytes, .. } => (held.len(), *bytes),
        }
    }
}

pub(super) fn add(bytes: usize, more: usize) -> usize {
    // Saturating: a byte estimate held against a cap; past usize::MAX it is
    // over any cap, which is all the number is for.
    bytes.saturating_add(more)
}

/// Drop what `f` covers from `held`, and the bytes with it.
pub(super) fn retain_uncovered<Ev>(held: &mut Held<Ev>, bytes: &mut usize, f: &Watermark) {
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
    pub(super) fn lock(&self) -> MutexGuard<'_, State<Ev>> {
        // A panic under the lock leaves a map half-retained at worst: still a
        // set of held writes, so poisoning is ignored.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn over_cap(&self, events: usize, bytes: usize) -> bool {
        events > self.limits.max_held_events || bytes > self.limits.max_held_bytes
    }

    pub(super) fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub(super) fn publish(&self, state: &State<Ev>) {
        let (held, bytes) = state.coverage.held();
        metrics::live_held(self.tenant, held, bytes);
        metrics::live_coverage(self.tenant, state.coverage.name());
    }

    /// One write from the tail or the scan: keep its `S` events, if any.
    /// Whether what a snapshot serves moved (an `S` event that will be shown).
    pub(super) fn arrive(&self, data: Data) -> bool {
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

    pub(super) fn insert(&self, state: &mut State<Ev>, id: DataId, entry: Entry<Ev>) -> bool {
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
    pub(super) fn retire(&self, state: &mut State<Ev>, f: Watermark) -> bool {
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

    pub(super) fn retire_now(&self) -> Result<(), Error> {
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
    pub(super) fn finish_scan(&self) {
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

impl<Ev> Inner<Ev> {
    /// Whether a snapshot taken now serves the entry `d`.
    pub(super) fn serves(&self, d: DataId) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match &state.coverage {
            Coverage::NotStarted => false,
            // Overflowed too: a snapshot then serves store@F, which has it.
            _ if state.folded.covers(d) => true,
            Coverage::Overflowed { .. } => false,
            Coverage::Scanning { held, .. } | Coverage::Complete { held, .. } => {
                held.contains_key(&d)
            }
        }
    }
}
