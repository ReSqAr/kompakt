//! The bookkeeping behind one lock: entries by id, bytes by group, what is
//! in flight, and the eviction order (`order.rs`). Every operation is
//! O(log n); choosing victims is here, deleting them is the evictor's.

use super::Limits;
use super::order::Order;
use crate::Ticket;
use crate::disk::{Group, Id};
use crate::shape::{Shape, Targets};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;

pub(super) type Outcome = Option<Result<(), Arc<str>>>;

/// Downloads under way: who is waiting, what is written so far, which were
/// predicted and what became of the predictions.
#[derive(Default)]
pub(super) struct Flights {
    /// Predictions for files still downloading: applied when they land.
    pub(super) predictions: HashMap<Id, SystemTime>,
    /// In-flight downloads started by a prediction: a request that joins one
    /// promotes it (it may be waiting in the prefetch lane).
    pub(super) promote: HashMap<Id, Arc<Ticket>>,
    /// Who waits for a download: the outcome, when it ends.
    pub(super) inflight: HashMap<Id, watch::Receiver<Outcome>>,
    /// `(announced size, bytes written)` of downloads in flight, for readers
    /// that follow a file while it is still arriving.
    pub(super) progress: HashMap<Id, watch::Receiver<(u64, u64)>>,
    /// Downloads started so far (tests count them).
    pub(super) started: u64,
}

#[derive(Default)]
pub(super) struct State {
    pub(super) limits: Limits,
    /// Per app: unpinned files unused for longer go at the next idle sweep.
    pub(super) max_idle: HashMap<&'static str, Duration>,
    pub(super) entries: HashMap<Id, Entry>,
    /// Bytes and file count per group.
    pub(super) by_group: HashMap<Group, (u64, u64)>,
    pub(super) used: u64,
    pub(super) reserved: u64,
    pub(super) clock: u64,
    /// Downloads under way and what is known about them.
    pub(super) flights: Flights,
    /// Who goes next. Changed only through `update`, `insert` and `take`.
    pub(super) order: Order,
}

pub(super) struct Entry {
    pub(super) size: u64,
    /// LRU order (a counter, cheap to compare) …
    pub(super) last_used: u64,
    /// … and the wall clock, for the idle limit and for recency on disk.
    pub(super) used_at: SystemTime,
    /// Used since the last `flush_recency`: the file's mtime is behind.
    pub(super) dirty: bool,
    pub(super) pins: u32,
    pub(super) hinted: bool,
    /// Fetched (or kept) for a prediction live until then: held — only room
    /// under hard may take it — until the tick after its deadline.
    pub(super) predicted: Option<SystemTime>,
}

/// Predictions may hold at most this share (‰) of the soft limit.
const PREDICTED_SHARE_PERMILLE: u64 = 500;

/// How a download is admitted and kept.
#[derive(Debug, Clone, Copy)]
pub(super) enum Mode {
    /// Someone waits: wait for room, keep the file pinned for them.
    Demand,
    /// A prediction live until then: only spare room, never waits, the
    /// file is kept unpinned.
    Predict(SystemTime),
}

impl State {
    pub(super) fn tick(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    /// Change an entry: out of its place, `change`, into its new place. The
    /// one way an entry's fields change once registered (the size never does).
    pub(super) fn update<R>(&mut self, id: &Id, change: impl FnOnce(&mut Entry) -> R) -> Option<R> {
        let e = self.entries.get_mut(id)?;
        let before = e.place(id.group);
        let result = change(e);
        let after = e.place(id.group);
        if before != after {
            let size = e.size;
            self.order.remove(*id, before, size);
            self.order.add(*id, after, size);
        }
        Some(result)
    }

    /// Register a file; returns the entry it replaced.
    pub(super) fn insert(&mut self, id: Id, entry: Entry) -> Option<Entry> {
        let previous = self.take(&id);
        let size = entry.size;
        self.order.add(id, entry.place(id.group), size);
        self.entries.insert(id, entry);
        self.used = self.used.saturating_add(size);
        let g = self.by_group.entry(id.group).or_default();
        g.0 = g.0.saturating_add(size);
        g.1 = g.1.saturating_add(1);
        previous
    }

    /// Forget a file; the evictor deletes it from disk.
    pub(super) fn take(&mut self, id: &Id) -> Option<Entry> {
        let e = self.entries.remove(id)?;
        self.order.remove(*id, e.place(id.group), e.size);
        self.used = self.used.saturating_sub(e.size);
        if let Some(g) = self.by_group.get_mut(&id.group) {
            g.0 = g.0.saturating_sub(e.size);
            g.1 = g.1.saturating_sub(1);
        }
        Some(e)
    }

    /// Over the retention budget: the evictor has work.
    pub(super) fn over_soft(&self) -> bool {
        self.used > self.limits.soft
    }

    /// `size` more bytes fit under hard now. `last_resort`: also admit an
    /// oversize file when nothing is pinned or in flight — waiting could
    /// never free room for it (asked only once the evictor has made what
    /// room it could).
    pub(super) fn fits(&self, size: u64, last_resort: bool) -> bool {
        let committed = self.used.saturating_add(self.reserved);
        committed.saturating_add(size) <= self.limits.hard
            || (last_resort && self.reserved == 0 && self.order.bytes.pinned == 0)
    }

    /// Reserve `size` if it [`fits`](Self::fits).
    pub(super) fn try_reserve(&mut self, size: u64, last_resort: bool) -> bool {
        let fits = self.fits(size, last_resort);
        if fits {
            self.reserved = self.reserved.saturating_add(size);
        }
        fits
    }

    /// Room for a prediction of `size`: within the predicted share and under
    /// hard. Never evicts and never waits — an admission over soft wakes the
    /// evictor, whose drain makes room among recent files (the prediction is
    /// held). Reserves it and returns true, or changes nothing.
    pub(super) fn reserve_prediction(&mut self, size: u64) -> bool {
        let share = self
            .limits
            .soft
            .saturating_mul(PREDICTED_SHARE_PERMILLE)
            .checked_div(1000)
            .unwrap_or(0);
        if self.order.bytes.held.saturating_add(size) > share {
            return false;
        }
        self.try_reserve(size, false)
    }

    /// The next victim, by policy:
    ///
    /// ```text
    ///   1. a done (hinted) file, least recently used, any group
    ///   2. else the least recently used kept file of the group furthest over
    ///      its target: over[g] = held[g] − target[g](max(used, soft));
    ///      ties: the more held, then the least recently used head
    ///   3. else, only with `room` (making room under hard): the held
    ///      prediction with the earliest deadline
    /// ```
    ///
    /// O(log n) for the sets plus a pass over the groups, which are few.
    pub(super) fn victim(&self, targets: &Targets, room: bool) -> Option<Id> {
        if let Some((_, id)) = self.order.done.first() {
            return Some(*id);
        }
        let total = self.used.max(self.limits.soft);
        let kept = self
            .order
            .heads()
            .max_by_key(|(group, head)| {
                let held = self.by_group.get(group).map_or(0, |g| g.0);
                let target = targets.target(*group, total);
                let over = i128::from(held).saturating_sub(i128::from(target));
                (over, held, std::cmp::Reverse(*head))
            })
            .map(|(_, (_, id))| id);
        if kept.is_some() {
            return kept;
        }
        if room {
            return self.order.held.first().map(|(_, id)| *id);
        }
        None
    }

    /// The drain's batch: up to `max` victims while over soft, held
    /// predictions never. Taken out of the state; the caller deletes them.
    pub(super) fn drain(&mut self, shape: &Shape, max: usize) -> Vec<Id> {
        let targets = shape.targets();
        let mut victims = Vec::new();
        while self.over_soft() && victims.len() < max {
            let Some(id) = self.victim(&targets, false) else {
                break;
            };
            self.evict(id, "drain");
            victims.push(id);
        }
        victims
    }

    /// Room for `bytes` under hard: victims while `used + reserved + bytes`
    /// is over it, held predictions last.
    pub(super) fn room(&mut self, bytes: u64, shape: &Shape) -> Vec<Id> {
        let targets = shape.targets();
        let mut victims = Vec::new();
        while !self.fits(bytes, false) {
            let Some(id) = self.victim(&targets, true) else {
                break;
            };
            self.evict(id, "room");
            victims.push(id);
        }
        victims
    }

    fn evict(&mut self, id: Id, reason: &'static str) {
        let Some(e) = self.take(&id) else {
            return;
        };
        if e.predicted.is_some() {
            metrics::counter!("objcache_predictions_total", "app" => id.group.app, "kind" => id.group.kind, "outcome" => "wasted").increment(1);
        }
        metrics::counter!("objcache_evicted_total", "app" => id.group.app, "kind" => id.group.kind, "reason" => reason).increment(1);
    }

    /// Predictions whose deadline has passed become ordinary files (kept,
    /// or done if hinted), counted as wasted: nobody opened them in time.
    pub(super) fn expire(&mut self, now: SystemTime) {
        while let Some(&(until, id)) = self.order.held.first()
            && until <= now
        {
            self.update(&id, |e| e.predicted = None);
            metrics::counter!("objcache_predictions_total", "app" => id.group.app, "kind" => id.group.kind, "outcome" => "wasted").increment(1);
        }
    }

    /// Unpinned files unused for longer than their app's idle limit, taken
    /// out of the state. Walks the done set and each group's kept set from
    /// the oldest and stops at the first file too recent for any limit — use
    /// ticks and `used_at` rise together — so its cost is what it evicts.
    /// (Expired predictions sit at tick 0, in front: they never stop it.)
    pub(super) fn idle(&mut self, now: SystemTime) -> Vec<Id> {
        let cutoff = |app: &str| self.max_idle.get(app).and_then(|max| now.checked_sub(*max));
        let is_idle = |id: &Id, e: &Entry| cutoff(id.group.app).is_some_and(|c| e.used_at < c);
        let latest = self.max_idle.keys().filter_map(|app| cutoff(app)).max();
        let Some(latest) = latest else {
            return Vec::new();
        };
        let mut idle = Vec::new();
        let sets = std::iter::once((None, &self.order.done)).chain(
            self.order
                .kept
                .iter()
                .map(|(group, set)| (Some(*group), set)),
        );
        for (group, set) in sets {
            // A kept set stops at its own app's cutoff; the done set, which
            // mixes apps, at the latest of them.
            let stop = match group {
                Some(group) => match cutoff(group.app) {
                    Some(c) => c,
                    None => continue,
                },
                None => latest,
            };
            for (tick, id) in set {
                let Some(e) = self.entries.get(id) else {
                    continue;
                };
                if *tick > 0 && e.used_at >= stop {
                    break;
                }
                if is_idle(id, e) {
                    idle.push(*id);
                }
            }
        }
        for id in &idle {
            self.take(id);
            metrics::counter!("objcache_evicted_total", "app" => id.group.app, "kind" => id.group.kind, "reason" => "idle").increment(1);
        }
        idle
    }
}

/// The index against an oracle: a naive pass over the entries implementing
/// the same policy, after every step of random operation sequences.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use super::super::order::{Place, Totals};
    use super::*;
    use proptest::prelude::*;

    const GROUPS: [Group; 3] = [
        Group {
            app: "a",
            kind: "x",
        },
        Group {
            app: "a",
            kind: "y",
        },
        Group {
            app: "b",
            kind: "x",
        },
    ];

    fn id(n: u8) -> Id {
        Id {
            group: GROUPS[usize::from(n % 3)],
            hash: [n; 32],
        }
    }

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[derive(Debug, Clone)]
    enum Op {
        Admit(u8, u64),
        Pin(u8),
        Unpin(u8),
        Hint(u8),
        Predict(u8, u64),
        Expire(u64),
        Weigh(usize, u32),
        Drain(usize),
        Room(u64),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0u8..24, 1u64..300).prop_map(|(n, s)| Op::Admit(n, s)),
            3 => (0u8..24).prop_map(Op::Pin),
            3 => (0u8..24).prop_map(Op::Unpin),
            1 => (0u8..24).prop_map(Op::Hint),
            1 => (0u8..24, 0u64..100).prop_map(|(n, t)| Op::Predict(n, t)),
            1 => (0u64..100).prop_map(Op::Expire),
            1 => (0usize..3, 0u32..5).prop_map(|(g, w)| Op::Weigh(g, w)),
            1 => (1usize..4).prop_map(Op::Drain),
            1 => (0u64..400).prop_map(Op::Room),
        ]
    }

    /// A group's rank: `(over, held, least recently used head)`.
    type Rank = (i128, u64, std::cmp::Reverse<(u64, Id)>);

    /// The policy as a pass over every entry.
    fn oracle(st: &State, targets: &Targets, room: bool) -> Option<Id> {
        let place = |id: &Id, e: &Entry| e.place(id.group);
        let done = st
            .entries
            .iter()
            .filter_map(|(id, e)| match place(id, e) {
                Place::Done(tick) => Some((tick, *id)),
                _ => None,
            })
            .min();
        if let Some((_, id)) = done {
            return Some(id);
        }
        let total = st.used.max(st.limits.soft);
        let mut best: Option<(Rank, Id)> = None;
        for group in GROUPS {
            let head = st
                .entries
                .iter()
                .filter_map(|(id, e)| match place(id, e) {
                    Place::Kept(g, tick) if g == group => Some((tick, *id)),
                    _ => None,
                })
                .min();
            let Some(head) = head else { continue };
            let held = st.by_group.get(&group).map_or(0, |g| g.0);
            let over = i128::from(held) - i128::from(targets.target(group, total));
            let key = (over, held, std::cmp::Reverse(head));
            if best.as_ref().is_none_or(|(k, _)| key > *k) {
                best = Some((key, head.1));
            }
        }
        if let Some((_, id)) = best {
            return Some(id);
        }
        if !room {
            return None;
        }
        st.entries
            .iter()
            .filter_map(|(id, e)| match place(id, e) {
                Place::Held(until) => Some((until, *id)),
                _ => None,
            })
            .min()
            .map(|(_, id)| id)
    }

    /// Every entry in exactly the set its place names, nothing else in the
    /// sets, and the totals what the entries add up to.
    fn check_index(st: &State) {
        let mut want = Totals::default();
        let mut in_sets = 0usize;
        for (id, e) in &st.entries {
            match e.place(id.group) {
                Place::Pinned => want.pinned += e.size,
                Place::Held(until) => {
                    assert!(st.order.held.contains(&(until, *id)));
                    want.held += e.size;
                    in_sets += 1;
                }
                Place::Done(tick) => {
                    assert!(st.order.done.contains(&(tick, *id)));
                    want.done += e.size;
                    in_sets += 1;
                }
                Place::Kept(group, tick) => {
                    assert!(st.order.kept[&group].contains(&(tick, *id)));
                    want.kept += e.size;
                    in_sets += 1;
                }
            }
        }
        let kept: usize = st.order.kept.values().map(|s| s.len()).sum();
        assert_eq!(in_sets, st.order.held.len() + st.order.done.len() + kept);
        assert!(st.order.kept.values().all(|s| !s.is_empty()));
        assert_eq!(st.order.bytes, want);
        let used: u64 = st.entries.values().map(|e| e.size).sum();
        assert_eq!(st.used, used);
    }

    proptest! {
        #[test]
        fn the_index_picks_what_the_oracle_picks(ops in proptest::collection::vec(op(), 1..120)) {
            let shape = Shape::default();
            for g in GROUPS {
                shape.add_kind(g.app, g.kind, 1);
            }
            let mut st = State {
                limits: Limits { soft: 1000, hard: 1500 },
                ..State::default()
            };
            for op in ops {
                match op {
                    Op::Admit(n, size) => {
                        let last_used = st.tick();
                        st.insert(id(n), Entry {
                            size,
                            last_used,
                            used_at: SystemTime::UNIX_EPOCH,
                            dirty: false,
                            pins: 0,
                            hinted: false,
                            predicted: None,
                        });
                    }
                    Op::Pin(n) => {
                        let now = st.tick();
                        st.update(&id(n), |e| {
                            e.pins += 1;
                            e.last_used = now;
                            e.predicted = None;
                            e.hinted = false;
                        });
                    }
                    Op::Unpin(n) => {
                        st.update(&id(n), |e| e.pins = e.pins.saturating_sub(1));
                    }
                    Op::Hint(n) => {
                        st.update(&id(n), |e| e.hinted = true);
                    }
                    Op::Predict(n, until) => {
                        st.update(&id(n), |e| {
                            if e.pins == 0 {
                                e.predicted = e.predicted.max(Some(at(until)));
                            }
                        });
                    }
                    Op::Expire(now) => {
                        st.expire(at(now));
                        prop_assert!(st.order.held.first().is_none_or(|(u, _)| *u > at(now)));
                    }
                    Op::Weigh(g, w) => shape.set_kind_weight(GROUPS[g].app, GROUPS[g].kind, w),
                    Op::Drain(max) => {
                        let held: Vec<Id> = st.order.held.iter().map(|(_, id)| *id).collect();
                        let victims = st.drain(&shape, max);
                        let targets = shape.targets();
                        prop_assert!(victims.iter().all(|v| !held.contains(v)), "the drain never takes a held prediction");
                        prop_assert!(victims.len() == max || !st.over_soft() || oracle(&st, &targets, false).is_none());
                    }
                    Op::Room(bytes) => {
                        let held: Vec<Id> = st.order.held.iter().map(|(_, id)| *id).collect();
                        let victims = st.room(bytes, &shape);
                        let targets = shape.targets();
                        prop_assert!(st.fits(bytes, false) || oracle(&st, &targets, true).is_none());
                        if victims.iter().any(|v| held.contains(v)) {
                            prop_assert!(oracle(&st, &targets, false).is_none(), "a held prediction goes last");
                        }
                    }
                }
                check_index(&st);
                let targets = shape.targets();
                prop_assert_eq!(st.victim(&targets, false), oracle(&st, &targets, false));
                prop_assert_eq!(st.victim(&targets, true), oracle(&st, &targets, true));
            }
        }
    }
}
