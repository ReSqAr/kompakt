//! The eviction order: every entry in exactly one place, every unpinned one
//! in one ordered set, so choosing a victim costs O(log n) whatever the
//! cache holds.
//!
//! ```text
//!   Pinned                      in no set: never evicted
//!   Held  { until }   held:     by deadline        a live prediction: only for
//!                                                  room under hard, earliest first
//!   Done  { tick }    done:     by last use        hinted: first, any group
//!   Kept  { group, tick }  kept[group]: by last use  the group furthest over
//!                                                  its target gives its oldest
//! ```
//!
//! The place is computed from the entry ([`Entry::place`]), never stored, and
//! only `State`'s choke points (`update`, `insert`, `take`) move an entry
//! between places — so "in exactly one place" holds by construction.

use super::state::Entry;
use crate::disk::{Group, Id};
use std::collections::{BTreeSet, HashMap};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Place {
    Pinned,
    Held(SystemTime),
    Done(u64),
    Kept(Group, u64),
}

impl Entry {
    pub(super) fn place(&self, group: Group) -> Place {
        if self.pins > 0 {
            Place::Pinned
        } else if let Some(until) = self.predicted {
            Place::Held(until)
        } else if self.hinted {
            Place::Done(self.last_used)
        } else {
            Place::Kept(group, self.last_used)
        }
    }
}

/// Bytes per place kind: running totals, kept with the sets.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Totals {
    pub(super) pinned: u64,
    pub(super) held: u64,
    pub(super) done: u64,
    pub(super) kept: u64,
}

#[derive(Default)]
pub(super) struct Order {
    pub(super) held: BTreeSet<(SystemTime, Id)>,
    pub(super) done: BTreeSet<(u64, Id)>,
    pub(super) kept: HashMap<Group, BTreeSet<(u64, Id)>>,
    pub(super) bytes: Totals,
}

impl Order {
    pub(super) fn add(&mut self, id: Id, place: Place, size: u64) {
        let b = &mut self.bytes;
        match place {
            Place::Pinned => b.pinned = b.pinned.saturating_add(size),
            Place::Held(until) => {
                self.held.insert((until, id));
                b.held = b.held.saturating_add(size);
            }
            Place::Done(tick) => {
                self.done.insert((tick, id));
                b.done = b.done.saturating_add(size);
            }
            Place::Kept(group, tick) => {
                self.kept.entry(group).or_default().insert((tick, id));
                b.kept = b.kept.saturating_add(size);
            }
        }
    }

    pub(super) fn remove(&mut self, id: Id, place: Place, size: u64) {
        let b = &mut self.bytes;
        match place {
            Place::Pinned => b.pinned = b.pinned.saturating_sub(size),
            Place::Held(until) => {
                self.held.remove(&(until, id));
                b.held = b.held.saturating_sub(size);
            }
            Place::Done(tick) => {
                self.done.remove(&(tick, id));
                b.done = b.done.saturating_sub(size);
            }
            Place::Kept(group, tick) => {
                if let Some(set) = self.kept.get_mut(&group) {
                    set.remove(&(tick, id));
                    if set.is_empty() {
                        self.kept.remove(&group);
                    }
                }
                b.kept = b.kept.saturating_sub(size);
            }
        }
    }

    /// Each group's least recently used kept file.
    pub(super) fn heads(&self) -> impl Iterator<Item = (Group, (u64, Id))> + '_ {
        self.kept
            .iter()
            .filter_map(|(group, set)| set.first().map(|head| (*group, *head)))
    }
}
