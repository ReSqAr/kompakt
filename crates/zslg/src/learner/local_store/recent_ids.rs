//! Bounded set of `DataId`s **known to be durably written** — the local store's
//! cross-batch duplicate filter.
//!
//! Every learner subscribes to **every** replica, because a write is only
//! quorum-acked and so no single replica is guaranteed to hold it
//! (`learner/subscriber/manager.rs`). Each write therefore arrives once per
//! replica. Copies 2..n are byte-identical by construction — CLAUDE.md §8
//! guarantees a `data_id` always carries the same payload, and the local store
//! is keyed by `DataId` — so integrating them is pure waste that the LSM only
//! collapses later, at seal/merge.
//!
//! **The read and the write are deliberately separate calls.** [`Dedup::contains`]
//! is consulted during batch assembly; [`Dedup::record_committed`] is called
//! only after `append_batch` has returned `Ok`. A batch that fails therefore
//! leaves this set untouched, so the records it did not write are re-delivered
//! (the subscriber resumes from its durable cursor) and written on the retry.
//! If `contains` were allowed to insert, a failed batch would poison the filter
//! and those records would be skipped forever — silent data loss.
//!
//! A miss (id already evicted) is harmless: it costs the write that would have
//! happened anyway. That is why the capacity is a performance tunable and not a
//! correctness parameter, and why a race between two workers both admitting the
//! same id is acceptable — both write it, the LSM collapses them, nothing is
//! lost.

use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;

use crate::ids::DataId;

/// Whether this store filters duplicates. `Off` is matched **once per batch**,
/// not once per record — the disabled path costs nothing on the hot loop.
pub(in crate::learner::local_store) enum Dedup {
    Off,
    On(RecentIds),
}

impl Dedup {
    #[must_use]
    pub(in crate::learner::local_store) fn new(capacity: Option<NonZeroUsize>) -> Self {
        match capacity {
            None => Self::Off,
            Some(cap) => Self::On(RecentIds::new(cap)),
        }
    }

    /// `true` = this id is already durably in the store; skip the write.
    #[must_use]
    pub(in crate::learner::local_store) fn contains(&self, id: DataId) -> bool {
        match self {
            Self::Off => false,
            Self::On(r) => r.set.contains(&id),
        }
    }

    /// Remember ids whose batch **committed**. Call only after `append_batch`
    /// returns `Ok` — see the module docs for why the ordering is load-bearing.
    pub(in crate::learner::local_store) fn record_committed(
        &mut self,
        ids: impl IntoIterator<Item = DataId>,
    ) {
        if let Self::On(r) = self {
            for id in ids {
                r.insert(id);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::Off => 0,
            Self::On(r) => r.ring.len(),
        }
    }
}

/// A fixed-capacity insertion-ordered set: the `HashSet` answers membership,
/// the `VecDeque` remembers eviction order. Both hold the same ids, so
/// residency is `capacity`, never the number of ids that streamed through.
pub(in crate::learner::local_store) struct RecentIds {
    set: HashSet<DataId>,
    ring: VecDeque<DataId>,
    cap: NonZeroUsize,
}

impl RecentIds {
    #[must_use]
    fn new(cap: NonZeroUsize) -> Self {
        Self {
            set: HashSet::with_capacity(cap.get()),
            ring: VecDeque::with_capacity(cap.get()),
            cap,
        }
    }

    fn insert(&mut self, id: DataId) {
        if !self.set.insert(id) {
            return; // already resident — re-queueing would evict a peer early
        }
        self.ring.push_back(id);
        if self.ring.len() > self.cap.get()
            && let Some(evicted) = self.ring.pop_front()
        {
            self.set.remove(&evicted);
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
    use crate::tests::make_data;

    fn ring(n: usize) -> Dedup {
        Dedup::new(Some(std::num::NonZeroUsize::new(n).unwrap()))
    }

    fn id(seed: u8) -> crate::ids::DataId {
        make_data(seed, u64::from(seed), b"payload").data_id
    }

    /// `contains` is the read half; nothing is remembered until a commit is
    /// reported. This split is the safety property, not an optimisation.
    #[test]
    fn nothing_is_remembered_until_a_commit_is_reported() {
        let mut d = ring(8);
        let a = id(1);
        assert!(!d.contains(a), "unseen id");
        assert!(!d.contains(a), "still unseen — contains must not insert");
        d.record_committed([a]);
        assert!(d.contains(a), "committed id is remembered");
    }

    #[test]
    fn distinct_committed_ids_are_all_remembered() {
        let mut d = ring(8);
        let ids: Vec<_> = (0..8u8).map(id).collect();
        d.record_committed(ids.iter().copied());
        for i in ids {
            assert!(d.contains(i));
        }
    }

    /// Bounded: the oldest id is evicted, and a copy arriving after eviction is
    /// written again. Safe — it costs the write we would have made anyway.
    #[test]
    fn evicts_oldest_beyond_capacity() {
        let mut d = ring(2);
        d.record_committed([id(1), id(2), id(3)]);
        assert!(!d.contains(id(1)), "oldest was evicted");
        assert!(d.contains(id(2)));
        assert!(d.contains(id(3)));
    }

    /// Re-committing a resident id must not grow the ring or evict a peer.
    #[test]
    fn recommitting_a_resident_id_is_idempotent() {
        let mut d = ring(2);
        d.record_committed([id(1), id(2), id(1), id(1)]);
        assert!(d.contains(id(1)));
        assert!(d.contains(id(2)));
        assert_eq!(d.len(), 2);
    }

    /// `None` disables the filter — every record is written, exactly as today.
    #[test]
    fn disabled_filter_never_reports_a_hit() {
        let mut d = Dedup::new(None);
        d.record_committed([id(1)]);
        assert!(!d.contains(id(1)));
    }

    #[test]
    fn memory_is_bounded_by_capacity_not_by_throughput() {
        let mut d = ring(4);
        for i in 0..200u8 {
            d.record_committed([id(i)]);
        }
        assert_eq!(d.len(), 4);
    }
}
