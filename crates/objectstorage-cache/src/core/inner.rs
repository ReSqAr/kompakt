//! The shared half of the core: pinning, settling downloads, writing
//! recency back and publishing the gauges.

use super::Inner;
use super::file::{CachedFile, Pin};
use super::state::{Entry, State};
use crate::disk::Id;
use std::sync::Arc;
use std::time::SystemTime;

impl Inner {
    pub(super) fn pin(self: &Arc<Self>, st: &mut State, id: Id) -> Option<CachedFile> {
        if !st.entries.contains_key(&id) {
            return None;
        }
        let now = st.tick();
        let len = st.update(&id, |e| {
            e.pins = e.pins.saturating_add(1);
            e.last_used = now;
            e.used_at = SystemTime::now();
            e.dirty = true;
            if e.predicted.take().is_some() {
                metrics::counter!("objcache_predictions_total", "app" => id.group.app, "kind" => id.group.kind, "outcome" => "useful").increment(1);
            }
            e.hinted = false;
            e.size
        })?;
        Some(CachedFile {
            path: self.disk.path(&id),
            len,
            _pin: Pin {
                inner: self.clone(),
                id,
            },
        })
    }

    /// Settle a download: release its reservation and, if it completed,
    /// register the file already pinned for the caller.
    /// `predicted`: a prediction's deadline — the file is kept unpinned
    /// (none returned). Predictions made while it downloaded apply too.
    pub(super) fn finish(
        self: &Arc<Self>,
        id: Id,
        reserved: u64,
        size: Option<u64>,
        predicted: Option<SystemTime>,
    ) -> Option<CachedFile> {
        let (file, over) = {
            let mut st = self.state.lock();
            st.reserved = st.reserved.saturating_sub(reserved);
            // A prediction's own download is nobody's yet: kept unpinned. A
            // demand download that was also predicted is still pinned for
            // its caller (and counts as useful at once).
            let unpinned = predicted.is_some();
            let predicted = predicted.max(st.flights.predictions.remove(&id));
            let file = size.and_then(|size| {
                st.insert(
                    id,
                    Entry {
                        size,
                        last_used: 0,
                        used_at: SystemTime::now(),
                        dirty: true,
                        pins: 0,
                        hinted: false,
                        predicted,
                    },
                );
                if unpinned {
                    metrics::counter!("objcache_predicted_total", "app" => id.group.app, "kind" => id.group.kind).increment(1);
                    return None;
                }
                self.pin(&mut st, id)
            });
            (file, st.over_soft())
        };
        self.publish();
        self.room.notify_waiters();
        self.wake_if(over);
        file
    }

    /// Write the recency of files used since the last call into their mtime,
    /// so that a restart orders them by use, not by download.
    pub(super) fn flush_recency(&self) -> usize {
        let dirty: Vec<(Id, SystemTime)> = {
            let mut st = self.state.lock();
            st.entries
                .iter_mut()
                .filter(|(_, e)| e.dirty)
                .map(|(id, e)| {
                    e.dirty = false;
                    (*id, e.used_at)
                })
                .collect()
        };
        let mut n = 0usize;
        for (id, at) in &dirty {
            // Evicted in between: nothing to record.
            if self.disk.touch(id, *at).is_ok() {
                n = n.saturating_add(1);
            }
        }
        n
    }

    pub(super) fn publish(&self) {
        let st = self.state.lock();
        let targets = self.shape.targets();
        let total = st.used.max(st.limits.soft);
        for group in self.shape.groups() {
            let (bytes, files) = st.by_group.get(&group).copied().unwrap_or((0, 0));
            let labels = [("app", group.app), ("kind", group.kind)];
            metrics::gauge!("objcache_used_bytes", &labels).set(precise(bytes));
            metrics::gauge!("objcache_entries", &labels).set(precise(files));
            metrics::gauge!("objcache_target_bytes", &labels)
                .set(precise(targets.target(group, total)));
        }
        metrics::gauge!("objcache_held_bytes").set(precise(st.order.bytes.held));
        metrics::gauge!("objcache_drain_backlog_bytes")
            .set(precise(st.used.saturating_sub(st.limits.soft)));
    }
}

/// A gauge value, exact for any byte count a disk holds.
#[allow(
    clippy::float_arithmetic,
    reason = "a gauge is a float: two exact halves, summed"
)]
fn precise(n: u64) -> f64 {
    let high = u32::try_from(n >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(n & 0xffff_ffff).unwrap_or(0);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}
