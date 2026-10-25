//! What changes while the cache runs: hints, idle limits, weights, recency,
//! and what the cache holds.

use super::{Core, Held, Limits};
use crate::disk::Id;
use std::time::Duration;

impl Core {
    /// The caller is done with the file: it goes first when room is needed.
    pub(crate) fn done_with(&self, id: Id) {
        let over = {
            let mut st = self.inner.state.lock();
            st.update(&id, |e| e.hinted = true);
            st.over_soft()
        };
        self.inner.wake_if(over);
    }

    /// An app's idle limit: its unpinned files unused for longer go at the
    /// next sweep (none: nothing goes for idleness).
    pub(crate) fn set_max_idle(&self, app: &'static str, idle: Option<Duration>) {
        let mut st = self.inner.state.lock();
        match idle {
            Some(idle) => {
                st.max_idle.insert(app, idle);
            }
            None => {
                st.max_idle.remove(app);
            }
        }
    }

    /// The weights changed: the evictor drains toward the new mix if over
    /// soft.
    pub(crate) fn reweigh(&self) {
        let over = self.inner.state.lock().over_soft();
        self.inner.publish();
        self.inner.wake_if(over);
    }

    /// Write the recency of files used since the last call into their mtime
    /// (at shutdown; the evictor does it once a minute).
    pub(crate) fn flush_recency(&self) -> usize {
        self.inner.flush_recency()
    }

    /// What each registered group holds, against its target.
    pub(crate) fn held(&self) -> Vec<Held> {
        let st = self.inner.state.lock();
        let targets = self.inner.shape.targets();
        self.inner
            .shape
            .groups()
            .into_iter()
            .map(|group| {
                let (bytes, files) = st.by_group.get(&group).copied().unwrap_or((0, 0));
                Held {
                    group,
                    bytes,
                    files,
                    target: targets.target(group, st.used.max(st.limits.soft)),
                }
            })
            .collect()
    }

    pub(crate) fn used_total(&self) -> u64 {
        self.inner.state.lock().used
    }

    pub(crate) fn limits(&self) -> Limits {
        self.inner.state.lock().limits
    }

    #[cfg(test)]
    pub(crate) fn downloads(&self) -> u64 {
        self.inner.state.lock().flights.started
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> u64 {
        self.inner.state.lock().used
    }

    /// What counts against hard: complete files and downloads in flight.
    #[cfg(test)]
    pub(crate) fn committed(&self) -> u64 {
        let st = self.inner.state.lock();
        st.used.saturating_add(st.reserved)
    }

    #[cfg(test)]
    pub(crate) fn entries(&self) -> usize {
        self.inner.state.lock().entries.len()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, id: &Id) -> bool {
        self.inner.state.lock().entries.contains_key(id)
    }

    #[cfg(test)]
    pub(crate) fn path_of(&self, id: &Id) -> std::path::PathBuf {
        self.inner.disk.path(id)
    }
}
