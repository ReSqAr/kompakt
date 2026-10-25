//! What an app says about its metrics, as data.
//!
//! ```text
//!   App::describe_metrics()   help text for the series it records
//!   App::histograms()         the bounds of the histograms it records
//!        └── histograms(entries) ─▶ the process's list: the apps', then the cache's
//! ```
//!
//! An app names a metric and a ladder and never touches the exporter; the
//! process applies the list when it installs its recorder.

use crate::app::Entry;

/// The bounds of one histogram: seconds, bytes per second, whatever its unit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Histogram {
    pub metric: &'static str,
    pub bounds: &'static [f64],
}

/// Every histogram the process records, apps in list order, then the cache's.
#[must_use]
pub fn histograms(entries: &[Entry]) -> Vec<Histogram> {
    entries
        .iter()
        .flat_map(|e| e.histograms().iter().copied())
        .chain(
            objectstorage_cache::HISTOGRAMS
                .iter()
                .map(|&(metric, bounds)| Histogram { metric, bounds }),
        )
        .collect()
}

/// Register the help text of every app's series.
pub fn describe_metrics(entries: &[Entry]) {
    for entry in entries {
        entry.describe_metrics();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cache_declares_its_histograms_even_for_an_empty_list() {
        let declared = histograms(&[]);
        assert!(
            declared
                .iter()
                .any(|h| h.metric == "objcache_store_queue_seconds")
        );
        assert!(declared.iter().all(|h| !h.bounds.is_empty()));
    }
}
