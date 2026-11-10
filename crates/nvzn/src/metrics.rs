//! Canonical metric registry.
//!
//! Every metric this crate emits is declared here as a [`MetricDef`] in
//! [`all_metrics`] — name, kind, unit and help — and every emission goes
//! through one of the helpers below, never a raw `counter!`/`gauge!` at a call
//! site. Because the helpers are `pub(crate)`, an unwired metric is an unused
//! function and `clippy -D warnings` rejects it.
//!
//! **This crate emits; the application installs.** `metrics` is a facade: with
//! no recorder installed every macro is a no-op, so nvzn never builds an
//! exporter. Call [`describe_metrics`] once after installing one — descriptions
//! sent to the no-op recorder are dropped, not replayed.
//!
//! Conventions, matching `zslg`'s registry so one endpoint reads as one system:
//! every name is under the `nvzn_` namespace; counters end in `_total`;
//! histograms end in `_seconds` or `_entries`; HLC-valued gauges export the
//! *physical-millisecond part*, never `Hlc::raw()`, which exceeds f64's exact
//! integer range. Label values come from the enums below via `as_str`, so every
//! `{label="value"}` pair is enumerable at compile time — with two deliberate
//! exceptions, `tenant` and `projection`, which are registration-time names: both
//! are bounded by the projections a binary declares, neither grows with traffic
//! or with hosts. (`cf` takes the store's column-family name constants, which
//! are as fixed as an enum.)

use metrics_kit::{MetricDef, MetricKind, Unit};

/// The namespace every metric this crate owns is exported under.
///
/// Test-only: the live value is the literal on each name const, and this exists
/// so `tests::every_metric_is_namespaced` asserts against a name rather than
/// re-deriving one.
#[cfg(test)]
pub(crate) const METRIC_PREFIX: &str = "nvzn_";

// ---------------------------------------------------------------------------
// Label keys and vocabularies
// ---------------------------------------------------------------------------

/// Every metric the nvzn crates declare: this facade's slice, then each
/// emitting crate's.
pub(crate) fn all_metrics() -> impl Iterator<Item = &'static MetricDef> {
    nvzn_dispatcher::metrics::all()
}

/// The namespace every metric this crate owns is exported under.
pub const METRIC_NAMESPACE: &str = "nvzn_";

/// Latency ladder, in seconds. Matches zslg's so one dashboard reads both
/// crates on the same scale.
const SECONDS_BUCKETS: &[f64] = &[
    0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0,
];

/// Count ladder: entries per fold batch and per live-view scan.
const COUNT_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 512.0, 4096.0];

/// Bytes ladder: a fold batch is cut at 1 MB, so the top buckets say saturated.
const BYTES_BUCKETS: &[f64] = &[1e3, 1e4, 1e5, 2.5e5, 5e5, 1e6, 2e6, 4e6, 1.6e7];

/// Buckets for one histogram, keyed by the unit it declares.
const fn buckets_for(unit: Option<Unit>) -> &'static [f64] {
    match unit {
        Some(Unit::Count) => COUNT_BUCKETS,
        Some(Unit::Bytes) => BYTES_BUCKETS,
        _ => SECONDS_BUCKETS,
    }
}

/// Every histogram this crate records, with the buckets it should get.
/// Recorder-agnostic; `metrics-exporter-prometheus` users want
/// [`configure_prometheus`], which applies exactly this.
pub fn histogram_buckets() -> impl Iterator<Item = (&'static str, &'static [f64])> {
    all_metrics()
        .filter(|d| d.kind == MetricKind::Histogram)
        .map(|d| (d.name, buckets_for(d.unit)))
}

/// Apply this crate's histogram buckets to a Prometheus builder.
///
/// Configuration only: the builder is yours, installing it stays your call, and
/// each histogram gets an exact `Matcher::Full` so nothing here reaches another
/// crate's metrics.
///
/// ```no_run
/// # fn main() -> Result<(), nvzn::Error> {
/// use metrics_exporter_prometheus::PrometheusBuilder;
///
/// let builder = PrometheusBuilder::new().with_http_listener(([0, 0, 0, 0], 9090));
/// // A second instrumented crate composes by threading the same builder
/// // through its own `configure_prometheus` before `install`.
/// let builder = nvzn::configure_prometheus(builder)?;
/// builder.install().expect("install recorder");
///
/// // After the recorder exists: descriptions sent to the no-op recorder are
/// // dropped, not replayed.
/// nvzn::describe_metrics();
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "prometheus")]
pub fn configure_prometheus(
    mut builder: metrics_exporter_prometheus::PrometheusBuilder,
) -> Result<metrics_exporter_prometheus::PrometheusBuilder, crate::Error> {
    use metrics_exporter_prometheus::Matcher;

    for (name, buckets) in histogram_buckets() {
        builder = builder
            .set_buckets_for_metric(Matcher::Full(name.to_owned()), buckets)
            .map_err(|e| crate::Error::Observability(format!("bucket config for {name}: {e}")))?;
    }
    Ok(builder)
}

/// Register every metric's help text and unit with the installed recorder.
///
/// Call once, **after** installing a recorder: descriptions sent to the no-op
/// recorder are dropped, not replayed.
pub fn describe_metrics() {
    for def in all_metrics() {
        match def.kind {
            MetricKind::Counter => match def.unit {
                Some(u) => metrics::describe_counter!(def.name, u, def.help),
                None => metrics::describe_counter!(def.name, def.help),
            },
            MetricKind::Gauge => match def.unit {
                Some(u) => metrics::describe_gauge!(def.name, u, def.help),
                None => metrics::describe_gauge!(def.name, def.help),
            },
            MetricKind::Histogram => match def.unit {
                Some(u) => metrics::describe_histogram!(def.name, u, def.help),
                None => metrics::describe_histogram!(def.name, def.help),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Emission
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    #[cfg(feature = "prometheus")]
    use nvzn_dispatcher::metrics::{FOLD_BATCH_ENTRIES, FOLD_BATCH_SECONDS};
    use nvzn_types::{StopReason, Tenant};
    use std::time::Duration;

    /// The namespace is a contract with every dashboard and alert, and this
    /// crate shares an endpoint with zslg, whose own names are `zslg_`.
    #[test]
    fn every_metric_is_namespaced() {
        for def in all_metrics() {
            assert!(
                def.name.starts_with(METRIC_PREFIX),
                "{} is outside the {METRIC_PREFIX} namespace",
                def.name,
            );
        }
    }

    #[test]
    fn naming_conventions_hold() {
        for def in all_metrics() {
            match def.kind {
                MetricKind::Counter => assert!(
                    def.name.ends_with("_total"),
                    "counter must end in _total: {}",
                    def.name,
                ),
                MetricKind::Histogram => assert!(
                    def.name.ends_with("_seconds")
                        || def.name.ends_with("_entries")
                        || def.name.ends_with("_bytes"),
                    "histogram must end in a unit suffix: {}",
                    def.name,
                ),
                MetricKind::Gauge => assert!(
                    !def.name.ends_with("_total"),
                    "gauge must not end in _total: {}",
                    def.name,
                ),
            }
            assert!(
                def.help.len() >= 20,
                "help text too short to be useful: {}",
                def.name,
            );
        }
    }

    /// The point of deriving from the declared unit: an `_entries` histogram
    /// must not land on a latency ladder, and every histogram must get buckets
    /// without anyone naming it.
    #[test]
    fn every_histogram_gets_buckets_matching_its_unit() {
        let mut seen = 0usize;
        for (name, buckets) in histogram_buckets() {
            seen = seen.saturating_add(1);
            assert!(name.starts_with(METRIC_NAMESPACE), "{name} is unnamespaced");
            assert!(!buckets.is_empty(), "{name} got no buckets");

            let top = buckets.last().copied().unwrap_or_default();
            if name.ends_with("_entries") {
                assert!(top >= 64.0, "{name} counts entries but tops out at {top}");
            } else if name.ends_with("_seconds") {
                assert!(top <= 3_600.0, "{name} is seconds but tops out at {top}");
            } else if name.ends_with("_bytes") {
                assert!(top >= 1e6, "{name} is bytes but tops out at {top}");
            }
        }
        assert_eq!(
            seen,
            all_metrics()
                .filter(|d| d.kind == MetricKind::Histogram)
                .count(),
            "every registered histogram must be bucketed",
        );
    }

    /// `configure_prometheus` end to end: the ladders reach the endpoint, and
    /// the two units get *different* ones — which is the whole reason buckets
    /// are derived from the declared unit rather than set once for the crate.
    ///
    /// Built, not installed: a recorder is process-global, and a test must not
    /// take that from the process. A second instrumented crate composes by
    /// threading this same builder through its own `configure_prometheus`.
    #[cfg(feature = "prometheus")]
    #[test]
    fn configure_prometheus_gives_each_unit_its_own_ladder() {
        use metrics_exporter_prometheus::PrometheusBuilder;

        let builder = configure_prometheus(PrometheusBuilder::new()).expect("nvzn buckets");
        let recorder = builder.build_recorder();
        let handle = recorder.handle();

        ::metrics::with_local_recorder(&recorder, || {
            ::metrics::histogram!(FOLD_BATCH_ENTRIES).record(3.0);
            ::metrics::histogram!(FOLD_BATCH_SECONDS).record(0.003);
        });

        let rendered = handle.render();
        assert!(
            rendered.contains(&format!("{FOLD_BATCH_ENTRIES}_bucket{{le=\"128\"}}")),
            "a count histogram must get the count ladder:\n{rendered}"
        );
        assert!(
            rendered.contains(&format!("{FOLD_BATCH_SECONDS}_bucket{{le=\"0.025\"}}")),
            "a latency histogram must get the seconds ladder:\n{rendered}"
        );
        assert!(
            !rendered.contains(&format!("{FOLD_BATCH_ENTRIES}_bucket{{le=\"0.025\"}}")),
            "a count histogram must not land on the latency ladder:\n{rendered}"
        );
    }

    /// The claim the split rests on: two instrumented crates configure one
    /// exporter by threading the same builder, and both get their own ladders
    /// on one endpoint. Built, not installed — a recorder is process-global,
    /// and a test must not take that from the process.
    #[cfg(feature = "prometheus")]
    #[test]
    fn both_crates_configure_one_exporter() {
        use metrics_exporter_prometheus::PrometheusBuilder;

        let builder = zslg::configure_prometheus(PrometheusBuilder::new()).expect("zslg buckets");
        let builder = configure_prometheus(builder).expect("nvzn buckets");
        let recorder = builder.build_recorder();
        let handle = recorder.handle();

        ::metrics::with_local_recorder(&recorder, || {
            ::metrics::histogram!(FOLD_BATCH_ENTRIES).record(3.0);
            ::metrics::histogram!("zslg_hlc_drift_seconds").record(0.15);
        });

        let rendered = handle.render();
        assert!(
            rendered.contains(&format!("{FOLD_BATCH_ENTRIES}_bucket{{le=\"128\"}}")),
            "nvzn's count ladder is missing from the shared endpoint:\n{rendered}"
        );
        assert!(
            rendered.contains("zslg_hlc_drift_seconds_bucket{le=\"0.175\"}"),
            "zslg's drift override is missing from the shared endpoint:\n{rendered}"
        );
    }

    #[test]
    fn all_metric_names_are_unique() {
        let mut names: Vec<&str> = all_metrics().map(|d| d.name).collect();
        let before = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            before,
            names.len(),
            "duplicate metric name in all_metrics()"
        );
    }

    /// Every helper, once, under a local recorder: the names and label sets a
    /// dashboard will query, asserted rather than assumed. The *wiring* needs
    /// no test — a helper nothing calls is an unused `pub(crate)` fn, which
    /// `clippy -D warnings` rejects as dead code.
    #[test]
    fn every_helper_emits_its_registered_name() {
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let tenant = Tenant::new("acct", 0);
        metrics::with_local_recorder(&recorder, || {
            nvzn_dispatcher::metrics::fold_watermark(
                Some(zslg::LogIndex::from_bytes(7u64.to_be_bytes())),
                Some(zslg::Hlc::from_bytes(1u64.to_be_bytes())),
            );
            nvzn_dispatcher::metrics::fold_batch(3, 1_000, Duration::from_millis(1));
            nvzn_dispatcher::metrics::fold_entry(nvzn_dispatcher::metrics::EntryOutcome::Applied);
            nvzn_dispatcher::metrics::fold_stopped(StopReason::PoisonEntry);
            nvzn_projector::metrics::projection_row(
                tenant,
                "Todo",
                nvzn_projector::metrics::RowOp::Upsert,
            );
            nvzn_dispatcher::metrics::store_fence(
                nvzn_dispatcher::metrics::FenceOutcome::Ok,
                Duration::from_millis(1),
            );
            nvzn_store_rocksdb::metrics::store_live_sst_files_size_bytes("store", 4096);
            nvzn_dispatcher::metrics::store_reset();
            nvzn_types::metrics::write(tenant, 4);
            nvzn_dispatcher::metrics::read(tenant, nvzn_dispatcher::metrics::ReadKind::Live);
            nvzn_dispatcher::metrics::live_held(tenant, 2, 300);
            nvzn_dispatcher::metrics::live_coverage(tenant, "complete");
            nvzn_dispatcher::metrics::live_materialise(2, Duration::from_millis(1));
            nvzn_dispatcher::metrics::live_scan(5, Duration::from_millis(1));
            nvzn_dispatcher::metrics::live_undecodable(tenant);
        });

        let snapshot = snapshotter.snapshot().into_hashmap();
        let emitted: Vec<&str> = snapshot.keys().map(|k| k.key().name()).collect();
        for def in all_metrics() {
            assert!(
                emitted.contains(&def.name),
                "{} is registered but no helper emitted it",
                def.name,
            );
        }

        let labels = |name: &str| -> Vec<String> {
            let mut ls: Vec<String> = snapshot
                .keys()
                .find(|k| k.key().name() == name)
                .map(|k| {
                    k.key()
                        .labels()
                        .map(|l| format!("{}={}", l.key(), l.value()))
                        .collect()
                })
                .unwrap_or_default();
            ls.sort();
            ls
        };
        assert_eq!(
            labels(nvzn_projector::metrics::PROJECTION_ROWS_TOTAL),
            vec!["op=upsert", "projection=Todo", "tenant=acct"],
        );
        assert_eq!(
            labels(nvzn_store_rocksdb::metrics::STORE_LIVE_SST_FILES_SIZE_BYTES),
            vec!["cf=store"]
        );
        assert_eq!(
            labels(nvzn_dispatcher::metrics::READS_TOTAL),
            vec!["kind=live", "tenant=acct"]
        );
        assert_eq!(
            labels(nvzn_dispatcher::metrics::FOLD_STOPPED_TOTAL),
            vec!["reason=poison_entry"]
        );
        // `foreign_tenant` ids are unbounded on a shared log, so this one
        // carries no tenant label by design.
        assert_eq!(
            labels(nvzn_dispatcher::metrics::FOLD_ENTRIES_TOTAL),
            vec!["outcome=applied"]
        );
    }

    #[test]
    fn short_type_name_takes_the_last_segment() {
        assert_eq!(
            nvzn_projector::metrics::short_type_name::<nvzn_fixtures::Todo>(),
            "Todo"
        );
    }

    const GOLDEN_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/metrics.golden");

    /// The exported metric set — names, kinds, units and help text — is a
    /// contract with dashboards and alerts. A restructuring of the code that
    /// emits them must not change it. `UPDATE_GOLDEN=1` rewrites the file.
    #[test]
    fn the_registry_matches_the_golden_dump() {
        let mut lines: Vec<String> = all_metrics()
            .map(|def| {
                format!(
                    "{}\t{:?}\t{:?}\t{}\n",
                    def.name, def.kind, def.unit, def.help
                )
            })
            .collect();
        lines.sort();
        let now = lines.concat();
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            std::fs::write(GOLDEN_PATH, &now).unwrap();
            return;
        }
        let golden = std::fs::read_to_string(GOLDEN_PATH).unwrap();
        assert!(
            golden == now,
            "metric registry differs from src/metrics.golden"
        );
    }
}
