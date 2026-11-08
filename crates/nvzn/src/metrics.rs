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

use crate::{StopReason, Tenant};
use metrics_kit::{MetricDef, MetricKind, Unit, lossy_f64, metrics, unix_now_secs};
use std::time::Duration;

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

const LABEL_TENANT: &str = "tenant";
const LABEL_OUTCOME: &str = "outcome";
const LABEL_REASON: &str = "reason";
const LABEL_KIND: &str = "kind";
const LABEL_STATE: &str = "state";

/// What the fold loop did with a log entry.
#[derive(Debug, Clone, Copy)]
pub(crate) enum EntryOutcome {
    /// Decoded and handed to the projectors.
    Applied,
    /// At or below the stored watermark: already folded.
    Replayed,
    /// A tenant this dispatcher hosts no handler for.
    ForeignTenant,
    /// A watermark advance, carrying no payload.
    Watermark,
}

impl EntryOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Replayed => "replayed",
            Self::ForeignTenant => "foreign_tenant",
            Self::Watermark => "watermark",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum FenceOutcome {
    Ok,
    Failed,
}

impl FenceOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// Which snapshot a read was served from. `stable` sees only what has gone
/// stable in the log; `live` adds the live events this node has received.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReadKind {
    Stable,
    Live,
}

impl ReadKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Live => "live",
        }
    }
}

// ---------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------

metrics!("nvzn_", nvzn; {
    custom live_held_events LIVE_HELD_EVENTS "live_held_events" Gauge Count
        "Writes a live view holds above the fold's watermark, by tenant: the \
               `S` events that reached this node and are not folded yet. Small \
               unless the fold stalls or `S` events flood.";
    custom live_held_bytes LIVE_HELD_BYTES "live_held_bytes" Gauge Bytes
        "Payload bytes of the writes a live view holds, by tenant; with \
               nvzn_live_held_events, what the overflow cap is checked against.";
    custom live_coverage LIVE_COVERAGE "live_coverage" Gauge None
        "A live view's state, by tenant and state: 1 for the current one of \
               `scanning` (cold start, snapshots wait), `complete`, `overflowed` \
               (over the cap: snapshots serve the fold's state alone until it \
               passes what was dropped).";
    custom live_materialise_seconds LIVE_MATERIALISE_SECONDS "live_materialise_seconds" Histogram Seconds
        "Time to build one live snapshot: open the store, fold the held \
               events through the live projections.";
    custom live_materialise_entries LIVE_MATERIALISE_ENTRIES "live_materialise_entries" Histogram Count
        "Held writes folded into one live snapshot. Repeated per snapshot, \
               so this is the per-read cost the held set adds.";
    custom live_scan_seconds LIVE_SCAN_SECONDS "live_scan_seconds" Histogram Seconds
        "Duration of a live view's cold-start scan of the stable log, \
               which the first snapshots wait for.";
    custom live_scan_entries LIVE_SCAN_ENTRIES "live_scan_entries" Histogram Count
        "Stable-log entries a live view's cold-start scan read: the gap \
               between the store's watermark and the stable watermark at start.";
    custom live_undecodable_total LIVE_UNDECODABLE_TOTAL "live_undecodable_total" Counter None
        "Payloads a live view could not decode, by tenant: logged and \
               skipped (the fold stops on the same entry once it is stable).";
    custom fold_watermark_advanced_timestamp_seconds FOLD_WATERMARK_ADVANCED_TIMESTAMP_SECONDS "fold_watermark_advanced_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds of the last fold-watermark advance. THE staleness \
               alert, evaluated as `time() - nvzn_fold_watermark_advanced_timestamp_seconds`: \
               a lag gauge goes stale exactly when the dispatcher dies, this keeps climbing.";
    custom fold_watermark_log_index FOLD_WATERMARK_LOG_INDEX "fold_watermark_log_index" Gauge Count
        "Stable-log index the read model has folded through. Where the \
               projections are, in log terms.";
    custom fold_watermark_hlc_physical_ms FOLD_WATERMARK_HLC_PHYSICAL_MS "fold_watermark_hlc_physical_ms" Gauge Milliseconds
        "Physical-millisecond part of the folded watermark's HLC. Distance to \
               zslg's zslg_manifest_end_hlc_physical_ms is projection lag in \
               wall-clock terms.";
    custom fold_batches_total FOLD_BATCHES_TOTAL "fold_batches_total" Counter None
        "Batches committed by the fold loop. One batch is one store transaction \
               covering whole log entries until it holds `fold_commit` bytes of writes.";
    custom fold_batch_seconds FOLD_BATCH_SECONDS "fold_batch_seconds" Histogram Seconds
        "Time to fold and commit one batch: decode, project, and commit.";
    custom fold_batch_entries FOLD_BATCH_ENTRIES "fold_batch_entries" Histogram Count
        "Log entries per committed batch.";
    custom fold_batch_bytes FOLD_BATCH_BYTES "fold_batch_bytes" Histogram Bytes
        "Bytes a committed batch staged (rows and index entries, estimated). \
               At `fold_commit` (4 MiB unless configured) a burst folds in several commits.";
    custom fold_entries_total FOLD_ENTRIES_TOTAL "fold_entries_total" Counter None
        "Log entries seen by the fold loop, by what became of them. Deliberately \
               unlabelled by tenant: `foreign_tenant` counts entries for tenants this \
               dispatcher does not host, whose ids are unbounded on a shared log.";
    custom fold_stopped_total FOLD_STOPPED_TOTAL "fold_stopped_total" Counter None
        "Fold-loop exits by reason. `poison_entry` and `non_monotone_data_id` \
               are permanent: the entry stays in the log, so a restart fails \
               identically and the read model is frozen until an operator acts.";
    custom store_fence_total STORE_FENCE_TOTAL "store_fence_total" Counter None
        "Store fsyncs by outcome. `failed` is terminal — nothing this dispatcher \
               commits can be made durable any more, so the fold loop stops.";
    custom store_fence_seconds STORE_FENCE_SECONDS "store_fence_seconds" Histogram Seconds
        "Time for one store fsync (flush_wal).";
    custom store_resets_total STORE_RESETS_TOTAL "store_resets_total" Counter None
        "Config-hash mismatches that wiped the store and refolded the log from \
               index zero. Expected on a deploy that changes the registered \
               projections; a steady rate is not.";
    custom reads_total READS_TOTAL "reads_total" Counter None
        "Snapshots opened, by tenant and freshness. `stable` sees only what has \
               gone stable in the log, so a caller expecting read-your-own-writes \
               and finding `stable` here is reading the wrong one.";
});

const OWN_METRICS: &[MetricDef] = &nvzn!();

/// Every metric the nvzn crates declare: this facade's slice, then each
/// emitting crate's.
pub(crate) fn all_metrics() -> impl Iterator<Item = &'static MetricDef> {
    OWN_METRICS
        .iter()
        .chain(nvzn_projector::metrics::METRICS.iter())
        .chain(nvzn_store_rocksdb::metrics::METRICS.iter())
        .chain(nvzn_types::metrics::METRICS.iter())
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

pub(crate) fn fold_watermark(index: Option<zslg::LogIndex>, hlc: Option<zslg::Hlc>) {
    metrics::gauge!(FOLD_WATERMARK_ADVANCED_TIMESTAMP_SECONDS).set(lossy_f64(unix_now_secs()));
    if let Some(index) = index {
        metrics::gauge!(FOLD_WATERMARK_LOG_INDEX).set(lossy_f64(index.raw()));
    }
    if let Some(hlc) = hlc {
        metrics::gauge!(FOLD_WATERMARK_HLC_PHYSICAL_MS).set(lossy_f64(hlc.physical_ms()));
    }
}

pub(crate) fn fold_batch(entries: usize, bytes: usize, elapsed: Duration) {
    metrics::counter!(FOLD_BATCHES_TOTAL).increment(1);
    metrics::histogram!(FOLD_BATCH_SECONDS).record(elapsed);
    metrics::histogram!(FOLD_BATCH_ENTRIES).record(lossy_f64(u64::try_from(entries).unwrap_or(0)));
    metrics::histogram!(FOLD_BATCH_BYTES).record(lossy_f64(u64::try_from(bytes).unwrap_or(0)));
}

pub(crate) fn fold_entry(outcome: EntryOutcome) {
    metrics::counter!(FOLD_ENTRIES_TOTAL, LABEL_OUTCOME => outcome.as_str()).increment(1);
}

pub(crate) fn fold_stopped(reason: StopReason) {
    metrics::counter!(FOLD_STOPPED_TOTAL, LABEL_REASON => reason.as_str()).increment(1);
}

pub(crate) fn store_fence(outcome: FenceOutcome, elapsed: Duration) {
    metrics::counter!(STORE_FENCE_TOTAL, LABEL_OUTCOME => outcome.as_str()).increment(1);
    metrics::histogram!(STORE_FENCE_SECONDS).record(elapsed);
}

pub(crate) fn store_reset() {
    metrics::counter!(STORE_RESETS_TOTAL).increment(1);
}

pub(crate) fn read(tenant: Tenant, kind: ReadKind) {
    metrics::counter!(READS_TOTAL, LABEL_TENANT => tenant.name(), LABEL_KIND => kind.as_str())
        .increment(1);
}

pub(crate) fn live_held(tenant: Tenant, events: usize, bytes: usize) {
    metrics::gauge!(LIVE_HELD_EVENTS, LABEL_TENANT => tenant.name())
        .set(lossy_f64(u64::try_from(events).unwrap_or(u64::MAX)));
    metrics::gauge!(LIVE_HELD_BYTES, LABEL_TENANT => tenant.name())
        .set(lossy_f64(u64::try_from(bytes).unwrap_or(u64::MAX)));
}

pub(crate) fn live_coverage(tenant: Tenant, current: &'static str) {
    for state in ["scanning", "complete", "overflowed"] {
        metrics::gauge!(LIVE_COVERAGE, LABEL_TENANT => tenant.name(), LABEL_STATE => state)
            .set(if state == current { 1.0 } else { 0.0 });
    }
}

pub(crate) fn live_materialise(entries: usize, elapsed: Duration) {
    metrics::histogram!(LIVE_MATERIALISE_SECONDS).record(elapsed);
    metrics::histogram!(LIVE_MATERIALISE_ENTRIES)
        .record(lossy_f64(u64::try_from(entries).unwrap_or(u64::MAX)));
}

pub(crate) fn live_scan(entries: u64, elapsed: Duration) {
    metrics::histogram!(LIVE_SCAN_SECONDS).record(elapsed);
    metrics::histogram!(LIVE_SCAN_ENTRIES).record(lossy_f64(entries));
}

pub(crate) fn live_undecodable(tenant: Tenant) {
    metrics::counter!(LIVE_UNDECODABLE_TOTAL, LABEL_TENANT => tenant.name()).increment(1);
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
            fold_watermark(
                Some(zslg::LogIndex::from_bytes(7u64.to_be_bytes())),
                Some(zslg::Hlc::from_bytes(1u64.to_be_bytes())),
            );
            fold_batch(3, 1_000, Duration::from_millis(1));
            fold_entry(EntryOutcome::Applied);
            fold_stopped(StopReason::PoisonEntry);
            nvzn_projector::metrics::projection_row(
                tenant,
                "Todo",
                nvzn_projector::metrics::RowOp::Upsert,
            );
            store_fence(FenceOutcome::Ok, Duration::from_millis(1));
            nvzn_store_rocksdb::metrics::store_live_sst_files_size_bytes("store", 4096);
            store_reset();
            nvzn_types::metrics::write(tenant, 4);
            read(tenant, ReadKind::Live);
            live_held(tenant, 2, 300);
            live_coverage(tenant, "complete");
            live_materialise(2, Duration::from_millis(1));
            live_scan(5, Duration::from_millis(1));
            live_undecodable(tenant);
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
        assert_eq!(labels(READS_TOTAL), vec!["kind=live", "tenant=acct"]);
        assert_eq!(labels(FOLD_STOPPED_TOTAL), vec!["reason=poison_entry"]);
        // `foreign_tenant` ids are unbounded on a shared log, so this one
        // carries no tenant label by design.
        assert_eq!(labels(FOLD_ENTRIES_TOTAL), vec!["outcome=applied"]);
    }

    #[test]
    fn short_type_name_takes_the_last_segment() {
        assert_eq!(
            nvzn_projector::metrics::short_type_name::<crate::testsupport::Todo>(),
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
