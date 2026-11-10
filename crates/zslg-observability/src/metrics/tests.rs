//! Tests for `metrics`.

use super::*;

#[test]
fn all_metric_names_are_unique_and_non_empty() {
    let mut seen = std::collections::HashSet::new();
    for def in ALL_METRICS.iter().copied().flatten() {
        assert!(!def.name.is_empty(), "metric name must not be empty");
        assert!(seen.insert(def.name), "duplicate metric name: {}", def.name);
    }
}

/// Prometheus stamps `job` and `instance` on every scraped series from the
/// scrape config. Without `honor_labels: true` (the harness does not set it,
/// and should not have to) an exposed label of the same name is silently
/// renamed to `exported_<name>`, so every dashboard selector on it breaks in
/// production while passing locally. No emitted label key may collide.
#[test]
fn no_label_key_collides_with_prometheus_scrape_labels() {
    const RESERVED: [&str; 2] = ["job", "instance"];
    for key in ALL_LABEL_KEYS {
        assert!(
            !RESERVED.contains(key),
            "label key {key:?} collides with a Prometheus scrape label and \
             would be exposed as exported_{key}",
        );
    }
}

#[test]
fn rocksdb_properties_are_registered() {
    // `actual-delayed-write-rate` is the one that answers "was the seize-up
    // the 20 MiB budget or the host?" — the question the 2026-08-17 harness
    // run could not answer, because nothing exported it.
    for want in [
        "zslg_storage_num_immutable_memtable",
        "zslg_storage_memtable_flush_pending",
        "zslg_storage_compaction_pending",
        "zslg_storage_estimate_num_keys",
        "zslg_storage_live_sst_files_size_bytes",
        "zslg_storage_block_cache_usage_bytes",
        "zslg_storage_actual_delayed_write_rate",
    ] {
        assert!(
            ALL_METRICS
                .iter()
                .copied()
                .flatten()
                .any(|m| m.name == want),
            "{want} is not registered in ALL_METRICS",
        );
    }
}

/// The namespace is a contract with every dashboard and alert, so it is
/// asserted rather than left to the macro's good behaviour. `process_*` is
/// the documented exemption: those names are the Prometheus-standard
/// collector namespace, shared with every other client library.
#[test]
fn every_metric_is_namespaced() {
    for def in ALL_METRICS.iter().copied().flatten() {
        assert!(
            def.name.starts_with(METRIC_PREFIX) || def.name.starts_with("process_"),
            "{} is outside the {METRIC_PREFIX} namespace and is not a \
             standard process_* metric",
            def.name,
        );
    }
}

#[test]
fn no_metric_refers_to_a_deleted_engine() {
    // The engine is gone from the binary; this stops its names creeping back
    // into the registry, where they would look alive to a dashboard.
    for m in ALL_METRICS.iter().copied().flatten() {
        assert!(
            !m.name.contains("strata"),
            "{} names an engine that no longer exists",
            m.name,
        );
    }
}

#[test]
fn naming_conventions_hold() {
    for def in ALL_METRICS.iter().copied().flatten() {
        match def.kind {
            MetricKind::Counter => assert!(
                def.name.ends_with("_total"),
                "counter must end in _total: {}",
                def.name,
            ),
            MetricKind::Histogram => assert!(
                def.name.ends_with("_seconds")
                    || def.name.ends_with("_bytes")
                    || def.name.ends_with("_entries"),
                "histogram must end in a unit suffix (_seconds/_bytes/_entries): {}",
                def.name,
            ),
            MetricKind::Gauge => assert!(
                !def.name.ends_with("_total"),
                "gauge must not end in _total: {}",
                def.name,
            ),
        }
    }
}

#[test]
fn histograms_declare_units() {
    for def in ALL_METRICS.iter().copied().flatten() {
        if def.kind == MetricKind::Histogram {
            assert!(
                def.unit.is_some(),
                "histogram must declare a unit: {}",
                def.name,
            );
        }
    }
}

#[test]
fn every_metric_has_help_text() {
    for def in ALL_METRICS.iter().copied().flatten() {
        assert!(
            def.help.len() >= 20,
            "help text too short to be useful: {}",
            def.name,
        );
    }
}

#[test]
fn all_design_doc_metrics_present() {
    let set: std::collections::HashSet<&str> = ALL_METRICS
        .iter()
        .copied()
        .flatten()
        .map(|d| d.name)
        .collect();
    // The design doc §"Observability" list, verbatim.
    for required in [
        "zslg_raft_is_member",
        "zslg_raft_is_leader",
        "zslg_raft_has_leader",
        "zslg_raft_term",
        "zslg_raft_applied_log_index",
        "zslg_raft_leader_changes_total",
        "zslg_raft_in_joint_config",
        "zslg_raft_config_generation",
        "zslg_raft_fenced_writes_total",
        "zslg_replica_nodes_count",
        "zslg_membership_changes_total",
        "zslg_node_rejoins_total",
        "zslg_write_duration_seconds",
        "zslg_write_fanout_responses_total",
        "zslg_writes_in_flight",
        "zslg_replica_write_requests_total",
        "zslg_hlc_guards_in_flight",
        "zslg_hlc_cutoff_sets_total",
        "zslg_hlc_cutoff_drain_duration_seconds",
        "zslg_log_writes_total",
        "zslg_log_index_frontier",
        "zslg_log_subscription_reconnects_total",
        "zslg_local_store_integrate_queue_depth",
        "zslg_purged_entries_total",
        "zslg_manifest_watermark_advanced_timestamp_seconds",
        "zslg_stable_log_end_hlc_advanced_timestamp_seconds",
        "zslg_snapshot_creation_total",
        "zslg_snapshot_creation_duration_seconds",
        "zslg_snapshot_size_bytes",
        "zslg_snapshot_entries_count",
        "zslg_manifest_entries_count",
        "zslg_compaction_runs_total",
        "zslg_leader_tasks_active",
        "zslg_object_store_request_duration_seconds",
        "zslg_object_store_transfer_bytes_total",
        "zslg_hlc_now_physical_ms",
        "zslg_hlc_logical",
        "zslg_hlc_wall_clock_drift_ms",
        "zslg_hlc_cutoff_physical_ms",
        "zslg_manifest_end_hlc_physical_ms",
        "zslg_reads_total",
        "zslg_read_token_wait_duration_seconds",
        "zslg_read_token_wait_timeouts_total",
        "zslg_quorum_reads_total",
        "zslg_rpc_server_requests_total",
        "zslg_rpc_server_request_duration_seconds",
        "zslg_rpc_connections_active",
        "zslg_rpc_conn_checkout_seconds",
        "zslg_rpc_conn_reuse_total",
        "zslg_rpc_open_stream_seconds",
        "zslg_rpc_roundtrip_seconds",
    ] {
        assert!(
            set.contains(required),
            "missing required metric: {required}"
        );
    }
}

// ── emission smoke tests ─────────────────────────────────────────────

#[test]
fn set_flag_and_set_gauge_are_observable() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        set_flag(zslg_raft_state::metrics::RAFT_IS_LEADER, true);
        set_gauge(zslg_raft_state::metrics::RAFT_TERM, 7);
    });
    let snap = snapshotter.snapshot();
    let map = snap.into_hashmap();
    let found = map
        .keys()
        .any(|ck| ck.key().name() == zslg_raft_state::metrics::RAFT_IS_LEADER);
    assert!(found, "raft_is_leader gauge should be present");
}

#[test]
fn counter_emission_is_observable() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        ::metrics::counter!(zslg_raft_node::metrics::RAFT_FENCED_WRITES_TOTAL).increment(1);
    });
    let snap = snapshotter.snapshot();
    let map = snap.into_hashmap();
    let found = map
        .keys()
        .any(|ck| ck.key().name() == zslg_raft_node::metrics::RAFT_FENCED_WRITES_TOTAL);
    assert!(found, "raft_fenced_writes_total counter should be present");
}

#[test]
fn probe_finish_records_chosen_outcome() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        zslg_stable_log::metrics::stable_log_append_probe().finish(OpState::Success);
    });
    let snap = snapshotter.snapshot();
    let map = snap.into_hashmap();
    let found = map.keys().any(|ck| {
        ck.key().name() == zslg_stable_log::metrics::STABLE_LOG_APPEND_DURATION_SECONDS
            && ck
                .key()
                .labels()
                .any(|l| l.key() == LABEL_STATE && l.value() == OpState::Success.as_str())
    });
    assert!(found, "probe.finish should record the chosen outcome label");
}

#[test]
fn probe_dropped_without_finish_records_default_outcome() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let _probe = zslg_stable_log::metrics::stable_log_append_probe();
        // dropped at end of scope without finish -> default outcome (Failure).
    });
    let snap = snapshotter.snapshot();
    let map = snap.into_hashmap();
    let found = map.keys().any(|ck| {
        ck.key().name() == zslg_stable_log::metrics::STABLE_LOG_APPEND_DURATION_SECONDS
            && ck
                .key()
                .labels()
                .any(|l| l.key() == LABEL_STATE && l.value() == OpState::Failure.as_str())
    });
    assert!(
        found,
        "a dropped probe should record its default outcome label"
    );
}

#[test]
fn watermark_advance_stamps_emit_epoch_timestamps() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        zslg_raft_state::metrics::manifest_watermark_advanced();
        zslg_stable_log::metrics::stable_log_end_advanced();
    });
    let map = snapshotter.snapshot().into_hashmap();
    // Each stamp must be a wall-clock epoch value. A near-zero reading
    // means it is measuring a duration instead, and Prometheus renders the
    // subtraction as nonsense.
    let is_epoch = |name: &str| {
        map.iter().any(|(ck, (_, _, v))| {
            ck.key().name() == name
                && matches!(
                    v,
                    metrics_util::debugging::DebugValue::Gauge(g) if g.0 > 1_700_000_000.0
                )
        })
    };
    assert!(
        is_epoch(zslg_raft_state::metrics::MANIFEST_WATERMARK_ADVANCED_TIMESTAMP_SECONDS),
        "manifest_watermark_advanced should emit a Unix-epoch timestamp"
    );
    assert!(
        is_epoch(zslg_stable_log::metrics::STABLE_LOG_END_HLC_ADVANCED_TIMESTAMP_SECONDS),
        "stable_log_end_advanced should emit a Unix-epoch timestamp"
    );
}
