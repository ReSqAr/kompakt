use super::*;
use zslg_lifecycle_types::ConfigError;
use zslg_object_store_config::ObjectStoreConfig;

fn ok_config() -> ReplicaNodeConfig {
    let peers = vec![PeerAddr {
        addr: "127.0.0.1:1".into(),
        server_name: "localhost".into(),
    }];
    ReplicaNodeConfig::new(
        "localhost".into(),
        peers,
        "localhost".into(),
        "127.0.0.1:0".parse().expect("addr"),
        PathBuf::from("/tmp/unused"),
        ObjectStoreConfig::InMemory,
    )
    .expect("ok_config")
}

// ── validate: baseline ──────────────────────────────────────────────

#[test]
fn defaults_validate_with_one_peer() {
    ok_config().validate().expect("valid");
}

/// The 20 MiB low-water config is the one that finds sizing bugs, so it
/// stays runnable: it must validate, and — because every storage knob is
/// unset by default — every derived term must come from that budget rather
/// than from a constant that ignores it.
#[test]
fn the_low_water_budget_validates_and_derives_its_sizing() {
    let mut c = ok_config();
    c.storage_memory = ByteSize::from_mib(20);
    c.validate()
        .expect("the 20 MiB low-water config must stay valid");

    let low = c.storage_config();
    assert_eq!(low.memory, ByteSize::from_mib(20));
    assert!(
        low.write_buffer.is_none(),
        "sizing knobs default to derived"
    );
    assert!(low.max_total_wal.is_none());
    low.validate().expect("derived storage tuning is valid");
}

/// A knob a node *does* set is rejected at node-config validation, not at
/// `Storage::open` in the middle of startup, and the message names it.
#[test]
fn a_bad_storage_knob_is_rejected_by_node_validation() {
    let mut c = ok_config();
    c.storage_write_buffer = Some(ByteSize::from_bytes(1));
    let err = c.validate().expect_err("must reject");
    assert!(
        format!("{err}").contains("storage_write_buffer"),
        "error must name the offending knob, got: {err}"
    );
}

// ── validate: constraint rejections ─────────────────────────────────

#[test]
fn rejects_cleanup_retention_below_purge_grace() {
    let mut c = ok_config();
    c.replica_log_purge_grace_period = Duration::from_secs(120);
    c.snapshot_cleanup_retention = Duration::from_secs(60);
    assert!(matches!(
        c.validate(),
        Err(Error::Config(ConfigError::InvalidConfiguration(_)))
    ));
}

#[test]
fn rejects_purge_grace_below_quorum_read_timeout() {
    let mut c = ok_config();
    c.learner_quorum_read_timeout = Duration::from_secs(30);
    c.replica_log_purge_grace_period = Duration::from_secs(29);
    let err = c.validate().expect_err("must reject");
    assert!(
        format!("{err}").contains("learner_quorum_read_timeout"),
        "error must name both knobs, got: {err}"
    );
}

#[test]
fn accepts_purge_grace_equal_to_quorum_read_timeout() {
    let mut c = ok_config();
    c.learner_quorum_read_timeout = Duration::from_secs(30);
    c.replica_log_purge_grace_period = Duration::from_secs(30);
    c.validate().expect("equality boundary admitted");
}

#[test]
fn accepts_cleanup_retention_equal_to_purge_grace() {
    let mut c = ok_config();
    c.snapshot_interval = Duration::from_secs(60);
    c.snapshot_compaction_tiers[0] = Duration::from_secs(60).into();
    c.replica_log_purge_grace_period = Duration::from_secs(60);
    c.snapshot_cleanup_retention = Duration::from_secs(60);
    c.validate().expect("equality boundary admitted");
}

// ── ObjectStoreConfig ───────────────────────────────────────────────

#[test]
fn object_store_inmemory_builds() {
    let cfg = ObjectStoreConfig::InMemory;
    let _store = cfg.build().expect("InMemory store builds");
}

#[test]
fn rejects_empty_db_path() {
    let mut c = ok_config();
    c.db_path = PathBuf::new();
    assert!(matches!(
        c.validate(),
        Err(Error::Config(ConfigError::InvalidConfiguration(_)))
    ));
}

// Cross-field (moved here from SnapshotConfig::validate): the HLC skew bound
// must cover the snapshot cutoff timeout.
#[test]
fn rejects_max_clock_skew_below_snapshot_cutoff() {
    let mut c = ok_config();
    c.snapshot_cutoff_timeout = Duration::from_secs(1);
    c.hlc_max_clock_skew = Duration::from_millis(500);
    assert!(matches!(
        c.validate(),
        Err(Error::Config(ConfigError::InvalidConfiguration(_)))
    ));
}

#[test]
fn accepts_max_clock_skew_equal_to_snapshot_cutoff() {
    let mut c = ok_config();
    c.snapshot_cutoff_timeout = Duration::from_secs(1);
    c.hlc_max_clock_skew = Duration::from_secs(1);
    c.validate().expect("equality boundary admitted");
}

// ── validate: own config must include self ──────────────────────────

/// A node whose own `peers` omits its own server name is misconfigured: it
/// has been told it is not part of this cluster. That is fatal at startup,
/// and is deliberately different from the *group* removing it from the
/// voter set — which is not fatal (see the rejoin monitor).
#[test]
fn validate_rejects_config_whose_peers_omit_self() {
    let cfg = ReplicaNodeConfig::new(
        "node-a".into(),
        vec![PeerAddr {
            addr: "127.0.0.1:9001".into(),
            server_name: "node-b".into(),
        }],
        "node-a".into(),
        "127.0.0.1:9000".parse().expect("addr"),
        PathBuf::from("/tmp/zslg-validate-test"),
        ObjectStoreConfig::InMemory,
    )
    .expect("config builds");

    let err = cfg.validate().expect_err("must reject");
    assert!(
        matches!(err, Error::Config(ConfigError::InvalidConfiguration(ref m)) if m.contains("node-a")),
        "the error must name the missing server name: {err:?}"
    );
}

/// The ordinary case still validates: self present among the peers.
#[test]
fn validate_accepts_config_whose_peers_include_self() {
    let cfg = ReplicaNodeConfig::new(
        "node-a".into(),
        vec![
            PeerAddr {
                addr: "127.0.0.1:9000".into(),
                server_name: "node-a".into(),
            },
            PeerAddr {
                addr: "127.0.0.1:9001".into(),
                server_name: "node-b".into(),
            },
        ],
        "node-a".into(),
        "127.0.0.1:9000".parse().expect("addr"),
        PathBuf::from("/tmp/zslg-validate-test"),
        ObjectStoreConfig::InMemory,
    )
    .expect("config builds");

    cfg.validate().expect("self is present, must accept");
}
