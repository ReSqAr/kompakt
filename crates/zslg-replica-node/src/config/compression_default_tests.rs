use super::*;
use rocksdb_kit::Plain;
use zslg_learner::{LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF, STABLE_LOG_CF};
use zslg_replica_log::REPLICA_LOG_CF;

fn cfg() -> ReplicaNodeConfig {
    ReplicaNodeConfig::new(
        "localhost".into(),
        Vec::new(),
        "localhost".into(),
        "127.0.0.1:0".parse().expect("addr"),
        PathBuf::from("/tmp/unused"),
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    )
    .expect("config")
}

/// These defaults *are* the measured configuration (+22% for the local
/// store alone, up to +36% with the replica log too). A silent drift would
/// invalidate docs/OPTIMISATION.md, so pin them.
#[test]
fn compression_defaults_match_the_measured_configuration() {
    let c = cfg();
    assert!(!c.replica_log_compression);
    assert!(!c.learner_local_store_compression);
    assert!(c.stable_log_compression);
}

/// The flat fields must actually reach the column-family list — a default
/// that is never carried down is worse than no default at all. This is now
/// the *only* hop: compression is a property of the family, chosen once
/// when the node's one DB opens.
#[test]
fn compression_reaches_the_column_families() {
    let c = cfg();
    let compress = |s: ColumnFamily<Plain>| {
        c.column_families()
            .into_iter()
            .find(|spec| spec.cf == s)
            .map(|spec| spec.compress)
    };
    assert_eq!(compress(REPLICA_LOG_CF), Some(false));
    assert_eq!(compress(STABLE_LOG_CF), Some(true));
    assert_eq!(compress(LOCAL_STORE_DATA_CF), Some(false));
    assert_eq!(compress(LOCAL_STORE_CURSORS_CF), Some(false));
}

/// A replica runs seven families and no more. The list IS the schema — it
/// is what RocksDB creates, and its length is what sizes each memtable —
/// so a silent addition costs every other family memory and makes the
/// root un-openable by a build that does not name it.
#[test]
fn a_replica_runs_exactly_seven_column_families() {
    let names: Vec<&str> = cfg()
        .column_families()
        .iter()
        .map(|spec| spec.cf.name())
        .collect();
    assert_eq!(
        names,
        vec![
            "raft_log",
            "raft_meta",
            "replica_log",
            "stable_log",
            "local_store_data",
            "local_store_cursors",
            "node_id",
        ],
    );
}
