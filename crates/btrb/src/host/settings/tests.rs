use super::*;

/// Metrics must not be exported to the world because nobody said not to.
#[test]
fn metrics_default_to_loopback() {
    assert!(
        btrb_metrics::settings::DEFAULT_LISTEN_ADDR
            .ip()
            .is_loopback(),
        "the default metrics listener is reachable off-box"
    );
}

/// The two listeners must not collide, or one silently fails to bind.
#[test]
fn the_two_listeners_have_different_defaults() {
    assert_ne!(
        btrb_web::settings::DEFAULT_LISTEN_ADDR,
        btrb_metrics::settings::DEFAULT_LISTEN_ADDR
    );
}

#[test]
fn a_standalone_log_store_is_small_on_purpose() {
    let mut env = Env::from_pairs(&[
        ("ZSLG_SERVER_NAME", "a"),
        ("ZSLG_PEERS", "a@127.0.0.1:9000"),
        ("ZSLG_BOOTSTRAP_SERVER_NAME", "a"),
        ("ZSLG_LISTEN_ADDR", "127.0.0.1:9000"),
        ("ZSLG_DB_PATH", ":temp:"),
        ("ZSLG_OBJECT_STORE", ":memory:"),
    ]);
    let c =
        btrb_db::settings::zslg_config(&mut env, &mut Vec::new(), STANDALONE_ZSLG_MEMORY).unwrap();
    assert_eq!(c.storage_memory, zslg::ByteSize::from_mib(16));
    assert_eq!(STANDALONE_NVZN_MEMORY, zslg::ByteSize::from_mib(32));
    assert_eq!(node().storage_memory, SERVER_STORAGE_MEMORY);
}

fn node() -> zslg::ReplicaNodeConfig {
    let mut env = Env::from_pairs(&[
        ("ZSLG_SERVER_NAME", "a"),
        ("ZSLG_PEERS", "a@127.0.0.1:9000"),
        ("ZSLG_BOOTSTRAP_SERVER_NAME", "a"),
        ("ZSLG_LISTEN_ADDR", "127.0.0.1:9000"),
        ("ZSLG_DB_PATH", ":temp:"),
        ("ZSLG_OBJECT_STORE", ":memory:"),
    ]);
    btrb_db::settings::zslg_config(&mut env, &mut Vec::new(), SERVER_STORAGE_MEMORY).unwrap()
}
