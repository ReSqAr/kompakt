use super::fields::{
    compression, listen_addr, object_store_config, peers, spare_selection, tiers, with_prefix,
};
use super::*;
use std::net::SocketAddr;
use std::sync::Arc;
use zslg::{ObjectStoreConfig, SpareSelectionStrategy};

const V: &str = "ZSLG_TEST";

#[test]
fn a_peer_may_name_itself_separately_from_its_dial_target() {
    let peers = peers(V, "node-a@node-a.svc.cluster.local:9000").unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(&*peers[0].server_name, "node-a");
    assert_eq!(
        peers[0].addr.to_string(),
        "node-a.svc.cluster.local:9000",
        "the dial target stays unresolved so DNS is re-read on reconnect"
    );
}

#[test]
fn a_peer_without_an_at_takes_its_name_from_the_host() {
    let peers = peers(V, "node-b:9000").unwrap();
    assert_eq!(&*peers[0].server_name, "node-b");
    assert_eq!(peers[0].addr.to_string(), "node-b:9000");
}

#[test]
fn peers_split_on_commas_and_tolerate_whitespace() {
    let peers = peers(V, " node-a@a:9000 , node-b@b:9000 ,").unwrap();
    assert_eq!(peers.len(), 2);
    assert_eq!(&*peers[1].server_name, "node-b");
}

#[test]
fn an_ipv6_literal_keeps_its_colons() {
    let peers = peers(V, "node-c@[::1]:9000").unwrap();
    assert_eq!(peers[0].addr.to_string(), "[::1]:9000");
}

fn node(peers: &str) -> ReplicaNodeConfig {
    let mut env = Env::from_pairs(&[
        ("ZSLG_SERVER_NAME", "a"),
        ("ZSLG_PEERS", peers),
        ("ZSLG_BOOTSTRAP_SERVER_NAME", "a"),
        ("ZSLG_LISTEN_ADDR", "127.0.0.1:9000"),
        ("ZSLG_DB_PATH", ":temp:"),
        ("ZSLG_OBJECT_STORE", ":memory:"),
    ]);
    zslg_config(&mut env, &mut Vec::new(), zslg::ByteSize::from_mib(32)).unwrap()
}

#[test]
fn a_standalone_process_is_one_node() {
    assert!(alone(&node("a@127.0.0.1:9000")).is_ok());
    let refused = alone(&node("a@127.0.0.1:9000,b@127.0.0.1:9001")).unwrap_err();
    assert!(refused.to_string().contains("one node"), "{refused}");
}

#[test]
fn a_peer_needs_a_real_port() {
    assert!(peers(V, "node-a").is_err());
    assert!(peers(V, "node-a@node-a").is_err());
    assert!(peers(V, "node-a@a:0").is_err(), "port 0 cannot be dialled");
    assert!(peers(V, "node-a@a:99999").is_err());
    assert!(peers(V, "@a:9000").is_err());
    assert!(peers(V, "").is_err(), "an empty peer list is not a cluster");
}

#[test]
fn the_listen_port_may_not_be_ephemeral() {
    assert_eq!(
        listen_addr(V, "0.0.0.0:9000").unwrap(),
        "0.0.0.0:9000".parse::<SocketAddr>().unwrap()
    );
    assert!(listen_addr(V, "127.0.0.1:0").is_err());
    assert!(listen_addr(V, "not-an-address").is_err());
}

#[test]
fn compaction_tiers_are_a_duration_list() {
    let parsed = tiers(V, "1min, 15min, 1h, 1d").unwrap();
    assert_eq!(parsed.len(), 4);
    assert!(tiers(V, "1min,nonsense").is_err());
}

#[test]
fn spare_selection_accepts_either_word_separator() {
    assert!(matches!(
        spare_selection(V, "random").unwrap(),
        SpareSelectionStrategy::Random
    ));
    for spelling in ["lowest-latency", "lowest_latency", "LOWEST-LATENCY"] {
        assert!(matches!(
            spare_selection(V, spelling).unwrap(),
            SpareSelectionStrategy::LowestLatency
        ));
    }
    assert!(spare_selection(V, "fastest").is_err());
}

#[test]
fn compression_is_bounded_by_zstds_own_range() {
    assert!(compression(V, "0").is_ok());
    assert!(compression(V, "22").is_ok());
    assert!(compression(V, "23").is_err());
    assert!(compression(V, "-1").is_err());
}

/// Spelled out rather than routed through [`MEMORY`], so the literal an
/// operator types is what this pins.
#[test]
fn memory_selects_the_in_process_object_store() {
    let mut temp = Vec::new();
    let (config, prefix) = object_store_config(V, ":memory:", &mut temp).unwrap();
    assert!(matches!(config, ObjectStoreConfig::InMemory));
    assert!(prefix.is_empty(), "the heap store has no prefix");
    assert!(temp.is_empty(), "the heap store needs no directory");
}

/// No fuzzy matching: a directory named `memory:` is a thing someone may want.
#[test]
fn a_sentinel_without_both_colons_is_just_a_path() {
    let mut temp = Vec::new();
    let root = directory(V, TEMP, &mut temp).unwrap();
    let near_miss = root.join("memory:");
    assert!(matches!(
        object_store_config(V, &near_miss.to_string_lossy(), &mut temp)
            .unwrap()
            .0,
        ObjectStoreConfig::LocalFs { .. }
    ));
    assert!(near_miss.is_dir());
}

/// The deploy keeps every store in one bucket, so `ZSLG_OBJECT_STORE`
/// carries `bucket/zslg` as a prefix. zslg's config cannot hold one; the
/// platform applies it over the built store instead of rejecting it.
#[test]
fn an_s3_prefix_is_kept_for_the_store_to_wrap() {
    let mut temp = Vec::new();
    let (config, prefix) = object_store_config(
        V,
        "s3://bucket/bucket/zslg?endpoint=http://s3:9000&region=nbg1&access_key=k&secret_key=s",
        &mut temp,
    )
    .unwrap();

    assert!(matches!(config, ObjectStoreConfig::S3 { .. }));
    assert_eq!(prefix, "bucket/zslg");
    assert!(temp.is_empty(), "an S3 location needs no directory");
}

#[test]
fn a_store_without_a_prefix_is_left_unwrapped() {
    let mut temp = Vec::new();
    let (_, prefix) = object_store_config(
        V,
        "s3://zslg?endpoint=http://s3:9000&region=nbg1&access_key=k&secret_key=s",
        &mut temp,
    )
    .unwrap();

    assert!(prefix.is_empty());
}

/// Wrapping is real, not a marker: a key put through the wrapped store
/// lands under the prefix in the backend.
#[test]
fn a_prefix_moves_every_key_below_it() {
    use futures::StreamExt;
    use object_store::ObjectStoreExt;
    use object_store::PutPayload;
    use object_store::path::Path;

    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = with_prefix(inner.clone(), "bucket/zslg");
    futures::executor::block_on(async {
        store
            .put(&Path::from("snapshot"), PutPayload::from_static(b"bytes"))
            .await
            .unwrap();
        let keys: Vec<String> = inner
            .list(None)
            .map(|entry| entry.unwrap().location.to_string())
            .collect()
            .await;
        assert_eq!(keys, ["bucket/zslg/snapshot"]);
    });
}

#[test]
fn temp_yields_a_live_directory_and_its_guard() {
    let mut temp = Vec::new();
    let path = directory(V, TEMP, &mut temp).unwrap();
    assert!(path.is_dir());
    assert_eq!(temp.len(), 1);

    temp.clear();
    assert!(
        !path.exists(),
        "dropping the guard must remove the directory"
    );
}

#[test]
fn a_path_is_created_if_it_does_not_exist() {
    let mut temp = Vec::new();
    let root = directory(V, TEMP, &mut temp).unwrap();
    let nested = root.join("a").join("b");
    let made = directory(V, &nested.to_string_lossy(), &mut temp).unwrap();
    assert!(made.is_dir(), "intermediate components are created too");
    assert_eq!(temp.len(), 1, "a real path allocates no guard");
}
