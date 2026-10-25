//! Reads: miss, hit, identity, put, listing, ranges.

use super::{Rig, objects};
use crate::{Error, Streamed, Version};
use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStoreExt;

#[tokio::test]
async fn a_miss_downloads_and_a_second_open_hits() {
    let rig = Rig::new(1000, 2000, &[("original", 1)]);
    let store = rig.store(objects(&[("a.jpg", 100)]).await);
    let k = rig.kind("original");

    let f = store.open(k, "a.jpg", Version::Immutable).await.unwrap();
    assert_eq!(f.len(), 100);
    assert_eq!(std::fs::read(f.path()).unwrap().len(), 100);
    drop(f);
    let _again = store.open(k, "a.jpg", Version::Immutable).await.unwrap();
    assert_eq!(rig.cache.core().downloads(), 1);
}

#[tokio::test]
async fn concurrent_opens_share_one_download() {
    let rig = Rig::new(10_000, 20_000, &[("original", 1)]);
    let store = rig.store(objects(&[("a", 5000)]).await);
    let k = rig.kind("original").clone();
    let opens = (0..16).map(|_| {
        let (store, k) = (store.clone(), k.clone());
        tokio::spawn(async move {
            store
                .open(&k, "a", Version::Immutable)
                .await
                .map(|f| f.len())
        })
    });
    for r in futures::future::join_all(opens).await {
        assert_eq!(r.unwrap().unwrap(), 5000);
    }
    assert_eq!(rig.cache.core().downloads(), 1);
}

#[tokio::test]
async fn a_new_version_is_another_file_and_an_immutable_one_hits() {
    let rig = Rig::new(10_000, 20_000, &[("original", 1)]);
    let origin = objects(&[("a.jpg", 100)]).await;
    let store = rig.store(origin.clone());
    let k = rig.kind("original");
    let v1 = Version::Stat {
        size: 100,
        modified_seconds: 1_000,
        modified_nanos: 0,
    };

    drop(store.open(k, "a.jpg", v1).await.unwrap());
    drop(store.open(k, "a.jpg", v1).await.unwrap());
    assert_eq!(rig.cache.core().downloads(), 1, "the same version hits");

    // The file is replaced in place: another size and time → fetched again.
    origin
        .put(
            &object_store::path::Path::from("a.jpg"),
            vec![9u8; 150].into(),
        )
        .await
        .unwrap();
    let v2 = Version::Stat {
        size: 150,
        modified_seconds: 2_000,
        modified_nanos: 0,
    };
    let f = store.open(k, "a.jpg", v2).await.unwrap();
    assert_eq!(rig.cache.core().downloads(), 2);
    assert_eq!(std::fs::read(f.path()).unwrap(), vec![9u8; 150]);
}

#[tokio::test]
async fn the_kind_is_in_the_path_and_a_foreign_kind_is_refused() {
    let rig = Rig::new(10_000, 20_000, &[("original", 1), ("preview", 1)]);
    let store = rig.store(objects(&[("a", 10)]).await);
    let f = store
        .open(rig.kind("preview"), "a", Version::Immutable)
        .await
        .unwrap();
    let path = f.path().to_string_lossy().into_owned();
    assert!(path.contains("/app/preview/"), "{path}");
    drop(f);

    // A kind of another app on this store is a programming error, loudly.
    let mut other = crate::Cache::temporary(super::limits(10, 20)).unwrap();
    let foreign = other.app("other").kind("frame", 1).unwrap();
    assert!(matches!(
        store.open(&foreign, "a", Version::Immutable).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn put_uploads_and_the_next_open_downloads() {
    let rig = Rig::new(1000, 2000, &[("preview", 1)]);
    let origin = objects(&[]).await;
    let store = rig.store(origin.clone());
    let k = rig.kind("preview");

    store
        .put(
            k,
            "x/thumb.jpg",
            Version::Immutable,
            Bytes::from_static(b"jpeg"),
        )
        .await
        .unwrap();
    let got = origin
        .get(&object_store::path::Path::from("x/thumb.jpg"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&got[..], b"jpeg");
    let f = store
        .open(k, "x/thumb.jpg", Version::Immutable)
        .await
        .unwrap();
    assert_eq!(std::fs::read(f.path()).unwrap(), b"jpeg");
    assert_eq!(rig.cache.core().downloads(), 1, "an upload is not cached");
}

#[tokio::test]
async fn a_missing_object_is_not_found_and_leaves_no_trace() {
    let rig = Rig::new(1000, 2000, &[("original", 1)]);
    let store = rig.store(objects(&[]).await);
    assert!(matches!(
        store
            .open(rig.kind("original"), "nope", Version::Immutable)
            .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(rig.cache.core().used(), 0);
}

#[tokio::test]
async fn a_key_that_climbs_out_of_the_store_is_invalid() {
    let rig = Rig::new(1000, 2000, &[("original", 1)]);
    let store = rig.store(objects(&[]).await);
    assert!(matches!(
        store
            .open(rig.kind("original"), "../x", Version::Immutable)
            .await,
        Err(Error::InvalidKey(_))
    ));
}

#[tokio::test]
async fn a_range_of_a_complete_file_is_exact() {
    let rig = Rig::new(10_000, 20_000, &[("original", 1)]);
    let store = rig.store(objects(&[("v", 3000)]).await);
    let k = rig.kind("original");
    drop(store.open(k, "v", Version::Immutable).await.unwrap());
    let streamed = store.stream(k, "v", Version::Immutable).await.unwrap();
    assert!(matches!(streamed, Streamed::Complete(_)));
    let mut body = streamed.into_range(2990, 3000).await.unwrap();
    let mut got = Vec::new();
    while let Some(chunk) = body.next().await {
        got.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(got, vec![7u8; 10]);
}

#[tokio::test]
async fn listing_passes_through() {
    let rig = Rig::new(1000, 2000, &[("original", 1)]);
    let store = rig.store(objects(&[("p/a", 1), ("p/b", 2), ("q/c", 3)]).await);
    let mut listed: Vec<_> = store
        .list(Some("p"))
        .map(|m| m.map(|m| (m.key, m.size)))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()
        .unwrap();
    listed.sort();
    assert_eq!(listed, vec![("p/a".into(), 1), ("p/b".into(), 2)]);
}

#[tokio::test]
async fn a_prediction_is_fetched_ahead_and_the_request_hits() {
    let rig = Rig::new(10_000, 20_000, &[("thumbnail", 1)]);
    let store = rig.store(objects(&[("t", 100)]).await);
    let k = rig.kind("thumbnail");
    let until = std::time::SystemTime::now() + std::time::Duration::from_secs(30);
    store.predict(k, "t", Version::Immutable, std::sync::Arc::default(), until);
    let id = store.id(k, "t", Version::Immutable).unwrap();
    for _ in 0..200 {
        if rig.cache.core().contains(&id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(rig.cache.core().contains(&id), "fetched ahead");
    drop(store.open(k, "t", Version::Immutable).await.unwrap());
    assert_eq!(rig.cache.core().downloads(), 1, "the request was a hit");
}
