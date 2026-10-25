//! Activation: what makes a boot fail, what a start-up removes, what
//! survives a restart.

use super::{Rig, limits, objects};
use crate::{Cache, CacheConfig, Error, Version};

fn config(dir: &std::path::Path, weights: Option<Vec<(String, u32)>>) -> CacheConfig {
    CacheConfig {
        dir: dir.to_owned(),
        limits: limits(1000, 2000),
        weights,
    }
}

fn weights(pairs: &[(&str, u32)]) -> Option<Vec<(String, u32)>> {
    Some(pairs.iter().map(|(n, w)| ((*n).to_owned(), *w)).collect())
}

fn refused(r: Result<impl Sized, Error>) -> String {
    match r {
        Err(Error::Config(m)) => m,
        Err(e) => panic!("another error: {e}"),
        Ok(_) => panic!("must fail"),
    }
}

#[tokio::test]
async fn weights_must_name_exactly_the_apps_that_use_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let mk = |w| {
        let mut b = Cache::builder(config(dir.path(), w)).unwrap();
        b.app("app").kind("original", 1).unwrap();
        b.app("other").kind("frame", 1).unwrap();
        b.app("index"); // registers nothing: takes no part
        b.activate()
    };
    let m = refused(mk(weights(&[("app", 90)])));
    assert!(m.contains("missing [\"other\"]"), "{m}");
    let m = refused(mk(weights(&[("app", 90), ("other", 10), ("index", 1)])));
    assert!(m.contains("unknown [\"index\"]"), "{m}");
    let m = refused(mk(weights(&[("app", 90), ("other", 10), ("typo", 1)])));
    assert!(m.contains("typo"), "{m}");
    assert!(mk(weights(&[("app", 90), ("other", 10)])).is_ok());
}

#[tokio::test]
async fn hard_below_soft_stops_the_boot() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), None);
    cfg.limits = limits(2000, 1000);
    let m = refused(Cache::builder(cfg).unwrap().activate());
    assert!(m.contains("hard limit"), "{m}");
}

#[tokio::test]
async fn kinds_are_named_in_lowercase_and_once() {
    let dir = tempfile::tempdir().unwrap();
    let b = Cache::builder(config(dir.path(), None)).unwrap();
    let mut b = b;
    let app = b.app("app");
    assert!(refused(app.kind("Thumb", 1).map(|_| ())).contains("[a-z]+"));
    app.kind("thumb", 1).unwrap();
    assert!(refused(app.kind("thumb", 1).map(|_| ())).contains("twice"));
}

#[tokio::test]
async fn a_read_before_the_boot_is_not_active() {
    let dir = tempfile::tempdir().unwrap();
    let mut b = Cache::builder(config(dir.path(), None)).unwrap();
    let app = b.app("app");
    let k = app.kind("original", 1).unwrap();
    let store = app.store(
        objects(&[("a", 1)]).await,
        crate::Namespace::new("o", "originals"),
    );
    assert!(matches!(
        store.open(&k, "a", Version::Immutable).await,
        Err(Error::NotActive)
    ));
}

#[tokio::test]
async fn a_second_process_on_the_directory_fails_the_boot() {
    let rig = Rig::new(1000, 2000, &[("original", 1)]);
    let m = refused(Cache::builder(config(rig.dir.path(), None)));
    assert!(m.contains("in use"), "{m}");
}

#[tokio::test]
async fn files_of_unregistered_apps_and_kinds_are_removed_at_start() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut b = Cache::builder(config(dir.path(), None)).unwrap();
        let app = b.app("app");
        let (o, p) = (
            app.kind("original", 1).unwrap(),
            app.kind("preview", 1).unwrap(),
        );
        let gone = b.app("gone").kind("frame", 1).unwrap();
        let cache = b.activate().unwrap();
        let origin = objects(&[("a", 100)]).await;
        let ns = || crate::Namespace::new("o", "originals");
        for (app, k) in [(&app, &o), (&app, &p)] {
            drop(
                app.store(origin.clone(), ns())
                    .open(k, "a", Version::Immutable)
                    .await
                    .unwrap(),
            );
        }
        let _ = gone;
        cache.shutdown().await;
    }
    std::fs::create_dir_all(dir.path().join("gone/frame/ab")).unwrap();
    std::fs::write(dir.path().join("gone/frame/ab/x"), b"12345").unwrap();

    // The next boot: `preview` is no longer registered, `gone` neither.
    let mut b = Cache::builder(config(dir.path(), None)).unwrap();
    b.app("app").kind("original", 1).unwrap();
    let cache = b.activate().unwrap();
    assert!(dir.path().join("app/original").exists());
    assert!(!dir.path().join("app/preview").exists());
    assert!(!dir.path().join("gone").exists());
    assert_eq!(cache.core().used(), 100, "the registered file is kept");
}

#[tokio::test]
async fn a_restart_rebuilds_from_the_directory_and_drops_partials() {
    let rig = Rig::new(1000, 2000, &[("original", 1)]);
    let store = rig.store(objects(&[("a", 100)]).await);
    let k = rig.kind("original").clone();
    let id = {
        drop(store.open(&k, "a", Version::Immutable).await.unwrap());
        store.id(&k, "a", Version::Immutable).unwrap()
    };
    let path = rig.cache.core().path_of(&id);
    std::fs::write(format!("{}.part", path.display()), b"torn").unwrap();
    let dir = rig.dir.path().to_owned();
    rig.cache.shutdown().await;

    let mut b = Cache::builder(config(&dir, None)).unwrap();
    b.app("app").kind("original", 1).unwrap();
    let cache = b.activate().unwrap();
    assert!(cache.core().contains(&id));
    assert_eq!(cache.core().used(), 100);
    assert!(!std::path::Path::new(&format!("{}.part", path.display())).exists());
}

#[tokio::test]
async fn recency_survives_a_restart() {
    let rig = Rig::new(250, 1000, &[("original", 1)]);
    let store = rig.store(objects(&[("a", 100), ("b", 100), ("c", 100)]).await);
    let k = rig.kind("original").clone();
    drop(store.open(&k, "a", Version::Immutable).await.unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    drop(store.open(&k, "b", Version::Immutable).await.unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    // a is used again: it is now the most recent.
    drop(store.open(&k, "a", Version::Immutable).await.unwrap());
    let (ida, idb) = (
        store.id(&k, "a", Version::Immutable).unwrap(),
        store.id(&k, "b", Version::Immutable).unwrap(),
    );
    let dir = rig.dir.path().to_owned();
    rig.cache.shutdown().await; // flushes recency

    let mut cfg = config(&dir, None);
    cfg.limits = limits(250, 1000);
    let mut b = Cache::builder(cfg).unwrap();
    let app = b.app("app");
    let k = app.kind("original", 1).unwrap();
    let cache = b.activate().unwrap();
    let store = app.store(
        objects(&[("c", 100)]).await,
        crate::Namespace::new("o", "originals"),
    );
    drop(store.open(&k, "c", Version::Immutable).await.unwrap()); // 300: over 250
    cache.core().settled().await;
    // b was last used before a's second use: it goes, not a.
    assert!(cache.core().contains(&ida));
    assert!(!cache.core().contains(&idb), "the least recently used goes");
}

#[tokio::test]
async fn a_temporary_cache_is_gone_with_its_drop() {
    let b = Cache::temporary(limits(1000, 2000)).unwrap();
    let cache = b.activate().unwrap();
    assert_eq!(cache.report().soft, 1000);
}
