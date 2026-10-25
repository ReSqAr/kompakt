//! Who leaves when the cache is over its soft limit.

use super::{Rig, objects};
use crate::Version::Immutable;

#[tokio::test]
async fn above_the_soft_limit_the_least_recently_used_unpinned_file_goes() {
    let rig = Rig::new(250, 1000, &[("original", 1)]);
    let (store, k) = (
        rig.store(objects(&[("a", 100), ("b", 100), ("c", 100)]).await),
        rig.kind("original"),
    );
    drop(store.open(k, "a", Immutable).await.unwrap());
    rig.cache.core().settled().await;
    drop(store.open(k, "b", Immutable).await.unwrap());
    rig.cache.core().settled().await;
    drop(store.open(k, "a", Immutable).await.unwrap()); // a is more recent than b
    rig.cache.core().settled().await;
    drop(store.open(k, "c", Immutable).await.unwrap());
    rig.cache.core().settled().await;

    let core = rig.cache.core();
    assert!(core.contains(&store.id(k, "a", Immutable).unwrap()));
    assert!(
        !core.contains(&store.id(k, "b", Immutable).unwrap()),
        "b was least recently used"
    );
    assert!(core.contains(&store.id(k, "c", Immutable).unwrap()));
    assert_eq!(core.used(), 200);
}

#[tokio::test]
async fn pinned_files_survive_pressure_and_hinted_ones_go_first() {
    let rig = Rig::new(350, 1000, &[("original", 1)]);
    let (store, k) = (
        rig.store(objects(&[("a", 100), ("b", 100), ("c", 100), ("d", 100)]).await),
        rig.kind("original"),
    );
    let pinned = store.open(k, "a", Immutable).await.unwrap(); // oldest, pinned
    drop(store.open(k, "b", Immutable).await.unwrap());
    rig.cache.core().settled().await;
    drop(store.open(k, "c", Immutable).await.unwrap());
    rig.cache.core().settled().await;
    store.done_with(k, "c", Immutable); // newer than b, but hinted
    rig.cache.core().settled().await;
    drop(store.open(k, "d", Immutable).await.unwrap());
    rig.cache.core().settled().await;

    let core = rig.cache.core();
    assert!(core.contains(&store.id(k, "a", Immutable).unwrap()));
    assert!(core.contains(&store.id(k, "b", Immutable).unwrap()));
    assert!(
        !core.contains(&store.id(k, "c", Immutable).unwrap()),
        "the hinted file goes first"
    );
    drop(pinned);
}

#[tokio::test]
async fn a_download_waits_at_hard_until_a_pin_is_released() {
    let rig = Rig::new(100, 150, &[("original", 1)]);
    let store = rig.store(objects(&[("a", 100), ("b", 100)]).await);
    let k = rig.kind("original").clone();

    let a = store.open(&k, "a", Immutable).await.unwrap();
    let waiting = {
        let (store, k) = (store.clone(), k.clone());
        tokio::spawn(async move { store.open(&k, "b", Immutable).await.map(|f| f.len()) })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !waiting.is_finished(),
        "b must wait: a is pinned and b does not fit"
    );
    drop(a);
    assert_eq!(waiting.await.unwrap().unwrap(), 100);
    assert!(
        !rig.cache
            .core()
            .contains(&store.id(&k, "a", Immutable).unwrap())
    );
}

#[tokio::test]
async fn over_the_soft_limit_the_kind_furthest_above_its_weight_goes() {
    // Weights 75 / 20: thumbnails are what the cache is for.
    let rig = Rig::new(1000, 5000, &[("thumbnail", 75), ("preview", 20)]);
    let store = rig.store(
        objects(&[
            ("t1", 200),
            ("t2", 200),
            ("p1", 300),
            ("p2", 300),
            ("p3", 300),
        ])
        .await,
    );
    let (t, p) = (rig.kind("thumbnail"), rig.kind("preview"));
    // The thumbnails are the oldest: plain LRU would clear them first.
    for (k, key) in [(t, "t1"), (t, "t2"), (p, "p1"), (p, "p2")] {
        drop(store.open(k, key, Immutable).await.unwrap());
        rig.cache.core().settled().await;
    }
    let core = rig.cache.core();
    assert_eq!(core.used(), 1000);
    drop(store.open(p, "p3", Immutable).await.unwrap()); // 1300: over
    rig.cache.core().settled().await;

    // Back to exactly soft: the least recent preview goes (previews are over
    // their weight), the thumbnails never.
    let has = |k, key| core.contains(&store.id(k, key, Immutable).unwrap());
    assert!(has(t, "t1") && has(t, "t2"));
    assert!(!has(p, "p1"), "the least recent preview");
    assert!(has(p, "p2") && has(p, "p3"));
    assert_eq!(core.used(), 1000);
}

#[tokio::test]
async fn the_cache_stays_full_of_what_is_used_weights_only_pick_who_leaves() {
    // Only previews are asked for: they may fill the whole cache.
    let rig = Rig::new(1000, 5000, &[("thumbnail", 75), ("preview", 20)]);
    let store = rig.store(objects(&[("p1", 400), ("p2", 400), ("p3", 400)]).await);
    let p = rig.kind("preview");
    for key in ["p1", "p2", "p3"] {
        drop(store.open(p, key, Immutable).await.unwrap());
        rig.cache.core().settled().await;
    }
    let core = rig.cache.core();
    assert!(!core.contains(&store.id(p, "p1", Immutable).unwrap()));
    assert_eq!(core.used(), 800);
}

#[tokio::test]
async fn a_kind_of_weight_zero_goes_first_and_weights_change_at_runtime() {
    let rig = Rig::new(1000, 5000, &[("thumbnail", 1), ("preview", 1)]);
    let store = rig.store(objects(&[("t1", 300), ("t2", 300), ("p1", 300), ("p2", 300)]).await);
    let (t, p) = (rig.kind("thumbnail"), rig.kind("preview"));
    p.set_weight(0);
    assert_eq!(p.weight(), 0);
    for (k, key) in [(p, "p1"), (t, "t1"), (t, "t2")] {
        drop(store.open(k, key, Immutable).await.unwrap());
        rig.cache.core().settled().await;
    }
    store.done_with(t, "t1", Immutable); // done with: before any weight
    rig.cache.core().settled().await;
    drop(store.open(p, "p2", Immutable).await.unwrap()); // 1200: over
    rig.cache.core().settled().await;
    let core = rig.cache.core();
    let has = |k, key| core.contains(&store.id(k, key, Immutable).unwrap());
    assert!(!has(t, "t1"), "hinted: first");
    assert!(has(p, "p1"));
    drop(store.open(t, "t1", Immutable).await.unwrap()); // 1200 again
    rig.cache.core().settled().await;
    assert!(!has(p, "p1"), "weight 0: before any thumbnail");
    assert!(has(t, "t2"));
}

#[tokio::test]
async fn idle_files_go_at_the_sweep_and_pinned_ones_stay() {
    let rig = Rig::new(1000, 2000, &[("original", 1)]);
    let store = rig.store(objects(&[("a", 300), ("b", 300)]).await);
    let k = rig.kind("original");
    let pinned = store.open(k, "a", Immutable).await.unwrap();
    drop(store.open(k, "b", Immutable).await.unwrap());
    rig.cache.core().settled().await;
    rig.app.set_max_idle(Some(std::time::Duration::ZERO));
    std::thread::sleep(std::time::Duration::from_millis(5));
    let core = rig.cache.core();
    assert_eq!(
        core.tick_now().await,
        1,
        "everything unpinned is idle at zero"
    );
    assert_eq!(core.used(), 300, "the pinned file stays");
    drop(pinned);
}

/// Two apps, 50 : 50. One fills the whole cache while the other is idle (it
/// borrows); when the other arrives, the one over its target pays — never
/// the one below.
#[tokio::test]
async fn an_app_borrows_idle_space_and_pays_it_back_to_the_one_below_its_target() {
    use crate::{Cache, CacheConfig, Namespace, Urgency};
    let dir = tempfile::tempdir().unwrap();
    let mut builder = Cache::builder(CacheConfig {
        dir: dir.path().to_owned(),
        limits: super::limits(1000, 5000),
        weights: Some(vec![("app".to_owned(), 50), ("other".to_owned(), 50)]),
    })
    .unwrap();
    let (app, other) = (builder.app("app"), builder.app("other"));
    let (rk, pk) = (
        app.kind("original", 1).unwrap(),
        other.kind("frame", 1).unwrap(),
    );
    let cache = builder.activate().unwrap();
    let names: Vec<String> = (0..20).map(|i| format!("f{i}")).collect();
    let listed: Vec<(&str, usize)> = names.iter().map(|n| (n.as_str(), 100)).collect();
    let origin = objects(&listed).await;
    let rs = app
        .store(origin.clone(), Namespace::new("o", "originals"))
        .with_urgency(Urgency::Demand);
    let ps = other
        .store(origin, Namespace::new("o", "frames"))
        .with_urgency(Urgency::Demand);
    let held = |app: &str| {
        cache
            .report()
            .kinds
            .iter()
            .find(|k| k.app == app)
            .map(|k| k.bytes)
            .unwrap()
    };

    // app alone fills the cache: twice its 50 %.
    for key in &names[..10] {
        drop(rs.open(&rk, key, Immutable).await.unwrap());
        cache.core().settled().await;
    }
    assert_eq!(held("app"), 1000);

    // other arrives: app pays until other has its half; other is never
    // evicted for app's sake.
    for key in &names[10..15] {
        drop(ps.open(&pk, key, Immutable).await.unwrap());
        cache.core().settled().await;
    }
    let (r, p) = (held("app"), held("other"));
    assert!(r == 500 && p == 500, "{r} / {p}");

    // More other: other is over its target, so other pays itself.
    drop(ps.open(&pk, &names[15], Immutable).await.unwrap());
    cache.core().settled().await;
    let (r2, p2) = (held("app"), held("other"));
    assert!(r2 == 500 && p2 == 500, "{r2} / {p2}");

    // app reads something new: app is at its target, and so is other;
    // neither is crowded out by the other.
    drop(rs.open(&rk, &names[0], Immutable).await.unwrap());
    cache.core().settled().await;
    let (r, p) = (held("app"), held("other"));
    assert!(r == 500 && p == 500, "{r} / {p}");
}

/// What one more file costs once the cache is over its soft limit, with 8×
/// the entries: choosing a victim is O(log n), so the cost stays put (it was
/// a pass over the entries per victim — the 200k import's cliff). Explicit
/// (about 40 s unoptimised): `cargo test -p objectstorage-cache
/// eviction_cost -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement: about 40 s unoptimised"]
#[allow(clippy::print_stdout, reason = "a measurement reports")]
async fn eviction_cost_does_not_grow_with_the_entries() {
    let mut costs = Vec::new();
    for held in [5_000usize, 40_000] {
        let names: Vec<String> = (0..held + 2_000).map(|i| format!("f{i}")).collect();
        let pairs: Vec<(&str, usize)> = names.iter().map(|n| (n.as_str(), 100)).collect();
        let soft = u64::try_from(held * 100).unwrap();
        let rig = Rig::new(soft, soft * 2, &[("original", 1)]);
        let (store, k) = (rig.store(objects(&pairs).await), rig.kind("original"));
        for n in &names[..held] {
            drop(store.open(k, n, Immutable).await.unwrap());
        }
        rig.cache.core().settled().await;
        let started = std::time::Instant::now();
        for n in &names[held..] {
            drop(store.open(k, n, Immutable).await.unwrap());
        }
        rig.cache.core().settled().await;
        let per = started.elapsed() / 2_000;
        println!("{held} held: {per:?} per file over soft");
        assert_eq!(rig.cache.core().used(), soft);
        costs.push(per);
    }
    assert!(
        costs[1] < costs[0] * 2,
        "8× the entries, {costs:?}: the cost of a file follows them"
    );
}
