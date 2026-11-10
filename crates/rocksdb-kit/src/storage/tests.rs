//! Tests for `storage`.

use super::*;
use crate::config::{BACKGROUND_JOBS, MIN_WRITE_BUFFER_BYTES, RETIRE_SWEEP_DEFAULT};

const REPLICA_LOG: ColumnFamily = ColumnFamily::new("replica_log");
const STABLE_LOG: ColumnFamily = ColumnFamily::new("stable_log");

fn specs() -> Vec<CfSpec> {
    vec![
        CfSpec {
            cf: REPLICA_LOG,
            compress: false,
        },
        CfSpec {
            cf: STABLE_LOG,
            compress: true,
        },
    ]
}

/// Every RocksDB instance puts exactly one LOCK file in its own root, so
/// counting them under the storage root counts instances. No `walkdir` —
/// it is not a dependency and CLAUDE.md §15 does not want one for this.
fn count_lock_files(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|e| {
            let path = e.path();
            if path.is_dir() {
                count_lock_files(&path)
            } else {
                usize::from(path.file_name().is_some_and(|n| n == "LOCK"))
            }
        })
        .sum()
}

/// The bug this whole task exists to fix. `Engine::open` derived the DB
/// from `dir.parent()`, so a store nested one level deeper opened a
/// SECOND DB with a SECOND full-size cache.
#[tokio::test]
async fn a_node_opens_exactly_one_rocksdb() {
    let tmp = tempfile::tempdir().unwrap();
    let _s = Storage::open(
        tmp.path(),
        StorageConfig::new(ByteSize::from_mib(64)),
        &specs(),
    )
    .unwrap();
    assert_eq!(
        count_lock_files(tmp.path()),
        1,
        "a node must open exactly one RocksDB instance",
    );
}

/// The budget was a process-wide `static`, but loadgen runs six nodes in
/// one process. Two nodes with different budgets must get different
/// caches — under the static they could not.
#[tokio::test]
async fn the_budget_is_per_node_not_per_process() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let small = Storage::open(
        a.path(),
        StorageConfig::new(ByteSize::from_mib(16)),
        &specs(),
    )
    .unwrap();
    let large = Storage::open(
        b.path(),
        StorageConfig::new(ByteSize::from_mib(256)),
        &specs(),
    )
    .unwrap();
    assert!(
        small.cache_capacity() < large.cache_capacity(),
        "two nodes in one process must not share a budget",
    );
}

/// `charged_bytes` summed a registry of every DB the process had open.
/// With one DB per node it reads that node's cache and nothing else.
#[tokio::test]
async fn charged_bytes_reads_this_node_only() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let one = Storage::open(
        a.path(),
        StorageConfig::new(ByteSize::from_mib(64)),
        &specs(),
    )
    .unwrap();
    let two = Storage::open(
        b.path(),
        StorageConfig::new(ByteSize::from_mib(64)),
        &specs(),
    )
    .unwrap();
    let before = one.charged_bytes();
    let mut batch = Batch::new();
    let cf = two.cf(REPLICA_LOG).unwrap();
    for i in 0u64..2_000 {
        batch.put(&cf, i.to_be_bytes(), vec![0u8; 4096]);
    }
    two.write(batch).unwrap();
    two.fence().unwrap();
    let _ = two.cf(REPLICA_LOG).unwrap();
    assert_eq!(
        before,
        one.charged_bytes(),
        "writing to another node's DB must not move this node's charge",
    );
}

/// The memtable share was divided by a fixed PLANNED_COLUMN_FAMILIES = 8
/// because the live count was unknown at open. It is known now.
#[test]
fn write_buffer_is_sized_from_the_real_cf_count() {
    let cfg = StorageConfig::new(ByteSize::from_mib(64));
    let two = cfg.write_buffer_bytes(2);
    let eight = cfg.write_buffer_bytes(8);
    assert!(
        two > eight,
        "fewer families must each get a larger memtable"
    );
}

/// The knobs are overrides, not replacements: an unset one must still be
/// the number the budget derives, and the 20 MiB low-water config — the one
/// that finds sizing bugs — must stay runnable.
#[test]
fn unset_sizing_knobs_derive_from_the_budget() {
    let low = StorageConfig::new(ByteSize::from_mib(20));
    low.validate()
        .expect("the 20 MiB low-water config is valid");

    assert!(
        low.write_buffer_bytes(6) >= MIN_WRITE_BUFFER_BYTES,
        "a derived memtable must never fall below the stall floor",
    );
    assert!(
        low.max_total_wal_bytes() >= memtable_budget(low.memory),
        "retained WAL must cover at least the memtable share it backs",
    );
    assert!(
        low.target_file_size_bytes(6) >= low.write_buffer_bytes(6),
        "an SST target below the memtable that fills it is a split, not a target",
    );
    assert_eq!(low.background_jobs(), BACKGROUND_JOBS);
    assert!((16..=512).contains(&low.open_files()));

    // And every term scales with the budget rather than pinning at a
    // constant — the property "derived" actually means.
    let high = StorageConfig::new(ByteSize::from_mib(256));
    assert!(high.write_buffer_bytes(6) > low.write_buffer_bytes(6));
    assert!(high.max_total_wal_bytes() > low.max_total_wal_bytes());
    assert!(high.open_files() > low.open_files());
}

/// An explicit knob wins over the derivation, in both directions.
#[test]
fn an_explicit_knob_overrides_the_derived_value() {
    let mut cfg = StorageConfig::new(ByteSize::from_mib(256));
    cfg.write_buffer = Some(ByteSize::from_mib(1));
    cfg.max_total_wal = Some(ByteSize::from_mib(7));
    cfg.target_file_size = Some(ByteSize::from_mib(3));
    cfg.max_background_jobs = Some(5);
    cfg.max_open_files = Some(-1);
    cfg.validate().unwrap();

    assert_eq!(cfg.write_buffer_bytes(6), 1024 * 1024);
    assert_eq!(cfg.max_total_wal_bytes(), 7 * 1024 * 1024);
    assert_eq!(cfg.target_file_size_bytes(6), 3 * 1024 * 1024);
    assert_eq!(cfg.background_jobs(), 5);
    assert_eq!(cfg.open_files(), -1);
}

#[test]
fn retire_sweep_defaults_to_an_hour_and_honours_an_override() {
    let cfg = StorageConfig::new(ByteSize::from_mib(64));
    assert_eq!(cfg.retire_sweep_secs(), RETIRE_SWEEP_DEFAULT.as_secs());
    assert_eq!(cfg.retire_sweep_secs(), 3600);

    let cfg = StorageConfig {
        retire_sweep: Some(std::time::Duration::from_secs(120)),
        ..StorageConfig::new(ByteSize::from_mib(64))
    };
    assert_eq!(cfg.retire_sweep_secs(), 120);
}

/// A sub-second override must never truncate to zero: RocksDB reads
/// `periodic_compaction_seconds == 0` as *disabled*, so an operator asking
/// for an aggressive short sweep would otherwise silently turn reclamation
/// off — the exact inverse of what they configured.
#[test]
fn a_sub_second_retire_sweep_never_truncates_to_disabled() {
    let cfg = StorageConfig {
        retire_sweep: Some(std::time::Duration::from_millis(500)),
        ..StorageConfig::new(ByteSize::from_mib(64))
    };
    assert_eq!(cfg.retire_sweep_secs(), 1);
}

/// Validation names the knob and the constraint, so an operator does not
/// have to read this file to know what they set wrong.
#[test]
fn validation_names_the_knob_it_rejected() {
    let cases: Vec<(StorageConfig, &str)> = vec![
        (
            StorageConfig::new(ByteSize::from_bytes(0)),
            "storage_memory",
        ),
        (
            StorageConfig {
                write_buffer: Some(ByteSize::from_bytes(1)),
                ..StorageConfig::new(ByteSize::from_mib(20))
            },
            "storage_write_buffer",
        ),
        (
            StorageConfig {
                max_total_wal: Some(ByteSize::from_bytes(0)),
                ..StorageConfig::new(ByteSize::from_mib(20))
            },
            "storage_max_total_wal",
        ),
        (
            StorageConfig {
                target_file_size: Some(ByteSize::from_bytes(0)),
                ..StorageConfig::new(ByteSize::from_mib(20))
            },
            "storage_target_file_size",
        ),
        (
            StorageConfig {
                max_background_jobs: Some(0),
                ..StorageConfig::new(ByteSize::from_mib(20))
            },
            "storage_max_background_jobs",
        ),
        (
            StorageConfig {
                max_open_files: Some(0),
                ..StorageConfig::new(ByteSize::from_mib(20))
            },
            "storage_max_open_files",
        ),
    ];
    for (cfg, knob) in cases {
        let err = cfg.validate().expect_err("must reject");
        assert!(
            format!("{err}").contains(knob),
            "error must name {knob}, got: {err}"
        );
    }
}

/// The one live WAL of a closed storage root. RocksDB names it
/// `<number>.log` and keeps exactly one while nothing has rolled, so the
/// highest-numbered `.log` is the file the last batch was appended to.
fn live_wal(root: &Path) -> std::path::PathBuf {
    let mut logs: Vec<std::path::PathBuf> = std::fs::read_dir(root)
        .expect("read storage root")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    logs.sort();
    logs.pop().expect("a live WAL exists after a fenced write")
}

fn wal_len(root: &Path) -> u64 {
    std::fs::metadata(live_wal(root))
        .expect("stat the live WAL")
        .len()
}

/// Write one batch of `keys` into the replica-log family and fence it, so
/// it is committed and durable in the WAL before this returns.
fn put_batch_and_fence(storage: &Storage, keys: &[&[u8]]) {
    let cf = storage.cf(REPLICA_LOG).unwrap();
    let mut batch = Batch::new();
    for key in keys {
        batch.put(&cf, key, vec![7u8; 128]);
    }
    storage.write(batch).unwrap();
    storage.fence().unwrap();
}

fn has_key(storage: &Storage, key: &[u8]) -> bool {
    let cf = storage.cf(REPLICA_LOG).unwrap();
    storage.get_pinned(&cf, key).expect("read back").is_some()
}

/// Synchronous: no runtime means no background compaction task, so the
/// last `Arc` really is the last one and dropping it releases the LOCK —
/// which is what lets these tests reopen the root.
fn open_test_storage(root: &Path) -> Result<Arc<Storage>, StorageError> {
    Storage::open(root, StorageConfig::new(ByteSize::from_mib(64)), &specs())
}

/// CLAUDE.md §8's documented exception: a torn, un-fsynced WAL tail is
/// ordinary crash debris and is TRUNCATED, not refused — otherwise a
/// healthy node that was killed mid-append refuses to come back.
///
/// The garbage below fails the WAL record CRC, which is exactly what a
/// half-written record looks like; `AbsoluteConsistency` refuses such a
/// reopen and this test fails under it (mutation-verified).
#[test]
fn a_torn_wal_tail_is_truncated_not_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let keys: [&[u8]; 3] = [b"fenced/1", b"fenced/2", b"fenced/3"];
    {
        let storage = open_test_storage(tmp.path()).unwrap();
        put_batch_and_fence(&storage, &keys);
    }

    // A torn tail: a WAL record header — crc(4) | len(2, LE) | type(1) —
    // announcing a 200-byte body of which only 80 bytes made it to disk.
    // That is exactly the shape a never-fsynced, half-written append
    // leaves behind, and it is the shape this recovery mode exists for.
    let mut debris: Vec<u8> = vec![0xA5, 0xA5, 0xA5, 0xA5, 200, 0, 1];
    debris.extend_from_slice(&[0x5Au8; 80]);
    let mut wal = std::fs::OpenOptions::new()
        .append(true)
        .open(live_wal(tmp.path()))
        .unwrap();
    std::io::Write::write_all(&mut wal, &debris).unwrap();
    std::io::Write::flush(&mut wal).unwrap();
    drop(wal);

    let storage = open_test_storage(tmp.path())
        .expect("a torn un-fsynced tail must be truncated, not refused");
    for key in keys {
        assert!(
            has_key(&storage, key),
            "every fenced key must survive a torn tail: {}",
            String::from_utf8_lossy(key),
        );
    }
}

/// CLAUDE.md §8: a crash mid-batch discards the batch WHOLE. The
/// interrupted batch below carries five keys and the assertion is that
/// NONE of them survives while every key of the earlier fenced batch
/// does — a partial batch would show up as some of the five.
///
/// The batch boundary is the WAL length *after* fencing the first batch:
/// a `WriteBatch` is one WAL record, so the second batch occupies exactly
/// the bytes past that offset, and cutting inside them is a crash inside
/// the record. Mutation-verified against `AbsoluteConsistency`, which
/// refuses the reopen.
#[test]
fn an_interrupted_batch_is_discarded_whole_on_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let fenced: [&[u8]; 3] = [b"fenced/1", b"fenced/2", b"fenced/3"];
    let torn: [&[u8]; 5] = [b"torn/1", b"torn/2", b"torn/3", b"torn/4", b"torn/5"];

    let (boundary, end) = {
        let storage = open_test_storage(tmp.path()).unwrap();
        put_batch_and_fence(&storage, &fenced);
        let boundary = wal_len(tmp.path());
        put_batch_and_fence(&storage, &torn);
        // Control: the batch really was written and fenced, so its absence
        // after the reopen can only come from the truncation below.
        for key in torn {
            assert!(has_key(&storage, key), "the batch must be written first");
        }
        (boundary, wal_len(tmp.path()))
    };
    assert!(end > boundary, "the second batch must have grown the WAL");

    // Cut half way through the second batch's record. checked_*: the
    // arithmetic is over two file lengths of the same file with
    // `end > boundary` asserted, so neither step can wrap.
    let span = end.checked_sub(boundary).unwrap();
    let cut = boundary.checked_add(span.checked_div(2).unwrap()).unwrap();
    assert!(cut > boundary && cut < end, "the cut must be mid-record");
    std::fs::OpenOptions::new()
        .write(true)
        .open(live_wal(tmp.path()))
        .unwrap()
        .set_len(cut)
        .unwrap();

    let storage =
        open_test_storage(tmp.path()).expect("an interrupted batch is crash debris, not a refusal");
    for key in fenced {
        assert!(
            has_key(&storage, key),
            "the earlier fenced batch must survive whole: {}",
            String::from_utf8_lossy(key),
        );
    }
    for key in torn {
        assert!(
            !has_key(&storage, key),
            "no key of the interrupted batch may survive: {}",
            String::from_utf8_lossy(key),
        );
    }
}

/// The other half of CLAUDE.md §8, and what makes the exception above
/// *narrow*: a WAL record that is present in full and fails its checksum
/// is genuine corruption of committed state, and the open REFUSES.
///
/// This is the assertion that pins the recovery mode itself — rust-rocksdb
/// exposes no getter for `wal_recovery_mode`, so behaviour is the only
/// readback available. Under the self-heal-by-discard modes §8 rejects
/// (`PointInTimeRecovery`, `SkipAnyCorruptedRecord`) the open succeeds and
/// this test fails (mutation-verified).
#[test]
fn a_checksum_failing_wal_record_refuses_to_open() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let storage = open_test_storage(tmp.path()).unwrap();
        put_batch_and_fence(&storage, &[b"fenced/1"]);
    }

    // A complete record — crc(4) | len(2, LE) | type(1) | 200-byte body —
    // whose checksum is garbage. Nothing is missing; the bytes are wrong.
    let mut corrupt: Vec<u8> = vec![0xA5, 0xA5, 0xA5, 0xA5, 200, 0, 1];
    corrupt.extend_from_slice(&[0x5Au8; 200]);
    let mut wal = std::fs::OpenOptions::new()
        .append(true)
        .open(live_wal(tmp.path()))
        .unwrap();
    std::io::Write::write_all(&mut wal, &corrupt).unwrap();
    std::io::Write::flush(&mut wal).unwrap();
    drop(wal);

    let err = open_test_storage(tmp.path())
        .err()
        .expect("a checksum-failing WAL record must refuse to open");
    // The refusal is the invariant (CLAUDE.md §8): a *complete* record
    // failing its CRC must fail open, not self-heal by discard. The exact
    // wording comes from rust-rocksdb and is not part of the contract, so
    // it is not asserted. Mutation-verified: under
    // `PointInTimeRecovery` / `SkipAnyCorruptedRecord` the open returns
    // `Ok` and the `expect` above trips.
    let _ = err;
}
