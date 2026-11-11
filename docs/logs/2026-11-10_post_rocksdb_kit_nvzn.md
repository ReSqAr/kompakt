# nvzn-store on rocksdb-kit — what changed from the design

Branch `rocksdb-kit-nvzn`. Design: `2026-11-10_design_rocksdb_kit_nvzn.md`; decisions: `2026-11-10_log_rocksdb_kit_nvzn.md`
(D1–D8).

## Result

`nvzn-store-rocksdb` no longer names `rust_rocksdb`: it opens one typed `rocksdb_kit::Storage`. The kit is a typed
wrapper around the engine and owns no durability policy.

```text
  rocksdb-kit   ColumnFamily<Plain | Lookup | Prefixed<N>>   Snapshot   scan / scan_prefix   write / fence (unchanged)
  zslg stores   ColumnFamily<Plain>: the option set they have always had
  nvzn store    config_hash, watermark, store: Lookup      index: Prefixed<{ INDEX_PREFIX_WIDTH }>
                its own dirty flag around the kit's write and fence
  nvzn-types    Error::Storage(StorageError) in place of RocksDB(..) and RocksDBColumnFamilyMissing(..); no rust-rocksdb
  nvzn-store    StoreConfig keeps its fields; the budget arithmetic and its constants are gone (the kit's copy is the one)
```

## Deviations

| area | design | as built | why |
|---|---|---|---|
| default type parameter | `ColumnFamily<A = Plain>` (D1) | none; the mode is spelled everywhere (D8) | the user prefers explicit over implicit; zslg's declarations now say `ColumnFamily<Plain>` and `Cf<'_, Plain>` |
| `CfSpec` | a struct literal | `CfSpec::new(family, compress)`, carrying the mode as data (`AnyFamily`) | a literal cannot hold a generic family in one schema list |
| `Storage::cf` | returns the handle | also refuses a family asked for in a different mode than it was opened with | a mismatch is a wiring bug and would otherwise read a family the wrong way |
| steps | snapshot in its own step | one commit for access modes and snapshot | the snapshot tests need the prefixed family |
| pinning `Plain` | compare the options RocksDB applied | compare the `OPTIONS-*` file RocksDB writes into the directory against `plain_options.golden`, captured from the code before the change | `Options` has no getters; the file is the engine's own record of every database, family and table option |
| `rocksdb_missing_cf_errors` | not mentioned | removed | A3 holds: the kit creates missing families on open, so a store can no longer open without one |
| test-only API | not mentioned | `Storage::flush_memtable` (feature `testing`), beside the existing `compact` | nvzn's tests put rows into SSTs |
| inventory | rows for the changed crates | all rows regenerated from `cargo metadata` | many rows were stale from earlier steps (narrowed dependencies); the diff is larger than this change |

Assumptions: A1 (`nvzn-types` depends on `rocksdb-kit`) and A2 (the config conversion lives in `nvzn-store-rocksdb`)
held. A3 held. A4 held: nvzn holds one snapshot per read transaction.

## Verification

- `a_plain_family_gets_exactly_the_options_zslg_has_always_had` passes against the golden captured before the
  change: zslg's tuning is unchanged.
- Kit tests: a prefix scan yields exactly the keys under its prefix, in a memtable and after a compaction; a
  whole-family scan crosses prefixes and seeks to both ends; a snapshot does not see later writes; a mode mismatch is
  refused; each mode applies its own filter and extractor options (read back from the OPTIONS file).
- nvzn: all 23 store tests pass on the kit, including `rocksdb_every_write_path_arms_the_fence`, the prefix-scan
  test across many SSTs and the reset tests; the 25-metric golden dump is unchanged.
- Workspace: clippy clean, 205 test binaries ok, none ignored. The generated-faults proptest at 48 cases passes
  (one more data point for the open durability failure; it does not explain it).
- Not done: nlnr's pin and `e2e quick` against the new kompakt (after this merges).

## What stays as it is

The dirty flag and the timer fsync live in nvzn and in `zslg-local-store`, each beside its own durability decision
(D7). The kit's `write` and `fence` are byte-for-byte what they were.
