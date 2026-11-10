# nvzn-store on rocksdb-kit — decision log

Process: `nlnr/docs/iaic.md`. Append-only: settled questions, decisions, why, rejected alternatives.
Predecessors: the crate split (`2026-11-07_*`) and the btrb decomposition (`2026-11-09_log_btrb_decomposition.md`).

## Starting facts (not decisions)

- `rocksdb-kit` (role `library`) is zslg's engine layer: one RocksDB, one block cache, one family per store;
  `Storage::{open, cf, get_pinned, scan, write, fence}`, `Batch`, property gauges, fault seams, `StorageConfig`.
  Callers never name `rust_rocksdb`.
- `nvzn-store-rocksdb` (621 lines) talks to `rust_rocksdb` directly and rebuilds the same machinery: `db_options`,
  `cf_options`, a write-buffer manager charged to the cache, column-family lookups, property gauges, its own fence.
- Already identical: the database options (atomic flush, WAL recovery mode, paranoid checks, budget shares) and the
  budget constants (`BACKGROUND_JOBS`, `WRITE_BUFFERS_PER_CF`, `MIN_WRITE_BUFFER_BYTES`, `MEMTABLE_SHARE_PERCENT`,
  `RETIRE_SWEEP_DEFAULT`); `StoreConfig` carries the same six optional tunables as `StorageConfig`.
- Differences found:
  1. nvzn's per-family options add a **bloom filter** on point-lookup families (`set_bloom_filter(10.0)`); the
     kit sets partitioned filters but no filter policy, so its families have none.
  2. nvzn's `index` family is **scan-only**: whole-key filtering off, fixed-width prefix extractor, memtable
     prefix bloom, `prefix_same_as_start` reads; walking a family end to end needs `total_order_seek`.
  3. nvzn reads through **snapshots**.
  4. nvzn keeps a **dirty flag** (armed after every write, cleared by the fence) so an idle fence skips the fsync;
     the kit's fence always `flush_wal(true)`.

## Decisions

### D1 — the kit describes access patterns as data, typed and opt-in (settled)

The kit models how a family is read as a type parameter of its handle, not as a runtime flag and not as a raw
options escape hatch. Nothing outside the kit names `rust_rocksdb`.

```text
  ColumnFamily<A = Plain>       zslg's families, unchanged: the option set zslg gets today
  CfSpec::new(cf)               reads its options from A: there is no flag to set wrongly
  Cf<Prefixed<N>>               scan_prefix(&[u8; N]) and scan_all; the width is checked at compile time
```

Why: the real risk is changing zslg's tuning by accident. A defaulted type parameter means zslg code and
signatures do not change, and the prefix options are unreachable from a zslg family. The prefix width and the
two seek modes (prefix, total order) become methods that cannot be mixed up.

Rejected: a raw-options escape hatch (leaks the engine back out); a runtime `access` field on `CfSpec` (a wrong
value compiles).

Guard: a test pins the option set `Plain` builds, so a change to zslg's tuning has to be deliberate.

Open from the facts above: finding 1 means `Plain` cannot also serve nvzn's point families, so the type set is
probably three modes (plain, point-lookup with bloom, prefixed). Settled in D3.

### D2 — nvzn keeps its own `Storage`: one kit, two instances (settled)

`nvzn-store-rocksdb` calls `Storage::open` on its own path with its own budget. `NVZN_DB_PATH` and `NVZN_MEMORY`
stay as they are. `StoreConfig` converts field by field into `StorageConfig` (the six optional tunables and the
budget constants are already identical); `flush_interval`, `fold_commit` and `compression` stay in nvzn.

Why: projections are derived, rebuildable state and the log is their source of truth, so nvzn's durability and
memory budget are its own. The kit is a library from both sides; sharing one database would make nvzn depend on
zslg's node wiring, tie the two memory budgets and fsync cadences together, and make a node without nvzn carry
nvzn's families.

Rejected: one shared RocksDB for zslg and nvzn.

### D3 — three access modes: `Plain`, `Lookup`, `Prefixed<N>` (settled)

- `Plain`: the option set zslg gets today, unchanged (no filter policy).
- `Lookup`: `Plain` plus the 10-bit bloom filter; nvzn's `store` and its single-row families.
- `Prefixed<const N: usize>`: whole-key filtering off, fixed-width prefix extractor, memtable prefix bloom;
  `scan_prefix(&[u8; N])` and `scan_all` (total-order seek); nvzn's `index` family.

Why: finding 1 shows nvzn's point families already differ from zslg's. Naming each mode keeps every option set
explicit, and zslg cannot receive a filter it never had.

Rejected: two modes with the bloom filter on `Plain`; it would change zslg's cache use and read profile, the
accident D1 exists to prevent.

### D4 — dirty tracking and the periodic sync live in the kit, opt-in; the local store converges in this round (settled)

Durability belongs in the kit, but no store is forced onto it. Two contracts, kept apart by name:

```text
  Storage::write(batch)        arms an internal flag after db.write returns, whatever the outcome
  Storage::fence()             unchanged: unconditional flush_wal(true); the commit fence zslg relies on
  Storage::sync_if_dirty()     the periodic contract: swap(false), flush_wal, re-arm on failure
  Storage::snapshot()          a handle borrowing the Storage: get_pinned and scan on a Cf
```

Why: the "dirty flag + timer fsync" pattern exists twice already, in `nvzn-store-rocksdb` (`write_armed`,
`flush`) and in `zslg-local-store` (`mark_dirty`, `flush_if_dirty`), each with its own ordering comment. A
commit fence promises that everything written before the call is durable; the periodic sync promises only a
bounded loss window, and a sync racing an in-flight write may return `Ok` without covering it. The ordering
argument (arm after the write, never before) is written once, in the kit.

Guards: `fence` must still sync when the flag is clear (test); both methods carry the contract in their docs.

Scope: `zslg-local-store` drops its copy and uses `sync_if_dirty` in this round, so the pattern converges
instead of becoming a third copy. zslg's commit paths are untouched.

Rejected: dirty tracking in nvzn only (a third copy beside the local store's); a tracked fence that replaces
`fence` (would weaken the commit fence).

Note (not a decision): the engine-property gauges stay with their callers. The kit exposes
`Storage::property_u64`; nvzn keeps `nvzn_store_live_sst_files_size_bytes` and its name.
