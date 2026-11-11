# nvzn-store on rocksdb-kit — design

Status: written after the interview (`2026-11-10_log_rocksdb_kit_nvzn.md`, D1–D7); awaiting sign-off.
Mental model: `2026-11-10_internal_rocksdb_kit_nvzn.md`.

## 1. The idea

`rocksdb-kit` is zslg's engine layer. `nvzn-store-rocksdb` rebuilds the same machinery by hand against
`rust_rocksdb`. After this change nvzn uses the kit too: one engine layer, two instances (D2). The kit is a typed
wrapper around the engine and owns no durability policy (D7).

```text
  before    zslg stores ──▶ rocksdb-kit ──▶ rust_rocksdb        nvzn-store-rocksdb ──▶ rust_rocksdb
  after     zslg stores ──▶ rocksdb-kit ◀── nvzn-store-rocksdb      rust_rocksdb is named by the kit alone
```

## 2. Goals, non-goals, success criteria

Goals
- nvzn and zslg share the engine layer; nothing outside the kit names `rust_rocksdb`.
- zslg's RocksDB configuration is **unchanged**, provably (a test pins it).
- nvzn's budget arithmetic exists once (the kit's), not twice.

Non-goals
- One database for both (D2). Any durability policy in the kit (D7): `write` and `fence` are unchanged, and the
  dirty flag stays with its callers. Any change to nvzn's key encodings, families
  or read semantics. Compaction or tuning changes.

Success: workspace clippy and tests clean; the 25-metric golden dump identical; zslg's option set unchanged;
nlnr builds and `e2e quick` passes against the new kompakt.

## 3. Current state

- Kit: `Storage::{open, cf, get_pinned, scan, write, fence}`, `Batch`, `Scan`, `Pinned`, `StorageConfig`,
  `CfSpec { cf, compress }`, fault seams. Plain families only; the fence always `flush_wal(true)`.
- nvzn: `RocksDBStore` over `DB`; four families (`config_hash`, `watermark`, `store`, `index`); `Access` enum
  (`PointLookups` / `ScansOnly{prefix}`); bloom on point families; prefix extractor, prefix bloom and
  `prefix_same_as_start` on `index`; snapshot reads; `dirty` flag with `write_armed` and `flush`.
- `zslg-local-store`: its own `dirty` flag, `mark_dirty`, `flush_if_dirty`.
- Identical already: database options, budget constants, the six optional tunables.

## 4. Constraints and assumptions

### 4.1 Settled (D1–D7)
Typed opt-in access modes, spelled explicitly with no default parameter (D8); three modes; nvzn keeps its own `Storage`; the kit stays a type wrapper and durability
stays with the callers (D7, which narrows D4); nvzn's error gets one `Storage` variant.

### 4.2 Assumptions I took — strike any of them
- **A1.** `nvzn-types` (a types crate) may depend on `rocksdb-kit` (a library). The layering test allows it.
- **A2.** `StoreConfig` keeps its shape; a `StoreConfig → StorageConfig` conversion lives in
  `nvzn-store-rocksdb`, not in `nvzn-store`, so `nvzn-store` stays engine-free.
- **A3.** The kit's `Storage::open` creates families missing from an existing directory, as nvzn's open does today.
  If it does not, that is a finding for step 5, not an assumption to hide.
- **A4.** nvzn's `Snapshot` use is a single snapshot per read view, held for the view's life.

## 5. Architecture

No new crates. Changes by crate:

| crate | change |
|---|---|
| `rocksdb-kit` | `Access` (`Plain`, `Lookup`, `Prefixed<N>`), typed `ColumnFamily<A>` / `Cf<A>`, `scan_prefix`, `scan_all`, `Snapshot`. `write` and `fence` unchanged. |
| `nvzn-types` | `Error::Storage(StorageError)`; drop `rust-rocksdb` |
| `nvzn-store-rocksdb` | onto the kit; `StoreConfig → StorageConfig`; families `Lookup` and `Prefixed`; its own dirty flag around the kit's `write` and `fence` |

## 6. Interfaces

### 6.1 Access modes (D1, D3)

```text
  trait Access     the options of a family, as data; sealed
  Plain            today's option set; no filter policy. Every zslg family.
  Lookup           Plain + 10-bit bloom filter. nvzn: store, watermark, config_hash.
  Prefixed<const N: usize>
                   whole-key filtering off, fixed prefix extractor of width N, memtable prefix bloom.
                   nvzn: index.

  ColumnFamily<A>::new(name)                    const; the name is the on-disk schema; A is always written
  CfSpec::new(cf) / .compressed(bool)            options come from A; there is no access flag
  Storage::cf::<A>(family) -> Cf<'_, A>
  Storage::scan(&Cf<A>)                          total order. Plain/Lookup: today's iterator, unchanged.
                                                 Prefixed: total_order_seek, so seek_to_first/last work.
  Storage::scan_prefix(&Cf<Prefixed<N>>, &[u8; N])   prefix_same_as_start; cannot leave the prefix
```

There is no default type parameter (D8): zslg's families are written `ColumnFamily<Plain>` and `Cf<'_, Plain>`, so
the mode is visible at every declaration. A zslg family cannot reach the prefix options. `CfSpec::new(family,
compress)` erases the mode into data (`AnyFamily`), so a schema list stays one slice. `scan_prefix` takes `&[u8; N]`, so a prefix of the wrong width does not compile. What the compiler cannot
check is that the key layout puts constant-width fields first; that stays a doc and a test.

### 6.2 Snapshot (D4)

```text
  Storage::snapshot() -> Snapshot<'_>            borrows the Storage; cannot outlive it
  Snapshot::get_pinned(&Cf<A>, key)    Snapshot::scan(&Cf<A>)    Snapshot::scan_prefix(&Cf<Prefixed<N>>, &[u8; N])
```

### 6.3 Durability stays with the callers (D7)

The kit adds no durability API: `Storage::write` and `Storage::fence` (an unconditional `flush_wal(true)`) are as
they are, so zslg's commit path does not move. nvzn's store keeps its dirty flag itself:

```text
  write(batch)   storage.write(batch), then dirty.store(true)       armed after the write, whatever the outcome
  flush()        if dirty.swap(false) { storage.fence() }           re-arm on failure
```

The ordering argument (arm after the write, never before) stays in nvzn's comment. A flush racing an in-flight
write may return `Ok` without covering it; the write's own arming forces a later sync, so this is a timer, not a
commit fence. `zslg-local-store` keeps its own copy of the pattern.

### 6.4 nvzn's error (D5)

`nvzn_types::Error` replaces `RocksDB(rust_rocksdb::Error)` and `RocksDBColumnFamilyMissing(String)` with
`Storage(#[from] rocksdb_kit::StorageError)`. Nothing in nvzn or nlnr matches on the old variants.

### 6.5 Metrics

The kit exposes `Storage::property_u64`; it names no series. nvzn keeps `nvzn_store_live_sst_files_size_bytes` and
reads the property through the kit. The 25-metric golden dump is unchanged.

## 7. State transitions

None in the kit. nvzn's dirty flag moves as it does today: `clear → armed` after a write, `armed → clear` before the
fsync in `flush`, `clear → armed` again if the fsync fails.

## 8. Complexity

Not a hot-path change: `write` and `fence` are untouched. `scan` for `Plain` and `Lookup` is the iterator it is
today. nvzn's reads gain no indirection beyond the typed handle.

## 9. Edge cases

- A prefix scan on a family opened `Plain` does not compile.
- `seek_to_first` on a prefix-mode iterator is unreachable: `scan` on `Prefixed` opts into total-order seek.
- A failed fence in nvzn's `flush` re-arms its flag, so the next tick retries (as today).
- Families missing from an existing directory: see A3.

## 10. Alternatives considered

Raw-options escape hatch; a runtime `access` field; two modes with a bloom on `Plain`; one shared database; dirty
tracking in the kit (D4, superseded by D7); a tracked fence replacing `fence`. All rejected in the log with reasons.

## 11. Testing strategy

- The option set `Plain` builds is pinned (zslg's tuning cannot change silently).
- nvzn's flush skips the fence when clear, fences when armed, and re-arms on failure.
- A `Prefixed` scan returns exactly the keys with the prefix, and `scan_all` walks the family end to end.
- Where RocksDB does not expose an option's value, a behavioural test covers it and the post note says which.
- nvzn's tests pass, including `every_write_path_arms_the_fence`; zslg's tests pass unchanged.
- The 25-metric golden dump; the generated-faults proptest after step 3.
- nlnr: `cargo check --all-targets` against the patched workspace; `e2e quick`.

## 12. Implementation sequence

1. **Kit: access modes.** `Access`, typed `ColumnFamily<A>` / `Cf<A>`, `scan_prefix`, `scan_all`; the pinning test.
2. **Kit: snapshot.**
3. **`nvzn-store-rocksdb` onto the kit.** `StoreConfig → StorageConfig`; `Lookup` and `Prefixed` families; its dirty
   flag around the kit's `write` and `fence`; `nvzn-types` error; proptest; golden dump.
4. **Close.** `crates/README.md`, `docs/crate_inventory.md`, the post note, nlnr pin and `e2e quick`.

## 13. Operational concerns

None for operators: no environment variable, metric or on-disk format changes. nvzn's database is reopened with
new option objects but the same settings, and RocksDB reads SSTs whatever options wrote them.
