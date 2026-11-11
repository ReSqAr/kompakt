# nvzn-store on rocksdb-kit — my mental model

## What I think we are doing

Make nvzn use the same engine layer as zslg. The two stores' option building, budget arithmetic, write and fence
are already nearly the same code; the differences are exactly three access patterns, which the kit models as types.

## The shape

```text
  rocksdb-kit   Access = Plain | Lookup | Prefixed<N>   (types, not flags)
                Snapshot
                write and fence unchanged: the kit is a type wrapper, durability stays with callers
  zslg stores   Plain; fence for commits; the local store keeps its own dirty flag
  nvzn store    Lookup + Prefixed<N>; its own dirty flag around write/fence; its own Storage and budget
```

## What would make this wrong

- If `Storage::open` cannot create missing families in an existing directory (A3): nvzn's open would need a path.
- If nvzn's snapshot is held across more than one read view (A4): the borrow in `Snapshot` would not fit.
- If a `Plain` option set differs from what zslg builds today by anything I did not read: the pinning test is the
  guard, and a step-1 finding would go to the log.

## Status

Implemented (steps 1-3); closing. See the post note.
