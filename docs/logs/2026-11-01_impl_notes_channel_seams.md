# Channel seams — implementation notes

Running record of things that were unusual during implementation, for the
post-implementation doc. Not a design doc; the design is
`2026-11-01_design_channel_seams.md`.

## Slice 1

- **`AbortReason::ConsumerGone` becomes unreachable.** Its only producer was
  `into_stream`'s liveness guard (`Guarded` with `fired: None`). With
  `into_stream` gone (D29), `Guarded` is only built by `until`, so `fired` is
  always present and the variant has no source. Removed with it; D18's table
  listed it as "does not resume", which still holds vacuously.
- **Pre-existing quirk in `LeafState::end_reason`.** In the `Pending` state, a
  closer that disconnects without closing resolves the leaf to
  `RoutingViolation` but returns `ProducerGone` for that same read; every later
  read then says `RoutingViolation`. The two reads disagree. Looks like a typo
  for `ProducerGone`; fixed to `ProducerGone` on both paths.
- **The compaction data-loss path (D25) is closed already in slice 1.** Removing
  `IntoIterator` forced `feed_sources` off `for item in ch`; its interim `recv`
  loop forwards an aborted source as `StreamTruncated`, so the fix lands before
  slice 2 replaces the feeder with a declaration.
- **`shutdown_on_token` removed.** It had no caller outside the crate's own
  test; with `Shutdown` folded into `Fanout`, it would have needed a
  `Fanout`-shaped replacement for nobody (no-caller rule, D16/D21).
- **`Fanout` has no `Drop` of its own.** Dropping it drops the private
  `Shutdown`, which cancels — so the four hand-written `Drop` impls go without a
  replacement. One consequence worth knowing: drop-cancel is biased ahead of
  queued data, so elements still in the data queue at drop may be discarded.
  The old crate test "dropping the data sender drains then ends" no longer
  holds and was rewritten to drop after delivery.
- **`Level` clone semantics.** A clone is a *fresh* reader that has seen the
  current value (like `watch::Sender::subscribe`), not a copy of the source's
  "seen" marker. `purge.rs` relied on the watch's fresh-receiver behaviour;
  copying the marker would have given it one spurious early wake.
- **`SnapshotWrite.last` is `Option<EntryMeta>`, not `EntryMeta`** (deviates
  from §9.6). `write_snapshot` is generic — compaction and tests call it too —
  so an empty write is representable; the snapshot builder turns `None` after a
  non-empty peek into an error.
- **Snapshot `end_index` comes from `SnapshotWrite.last.index`** instead of
  `start + count - 1`: the writer already saw the last entry, so the
  arithmetic (and its overflow checks) went.
- **`WatermarkGate` lost its `Mutex` in slice 1, not slice 4.** It became plain
  `FnMut` state as soon as `filter_map` took `FnMut`. The stable cursor's
  `AtomicU64` stays until slice 4: it is shared by two closures (the resume
  factory and the filter), which only `Resumable` removes.
- **Tests needed a lazy, verdict-ignoring read.** Removing `IntoIterator` and
  `into_stream` left ~70 test sites that only compare items. They use a
  test-only `ChannelTestExt` in zslg (`items()`, `into_test_stream()`), not a
  crate API. Two traps found: `items()` must be lazy — several replica tests
  take `.take(5)` of an infinite live tail, and an eager collect hangs — and
  `into_test_stream` must keep the old bridge's cancellation guard, or a
  dropped stream on a quiet infinite channel parks a blocking thread and hangs
  runtime shutdown.
- **The network bridges move to slice 3, not slice 1.** An infinite
  `produce_async` needs `Feed<T, Infinite>` (no `close()`), which is a kind;
  a pre-kind version would have been a throwaway. They stay hand-wired until
  slice 3.
- **`examples/write_tracker` got its own `drain` helper** (recv on the blocking
  pool, guarded by a `CancelHandle` the receiver owns) — the same shape as the
  test helper, since examples cannot use test code.

## Slice 2
- **D18 lands in slice 2, not slice 4.** The lazy `HistoricalReplay` now
  propagates an order violation as the stream's abort (the old one converted
  it into an `Err` item and closed cleanly). Today's stable resync loop
  resumed on any abort, so an order violation would have spun it. Both resync
  closures now decline unless the previous end is `ProducerGone` (a small
  `resumable` helper); slice 4 moves the rule into the crate.
- **`Contiguous` from `None` now has a defined first key.** Before, a missing
  start meant "no expectation" for the first item; with exclusive `after`,
  `None` means "from the beginning", so `Successor::next_after(None)` is owed.
  No production site relied on the old meaning (every contiguous boundary
  passes a start).
- **`ObjectStoreArchiveReader` captures `Handle::current()` at construction**
  rather than taking a handle parameter: every constructor call is already in
  a runtime, so the explicit handle (D26.7) lives in the struct without
  threading it through four call sites. The compaction merge reads
  `Handle::current()` inside its async fn for the same reason.
- **The archive's tiling failure changes shape.** "Not fully tiled" used to be
  an `Err(StableLogGap)` item; it is now `Aborted(OrderViolation { "archive
  window" })` from `covers`, surfacing as `StreamTruncated` at the zslg
  boundary. Tests asserting the old error text are updated.
- **`stitch` keeps its own fallback endpoint check.** The design said the
  archive's `covers` replaces "the fallback's hand endpoint check"; that check
  lives in the crate's `StitchSource`, not in zslg, and stays as the generic
  guarantee for any fallback. zslg has no hand check left.
- **Two buffers disappear with the forwarding hops**: the merge feeder's
  `SNAPSHOT_WRITE_FORWARDER_CAP` and the replay's `STREAM_CHANNEL_CAP`. Entries
  now flow straight from `read_snapshot`'s channel to the consumer.
- **The inclusive-to-exclusive hazard (D36) bit exactly where predicted.**
  nvzn's three stable-stream test doubles passed `Some(from)` — an inclusive
  start — to `StableStream::new`, whose second argument is now `after`. Every
  fold/supervisor/live/reader test, and btrb's live-write test through nvzn's
  `testing` double, then skipped index `from` and stopped with
  `SubscriptionAborted`. Fixed with `from.before()` (`LogIndex::before`, the
  inverse of `next_after`).
