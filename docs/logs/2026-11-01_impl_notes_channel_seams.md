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
