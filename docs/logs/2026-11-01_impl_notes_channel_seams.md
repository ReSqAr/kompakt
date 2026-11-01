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
- **A full disk masqueraded as test failures.** One rerun reported failures
  in replica/cluster proptests and the snapshot lifecycle; the cause was
  `target/debug/incremental` reaching 15 GB and the disk filling (a proptest
  even recorded a spurious regression seed, discarded). Verification runs now
  use `CARGO_INCREMENTAL=0`; with that, slice 2 is green.

## Slice 3
- **The kinds caught four real bugs on first compile.** Each returned a
  finite stream where a subscription was owed, which the old single-type
  channel allowed silently (a clean `Complete` where a consumer expects a
  subscription never to end): `ErrorTransport::write` (a finite map; now
  chained with `Channel::aborted("router shutting down")`),
  `ErrorTransport::subscribe_from` and the direct-dialed `subscribe_from`
  (both `Channel::empty()`; now `Channel::aborted(..)`), and the direct-dialed
  `write` (`once(Err)` with no infinite tail; now `.chain(Channel::aborted(
  "direct-dialed peer"))`).
- **`into_finite()` is new, not in the design.** `dyn` APIs that accept either
  kind (`integrate_loop`, the coordinator's and stream handler's `write`
  input) take `Channel<_, Finite>`; an infinite subscription is widened with
  `into_finite()`, the only sound direction (an infinite stream never
  reports `Complete`, so nothing can be misread).
- **`resume_on_abort` now aborts when the first open is declined** (it used to
  look like a clean `Complete`, which an infinite consumer could not tell from
  success). The old test that resumed after `Complete` is deleted: an infinite
  stream cannot complete.
- **`StopReason::SubscriptionClosed` is deleted.** The fold's input is an
  infinite channel, so a clean end is unrepresentable; the variant was only
  reachable from `End::Complete`. Its metric label `subscription_closed`
  disappears with it (a dashboard keyed on it would go quiet).
- **The live range's sentinel is gone** (design §10.1). `LiveSeam` now
  implements `Seam`; `range` ends the infinite stable subscription with
  `take_until`, so a scan that stops before the cut aborts the range, and
  `begin` uses `Segment::open(&seam).cut()` / `.merge()`. The
  `FoldItem::ScanFailed` for the old `StreamTruncated` sentinel is still used
  for error items on the stream itself.
- **Test doubles that returned a closed channel from an infinite signature**
  (`Channel::closed(out_rx)` in the faulty replicas, `from(rx)` + `close()` in
  the coordinator's fake) now use `Channel::from_infinite`; their "end" is a
  `ProducerGone` abort, which is what a real dropped connection looks like.
- **A crate-level dependency gate.** nvzn's `crossbeam_channel` is only used by
  test doubles and the `testing` feature; `-D unused-crate-dependencies` needed
  a `cfg(not(any(test, feature = "testing")))` `use … as _`.
- **Not done in slice 3: the network bridges** (`subscribe_stream.rs`,
  `write_stream.rs`) still wire by hand rather than via
  `produce_async::<_, Infinite>`; see the open items in the post doc.

## Slice 4

- **`Resumable` is a unit-struct namespace, not a value.** `Resumable::new`
  returns an `OrderedChannel<_, P, Infinite>`, so the type carries no state; it
  exists to give the constructor the design's spelling
  (`#[allow(clippy::new_ret_no_self)]`). A free function would read the same.
- **The cursor lives in the resume source, the order check outside it.**
  `Resumable` is `Channel::resume(..).ordered::<P>(after, what)`: the source
  tracks the last key it emitted (`track: fn(&T, &mut C)`, a no-op for
  `resync`'s `C = ()`), and the policy sees the stream across cycles, so a
  factory that restarts at the wrong index is an `OrderViolation`, not a
  silent repeat. No dedup anywhere (D3).
- **The stable seam's `above` needs `after` as well as the frontier.** The old
  outer `AtomicU64` filter also dropped live entries below the caller's start
  (`from` past the frontier: live entries up to `from` would otherwise break
  `Contiguous`). `StableSeam::above` is now
  `filter_above::<LogIndex>(frontier.max(after))`, which carries that duty.
- **Two new `Key` impls for the one-line `above`s:** `Hlc` for
  `Result<UnstableStep, Error>` (zslg) and `Hlc` for `FoldItem` (nvzn). Only
  live entries have a key; markers pass. Orphan-rule fine (local item type).
- **`Reader::subscribe_stable_log(after: Option<LogIndex>)`** (D26.3) landed
  with the inclusive→exclusive conversion at every site. Production callers got
  *simpler*: the supervisor held `watermark: Option<LogIndex>` and used to
  convert it to `next_after(..)`/`ZERO` for the call; it now passes the
  watermark itself. nvzn's live range holds `after = f.index` likewise.
  Tests: `LogIndex::ZERO`/`LogIndex(0)` → `None`, `LogIndex(1)` →
  `Some(LogIndex(0))`; `write_tracker` passes `floor.before()`.
- **`TestLog::subscribed_from()` became `subscribed_after()`** (a public
  `testing` API, `Vec<Option<LogIndex>>`). A rename that breaks downstream
  test code for the same reason as the trait: the number meant "first wanted",
  now means "last seen".
- **`StableStream::new(.., after)` call sites got simpler too:** the test
  doubles pass `after` straight through; the `from.before()` workaround from
  slice 2 is gone.
- **Deleted:** `Tail`, `Ranged`, `blend_chain`, `blend.rs`, `Channel::
  resume_on_abort`, `stable_stream`, `StableSeam`'s `AtomicU64` cursor and
  outer filter, `resumable()`. The channel unit test for `resume_on_abort`'s
  verdict hand-off went with it (its cases live in `resume.rs`: reopen after
  `ProducerGone`, `Opening` passed through, first-open decline, cancel not
  resumed, wrong restart caught); the model proptest now exercises `resync`.
- **`Opening` instead of the previous `End`.** The factory used to receive the
  whole previous `Option<End>`; it now sees `First` or
  `AfterProducerGone { what }`, the only two cases the D18 rule lets through.
  The warn logs print `what` instead of `?previous`.

## Verification: the load sensitivity

- **A real fan-out race, found by stressing the zslg lib under CPU load** (four
  to six busy loops on four cores; 1 failing run in 3 before the fix). The
  failure was `subscribe_unstable_tail_replays_then_follows_live` seeing an
  entry twice. Cause, in `Fanout` from the polish round, not from these slices:
  `subscribe` queues a registration, the distribution thread prefers
  registrations over queued data, and `publish` only queues. So an element
  published *and returned* before `subscribe` was called could still be
  delivered to the new subscriber if the thread had not dequeued it yet. The
  unstable tail's scan then also read it from the store: a duplicate. The
  contract allows duplicates there (at-least-once), so production was not
  wrong, but the crate's ordering promise was weaker than its docs implied,
  and the test (rightly) assumed the strong one.
- **Fix:** `Fanout` numbers every publish (`State::published`); `subscribe`
  records the count when it registers; the thread withholds every element
  numbered below a subscriber's record. "A subscriber sees exactly what is
  published after its `subscribe` returns" is now true; the existing bias
  (a registration queued before an element is live for it) still holds. A
  failed publish consumes a number, which is harmless. Regression test
  `a_subscriber_never_sees_what_was_published_before_it_subscribed`, which
  deadlocks the old code (the late subscriber's unread queue blocks the
  thread) and so runs behind a 10 s watchdog: it fails in 10 s without the
  fix.
- **Not reproduced on its own:** `three_node_tcp_write_round_trip` passed 12 of
  12 repeats with six CPU hogs running. The one failure seen in a full
  workspace run is unexplained; see the post doc.
- **Second fan-out finding: drop-cancel discards queued refs** (reported on
  review, `a_stopped_fold_ends_the_channel_after_its_last_ref`, 2 of 30 under
  four CPU hogs; 1 of 60 here before the change, 0 of 40 after). `publish`
  returns once an element is queued; `Shutdown` cancels, and the thread
  prefers cancellation over queued data, so a clean fold stop could discard
  the last ref after `sync()` had returned. Decision: a draining shutdown, not
  an ack per publish (cost on every publish) and not a weaker test (the
  contract is delivery). `Fanout::drain(grace)` closes the data queue, lets the
  thread empty it, and cancels only after `grace` so a `Block` subscriber that
  never reads cannot hold the caller. nvzn's `Subscribers::cancel_if` drains
  (5 s) unless the recorded reason is a cancel, which still stops at once.
  Tests: a slow reader keeps the queue full when the drain starts (`shutdown`
  gets `[]`, `drain` all 30), and a stuck subscriber bounds the drain.
- **Pitfall in my own verification:** a "negative check" that reverted the fix
  failed to compile (an unused constant is an error here), the loop counted the
  empty binary path as 60 failures, and I almost read it as a reproduction.
  Check that the binary built before trusting a failure count.
- **Stress timeout, not a failure:** one of six full-lib runs under six CPU
  hogs hit the 400 s limit with `cluster_upholds_invariants_under_generated_faults`
  unfinished (about 106 s unloaded). No assertion failed in any of the six.
