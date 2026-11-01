# Post — channel seams

Implementation: four slices on `channel-extraction` (mechanical; completeness
and the archive; kinds and the seam; resumption). Design:
`2026-11-01_design_channel_seams.md`; decisions `..._log_channel_seams.md`
(D1–D36); running record of surprises `..._impl_notes_channel_seams.md`, which
this document summarises and does not repeat.

## 1. Code review

```text
channel.rs   Channel<T, K>, End/AbortReason, combinators, the private Source
             tree, one resume source (Resume)
kind.rs      Finite / Infinite, Kind, MergeKind              (new)
ordered.rs   Key, Successor, policies, OrderedChannel, covers / also / stitch
seam.rs      Seam, Segment::open → cut / chain / merge       (new)
resume.rs    Opening, Resumable, resync                      (new)
fanout.rs    broadcast/keyed, Registry, Fanout, Wakes
feed.rs      Feed<T, K>, produce_blocking / produce_async
level.rs     Level / LevelSetter                              (new)
```

`blend.rs` (`Tail`, `Ranged`, `blend_chain`) is gone. `Source` is still
private; nothing public names it.

What the kind parameter changed in practice: every signature in zslg and nvzn
now says whether a stream can end cleanly. The compiler rejected four places
that returned a finite stream where a subscription was owed (§3). The
`compile_fail` doctests pin the rules: an empty channel is not a
subscription, an infinite channel has no `chain`, `covers` needs a finite
contiguous stream.

zslg's channel code is now mostly declarations: the archive is
`chain_all(defer_async(read_snapshot))` with `take_while`, `filter_above`,
`ordered::<Contiguous>`, `covers` and `also`; the stable and unstable tails are
a `Seam` each, opened through `Resumable` / `resync`; the replica and the nvzn
live view use `Segment::open`. What remains of the old hand-written loops is
domain policy: which source, which cut, which key, the metric and the log line.

## 2. Design fidelity and deviations

Implemented as designed:

- `Channel<T, K>` and `OrderedChannel<T, P, K>`, no default kind; the kind
  rules of `kind.rs`; `into_finite` is the one widening.
- Exclusive `after` everywhere (D19/D26.3), `Successor::next_after(Option<Self>)`,
  `LogIndex::before` as its inverse; `Reader::subscribe_stable_log(after)`.
- `Seam` / `Segment::open`, no `Live` token; `covers`, `also`, `stitch`,
  `take_at_least`, `dedup_by_key`, `filter_above`.
- `Feed<T, K>`, `produce_*` and `defer_async` generic over the kind; `Fanout`
  with `Stopped`; `Level`.
- D18: reopen only after `ProducerGone`; one private resume source behind
  `Resumable` and `resync`.
- The nvzn live range without a sentinel (§10.1).
- `into_stream`, `IntoIterator`, `drain_into`, `ConsumerGone`,
  `shutdown_on_token` and `Tail`/`Ranged`/`blend_chain` removed.

Deviations (details in the impl notes):

- **`into_finite()` is new.** `dyn` APIs that accept either kind needed a sound
  widening (`integrate_loop`, `Replica::write`'s input).
- **`SnapshotWrite.last` is an `Option`**; snapshot `end_index` comes from it.
- **`Resumable` is a unit-struct namespace** (`Resumable::new` returns an
  `OrderedChannel`); the order check wraps the resume source instead of living
  inside it.
- **`Opening` replaced the previous `End`** as the factory's argument.
- **`StopReason::SubscriptionClosed` is deleted**, with its metric label
  `subscription_closed`. A dashboard keyed on it goes quiet.
- **`TestLog::subscribed_from` became `subscribed_after`** (public `testing`
  API, `Vec<Option<LogIndex>>`).
- **`StableSeam::above` filters at `max(frontier, after)`.** The design's
  one-liner used the frontier alone; the old outer cursor filter also dropped
  live entries below the caller's start, which now lives in the seam.
- **Network bridges** (`subscribe_stream`, `write_stream`) moved to slice 4 and
  use `produce_async`; their reader loops hold a `Feed<_, Infinite>`.
- Two `Key<_> for Hlc` impls (`UnstableStep`, nvzn's `FoldItem`) so each
  seam's `above` is `filter_above::<Hlc>`.

## 3. What we learned

- **The kind types earned their keep on the first compile.** `ErrorTransport`'s
  `write` returned a finite map where a subscription was owed;
  `ErrorTransport::subscribe_from` and the direct-dialed `subscribe_from`
  returned `Channel::empty()`; the direct-dialed `write` was `once(Err)` with no
  infinite tail. Each would have shown a consumer a clean end where an abort was
  due. None was visible at the call sites before.
- **The inclusive→exclusive switch is the dangerous part of any cursor
  change.** D36 predicted it and it hit exactly there: nvzn's three
  stable-stream test doubles passed an inclusive start to an exclusive
  parameter and every fold test silently skipped one entry. `LogIndex::before`
  and moving the boundary to `after` made production callers simpler (the
  supervisor and live view pass the stored watermark as it is).
- **Making the stream infinite makes tests honest.** Doubles that returned a
  "closed" channel from an infinite signature now end `ProducerGone`, which is
  what a dropped connection looks like. Two tests that relied on a clean end
  were rewritten; one (resuming after `Complete`) was deleted because it can no
  longer be expressed.
- **Lazy beats eager for test helpers.** A test-only `items()` that collected
  eagerly hung every test taking a prefix of an infinite tail; it is now lazy.
- **Stressing found a bug that was not ours.** Running the zslg lib under
  CPU load (about 1 run in 3 failed) exposed a race in `Fanout`: a subscriber
  could be handed elements published before it subscribed, because the thread
  prefers registrations over queued data. Fixed with a publish count
  (`Fanout` numbers publishes; `subscribe` records the count; the thread
  withholds older elements) and a regression test that deadlocks the old
  code. It predates these slices, and a plain `cargo test` rarely hits it.
- **Verification traps.** A full disk (`target/debug/incremental` at 15 GB)
  produced spurious test failures and a spurious proptest seed; verification
  now runs with `CARGO_INCREMENTAL=0`. One full-workspace run timed out and
  failed `three_node_tcp_write_round_trip`; the test passes alone and 12 of 12
  with six CPU hogs, so that failure is still unexplained.
- **One rule in one place.** D18 existed as two hand-written closures that both
  had to remember it; it is now the resume source's `ended`. A third consumer
  cannot forget it.

## 4. What is next

- The one unexplained `three_node_tcp_write_round_trip` failure (see §3). If
  it recurs, run the cluster tests as their own target with the output kept.
- `Fanout`'s drop-cancel is biased ahead of queued data: elements still in the
  queue at drop may be discarded (slice 1 note). Decide whether any consumer
  needs a draining shutdown.
- `produce_async` bodies for the network bridges still own their socket
  shutdown by hand (the blocking read cannot be cancelled by a token); a crate
  helper for "blocking loop + force-close handle" would remove the repeated
  shape in `subscribe_stream`, `write_stream` and the server handlers.
- The gap-fill `stitch` keeps its own endpoint check in the crate; if a second
  caller never needs it, `covers` alone could carry it.
- Re-run the full matrix (both feature sets, clippy, doctests) once more
  before merging.
