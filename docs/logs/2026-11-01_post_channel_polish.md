# Post — channel polish

Implementation: `8f3e545` plus the seam follow-up commit on
`channel-extraction`. Design: `2026-10-31_design_channel_polish.md`.

## 1. Code review

The split is now clear:

```text
channel.rs   Channel<T>, End/AbortReason, combinators (empty/once/take_until/dedup)
ordered.rs   Key, Successor, policies, OrderedChannel, stitch
fanout.rs    broadcast/keyed, Registry/KeyedRegistry, Shutdown, Wakes
feed.rs      Feed
blend.rs     Tail/Ranged/blend_chain
```

`fanout.rs` is the largest new surface. The thread owns the subscriber set and
the select is the only control flow; there is no lock around routing and no
per-subscriber pending queue. The cost is one thread per fan-out and a
registration handshake that had to be made explicit (see below). The `Block`
path clones senders per element to keep the set mutable while a send is
outstanding; the `Shed` path is a simple `try_send` + descending removal.

The typed boundaries are thin wrappers: `LocalStoreView` wraps its scans after
the existing filter, the archive wraps its producer, and the loader composes
`dedup_by_key` + `Contiguous`. The error surface is intentionally less specific
(`OrderViolation`/`StreamTruncated`), with the boundary in the `what` label.

`LiveSeam::range` is now a five-combinator expression. The sentinel error is
the one non-obvious trick; `take_until` drops it on the normal cut and lets it
through only on a clean early end.

## 2. Design fidelity and deviations

Implemented as designed:

- `Channel` rename; `Contiguous`/`StrictlyIncreasing`/`NonDecreasing`.
- Typed local-store and archive boundaries; generic order errors.
- `dedup_by_key` + `Contiguous` stable-log loader.
- Threaded `(tx, registry, shutdown)` fan-out; `Block`/`Shed`; `T: Clone`
  (the caller chooses `Arc` if wanted); bounded data/register/subscriber
  queues; `Shutdown` one-shot + join; Tokio bridge.
- `zslg` migrated to the tuple; `nvzn` to the keyed registry.
- Lazy LiveSeam with `take_until` + sentinel error; no `Feed`/`pump`/`ScanEnded`.
- `empty`, `once`, `take_until`, `dedup_by_key`; `take_while` retained.
- `Fanout` removed; `Wakes` retained.

Deviations forced by the implementation:

- Phase 1 is biased **cancel, register, data**, not “register last”. A
  subscriber enqueued before an element must be live for that element; the
  design’s order could lose the first element after `subscribe`.
- The thread closes registration and drains the queue on exit. Without it, a
  live `Registry` handle could keep a queued subscriber sender alive after the
  thread stopped, and the subscriber would hang instead of ending.
- `nvzn` retains a small reader-side `Subscriptions` wrapper for the live-view
  `Cancellation` and armed handle. The registration queue, `take_up`, and the
  fold-side subscriber list are gone; the wrapper is lifecycle, not routing.
- `subscription_shed` and `unstable_fanout_hangups` are removed. The subscriber
  observes `ProducerGone` and logs; a producer-side observer can be added later.
- `LiveSeam` uses `StopReason::LiveStreamEnded` for a generic merged `End`;
  `ScanEnded` is gone.
- The loader drops any index below the start, wherever it appears, preserving
  the overlapping re-append contract; a backward step at or above the start is
  an order violation.
- The zslg local-store overrun test moved to the crate-level `Shed` test: with a
  threaded fan-out, the consumer can drain between elements, so the old
  synchronous overrun timing is not a stable domain test.
- `PENDING_SUBSCRIBERS` is 1024; the old “idle fold leaves registrations
  pending” test no longer applies because registration is a thread select.

## 3. What we learned

- **Asynchronous registration is an ordering contract, not a detail.** The
  first successful design preserved only data delivery; the test suite exposed
  that `subscribe` then publish needs registration to win the select. Bias and
  an explicit close/drain are part of correctness.
- **`Block` means the slowest subscriber paces the element.** A single thread
  makes that literal; no per-subscriber pending buffer is needed. The data queue
  is the only buffer, and its bound is the backpressure.
- **`take_until` is a useful primitive.** It turns an after-the-fact marker into
  a conditional terminal: the normal cut stops consumption before the chained
  marker is reached. This avoided both a manual loop and a `Bound` flag.
- **Stopping a fan-out needs a lifecycle owner, not a `Cancellation` clone.**
  `Shutdown` as a non-cloneable one-shot plus join is simpler and keeps the
  deliberate-stop verdict (`ProducerGone`) honest.
- The remaining complexity is not channel plumbing; it is the live-view
  lifecycle (fold cancellation vs fan-out shutdown) and the one-thread-per-
  fan-out resource choice.
- The fan-out is generic over `T: Clone`; it does not impose `Arc`. `zslg`'s
  `Data`/`StableLogEvent` are `Bytes`-backed and `nvzn`'s written-ref triple is
  `Copy`, so the per-subscriber clone is cheap. A future expensive payload can
  use `T = Arc<Payload>` at the call site.

## 4. Seam audit

The channel *mechanics* are crate-owned; the domain names the pipeline. A
follow-up pass closed the places where the old framing still leaked:

- **Subscription failure is a crate decision.** Four `zslg` sites repeated
  `registry.subscribe().unwrap_or_else(|_| Channel::aborted("..."))`, including
  a second copy of the fan-out's label. `Registry::subscribe_infinite` and
  `KeyedRegistry::subscribe_infinite` own that mapping and use the fan-out's own
  `what`; a site can no longer substitute `Channel::empty()` and turn an
  infinite source's failure into a clean `Complete`.
- **History-then-live is a crate decision.** The two resuming tails opened the
  seam by hand and rebuilt `range(cut).chain(live)`, bypassing `blend_chain`
  only because they also needed the cut for logging. `Tail::blend` returns the
  segment **and** the cut, so the loops keep their metrics while the seam's one
  ordering rule stays in `blend.rs`.
- **Infinite streams never complete on a gate.** `GatedReader`'s stable and
  unstable subscriptions emitted a terminal error and then `Complete`; they now
  chain `Channel::aborted`, matching the contract the docs state.
- **Completion has one constructor.** `Channel::from(..) + Closer::close()`
  for an already-finished stream became `Channel::empty` / `Channel::closed` /
  `Channel::once`; the blocking scan producers (`pump_range`,
  `StableLogSnapshot::range`, both local-store scans, `read_snapshot`) now use
  `Feed::channel`, which pairs the sender with its close proof. `Channel::from`
  remains only at the genuine adapter sites that share a sender with a nested
  task (the archive stream, `HistoricalReplay`, the merge feeder).

What remains in `zslg`/`nvzn` is domain pipeline, not channel plumbing: which
source, which cut, which key, and the metric/log hook around it. The resuming
loop, the cursor, and the watermark gate are domain policy; the transport
(subscribe failure, history/live order, completion vs abort, close discipline)
is the crate's.

## 5. What is next

- Optional producer-side observer for shed events; the subscriber log is the
  only signal today.
- Per-key backpressure for `KeyedRegistry` if one slow key must not pace the
  whole data stream.
- Consider a shared fan-out thread pool if the per-fan-out thread count becomes
  a problem in large multi-tenant deployments.
- Revisit `StableLogSnapshot::range` typing; `HistoricalReplay` still wraps it
  because the local scan may have gaps.
- Add a model test for the fan-out (`Block`/`Shed`, registration ordering) and a
  stress test for shutdown with a stalled receiver.
- Run the full workspace/clippy matrix once more before merging.
