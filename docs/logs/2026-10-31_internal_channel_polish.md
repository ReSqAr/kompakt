# Internal model — channel polish

Working document for the IAIC interview. Not a design doc.

## What I currently believe

- The write half has three possible layers: broadcast `Fanout<T>` (shared,
  sheds), keyed routing (subset by key, block or shed), and a subscription
  registry (pending registration + take-up + cancellation).
- `Fanout` is `&self` because the stable log and local store hold it in an
  `Arc`. A keyed router can be `&mut` if it is not merged with `Fanout`;
  merging forces `&self + Mutex` and a clone-out path for `Block`.
- `PENDING_SUBSCRIBERS` bounds the reader→fold registration queue, not the live
  subscriber count and not the per-subscriber data queue.
- `_keep_armed` is the subtle lifecycle: on a non-cancel stop the fold drops its
  handle and senders, but the reader's handle keeps the cancellation from
  firing, so streams end `Aborted(ProducerGone)` instead of `Cancelled`.
- `end_with` must see the `End`; success-only is `chain(once(...))` and lets a
  scan abort poison the merged tail.
- `LiveSeam::range` decomposes into `take_while` + `filter_map` + `end_with` +
  `Feed::pump`; the stable subscription is infinite, so `Complete` is the cut.
- The subscriber cannot distinguish “slow” from “owner gone” with
  `ProducerGone`; a `Lagged`/`Overflow` reason may be needed if the subscriber
  is expected to log that it was shed.

## Update after Q1

- Q1 is settled: the **registry** is in the crate. My prediction was correct.
- The user framed it as “a generic routing problem”: `nvzn` states the intent,
  the crate owns the mechanism. That also means the registry is not just a
  keyed fanout; it is the write-half lifecycle.
- Consequence for the next question: the route itself is now the main API
  surface. If routing is generic, the registry needs a keying concept.
- The fold keeps only *when* to take up and whether to cancel/abort. Those are
  policy, not mechanism; they should not collapse into the registry.

## Update after Q2

- Q2 is settled: a constructor closure `|&T| Option<K>` and `publish(&[T])`.
  My recommendation was accepted.
- The registry now has two generic parameters it did not have in the router
  sketch: the item `T` and the route key `K`. The route closure is stored once.
- The `publish(&[(K, T)])` shape is rejected: it keeps the routing knowledge at
  every call site.

## Open questions and predicted answers

1. **Registry vs router.** Settled: registry.
2. **How is a route expressed?** Settled: constructor closure.
3. **Registry shape: one shared object or core + handle?**
   - I expect the user to choose (2): they said `&mut` yes, and the fold is the
     only publisher. The reader needs a handle; the fold needs the mutable core.
   - The handle must hold an `Arc<CancelHandle>` so it stays armed; otherwise
     dropping the fold's core would end streams as `CancelHandleDropped`.
   - `abort()` clears the live senders but keeps the handle armed; `cancel()`
     fires it. This is `_keep_armed` made internal.
4. **Merge `Fanout` and the keyed router?**
   - The user's instinct is one keyed `Fanout<K, T>` with `K = ()` for
     broadcast. I had recommended separate types; this needs a real exploration,
     not a defence.
   - The concern is specifically `Block` on a **shared** `&self` fanout: the
     subscriber-list lock cannot be held across a blocking `send`. The unified
     type needs snapshot → send → prune (`same_channel`).
   - Gain: one type, one eviction implementation, one `OverflowPolicy`, no
     `KeyedFanout`/`Router`.
   - Cost: an uncontended mutex on the fold path, the clone-out `Block` path,
     `K = ()` noise at broadcast call sites, and weaker batch serialization for
     concurrent `Block` publishers.
   - Updated view: if the clone-out is acceptable, unification is coherent. The
     `&mut` keyed core is a local-reasoning benefit, not a performance one.
   - **Reframed by the user:** put the fan-out behind a distribution thread
     (actor). The thread owns the subscriber list, so `&self`/`&mut`, the lock,
     and the clone-out disappear; commands serialize register/publish/take-up/
     cancel/abort. This is a better core than arguing about locks.
   - A thread does not repeal the slow-subscriber trade-off: one thread blocked
     in `send` is head-of-line and cannot be joined/cancelled. `Shed`, bounded
     `Block { timeout }`, per-subscriber workers, or a `Select` event loop with
     per-subscriber pending queues are the ways out.
   - Prediction: the user will accept the thread. They care about not losing
     data, so they may prefer the event loop or a generous bounded `Block` over
     plain `Shed`.
   - **User direction:** the thread’s inbox is a merge of command channels
     (`cancel`, `register`, `publish`) mapped into one `Command` enum. This is
     the right use of `Channels::merge`; the thread is the sole consumer.
   - `merge` is fair; no bias unless a ready source can starve cancellation.
   - `Block { timeout }` is rejected: sync fails immediately or blocks; async
     `oneshot` + timeout owns waiting.
   - `Channels` → `Channel` is a candidate rename; one logical stream.
   - The event-loop subscriber-send side is not a merge; it is a `Select` over
     send operations. The simple actor can use `try_send`/shed and leave strict
     no-head-of-line `Block` to the event loop.
   - **Q5 prediction:** the user will choose sync non-blocking (option 1) because
     they rejected timeouts in the sync world. That means the fold's `Block`
     either moves to an async ack/retry or stays a separate synchronous core.
     I should make that consequence explicit rather than hide it.
   - **Correction:** the user chose unbounded sync `Block` for now, and an
     unbounded command inbox. So the prediction was wrong: they accept the
     shutdown/backpressure risk to keep `Block` simple. The open consequence is
     memory growth under a stalled subscriber; I should keep it visible.
   - **Q6:** cancellation does not directly drop the receiver; `Guarded` keeps
     the inner source until the consumer drops the `Channels`. The candidate fix
     is to drop the inner source when the guard observes the cancel. A consumer
     that never polls again is unforceable; that is a contract/bug boundary.
   - **Better fix:** the distribution thread must not park in `send`; it should
     `Select` over send operations and a cancellation receiver. Crossbeam has
     `Select::new_biased`, so cancellation can be first. On cancel the thread
     drops its senders; an IO-stalled consumer keeps its own receiver but cannot
     block the producer. `Guarded` drop-inner is then optional cleanup.
   - **Control queue:** bounded 1024 with `try_send` is a clean replacement for
     `PENDING_SUBSCRIBERS`; the data/publish queue is separate and selected
     alongside it.
   - **Prediction:** the user will accept the biased select loop; it directly
     answers the IO-stalled cancellation concern without timeouts.
   - **Inputs:** register queue bounded 1024 (`try_send`/`Full`), fold queue
     ordered `TakeUp | Publish(Arc<[T]>)`, cancel receiver, per-subscriber send
     ops. `Select` is the merge; `Channels::merge` cannot select sends.
   - **Key ordering:** take-up and publish share one queue so a subscriber is
     live before its first ref is routed. Register is separate and bounded.
   - **Bias:** cancel first, then sends, fold, register; round-robin sends. The
     exact order is the next thing to validate.
   - **Correction after user review:** the data queue is bounded; `pending` is
     bounded; the earlier `outbox` was wrong because it popped one item per
     subscriber and dropped the unselected ones. The correct loop registers send
     ops for non-empty `pending`, selects, then pops only the selected item.
   - **Semantics to state explicitly:** route each item to every matching
     subscriber; broadcast is `K = ()`; the send loop drains per-subscriber FIFO
     queues, not one global item.
   - **Round-robin:** unnecessary; a fair `Select` over the work ops avoids
     favouring the first subscriber, and the loop is sequential.
   - **Naming:** `data_rx`/`Data` instead of `fold_*`; `register_rx`, `cancel_rx`.
   - **Q8 simplification:** one element at a time, two phases. Phase 1 selects
     cancel/data and takes one `Arc<T>`. Phase 2 admits registrations and sends
     to every matching subscriber sequentially; the slowest paces. No
     per-subscriber pending, outbox, send_ops, can_route, or take_up command.
   - `take_up` was only “drain the registration queue”; phase 2 start does it.
   - `live` was the subscriber set; rename `subscribers`.
   - `send_ops` made it look like one subscriber got the element; the two-phase
     loop sends to all in sequence, so it is gone.
   - `Arc<T>` per element; one payload shared by all subscribers.
   - Broadcast gets a separate constructor; no `K=()` route closure at call
     sites.
   - **Q9:** `take_up` is unnecessary; `register_rx` is a select source in both
     phases and admits subscribers immediately. Phase 2 registers all matching
     send ops plus cancel/register in one `Select`; the slowest paces the
     element but cancel/registration are serviced. A subscriber admitted during
     phase 2 starts with the next element. Broadcast has no `()` parameter.
     `Shed` uses `try_send` and removes on failure.
   - **Q10:** the thread removes the `&mut` reason for a core+handle split. One
     cloneable handle (`Fanout<T>` / `KeyedFanout<K,T>`) held by the fold and
     readers is enough; the last drop stops the thread. I expect the user to
     collapse the split, since they are pushing for simplification.
   - **Correction:** the user prefers a `(tx, registry, cancel)` construction
     rather than one handle or a core/handle pair. `tx` is a raw bounded
     `Sender<Arc<T>>`; `registry` is the cloneable subscribe side; `cancel` is a
     cloneable `CancelHandle`. The thread is hidden. This is cleaner: the three
     tuple elements are exactly the three select inputs plus the data producer.
   - `CancelHandle` needs `Clone` (shared sender). The registry holds a
     `Cancellation` clone for `until`.
   - Open: whether `cancel` also joins/waits, or the thread stays detached.
   - **Q11:** the user prefers no `CancelHandle` wrapper. A fan-out `Shutdown`
     handle (cloneable) owns a cap-1 one-shot `Mutex<Option<Sender<()>>>` and the
     join handle. First `cancel()` succeeds without blocking; later calls no-op.
     `wait()` joins. Subscribers end by sender drop (`ProducerGone`); callers can
     bind `.until` themselves. Std thread is independent of Tokio; `wait` is
     blocking, `cancel`/`subscribe` are non-blocking.
   - Open: name, and `ProducerGone` vs `Cancelled` for deliberate shutdown.
   - **Q12:** deliberate shutdown → `ProducerGone` (settled). `Shutdown` is
     non-cloneable, owns the join handle, `cancel(&mut)`/`wait(self)`. A
     cloneable `ShutdownSignal` shares the one-shot sender for external/tokio
     cancellation; dropping it does not stop the fanout. `zslg`/`nvzn` each have
     one owner, so the split fits. The data `tx` is raw crossbeam and the
     producers are blocking; no tokio data path.
   - Open: split vs cloneable `Shutdown` with a Condvar.
   - **Q12 settled:** no `ShutdownSignal`, no Condvar; `Shutdown` non-cloneable,
     one owner. `cancel(&mut)`, `wait(self)`, Drop cancels no join. `zslg`/`nvzn`
     each have one owner; readers only need the registry. Tokio bridge takes
     `Shutdown` by value, cancels on the token, then `spawn_blocking(wait)` so
     the task outlives the fanout thread. If an owner must keep the `Shutdown`
     and also react to a token, add a signal later or select in the owner.
   - **Q13 settled:** the whole fan-out model above.
   - **Q14:** `LiveSeam::range` via `take_while` + `filter_map` + `end_with` +
     `Feed::pump`. The only caveat is the `Complete` ambiguity of `take_while`;
     the seam holds `Arc<dyn Reader>`, so the stable subscription cannot start
     with a declined factory; `Complete` is the cut. I expect the user to adopt,
     which keeps `take_while`; otherwise it is deleted.
   - **Correction after user challenge:** do not suppress a scan abort. The
     user is right: a scan abort is fatal; the view halts. Drop `end_with` and
     `ScanEnded`. Use `take_while` + `chain(once(ReplayDone))`; the abort
     propagates through the chain/feed/merge. `run` uses a `scanning` flag to
     report `LiveScanEnded` vs the tail. `Feed::pump` is the blocking bridge with
     correct close/abort, not an error suppressor.
   - **Further correction:** `Feed::pump` is unnecessary. The scan is already a
     lazy `Channels`; `range` can return the composed scan directly and `run`
     pulls it through the merge. The old `Feed`/thread only moved one channel
     into another. Drop `Feed::pump` from this design.
   - `scanning` is a local bool in `run`, set false on `ReplayDone`; it labels a
     subsequent `End` as scan or tail. The old `ScanEnded` encoded the same bit.
   - The defensible concern is a clean source end before the cut; `take_while`
     cannot distinguish it. Unreachable while the seam holds `Arc<dyn Reader>`;
     use a debug/coverage assertion, not production machinery.
   - **Q14 further:** scan and tail termination are equally fatal; the
     `scanning` flag is diagnostic only. `chain(once(...))` cannot detect an
     early clean end because it appends unconditionally. The clean generic is
     `take_while_bound` returning a `Bound` flag; `ReplayDone` carries it and
     `run` checks `bound.reached()`. The stable cut is HLC-based, so an index
     expectation would need a new API. If no bound flag, keep the manual loop
     and delete `take_while`.
   - **Q15:** drop the scan/tail distinction; one generic “live view stream
     ended” with the `End`. The scan is `take_while + filter_map + chain(once
     ReplayDone)`; no Feed/pump/end_with/ScanEnded/scanning. The precise
     early-truncation condition is a clean `Complete` without the predicate
     stopping: a bound flag is the minimal signal. `ReplayDone { bound }` lets
     `run` assert/log it. This is generic, not a fake terminal. If unwanted,
     trust the invariant (seam holds `Arc<dyn Reader>`).
   - **Correction:** my claim that the sentinel is always present was wrong.
     `scan.chain(once(EarlyTruncation)).take_until(cut)` drops the sentinel on
     the normal cut (take_until stops before the scan ends) and lets it through
     only on a clean early end. Use a `ScanItem` wrapper; `take_until` stops
     before the cut item; `Channels::once` supplies the sentinel and
     `ReplayDone`. No bound flag, no Feed/pump, no scanning. `ScanTruncated` is
     the precise alert condition; aborts still propagate.
   - **Q15 final:** no `ScanItem`; the sentinel is
     `Err(zslg::Error::StreamTruncated { what: "live scan ended before the cut" })`.
     `take_until` drops it on the normal cut; a clean early end passes it as
     `ScanFailed`. The LiveSeam concern is settled.
   - **Q16 settled:** no `Fanout` wrapper; all users migrate to the
     `(tx, registry, shutdown)` tuple. `StableLog`, `LocalStore`, replica hold
     the three fields; `nvzn` uses the keyed tuple.
   - **Q17:** names `Registry`/`KeyedRegistry`, constructors `broadcast`/`keyed`.
     I expect the user to accept; then write the design doc.
5. **`Block` vs `Shed` for nvzn.**
   - I expect `Block` to preserve behavior; `Shed` needs a resubscribe story.
6. **`LiveSeam` decomposition.**
   - I expect the user to like `end_with` + `Feed::pump`; then `take_while` has a
     production user and is not deleted.
7. **Shed reason.**
   - A distinct `AbortReason::Lagged`/`Overflow` may be needed for the
     subscriber-side log.

## Communication model

- One question per turn; compact, local, with a recommendation.
- The user is deciding scope and naming; low-level mechanics can be filled in
  later without surprising them.
- Names matter: `StrictlyIncreasing`, `Contiguous`, `OverflowPolicy`,
  `end_with` are the current vocabulary.

## Implementation notes

- The fan-out select bias must put **register before data**; otherwise a
  subscriber enqueued before an element can miss it. The design's “register
  last” was wrong for this order.
- The thread must close and drain the registration queue on exit; otherwise a
  live `Registry` handle can keep a queued subscriber sender alive and the
  subscriber hangs instead of ending.
- `nvzn`’s live-view cancellation is separate from the fan-out cancellation;
  the small `Subscriptions` wrapper is lifecycle, not a registration queue.
- The local-store overrun test was a synchronous-fan-out assumption; the
  deterministic `Shed` test lives in the crate.
