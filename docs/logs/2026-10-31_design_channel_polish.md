# Channel polish — design

Status: awaiting sign-off. Branch `channel-extraction`, base `c188268`.
Process: IAIC. Decision log: `2026-10-31_log_channel_polish.md`.

## 1. Goals

Finish the channel extraction so the transport and routing concerns live in
`crossbeam-combinators` and `nvzn`/`zslg` only state intent:

- every ordered boundary is typed;
- the fan-out is a generic threaded registry with bounded memory, explicit
  overflow policy, and cancellation that reaches a blocked sender;
- `nvzn` states how its items are routed; it does not implement a registration
  queue or a cancellation trick;
- small API cleanups: `Channel`, `empty`, `once`, `take_until`, policy names.

Success: no lost or reordered items on ordered channels; no unbounded buffers;
`zslg` and `nvzn` pass their existing suites; clippy/fmt clean.

## 2. Non-goals

- Tokio in the core. The distribution thread is `std::thread`; Tokio is only the
  optional feature for `into_stream` and the shutdown-token bridge.
- User-defined order policies. The policy set stays closed.
- `MergeN`, fusion, cached `Select`, or the V1a work.
- Gap metrics. They remain removed/deferred.
- `Fanout` wrapper types, `end_with`, `Feed::pump`, `ShutdownSignal`, Condvar,
  or timeouts in the sync API.

## 3. Current state

The previous round extracted `Channel<T>`, `OrderedChannel`, `Fanout`, `Feed`,
`Wakes`, and the `Tail`/`Ranged` seam. The remaining rough edges:

- `LocalStoreView` still returns bare `Channels` for scans that are in fact
  ordered.
- `StableArchiveReader::stream` checks gaplessness and HLC order by hand.
- `Fanout` is shared + `try_send`; `Block` would need a lock/clone-out.
- `nvzn` owns a registration queue, `take_up`, and `_keep_armed`.
- `LiveSeam::range` uses a manual loop, `Feed`, and `ScanEnded`.
- `take_while` has no production user; `Channels` is plural; policy names are
  long.

## 4. Vocabulary

- `Channel<T>` replaces `Channels<T>` everywhere. It is one logical stream even
  when it merges many physical channels. `ChannelIter`, `OrderedChannel`, and
  the combinator names are unchanged.
- Order policies: `Contiguous<K>`, `StrictlyIncreasing<K>`, `NonDecreasing<K>`.
  `Contiguous` is exact succession; `StrictlyIncreasing` allows gaps;
  `NonDecreasing` allows gaps and duplicates.
- `Key<T>`, `Successor`, `Gap`, `stitch` are unchanged except the policy names.
- Fan-out: `broadcast`/`keyed` return `(tx, registry, shutdown)`;
  `Registry`/`KeyedRegistry`; `OverflowPolicy`; `Shutdown`.
- `End` and `AbortReason` are unchanged.
- `take_until`, `once`, `empty` are the new small constructors/combinators.

## 5. Ordered boundaries

### 5.1 Local store

Both scans are in `DataId` order (the local-store key is `(hlc, data_id)`), so
the trait returns ordered channels:

```rust
fn stream_above_hlc(
    &self,
    threshold: Option<Hlc>,
) -> OrderedChannel<Result<Data, Error>, StrictlyIncreasing<DataId>>;

fn stream_window_ordered(
    &self,
    above: Option<Hlc>,
    upto: Hlc,
    max_entries: Option<NonZeroUsize>,
) -> OrderedChannel<Result<WindowItem, Error>, StrictlyIncreasing<DataId>>;
```

`StrictlyIncreasing`, not `Contiguous`: `DataId`s are sparse. `start` is `None`;
the scan range supplies the lower bound. `WindowItem::Complete` and
`TruncatedAt` are no-key items and pass through. `stream_above_hlc` wraps the
already-filtered `Data` channel; `stream_window_ordered` wraps the raw channel.
No error shape changes.

The test doubles update mechanically. A scan that violates the order aborts with
`OrderViolation` instead of silently feeding a snapshot.

### 5.2 Archive stream

`StableArchiveReader::stream` returns a contiguous stream:

```rust
async fn stream(
    &self,
    bounds: ArchiveBounds,
) -> Result<
    OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>>,
    Error,
>;
```

`start = Some(lower_index(bounds.from))`. The per-item `expected` check is
removed. The producer keeps:

- `Placement::Below` skipping, before sending;
- the HLC non-decreasing assertion (index order implies HLC order);
- the end-of-window tiling check (`last_sent + 1 == until`), which is a
  domain completeness check, not an order policy.

A gap becomes `End::Aborted { OrderViolation { what: "archive stream" } }`. The
consumer maps it to `StreamTruncated`; the exact expected/got detail is
deliberately not carried. The tiling failure remains a specific `StableLogGap`
item. Tests that assert `"archive stream: expected index N"` move to the
order-violation/tiling split.

`SnapshotStore::read_snapshot` stays a plain `Channel`; the archive wraps it.
`StableLogSnapshot::range` also stays plain/lax: `HistoricalReplay` must see a
forward gap to stitch it.

### 5.3 Loader: dedup then contiguous

The stable-log loader receives archive entries that may overlap at snapshot
boundaries. It needs “drop duplicates, then require exact succession”:

```text
entries: Channel<Result<SnapshotEntry, Error>>
  .dedup_by_key(global_index)          // consecutive duplicates only
  -> OrderedChannel<_, Contiguous<LogIndex>>::new(_, Some(next_after(frontier)))
```

`dedup_by_key` is a new generic combinator: it drops an item whose key equals
the previous item’s key, passes no-key/error items through, preserves `End`.
Duplicates at snapshot boundaries are consecutive; a non-consecutive duplicate
is a backward index and the `Contiguous` channel aborts.

The loader keeps the HLC non-decreasing check and commits the contiguous prefix
before surfacing an error. A gap or order violation becomes
`StreamTruncated`; the durable gap fence in `append` is unchanged.

The archive starts at `next_after(frontier)`. A **leading** below-start replay
is still dropped (the `append_tolerates_overlapping_reappend` contract); a
backward index at or above the start is an order violation. The old branch
skipped any index below the running expected; that broader tolerance is gone.

### 5.4 Boundary labels

`OrderViolation { what }` must name the boundary, not only the policy. The
ordered constructor takes a label (or a `named` constructor):

```rust
OrderedChannel::new(inner, start, "archive stream")
```

The policy label (`contiguous`, `strictly increasing`, ...) is included in logs
but the `what` identifies the source. This keeps the generic error useful
without typed `expected`/`got`.

### 5.5 `stitch`

`stitch` remains `StrictlyIncreasing<K> -> Contiguous<K>`. It buffers the
gap-triggering item, fills `[expected, got)`, verifies exact coverage, passes
no-key fallback items without advancing coverage, and propagates fallback
aborts. The outer `Contiguous` check remains as deliberate defence-in-depth.
An empty fallback aborts; it never completes.

## 6. Fan-out registry

### 6.1 Interface

```rust
let (tx, registry, shutdown) =
    broadcast(what, data_cap, register_cap, sub_cap, OverflowPolicy::Shed);

let (tx, registry, shutdown) =
    keyed(what, route, data_cap, register_cap, sub_cap, OverflowPolicy::Block);

tx.send(Arc::new(item))?;                 // bounded data queue; blocks
let sub: Channel<Arc<T>> = registry.subscribe()?;      // or subscribe(key)
shutdown.cancel();
shutdown.wait();
```

- `tx`: raw `crossbeam_channel::Sender<Arc<T>>`; no `publish` method. The
  producer owns/wraps the item. `send` blocks when the data queue is full;
  dropping all `tx` clones drains and ends the fan-out.
- `registry`: cloneable `Registry<T>` / `KeyedRegistry<K, T>`. `subscribe`
  creates a bounded `Channel<Arc<T>>`. It `try_send`s into the bounded
  registration queue (`register_cap`, 1024 in `nvzn`) and returns `Full` when
  full. It does not bind a `Cancellation`; a caller can `.until` itself.
- `shutdown`: non-cloneable one-owner `Shutdown`. It owns a cap-1 one-shot
  `Mutex<Option<Sender<()>>>` and the join handle. `cancel(&mut)` takes the
  sender and `try_send(())`; the first call cannot block, later calls no-op.
  `wait(self)` joins. `Drop` cancels but does not join.
- `OverflowPolicy::{Block, Shed}` is per fan-out instance.

### 6.2 Distribution thread

One `std::thread` per fan-out. It owns the data receiver, the registration
receiver, the shutdown receiver, the route closure, and the live subscriber
set.

Phase 1 — receive one element:

```text
Select (biased: cancel, register, data)
  cancel   -> stop
  register -> admit subscriber; continue
  data     -> one Arc<T>; enter phase 2
```

Registration is checked before data so a subscriber enqueued before an element
is live for that element. On exit the thread closes registration under the same
state lock and drains the queue, dropping any subscriber senders still waiting
there; otherwise a registry handle could keep those senders alive after the
thread stops.

Phase 2 — one element to every matching subscriber:

```text
fix the recipient set from the current subscribers
Select(cancel, register, send op for each remaining recipient)
  cancel   -> stop
  register -> admit; starts with the next element
  send     -> op.send(cloned Arc); remove from remaining
```

- The element is one `Arc<T>`; each subscriber gets an `Arc::clone`.
- `Block`: a subscriber whose queue is full has a non-ready send op; the others
  proceed; the element finishes only when all recipients have accepted or
  disconnected. The slowest subscriber paces the element.
- `Shed`: no send select; `try_send` each matching subscriber; on `Full` or
  `Disconnected`, remove it immediately and continue.
- The recipient set is fixed at phase 2 start; a registration during phase 2 is
  admitted but starts with the next element.
- The data queue is bounded; while phase 2 waits, the producer fills it and
  blocks. Memory is bounded by data queue + per-subscriber channel.
- The shutdown receiver is in both selects, so a full subscriber cannot hide
  cancellation. On stop the thread drops subscriber senders; a blocked
  `tx.send` returns once the data receiver drops.
- An IO-stalled consumer owns its own receiver; the thread does not wait for it
  beyond the send/policy. It sees the disconnect when it returns.
- Thread exit: data receiver disconnect -> drain queued elements, then stop;
  registration receiver disconnect -> stop admitting, keep serving; shutdown
  -> stop.

### 6.3 Shutdown and Tokio

No `ShutdownSignal`, no Condvar. One owner. Deliberate shutdown ends subscriber
streams as `Aborted(ProducerGone)`; a caller wanting `Cancelled` binds its own
`.until(&cancellation)`.

Tokio bridge (feature-gated) takes the `Shutdown` by value:

```rust
#[cfg(feature = "tokio")]
pub fn shutdown_on_token(
    mut shutdown: Shutdown,
    token: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        token.cancelled().await;
        shutdown.cancel();
        let _ = tokio::task::spawn_blocking(move || shutdown.wait()).await;
    })
}
```

The task does not complete until the thread has joined. The caller retains or
awaits the task handle. If a component must keep its own `Shutdown` and also
react to a token, it does the token select itself; a signal can be added later.

`tx.send` is blocking: producers are the blocking append/fold paths, never the
Tokio reactor. `registry.subscribe` is `try_send` and safe from async.
`shutdown.cancel` is non-blocking; `wait` is blocking.

### 6.4 Migration

`Fanout<T>` is removed; no wrapper. Each owner stores the three parts.

`zslg`:

- `StableLog`: `events_tx: Sender<Arc<StableLogEvent>>`,
  `events_registry: Registry<StableLogEvent>`, `events_shutdown: Shutdown`.
  `subscribe_events -> Channel<Arc<StableLogEvent>>`; consumers adapt to `Arc`.
  `Shed` policy, as today.
- `LocalStore`: the unstable fan-out is the same triple; `Shed`.
- `ReplicaLogStore`: the live fan-out is the same triple; the flush loop sends
  with `tx`; `ReplicaSeam::subscribe` uses `registry`; `Shed`.
- The historic `pump_range` is unchanged (bounded raw channel + `Closer`).

`nvzn`:

- The fold owns `tx` and `shutdown`; readers hold a cloned `registry`. The
  route key is `(TenantId, ProjectionTypeId)`; the item is the written-ref
  triple (`Arc<(TenantId, ProjectionTypeId, ProjectionId)>`).
- The registration queue, `take_up`, and the fold-side subscriber list are
  removed. A small reader-side `Subscriptions` wrapper remains for the
  **live-view cancellation** (registry + `Cancellation` + an armed
  `CancelHandle`); it is not a registration queue and has no `take_up`.
  `PENDING_SUBSCRIBERS` is the bounded registration queue; `subscribe` maps
  `Full` to `TooManyPendingSubscriptions`, and a `Closed` registry returns a
  channel that ends `ProducerGone`.
- `publish` sends each written ref with `tx.send`; `Block` policy. The fold
  does not drain a registration queue; the reader’s `subscribe` returns a
  `Channel<Arc<...>>`; the reader maps the `ProjectionId` into `Ref<P>`.
- The fan-out shutdown calls `shutdown.cancel()` when the fold stops for
  cancellation; otherwise the fold drops `shutdown`/`tx` for teardown. All
  subscriber streams end `ProducerGone`; `Cancelled` is no longer delivered by
  the fan-out. If a caller needs it, it binds its own cancellation.
- `subscription_shed` and the local-store `unstable_fanout_hangups` are not
  emitted by the new fan-out (the dropped reports were removed); a shed
  subscriber observes `ProducerGone` and logs. If a producer-side metric is
  required, add an observer later. `subscription_reorder` still fires on an
  order violation.

## 7. LiveSeam scan

The cold-start scan is lazy; no `Feed`, no separate thread.

```rust
let scan = reader
    .subscribe_stable_log(from)
    .into_channels()
    .chain(Channel::once(Err(zslg::Error::StreamTruncated {
        what: "live scan ended before the cut".into(),
    })))
    .take_until(|item| matches!(item, Ok(ev) if at_or_after_cut(until, ev)))
    .filter_map(|item| match item {
        Ok(StableLogEvent::Appended { data, .. }) => Some(FoldItem::Scan(data)),
        Ok(StableLogEvent::Watermark(_)) => None,
        Err(e) => Some(FoldItem::ScanFailed(e)),
    })
    .chain(Channel::once(FoldItem::ReplayDone));
```

- Normal cut: `take_until` stops before the cut item; the sentinel is never
  pulled and is dropped. `ReplayDone` marks the phase transition.
- Clean early end: the scan source completes, the chain reaches the synthetic
  `StreamTruncated` error, `take_until` does not match it, and it becomes
  `ScanFailed`; `run` halts.
- Abort: the chain does not advance; the abort propagates through the merge.
- `run` has no `scanning` flag and no `ScanEnded`; it handles `ReplayDone`,
  `ScanFailed`, the items, and a generic `End`. A generic `End` halts with
  `StopReason::LiveStreamEnded`.
- The stable subscription never ends cleanly while the seam holds
  `Arc<dyn Reader>`; the sentinel is defensive. It is a real error, not a fake
  terminal.

`take_until(pred)` stops before the first matching item (consumes/drops it);
it is equivalent to `take_while(|x| !pred(x))`.

## 8. Small APIs and removals

- `Channel<T>` rename; `Channels` disappears.
- `Channel::empty()`: closed, empty, `Complete`.
- `Channel::once(item)`: one item, then `Complete`.
- `Channel::take_until(pred)`: stop before the matching item.
- `take_while` stays (used by `take_until` and available directly).
- `dedup_by_key`.
- Remove `Fanout`; add `broadcast`/`keyed`/`Registry`/`KeyedRegistry`/
  `Shutdown`/`OverflowPolicy`.
- Do not add `end_with`, `Feed::pump`, `ShutdownSignal`, `Bound`, or a
  `take_while_bound` variant.
- Policy renames: `Gapless -> Contiguous`,
  `StrictlyMonotone -> StrictlyIncreasing`; `NonDecreasing` stays.

## 9. Testing

Crate:

- `Channel::empty/once/take_until`, `dedup_by_key`; end preservation and abort
  propagation.
- Ordered policies under the new names; `LocalStore`/archive closures.
- Fan-out: broadcast and keyed routing; every matching subscriber gets every
  element; order; Block slowest-paces; Shed removes a full subscriber; a
  registration during phase 2 starts next element; cancellation while all
  sends are full; shutdown joins; `tx` drop drains and stops; a stalled
  receiver does not block the thread after drop.
- Tokio bridge: token cancels and the task waits for the thread.
- Model/proptest: ordered policies, dedup, routing (each subscriber’s sequence
  is a subsequence of the published sequence).

`zslg`:

- Full lib suite; archive/loader error tests updated to order-violation/
  `StreamTruncated`; `LocalStoreView` typed test doubles; stable log/local
  store/replica fan-out migration; shutdown.
- Existing 652 + 1 ignored; clippy/fmt.

`nvzn`:

- Full lib/integration/doctests; fold publish/subscribe; registration flood
  returns `Full`; shutdown ends subscribers `ProducerGone`; no `_keep_armed`.
- Existing 154 lib + integration; clippy/fmt.

## 10. Implementation order

1. Rename `Channels -> Channel`; mechanical, no behavior. All new code uses the
   new name.
2. Policy renames; `LocalStoreView` typed; archive `Contiguous`; loader
   `dedup_by_key` + `Contiguous`; tests.
3. `Channel::empty/once/take_until`; `take_while` tests; no `end_with`.
4. Fan-out rewrite in the crate: `broadcast`/`keyed`, `Registry`, `Shutdown`,
   `OverflowPolicy`, thread, tests.
5. Migrate `zslg` stable log/local store/replica to the tuple; `Arc` adaptation;
   shutdown; tests.
6. Migrate `nvzn` fold/readers to the keyed tuple; remove
   `Subscribers`/`Subscriptions`/`take_up`/`_keep_armed`; tests.
7. LiveSeam lazy scan; remove manual loop/`Feed`/`ScanEnded`/`scanning`; tests.
8. Docs, post log, clippy/fmt, full matrix.

Steps 1–3 are independent of the fan-out; 4 must precede 5–6; 7 can follow 3.

## 11. Rejected alternatives

- `Fanout` wrapper/core+handle; `Router`/`KeyedRouter` names; one shared
  `Arc` registry.
- `ShutdownSignal`, Condvar, `Block { timeout }`; `ProducerGone` vs
  `Cancelled` for deliberate shutdown.
- `end_with`, `Feed::pump`, `ScanItem`, `ScanTruncated`, `take_while_bound`,
  `Bound`.
- Reusing `Key<T>` for routing (`Ord` vs `Eq`).
- Typed `expected/got` in `OrderViolation`.
- A `Fanout`-per-key map instead of a keyed registry.

## 12. Risks and open points

- Subscriber channels now yield `Arc<T>`; every consumer adapts (clone/deref).
  For `ProjectionId` the `Arc` is overhead but uniform.
- `subscription_shed` and `unstable_fanout_hangups` lose their producer-side
  emission; the subscriber log is the replacement. If operators depend on the
  metrics, add an observer.
- One thread per fan-out; thread lifetime and `Shutdown::Drop` (cancel, no
  join) must be documented and tested. `wait` is the deterministic path.
- `Shutdown` is one-owner; a component needing both token reaction and its own
  wait must select itself or a signal is added later.
- The loader’s below-start replay behavior changes from skip to order
  violation; the archive bound is expected to make it unreachable.
- The archive’s error detail is intentionally generic; tests and logs move to
  boundary labels.
- `OrderedChannel` takes a boundary label; the label API must be settled in
  implementation.
- The data/register/subscriber capacities are constructor parameters; defaults
  for `zslg`/`nvzn` follow existing config values.
- `take_until` must be stop-before-match; tests pin that.
