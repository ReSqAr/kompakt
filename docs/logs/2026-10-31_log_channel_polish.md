# Channel polish — decision log

Process: `/home/agent/nlnr/docs/iaic.md`. Branch `channel-extraction`, base
`c188268`. Append-only: settled questions and decisions, with rejected
alternatives.

## Context

The transport extraction is complete enough that `nvzn` and `zslg` data flow is
expressed through `crossbeam-combinators`. This round polishes the remaining
boundaries:

- typed ordered channels at `LocalStoreView`;
- a typed archive stream;
- `Fanout` vs keyed routing / subscription lifecycle;
- bounded scan termination (`end_with`, `Feed::pump`);
- duplicate dropping in the stable-log loader;
- small API (`empty`, `take_while`).

## Settled before the round

- Policy names: `StrictlyIncreasing<K>`, `NonDecreasing<K>`, `Contiguous<K>`.
- `LocalStoreView` streams: `StrictlyIncreasing<DataId>` (not contiguous;
  `DataId`s are sparse).
- Archive stream: candidate `Contiguous<LogIndex>`; a generic `OrderViolation`
  is acceptable; the boundary name goes in `what`, not the policy label.
- `dedup_by_key` is the duplicate-dropping primitive; `Contiguous` remains the
  exact-succession policy.
- `Channels::empty()` for a closed empty stream.
- `OverflowPolicy { Block, Shed }`.
- The keyed fan-out will not return a report; a shed subscriber observes its own
  end.
- `end_with` is proposed as `FnOnce(End) -> T`; success-only would be
  `chain(once(...))`, which does not keep a scan abort out of the merge.

## Q1 — Router or registry?

Status: open.

Should the crate expose only a keyed routing primitive (`KeyedFanout<K, T>`:
register/publish), or a `SubscriptionRegistry` that also owns pending
registration, take-up, and cancellation/abort?

Options:

1. **Router only.** Keep `Subscribers`/`Subscriptions` and `_keep_armed` in
   `nvzn`; the crate owns only the live keyed sender list.
2. **Registry.** Move the lifecycle into the crate; the fold keeps only *when*
   to take up and whether to cancel or abort.
3. **Router first, registry later.** Ship the small primitive now, revisit the
   lifecycle once the router has settled.

Rejected/held:

- (none yet)

## Q1 — Settled

Decision: the registry lives in `crossbeam-combinators`.

Rationale: routing, registration, take-up, and cancellation are a generic
problem; `nvzn` should state the routing intent, not implement the lifecycle.
Router-only was rejected because it leaves the pending queue and the
`_keep_armed` trick in the domain.

Consequences:

- `Subscribers`/`Subscriptions` collapse into the registry.
- The fold still decides *when* to `take_up` and whether to `cancel` or
  `abort`.
- `PENDING_SUBSCRIBERS` becomes a registry parameter.

## Q2 — How is a route expressed?

Status: open.

How does a publisher tell the registry where an item belongs?

Options:

1. **Key at publish.** `publish(&[(K, T)])`: the producer computes the key.
   Explicit, no trait, but the caller keeps the routing knowledge.
2. **Route closure.** `Registry::new(|item: &T| -> Option<K>)`;
   `publish(&[T])`: the intent is stated once at construction; the registry owns
   keying.
3. **Route trait.** `T: Route<K>`; `publish(&[T])`: the item carries its route;
   type-level and local, but needs an impl/wrapper and permits one route per
   item type.

Do not reuse `Key<T>`: routing needs `Eq`, not `Ord`, and `None` means “not
routed”, not “not ordered”.

Recommendation: (2) for now. It matches “nvzn states the intent” with the least
new vocabulary; (3) can be added later as a typed adapter without changing the
routing core.

## Q2 — Settled

Decision: the route is a constructor closure:

```rust
Registry::new(|item: &T| -> Option<K>, ...)
publish(&[T])
```

The intent is stated once; the registry owns keying. Rejected: key at every
publish (loses “state once”), reusing `Key<T>` (`Ord` is the wrong bound and
“ordered” is the wrong meaning), and a `Route<K>` trait for now (defer as an
adapter).

## Q3 — Registry shape: one shared object or core + handle?

Status: open.

The registry is used from two sides: readers subscribe, the fold publishes and
ends the streams.

1. **One shared object.** `Arc<SubscriptionRegistry>`; `subscribe`, `take_up`,
   `publish`, `cancel` are all `&self` with internal locks. `nvzn` holds one
   field everywhere. Cost: the fold's publish takes locks; `Block` must clone
   senders out of the lock; the `&mut` fold path is gone.
2. **Core + handle.** Fold-owned `SubscriptionRegistry` (`&mut`:
   `take_up`/`publish`/`cancel`/`abort`) plus a cloneable `SubscriptionHandle`
   (`subscribe`, holds the `Cancellation` and the armed handle) for readers.
   The pending queue is internal. This packages today's
   `Subscriptions`/`Subscribers` split, keeps the fold path lock-free, and makes
   `_keep_armed` an internal detail.

Recommendation: (2). The fold is the only publisher and the reader only
registers; a shared `Arc` buys one field at the cost of locks and a clone-out
publish.

## Q3 — Settled

Decision: core + handle.

- Fold-owned `SubscriptionRegistry` with `&mut` `take_up`/`publish`/`cancel`/
  `abort`.
- Cloneable `SubscriptionHandle` for readers, holding the `Cancellation` and the
  armed handle.
- Pending queue internal; `PENDING_SUBSCRIBERS` a constructor parameter.
- `cancel()` fires the handle; `abort()` clears the senders while keeping the
  handle armed.

## Q4 — Naming and the live routing core

Status: open, under exploration.

`Router` is dropped. The open question is whether the live routing core is one
unified `Fanout<K, T>` (`K = ()` for broadcast) or a separate
`Fanout<T>`/`KeyedFanout<K, T>` pair. The user's instinct is to unify; the
concern is the `Block` path on a shared `&self` fanout:

- `Shed` can hold the subscriber-list lock across `try_send`.
- `Block` cannot hold it across `send`; it must snapshot the matching senders,
  send without the lock, then prune disconnected senders by `same_channel`.
- A unified type therefore has a more complex `Block` branch and a weaker
  concurrent-publisher serialization contract; the fold's keyed path also pays
  an uncontended mutex.
- The gain is one type, one eviction implementation, one `OverflowPolicy`, and
  no separate `KeyedFanout`/`Router`.

No decision yet.

### Q4 — Direction: the command merge actor

- The actor’s inbox is a merge of command channels, mapped first:

  ```text
  cancel    ──map(Cancel)──┐
  register  ──map(Register)├── merge ──> distribution thread
  publish   ──map(Publish)─┘
  ```

  This is exactly `Channels::merge`; the thread is the sole consumer.
- `merge` is fair, not biased: crossbeam selects among ready leaves at random.
  Cancellation is a flag/stop checked each iteration; a strict bias is only
  needed if a ready data source could starve it, which fair selection avoids.
- `Block { timeout }` is rejected. In the sync world, either fail immediately
  (`try_send`) or block unbounded (`send`); a timeout belongs to an async caller
  with a `oneshot` ack, not to the sync API.
- `Channels` → `Channel` is a candidate rename: the value is one logical stream
  even when it merges many physical channels.
- A distribution thread changes “nothing here spawns”: the fan-out gains a
  thread, a join handle, a failure domain, and a per-fan-out resource. Dropping
  all command senders ends the merged inbox and the thread; a blocking
  subscriber send delays that exit.
- Caveat: `merge` is for the inbox (receives). The subscriber sends are a
  `Select` over send operations, not a merge. A strict no-head-of-line `Block`
  needs the event loop; the simple actor uses `try_send`/shed on `Publish`.

Open: sync non-blocking vs unbounded `Block`; `Channel` rename; event loop.

## Q5 — Sync actor: non-blocking or unbounded `Block`?

Status: open.

The sync world has no timeout. If the distribution thread may block on a
subscriber, `Block` is unbounded and shutdown/join can wait behind it. If the
sync actor is non-blocking, `Publish` uses `try_send` and either sheds or
reports `Full`; waiting belongs to an async caller with a `oneshot` ack.

Options:

1. **Sync non-blocking.** `Publish` is `try_send`; `Shed`/`Full`; async ack owns
   waiting. Clean shutdown; the fold needs an async/retry path or a separate
   sync core for `Block`.
2. **Sync may block.** `Publish` is `send`; `Block` is unbounded; no timeout. A
   slow subscriber can delay shutdown and all other subscribers.
3. **Event loop.** The thread selects over subscriber sends and commands; no
   head-of-line, but it is a scheduler with per-subscriber pending buffers.

No decision yet.

## Q5 — Settled

- The command inbox is **unbounded**: few commands are expected; it is one of
  the few deliberately unbounded queues.
- The sync distribution thread may use unbounded `Block` for now.
- Async acks/timeouts exist only under the `tokio` feature; the core stays sync.
- `Channels` → `Channel` is accepted as an early mechanical commit, before the
  behaviour changes.

Open consequence: `Block` plus an unbounded inbox means a stalled subscriber
grows memory instead of backpressuring the producer. Accepted for now; revisit
if `Publish` batches can pile up under a stalled subscriber.

## Q6 — Does cancellation drop the receiver?

Status: open.

A blocked `Sender::send` is released when the last receiver drops: crossbeam
returns `SendError`. The question is whether cancellation actually drops the
receiver.

Today it does not, directly: `Guarded` only observes the cancellation through
its guard leaf on the next `recv`; the inner source — and therefore the data
receiver — stays alive until the consumer drops the `Channels`. A consumer
parked in `recv` wakes, gets `Cancelled`, and drops promptly; a consumer stuck
elsewhere can leave the distribution thread blocked.

Candidate fix: when `Guarded` observes that the cancellation fired, drop the
inner source. Then the receiver is released as soon as the consumer polls the
cancel, even if it keeps the `Channels` object. Test: fill a subscriber queue,
block the producer, fire cancel, assert the send returns and the thread joins.

Remaining edge: a consumer that never polls again cannot be forced; that is a
consumer bug, and the contract must say the consumer owns the drop.

### Q6 — Direction: producer-side cancellation

The consumer-side drop cannot be forced: crossbeam disconnects only when the
last receiver drops, and the consumer owns it. `Guarded` dropping its inner
source is best-effort cleanup on the next poll.

The real fix is on the producer side. The distribution thread must never park in
`Sender::send`; it selects over send operations and a cancellation receiver:

```rust
let mut sel = Select::new_biased();   // cancellation first
let cancel_op = sel.recv(&cancel_rx);
let send_op = sel.send(&subscriber_tx); // only if there is a pending item
match sel.select() {
    op if op.index() == cancel_op => stop(),
    op => op.send(&subscriber_tx, item),
}
```

`Select::send` is not ready while the subscriber queue is full, but the
cancellation receiver is. A biased select makes cancellation win even when a
data operation is ready. On cancel the thread exits and drops its senders; the
consumer's channel disconnects even if the consumer is stuck in IO.

So: the IO-stalled consumer can hold its own receiver, but it cannot hold the
producer or the distribution thread. `Guarded` drop-inner is optional cleanup,
not the guarantee.

### Q7 — Control queue bounded?

The control queue can be bounded at 1024; `subscribe`/`register` uses
`try_send` and returns `Full` when the queue is full. The data/publish queue is
separate and the distribution loop selects over both.

No decision yet on the data queue capacity; the event loop drains it even when
one subscriber is full, until that subscriber's pending buffer is bounded.

## Q7 — Inputs and the select merge

Sources are separate channels, not one `Channels` merge:

```text
 register_rx : bounded(1024), (K, Sender<T>)     reader try_send -> Full
 fold_rx     : unbounded, Fold<T>                TakeUp | Publish(Arc<[T]>)
 cancel_rx   : CancelHandle receiver
 sends       : one Select send op per subscriber with a pending item
```

`Select` is the merge. `Channels::merge` is receive-only and hides its leaves;
it cannot select *send* operations, which is the whole point here. Use
`crossbeam_channel::Select` over the raw receivers and senders.

`TakeUp` and `Publish` share one ordered queue: the fold sends `TakeUp` after a
commit, then `Publish`; the thread must not route before the registration is
live. The register queue is separate and bounded; a subscribe flood returns
`Full`.

Thread state:

```rust
struct Subscriber<K, T> {
    key: K,
    tx: Sender<T>,
    pending: VecDeque<T>,
}
```

On `TakeUp`/`Publish`, drain `register_rx` into live, then route the batch into
per-subscriber `pending`. The select registers a send op only for subscribers
with a pending item; `SelectedOperation::send` consumes it.

Bias order: cancel first; then sends (drain); then fold (take-up/publish); then
register (admission). Round-robin the send insertion order so the first
subscriber cannot monopolise a biased select. This is the order to validate.

Open: per-subscriber pending bound and `Block`/`Shed` behaviour when full; fold
queue boundedness.

### Q7 — Correction

The sketch had a real bug and the wrong defaults:

- The data/fold queue is **bounded**; `publish` blocks when it is full.
- Per-subscriber `pending` is **bounded**. It is not an unbounded overflow
  buffer; when it is full, the loop stops reading data, so the bounded data
  queue backpressures the producer.
- Do **not** pop pending items before selection. The earlier `outbox` moved one
  item per subscriber out of `pending`, then sent only one and dropped the rest.
  Correct shape: register send ops for subscribers whose `pending` is non-empty;
  after `select`, pop only the selected subscriber's item and send it. The
  unselected pending queues are untouched.
- A fair `Select` is enough; round-robin is unnecessary because the loop is
  sequential. A subscriber whose pending is empty is simply not registered.
- Naming: `data_rx` / `Data::{TakeUp, Publish}` instead of `fold_*`;
  `register_rx`, `cancel_rx`, `send_ops`.

Semantics: route every item to **every matching subscriber** (broadcast is
`K = ()`; keyed is the matching subset). Each matching subscriber gets its own
copy in its own FIFO `pending`. The send loop drains one item per ready
subscriber per iteration; it never sends one item to only one subscriber.

Requirements and mechanisms:

| requirement | mechanism |
|---|---|
| every matching subscriber gets every item | route pushes a copy into each matching `pending` |
| per-subscriber order | single thread, FIFO `pending`, FIFO data queue |
| bounded memory | bounded data queue + bounded `pending` + bounded subscriber channel |
| backpressure | if a `pending` is full, stop reading data; the data queue fills; `publish` blocks |
| cancellation while all subscribers are full | `Select` includes the cancel receiver; send ops are not ready, cancel is |
| shutdown | drop data/register/cancel senders; the thread's `select` returns and it exits |

Still open: the `pending` bound (1 vs channel capacity), and whether a full
keyed subscriber stops the whole data stream or only its own key.

### Q8 — Simplified two-phase loop

The user’s simplification: one element at a time, two phases, and the slowest
subscriber sets the pace.

- **Phase 1 (receive):** `Select` between `cancel_rx` and `data_rx`; take one
  `Arc<T>`.
- **Phase 2 (send):** drain `register_rx` into `subscribers` (this is what
  `take_up` meant), then send the one element to every matching subscriber,
  sequentially. A blocking send waits for that subscriber; a shed send drops a
  full subscriber.

Consequences:

- No per-subscriber `pending` queue. The one in-flight element is the bound-1
  buffer. The slowest subscriber paces the broadcast.
- No `outbox`, `send_ops`, `can_route`, or `live` terminology. The subscriber
  set is `subscribers`; the element is local to the loop.
- No separate `take_up` command: registrations are admitted at the start of the
  send phase. A subscriber registered mid-send starts with the next element.
- `Arc<T>` is the in-flight element; each subscriber gets an `Arc` clone. The
  subscriber stream carries `Arc<T>`.
- The data queue is bounded. While phase 2 waits on a slow subscriber, the
  producer fills it and blocks. That is the backpressure.
- `cancel_rx` is still selected in both phases, so cancellation is observed even
  while a send is waiting.
- Broadcast is a separate constructor (`Fanout::broadcast`), not a route closure
  with `K = ()`; keyed is `Fanout::keyed(route, ...)`.
- The thread inputs are raw crossbeam receivers because `Select` needs them; the
  public handle wraps them. `Channel<T>` remains the pull-side subscriber output.
- `CancelHandle` is the sync cancellation; `Cancellation::from_token` (tokio
  feature) is the async bridge.

Open: `Shed` behaviour in phase 2 and the data queue capacity.

### Q9 — Register is a select source; parallel send phase

- `take_up` is removed. The old code drained the registration queue after a
  commit because the fold was the only publisher. The threaded registry selects
  `register_rx` directly; a subscriber is admitted as soon as the thread sees
  the command. There is no commit coupling.
- `register_rx` is a select source in **both** phases, so the thread never sits
  waiting for data while a subscriber waits to be admitted.
- Phase 2 is not sequential. The element’s recipient set is fixed when phase 2
  starts; all matching send operations are registered in one `Select` together
  with `cancel_rx` and `register_rx`. Ready sends complete; the slowest paces
  the element, but cancel and registration are still serviced.
- The subscriber registered during phase 2 is admitted immediately but is not
  part of the current element’s fixed recipient set; it starts with the next
  element. (Recommended; still open.)
- Broadcast has no `()` parameter: a separate broadcast type/constructor with
  `subscribe()`, while the keyed form has `subscribe(key)`.
- `Shed`: `try_send` in phase 2; on `Full` or `Disconnected`, remove the
  subscriber immediately and continue with the other recipients.

No `pending` queue, `outbox`, `can_route`, or `take_up` remains.

## Q9 — Settled

- `register_rx` is a select source in both phases; no `take_up`/commit coupling.
- Phase 2 fixes the recipient set, registers all matching sends in one `Select`
  with `cancel_rx` and `register_rx`; ready sends complete; the slowest paces the
  element.
- A subscriber admitted during phase 2 starts with the next element.
- Broadcast has a separate type/constructor with `subscribe()`; no unit key in
  the API.
- `Shed` uses `try_send` and removes a full/disconnected subscriber immediately.
- `Arc<T>` is the one in-flight element; each send clones the `Arc`.

## Q10 — Does the core + handle split survive the thread?

Status: open.

With the distribution thread owning the subscriber set and queues, the original
reason for a fold-owned `&mut` core disappears. The public side can be one
cloneable handle held by both the fold and readers:

```text
Fanout<T>            cloneable: publish / subscribe / cancel / abort
KeyedFanout<K, T>    same, with a route
```

The last handle drop stops the thread; an explicit `cancel()` or `abort()`
stops it earlier. The alternative is to keep `Registry` + `RegistryHandle` as a
deliberate lifecycle split.

Recommendation: collapse to one cloneable handle. It removes a type pair and
matches “the thread owns the mutable state”.

## Q10 — Settled: `(tx, registry, cancel)`

The construction returns the three sides of the same fan-out:

```rust
let (tx, registry, cancel) = broadcast(data_cap, register_cap, sub_cap);
let (tx, registry, cancel) = keyed(route, data_cap, register_cap, sub_cap);
```

- `tx`: raw `crossbeam_channel::Sender<Arc<T>>`, bounded data queue. The
  producer sends elements directly; there is no `publish` method. `send` blocks
  when the queue is full; dropping all `tx` clones drains and ends the fan-out.
- `registry`: cloneable subscribe side; `subscribe()` for broadcast,
  `subscribe(key)` for keyed. It `try_send`s into the bounded register queue and
  returns `Full` when full. It returns a `Channel<Arc<T>>` already bound to the
  fan-out cancellation.
- `cancel`: cloneable `CancelHandle`; `cancel()` stops the fan-out and fires the
  cancellation. Dropping the last handle unfired is teardown
  (`CancelHandleDropped`).
- `CancelHandle` becomes `Clone` (share the sender in an `Arc<Mutex<Option<_>>>`);
  `Cancellation` is already `Clone`.
- The thread is the hidden fourth side and owns the receivers and subscriber
  set:
  - data receiver disconnects → drain queued elements, then stop;
  - register receiver disconnects → stop admitting, keep serving existing
    subscribers;
  - cancel receiver disconnects or `cancel()` → stop.

No `publish`, no core/handle pair.

## Q11 — Does `cancel` also join?

Status: open.

The tuple has no explicit thread owner. Is `cancel` only the signal, leaving the
thread detached, or should it offer `wait()`/join for deterministic shutdown?
A fanout-specific `Shutdown` handle wrapping `CancelHandle` + the join handle
would keep `CancelHandle` general.

Recommendation: add `wait()` to the fan-out cancel handle; detached shutdown is
hard to test and to reason about at process teardown.

## Q11 — Settled: a fan-out `Shutdown`

- The third tuple element is a fan-out-specific `Shutdown`, cloneable, **not** a
  wrapper around `CancelHandle`. The general cancellation type is untouched.
- It owns a cap-1 one-shot signal:

  ```rust
  signal: Mutex<Option<Sender<()>>>,
  join:   Mutex<Option<JoinHandle<()>>>,
  ```

  `cancel()` takes the sender and `try_send(())`; the first call succeeds
  without blocking, later calls see `None` and are no-ops. If the receiver is
  gone, the failed send drops the sender and disconnects, which also stops the
  thread.
- `wait()` joins the thread. It is blocking; under the `tokio` feature it can be
  wrapped as `wait_async` via `spawn_blocking`. Only one waiter takes the join
  handle; a second call returns immediately (or the type can add a condvar if
  multiple waiters matter).
- Subscriber streams are not bound to a cancellation. The thread drops their
  senders on shutdown, so they end `Aborted(ProducerGone)`. A caller that wants
  `Cancelled` can bind `.until(&cancellation)` on the returned `Channel`
  itself.
- The thread is a plain `std::thread`, independent of Tokio. `cancel()` is
  non-blocking and safe from async; `wait()` is blocking. `tx.send` is blocking
  and must not run on the reactor; `registry.subscribe` is `try_send` and safe.
- A `CancellationToken` can be bridged by a task that awaits it and calls
  `shutdown.cancel()`; the fan-out itself does not depend on Tokio.

Open: the name (`Shutdown`/`Stop`/`Cancel`) and whether shutdown should end
subscribers as `ProducerGone` or `Cancelled`.

## Q12 — Ownership of `Shutdown`; token bridge

- A deliberate shutdown ends subscribers as `ProducerGone` (confirmed).
- `Shutdown` is **not cloneable**. It is the owner’s lifecycle handle:
  `cancel(&mut self)`, `wait(self)` (or `&mut`), one join handle, one waiter.
  Drop cancels/stops but does not join; call `wait` for determinism.
- A cloneable `ShutdownSignal` shares the one-shot `Mutex<Option<Sender<()>>>`
  but not the join handle. `Shutdown::signal()` returns one for external
  cancellation, including the Tokio bridge. Dropping a signal does not stop the
  fan-out.
- `tx` is the raw crossbeam data sender, not a Tokio channel. In `zslg`/`nvzn`
  the producer is the blocking append/fold path, so `tx.send` never runs on the
  reactor. The warning only applies to an async caller that holds `tx`.
- Tokio helper (feature-gated) takes a `ShutdownSignal` and a
  `CancellationToken`, spawns one task that awaits the token and calls
  `signal.cancel()`.
- The std thread is independent of Tokio. Dropping the owner `Shutdown` (or
  `tx`) disconnects a channel and stops the thread; a leaked owner/signal keeps
  it alive. Explicit `cancel`/`wait` is the contract.
- What `zslg`/`nvzn` need: one owner each — `StableLog`, `LocalStore`, and the
  fold — all of which can cancel from `Drop` or `&mut` stop. Readers only need
  the registry; they do not need the shutdown handle. So non-cloneable
  `Shutdown` + optional `ShutdownSignal` fits.

Open: confirm the `Shutdown` + `ShutdownSignal` split, or make `Shutdown`
cloneable with a Condvar and one type.

## Q12 — Settled: one owner, no signal for now

- No `ShutdownSignal` and no Condvar. `Shutdown` is non-cloneable:
  `cancel(&mut self)`, `wait(self)`, `Drop` cancels but does not join.
- `zslg`/`nvzn` each have one owner (`StableLog`, `LocalStore`, the fold).
  Readers only need the registry; they never stop or join the fan-out.
- A deliberate shutdown ends subscribers as `ProducerGone` (confirmed).
- The data `tx` is raw crossbeam and the producers are blocking, so no Tokio
  data path.
- The Tokio bridge takes `Shutdown` **by value** (handoff), cancels on token,
  then waits in `spawn_blocking` so the Tokio task does not complete before the
  fan-out thread has joined:

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

  The caller awaits/retains the returned task handle. If a component must keep
  its own `Shutdown` while also reacting to a token, it does the token select
  itself and calls `cancel`/`wait`; a cloneable signal can be added later if
  that becomes common.

Open: confirm the bridge-owns-`Shutdown` model, or keep the owner and add the
signal now.

## Q13 — Settled: fan-out model

The fan-out concern is settled:

- construction returns `(tx, registry, shutdown)`;
- `tx` is a raw bounded `Sender<Arc<T>>`; no `publish`;
- `registry` is the cloneable subscribe side; bounded registration with `Full`;
- `shutdown` is a non-cloneable one-owner `Shutdown` with a one-shot `cancel`
  and a joining `wait`;
- one std distribution thread; two phases; `Select` over cancel, data,
  registration, and per-element sends; `Block`/`Shed`;
- a subscriber admitted during phase 2 starts with the next element;
- deliberate shutdown ends subscribers as `ProducerGone`;
- Tokio bridge handoff uses `shutdown_on_token` and waits on the blocking pool.

## Q14 — `LiveSeam::range`: adopt `end_with` + `Feed::pump`?

Status: open.

The composition is:

```rust
scan
    .take_while(|ev| before(until, ev))
    .filter_map(|ev| match ev {
        Ok(StableLogEvent::Appended { data, .. }) => Some(FoldItem::Scan(data)),
        Ok(StableLogEvent::Watermark(_)) => None,
        Err(e) => Some(FoldItem::ScanFailed(e)),
    })
    .end_with(|end| match end {
        End::Complete => FoldItem::ReplayDone,
        end => FoldItem::ScanEnded(end),
    })
```

then `Feed::pump` sends it into the scan feed. `end_with` is what keeps a scan
abort in-band, so the range branch ends cleanly and the merged tail survives.

The one caveat: `take_while` reports `Complete` both at the cut and if the
source ends cleanly first. In this seam the source is the stable subscription,
and `LiveSeam` holds the `Arc<dyn Reader>` for the scan’s lifetime, so the
resumable tail cannot start with a declined factory; `Complete` is the cut. The
old `ScanEnded(Complete)` branch is defensive, not reachable. A debug assertion
can keep the guard if desired.

If adopted, `take_while` has a production user and stays. If not, it is deleted
and the manual loop stays.

### Q14 — Correction: do not suppress an abort

The earlier `end_with` proposal was wrong. A scan abort is a fault; the view
halts. Converting it into an in-band `ScanEnded` item and ending the range branch
`Complete` only keeps the merged tail alive, which is pointless once the scan
failed, and it makes the channel verdict claim a clean phase end. The user’s
instinct is right: do not suppress it.

Goal:

- Normal scan completion (`Complete` at the cut) is a phase transition. Emit
  `ReplayDone` so `run` can mark coverage complete; the live tail continues.
- A scan abort propagates. `chain` does not advance, the feed aborts, the merge
  aborts, and `run` halts the view. No `ScanEnded`, no `end_with`.
- `run` distinguishes scan vs tail by a local `scanning` flag: it starts true,
  clears on `ReplayDone`, and on `Err(end)` reports `LiveScanEnded` while
  scanning, otherwise the tail stop.
- A scan error item (`Err(e)`) becomes `ScanFailed(e)` and halts the view.
- `take_while` is the normal cut. `chain(Channels::once(ReplayDone))` is the
  phase marker. `Feed::pump` is the blocking bridge: forward items, close on
  `Complete`, abort on `Aborted`, stop when the consumer drops. It is not an
  error suppressor.

The only ambiguity left is a clean source end before the cut. In this seam the
stable subscription cannot start with a declined factory because `LiveSeam`
holds `Arc<dyn Reader>` for the scan; `Complete` is the cut. If defense is
wanted, assert it in the domain rather than converting the abort.

Open: confirm this simpler shape (no `end_with`/`ScanEnded`).

### Q14 — Further simplification: no `Feed::pump`

The scan is already a lazy `Channels` (`StableStream::into_channels`). `range`
can return the composed scan directly:

```rust
fn range(&self, cut: Option<Hlc>) -> Channels<FoldItem> {
    let Some(until) = cut else { return Channels::empty(); };
    self.reader.subscribe_stable_log(self.from)
        .into_channels()
        .until(&self.cancelled)
        .take_while(|ev| before(until, ev))
        .filter_map(/* Scan / ScanFailed */)
        .chain(Channels::once(FoldItem::ReplayDone))
}
```

`begin` already spawns `run` on a blocking thread and merges `range` with the
tail; `run` pulls both. The old `Feed` + `spawn_blocking` + `pump` only moved
one channel into another; it was a workaround for the manual loop, not a
concurrency win. Drop `Feed::pump` from this design.

The `scanning` flag is local to `run`:

```text
scan items ... ReplayDone ... tail items ... Err(end)   scanning=false -> tail
scan items ... Err(end)                                scanning=true  -> scan
```

The merged stream has one `End`; the flag says which phase was active when it
arrived. The old `ScanEnded` item was an in-band encoding of the same bit.

Defense concern: `take_while` ends `Complete` both at the cut and on a clean
source end before it. If the latter happened, `ReplayDone` would falsely mark
the scan complete and the view could serve a stale world. The seam holds
`Arc<dyn Reader>`, so the stable subscription cannot start with a declined
factory; this is unreachable. If wanted, add a domain `debug_assert`/coverage
check; do not add an in-band fake terminal.

### Q14 — Early truncation and the bound flag

- Scan and tail termination are equally unrecoverable for the live view. The
  `scanning` flag is only a stop-reason/log/metric distinction; drop it if that
  distinction is not operationally useful.
- The old loop was the scan state machine: it saw the cut-causing item, tracked
  the last position, and emitted `ReplayDone`/`ScanEnded`. The `Feed`/thread
  only exposed that loop as a `Channels` for the merge; it was not a concurrency
  need.
- `scan.chain(once(EarlyTruncation))` cannot detect an early return: `chain`
  appends the marker after any clean end, whether it reached the cut or not.
  You need the stop cause.
- The clean generic is a bound flag from `take_while`:

  ```rust
  let (scan, bound) = scan.take_while_bound(before);
  let scan = scan
      .filter_map(/* Scan / ScanFailed */)
      .chain(Channels::once(FoldItem::ReplayDone { bound }));
  ```

  `bound.reached()` is true only when the predicate stopped the stream. On a
  clean early end it is false; `run` halts on `ReplayDone { bound }` instead of
  marking coverage complete. On an abort, no `ReplayDone` is emitted; the abort
  propagates.
- The stable stream is `Contiguous<LogIndex>`, but the LiveSeam cut is an HLC;
  the last index at the watermark is not exposed. The bound flag is the generic
  “cut reached” signal. An index expectation would need an
  `end_index_at(watermark)` API; not worth it.
- If the bound flag is rejected, keep the manual loop (which naturally knows
  the stop cause) and delete `take_while`. Otherwise `take_while_bound` keeps
  `take_while` used.

Open: `take_while_bound` + `Bound`, or keep the loop.

### Q15 — Drop the scan/tail distinction; keep the generic shape

- Scan and tail termination are equally fatal; the `End` label already says
  which source ended. Drop `scanning`, `LiveScanEnded` vs tail, and
  `ScanEnded`. `run` reports one “live view stream ended” with the `End`.
- Then the scan is pure combinators:

  ```rust
  scan.take_while(before_cut)
      .filter_map(/* Scan / ScanFailed */)
      .chain(Channels::once(FoldItem::ReplayDone))
  ```

  `range` returns that lazily; `run` pulls it through the merge. No `Feed`, no
  `pump`, no `end_with`, no `ScanEnded`, no `scanning`.
- `ReplayDone` still marks the normal scan-to-tail transition for coverage.
- The precise early-truncation condition is: **the scan ends `Complete` without
  the predicate having stopped it**. That is a clean end before the cut, not an
  abort and not the tail. It is an invariant violation (the stable subscription
  is infinite while the seam holds `Arc<dyn Reader>`), and without a guard it
  is silent.
- `chain(once(EarlyTruncation))` cannot detect it: the marker is appended after
  any clean end and is always there. A bound flag is the minimal signal:
  `take_while_bound(pred) -> (Channels<T>, Bound)`; `Bound::reached()` is true
  only when the predicate stopped the stream. `ReplayDone { bound }` carries it
  to `run`, which can `debug_assert`/log if `!reached`. This does not
  reintroduce a fake terminal; it is a generic observation.
- If the bound API is not wanted, trust the invariant and omit the check. The
  generic composition is unchanged.

Open: add `take_while_bound`/`Bound` for the early-truncation check, or trust
the invariant.

### Q15 — Correction: `take_until` makes the sentinel conditional

My claim was wrong. With `take_until` **outside** the chain, the sentinel is not
always present:

```text
scan -> chain(once(EarlyTruncation)) -> take_until(cut) -> ...
```

- Normal cut: `take_until` stops at the cut item before the scan source ends.
  The chain never advances to `EarlyTruncation`; the marker is dropped.
- Clean early end: the scan source ends, the chain advances, the marker reaches
  `take_until`. The cut predicate never matched, so the marker passes.
- Abort: the chain does not advance; the abort propagates; no marker.

So the marker is a real early-truncation signal, not a fake terminal. The
precise condition to alert on is: **the stable scan completes before
`take_until` sees the cut item.** That is the invariant violation; a normal cut
drops the marker, and an abort is a separate propagated error.

The domain needs a wrapper because the marker is not a `StableLogEvent`:

```rust
enum ScanItem {
    Event(Result<StableLogEvent, Error>),
    EarlyTruncation,
}

scan.map(ScanItem::Event)
    .chain(Channels::once(ScanItem::EarlyTruncation))
    .take_until(|item| match item {
        ScanItem::Event(Ok(ev)) => at_or_after_cut(until, ev),
        _ => false,
    })
    .filter_map(|item| match item {
        ScanItem::Event(Ok(StableLogEvent::Appended { data, .. })) => Some(FoldItem::Scan(data)),
        ScanItem::Event(Ok(StableLogEvent::Watermark(_))) => None,
        ScanItem::Event(Err(e)) => Some(FoldItem::ScanFailed(e)),
        ScanItem::EarlyTruncation => Some(FoldItem::ScanTruncated),
    })
    .chain(Channels::once(FoldItem::ReplayDone))
```

`take_until` stops before the matching item (it can be `take_while(!pred)`), so
the cut item and everything after it — including the sentinel — are dropped on
the normal path. `Channels::once` supplies both the sentinel and `ReplayDone`.

This uses only generic combinators and removes the bound flag, `Feed::pump`,
`end_with`, `scanning`, and `ScanEnded`. `ScanTruncated` is emitted only on a
clean early end; aborts still propagate.

### Q15 — Final: the sentinel is an error

Drop `ScanItem` and `ScanTruncated`. The sentinel is a synthetic error of the
scan’s own type:

```rust
scan
    .chain(Channels::once(Err(zslg::Error::StreamTruncated {
        what: "live scan ended before the cut".into(),
    })))
    .take_until(|item| matches!(item, Ok(ev) if at_or_after_cut(until, ev)))
    .filter_map(|item| match item {
        Ok(StableLogEvent::Appended { data, .. }) => Some(FoldItem::Scan(data)),
        Ok(StableLogEvent::Watermark(_)) => None,
        Err(e) => Some(FoldItem::ScanFailed(e)),
    })
    .chain(Channels::once(FoldItem::ReplayDone))
```

Normal cut drops the sentinel; a clean early end passes it as `ScanFailed`; an
abort propagates. The precise alert condition is a `StreamTruncated` scan error
with that message.

## Q16 — Settled: migrate everything to the tuple

No `Fanout` wrapper. `StableLog`, `LocalStore`, and the replica log each hold
`tx`, `registry`, `shutdown` explicitly; `nvzn` uses the keyed tuple. `send` is
`tx.send(Arc::new(item))`; `subscribe` is `registry.subscribe()` or
`subscribe(key)`; `Drop` calls `shutdown.cancel`.

## Q17 — Names of the tuple parts and constructors

Status: open.

Recommended:

```text
broadcast(data_cap, register_cap, sub_cap)
    -> (Sender<Arc<T>>, Registry<T>, Shutdown)

keyed(route, data_cap, register_cap, sub_cap)
    -> (Sender<Arc<T>>, KeyedRegistry<K, T>, Shutdown)
```

- `Registry<T>` / `KeyedRegistry<K, T>`: `subscribe()` / `subscribe(key)`.
- `Shutdown`: settled.
- `broadcast` / `keyed`: free functions in the `fanout` module.
- Alternative middle names: `Subscriptions` / `KeyedSubscriptions`.

Recommendation: `Registry` / `KeyedRegistry`; `Subscriptions` sounds like a
collection, while `registry.subscribe(...)` reads as the verb.

After this, the interview has covered the wide-reaching decisions; the next
artifact is the design document.

## Q17 — Settled

Decision: `Registry` / `KeyedRegistry`; constructors `broadcast` / `keyed` in
the `fanout` module. `Shutdown` and `Channel` as above. The interview is
complete; the design document is
`docs/logs/2026-10-31_design_channel_polish.md`.

## Q18 — Implementation deviations

- The fan-out phase 1 select is biased **cancel, register, data** (not
  register last). A subscriber enqueued before an element must be live for it.
- On thread exit the fan-out closes registration under the state lock and drains
  queued registrations, dropping their subscriber senders even if a registry
  handle is still alive.
- `nvzn` keeps a small reader-side `Subscriptions` wrapper for the live-view
  `Cancellation` and armed handle. The registration queue, `take_up`, and the
  fold-side subscriber list are gone.
- `subscription_shed` and `unstable_fanout_hangups` are removed; a shed
  subscriber logs `ProducerGone`. `subscription_reorder` remains.
- `LiveSeam` adds `StopReason::LiveStreamEnded` for a generic merged-stream
  `End`; `ScanEnded` is removed.
- The loader drops a **leading** below-start replay, preserving the overlapping
  re-append test; a backward index at/above the start is an order violation.
- The zslg local-store overrun test is removed; `Shed` is covered by the
  crate-level fan-out test, where the shed is deterministic.
- `PENDING_SUBSCRIBERS` is 1024; the full-queue test is removed because the
  threaded registry admits registrations immediately.
