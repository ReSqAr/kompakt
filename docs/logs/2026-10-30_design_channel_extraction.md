# Design — extracting `crossbeam-combinators`

Status: implemented on `channel-extraction` as one squashed commit.
Base: `channel-extraction`, branched from `origin/main` (3202bcf) and
fast-forwarded to `live-started` (e69b361), including the `wait_for` change
and `TryFrom<Event>` for owned subsets.

Post-review: `blend_merge` was removed and `Tail::above` now filters the live
overlap in all four seams. The §5/§6/§9 references to `blend_merge` describe
the original design; the code has only `blend_chain`.

## 0. Summary

`zslg::channel` is a general-purpose blocking pipeline runtime (a `Channels<T>`
graph, an explicit `Closed`/`Aborted`/`Cancelled` verdict, cancellation, batch
drain and an async bridge) that happens to live inside a replicated log. The
same is true of `learner/fanout.rs` (the producer half) and of the
subscribe→cut→range→live seam, which is hand-written four times. This design
extracts those into a workspace crate, `crossbeam-combinators`, and rewrites
the four seams as expressions of two traits (`Tail`, `Ranged`) and two thin
schedulers (`blend_chain`, `blend_merge`).

The channel core is moved, not redesigned. Performance work is a later,
separate experiment on the resulting stable foundation.

## 1. Goals, non-goals, success criteria

### 1.1 Goals

- One generic crate owns the full channel lifecycle: `Channels<T>`, `End`,
  `Closer`, `Feed<T>`, `Fanout<T>`/`Wakes`, `Cancellation`, batch drain and the
  (feature-gated) async bridges.
- `zslg`'s data flow becomes an expression of intent. The four hand-rolled
  seams become `Tail`/`Ranged` + `blend_chain`; `nvzn`'s live view becomes
  `Tail`/`Ranged` + `blend_merge`.
- The crate carries the exhaustive semantics tests; the moved tests are the
  behaviour spec.
- The result is a stable foundation for later, broad performance experiments.

### 1.2 Non-goals

- No wait-model or `Source`-tree redesign in this change. The current
  `Select`-per-`recv` core moves as-is.
- No domain logic in the crate: archive replay, `WatermarkGate`, reconnect
  loops, folds, `Held`, coverage/overflow policy stay in `zslg`/`nvzn`.
- No wire-format, storage-format or deployment changes.
- No external API compatibility promise (the workspace is internal).

### 1.3 Success criteria

- `cargo test -p crossbeam-combinators --all-targets --all-features` is green,
  including the moved 53 core tests and the fan-out tests.
- `zslg`, `nvzn` and `btrb` tests are green after migration.
- `zslg`'s domain API is unchanged; the channel types are no longer re-exported.
  Consumers (`nvzn`, `btrb`) import `Channels`, `End`, `Cancellation`,
  `CancelHandle` from `crossbeam-combinators` directly; `nvzn`'s ~20 import
  sites are updated in the migration.
- Each migrated seam is a short expression with no duplicated ordering
  comment; the ordering rule lives in `Tail::open`.
- The moved `channel_bench` numbers are within noise of the pre-extraction
  baseline. (The bench path does not use `Closer`, so the dependency-boundary
  change should not move it.)

## 2. Current state

### 2.1 The channel core

`zslg/src/channel.rs` is 2,434 lines with 53 tests (≈1,450 non-test lines). A
`Channels<T>` owns a tree of `Source<T>` nodes:

```text
  LeafState (one receiver + close receiver)
  MergeState (fair binary select; dropping a dead branch releases its captures)
  ChainState (phases; an abort condemns the chain)
  Guarded (race against a liveness receiver / cancellation)
  Unfold (lazy resync segments; hands the previous End to `next`)
  FilterMap / Map (transform at pull time)
```

`recv` walks `leaf_count()`, builds a `Select`, walks `register()`, waits, then
walks `poll(ordinal)`. `try_recv` drains by `drain_first`. `ready_batch` takes
one item through `recv` and the rest through `drain_first`.

`End` has three verdicts: `Closed`, `Aborted { what: &'static str }`,
`Cancelled`. A producer-side `Closer` distinguishes "I finished" from "I died";
an infinite stream has no closer, so any end is an abort.

### 2.2 The seam copies

The same `subscribe → cut → range → live filtered above the cut` shape is
written by hand at four sites:

| Site | Live | Cut | Range | Live filter |
| --- | --- | --- | --- | --- |
| `learner/read.rs` stable | `subscribe_events` | frontier + watermark | `HistoricalReplay` | shared cursor |
| `learner/read.rs` unstable | `subscribe_unstable` + watermark moves | `end_hlc` | `stream_above_hlc` + opening | `WatermarkGate` |
| `replica/log.rs` | fan-out registration | `frontier.index()` | `pump_range` | `idx > cutover` |
| `nvzn/live.rs` | `subscribe_unstable_tail` | `stable_watermark` | `subscribe_stable_log` to cut | identity (`Held` dedups) |

The ordering hazard is identical: the live subscription must be opened
**before** the cut is read, or an append racing the cut is both outside the
range and absent from the live tail — a silent gap.

### 2.3 The producer half

`learner/fanout.rs` (265 lines, 6 tests) owns `Fanout<T>` (bounded
per-subscriber queues; a full queue drops the subscriber, and that disconnect
is the lag protocol) and `Wakes` (a payload-free level signal). Both are
generic and crossbeam-only.

Every finite producer site hand-wires the same shape: create a bounded channel,
call `Channels::from` to get `(Channels, Closer)`, spawn a thread/task, send
items, call `closer.close()` on the normal exit, and rely on a panic dropping
the closer to produce `Aborted`. `pump_range`, `StableLogSnapshot::range` and
`HistoricalReplay` are three copies.

### 2.4 Dependency and benchmark state

`channel.rs` reaches tokio/futures in exactly three places: `Closer`
(`tokio::sync::oneshot`), `into_stream` (`tokio::sync::mpsc`,
`spawn_blocking`, `futures`/`tokio-stream`), and `Cancellation::from_token`
(`tokio-util`). Everything else is crossbeam-only.

`zslg/examples/channel_bench.rs` measures `recv` routing at 1/2/4/8/16 leaves,
a filtered retry, and `ready_batch` as a control. No numbers are pinned and it
does not run in CI.

## 3. Constraints and assumptions

- Workspace lints apply: `clippy::all = deny`, `unreachable_pub`,
  `unused_crate_dependencies`, `must_use_candidate`, `large_futures`, etc.
- Edition 2024, stable toolchain.
- The crate is `publish = false` for now, like the other workspace crates, but
  is designed as a generic library: no workspace-specific types, no domain
  knowledge.
- `zslg`, `nvzn`, `btrb` are the only consumers. Breaking internal APIs is
  allowed. `zslg` does not re-export channel types: it uses the crate
  internally, its public traits name the crate's types, and consumers import
  them from the crate. This is a mechanical update at ~20 `nvzn` import sites.
- No `unsafe`.
- `crossbeam-channel` is the base dependency. Tokio/futures are optional.
- The current channel core is frozen; the extraction is behaviour-preserving.
- Performance experiments start only after the foundation (extraction +
  migration) is green.

## 4. Architecture

### 4.1 Crate boundary

```text
  crossbeam-combinators              zslg                        nvzn
  ─────────────────────              ────                        ────
  Channels / End / Closer            Reader/Replica domain       folds, live views
  Feed / Fanout / Wakes              archive replay              Held / Coverage
  Cancellation / batch               WatermarkGate               ReplayDone
  Tail / Ranged / blend_*            seam impls                  seam impl
```

The crate knows nothing about logs, HLCs, indices, watermarks or folds. It
knows streams, verdicts, close, cancellation and the subscribe/cut/range
protocol.

### 4.2 The two halves

- **Read half:** `Channels<T>` plus `End`. The consumer pulls; no threads.
- **Write half:** `Feed<T>` (one producer, explicit close) and `Fanout<T>`
  (many subscribers, lag hang-up) plus `Wakes`.
- **Seam:** `Tail` (live + cut + filter) and `Ranged` (history bounded by the
  cut), composed by `blend_chain` or `blend_merge`.

### 4.3 Module map

```text
  crates/crossbeam-combinators/src/
    lib.rs        crate docs, re-exports
    channel.rs    Channels, Source tree, End, Closer, Cancellation, batch, stream bridge
    feed.rs       Feed, Feed::channel
    fanout.rs     Fanout, Wakes
    blend.rs      Tail, Ranged, blend_chain, blend_merge
```

`Source` and the concrete node states stay private. The public surface is
listed in §5.6.

### 4.4 `zslg` after extraction

`src/channel.rs` is deleted. `zslg` imports the crate directly wherever it used
`crate::channel::{…}`: 88 references across 38 files, a mechanical rewrite to
`use crossbeam_combinators::{…}`. There is no alias module and there are **no
public re-exports**: `zslg::Channels`, `zslg::ChannelEnd`,
`zslg::Cancellation` and `zslg::CancelHandle` go away. `zslg`'s public traits
(`Reader`, `Replica`, …) name `crossbeam_combinators::Channels`/`End` directly;
their docs link to the crate. `zslg` enables the crate's `tokio` feature
because it uses `into_stream` and `Cancellation::from_token`.

`Feed` is used internally where a producer needs it; it is not re-exported.
`zslg`'s domain API (`Reader`, `Replica`, `Data`, `StableLogEvent`, …) is
unchanged.

### 4.5 `nvzn` after extraction

`nvzn` adds `crossbeam-combinators` as a workspace dependency and imports the
generic types directly: `Channels`, `End`, `Cancellation`, `CancelHandle`,
`Tail`, `Ranged`, `blend_merge`. It still imports domain types from `zslg`.
There is no re-export path to keep in sync: each type has one name.

## 5. Interfaces

### 5.1 `Channels`, `End`, `Closer`

```rust
pub enum End {
    Closed,
    Aborted { what: &'static str },
    Cancelled,
}

/// The producer's half of an explicit close.
pub struct Closer(/* crossbeam-backed state */);
impl Closer { pub fn close(self); }

pub struct Channels<T> { /* private Source tree */ }

impl<T: Send + 'static> Channels<T> {
    /// Wrap a finite receiver; the returned Closer is the completion proof.
    pub fn from(rx: Receiver<T>, what: &'static str) -> (Self, Closer);
    /// Never ends; any termination is Aborted.
    pub fn from_infinite(rx: Receiver<T>, what: &'static str) -> Self;
    /// Complete by construction.
    pub fn from_vec(items: Vec<T>) -> Self;

    pub fn merge(self, other: Self) -> Self;
    pub fn chain(self, next: Self) -> Self;
    pub fn unfold<S, F>(state: S, next: F) -> Self
    where S: Send + 'static,
          F: FnMut(&mut S, Option<End>) -> Option<Self> + Send + 'static;
    pub fn filter_map<U, F>(self, f: F) -> Channels<U>
    where U: Send + 'static, F: Fn(T) -> Option<U> + Send + Sync + 'static;
    pub fn map<U, F>(self, f: F) -> Channels<U>
    where U: Send + 'static, F: Fn(T) -> U + Send + Sync + 'static;
    pub fn until(self, cancel: &Cancellation) -> Self;

    pub fn recv(&mut self) -> Result<T, End>;
    pub fn try_recv(&mut self) -> Option<T>;
    pub fn end_reason(&mut self) -> End;
    pub fn ready_batch(&mut self, max: usize) -> Option<ReadyBatch<'_, T>>;

    #[cfg(feature = "tokio")]
    pub fn into_stream(self) -> BoxStream<'static, T>;
}
```

`chain` and `unfold` become public: they are general and the seam needs
`chain`.

`Closer` is crossbeam-backed (capacity-1 close channel; `try_recv` yields
`Ok`/`Disconnected`/`Empty`), removing the tokio oneshot from the core. The
close-before-last-sender-drop rule stays documented.

### 5.2 `Feed`

```rust
pub struct Feed<T> { /* Sender<T> + Closer */ }

impl<T: Send + 'static> Feed<T> {
    /// Create the pair. The caller spawns and runs the loop.
    pub fn channel(cap: usize, what: &'static str) -> (Self, Channels<T>);
}

impl<T> Feed<T> {
    /// Blocking send. `false` means the consumer is gone; stop.
    pub fn send(&self, item: T) -> bool;
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>>;
    /// Normal completion: close, then drop the sender.
    pub fn close(self);
}
```

The caller chooses the thread/executor. `Feed` never spawns. Dropping it
without `close()` yields `Aborted`; a panic inside the producer loop does the
same. This replaces the repeated `Channels::from` + `Closer` plumbing.

### 5.3 `Fanout` and `Wakes`

Moved as-is:

```rust
pub struct Fanout<T> { /* bounded per-subscriber queues */ }
impl<T: Clone + Send + 'static> Fanout<T> {
    pub fn new(capacity: NonZeroUsize, what: &'static str) -> Self;
    pub fn subscribe(&self) -> Channels<T>;
    pub fn send(&self, event: &T) -> usize;
    pub fn send_all(&self, events: &[T]) -> usize;
}

pub struct Wakes { /* one level slot per subscriber */ }
impl Wakes {
    pub fn new() -> Self;
    pub fn subscribe(&self) -> Channels<()>;
    pub fn wake(&self);
}
```

### 5.4 The seam

```rust
pub trait Tail {
    type Item;
    type Cut: Copy;

    fn subscribe(&self) -> Channels<Self::Item>;   // called first
    fn cut(&self) -> Self::Cut;                    // called second

    /// Pure filter, same item type.
    fn above(&self, live: Channels<Self::Item>, cut: Self::Cut)
        -> Channels<Self::Item>;

    fn open(&self) -> (Channels<Self::Item>, Self::Cut) {
        let live = self.subscribe();
        let cut = self.cut();
        (self.above(live, cut), cut)
    }
}

pub trait Ranged<C> {
    type Item;
    /// History bounded at `cut`, closing only after the cut is served.
    fn range(&self, cut: C) -> Channels<Self::Item>;
}

pub fn blend_chain<T, R>(tail: &T, ranged: &R) -> Channels<T::Item>
where T: Tail, R: Ranged<T::Cut, Item = T::Item>
{
    let (live, cut) = tail.open();
    ranged.range(cut).chain(live)
}

pub fn blend_merge<T, R>(tail: &T, ranged: &R) -> Channels<T::Item>
where T: Tail, R: Ranged<T::Cut, Item = T::Item>
{
    let (live, cut) = tail.open();
    ranged.range(cut).merge(live)
}
```

There is exactly one `Item` type per seam. Neither trait nor either blend
converts it. The cut is `Copy`, not `Eq`: there is no token and no `verify`.

### 5.5 Feature layout

- Core: `crossbeam-channel` only.
- `tokio` feature: `into_stream`, `Cancellation::from_token`. It enables
  optional `tokio` (`rt`, `sync`), `tokio-util`, `tokio-stream`, `futures`.
  This is the only async feature; the core build has no tokio.

`zslg` enables `tokio`; `nvzn` does not need it. `btrb` gets `Channels`/
`Cancellation` transitively through `nvzn`/`zslg` or its own crate dependency
if it uses them.

### 5.6 Public vs private

Public in `crossbeam-combinators`: `Channels`, `End`, `Closer`, `Feed`,
`Fanout`, `Wakes`, `Cancellation`, `CancelHandle`, `ReadyBatch`,
`ChannelIter`, `Tail`, `Ranged`, `blend_chain`, `blend_merge`, and the
feature-gated `into_stream`/`from_token`.

`zslg` re-exports none of these; consumers import them from the crate.

Private: `Source`, `LeafState`, `MergeState`, `ChainState`, `Guarded`,
`Unfold`, `FilterMap`, `Poll` and the `Select` routing. The moved tests keep
access through `#[cfg(test)]` internals.

## 6. Seam data flow

### 6.1 The rule

```text
  t0  live = subscribe()          must be first
  t1  cut  = snapshot()           after the subscription
  t2  history = range(cut)        covers everything up to cut
  t3  live = above(live, cut)     drops what history covered
  t4  blend(history, live)        chain or merge
```

A gap is impossible if `range` honours `cut` and `open` honours the order. An
overlap is expected and is the domain's `above` (or its own buffer) to remove.

### 6.2 `Tail::open`

`open` is the only ordering-sensitive call: `subscribe` → `cut` → `above`. A
domain implements `subscribe`, `cut`, `above`; it cannot reorder them by
accident. `above` is a pure filter, so it cannot change the item type.

### 6.3 `Ranged::range`

`range(cut)` returns a finite stream bounded at `cut` and closes only after the
cut is served. There is no generic coverage check: the `Closer` already
distinguishes a clean end from an abort, and a keyed “did the last item reach
the cut” assertion is domain work inside the `range` implementation. A range
that closes cleanly early is a domain bug, pinned by that domain's tests.

### 6.4 `blend_chain` vs `blend_merge`

- `blend_chain`: history, then live. The chain's phase change is the handoff.
  The consumer does not observe the boundary; it only reads items. This is
  `zslg`'s shape.
- `blend_merge`: interleaved. A merge hides branch ends, so a consumer that
  must observe “history finished” puts that event in its own `Item` type. This
  is `nvzn`'s shape. The crate does not know the variant.

Both are one-liners over existing `chain`/`merge`; neither inspects the cut.

### 6.5 Migration map

| Site | `Item` | `Cut` | `Tail::above` | `Ranged::range` | Blend |
| --- | --- | --- | --- | --- | --- |
| stable read | `Result<StableLogEvent, Error>` | `StableCut { frontier, watermark }` | `index <= frontier` (+ outer cursor) | `HistoricalReplay` + trailing watermark | `chain` |
| replica | `Result<(LogIndex, Data), Error>` | `Option<LogIndex>` | `idx <= cut` | `pump_range(start, cut)` | `chain` |
| unstable | `Result<UnstableStep, Error>` | `Option<Hlc>` | `hlc <= cut` (+ gate outside) | opening + `stream_above_hlc` | `chain` |
| nvzn live | `Result<FoldItem, Error>` | `Option<Hlc>` | `hlc <= cut` (+ `Held`) | stable scan to cut + `ReplayDone` | `merge` |

Notes:

- Stable: the cut is a named
  `StableCut { frontier: Option<LogIndex>, watermark: Option<Hlc> }`, not a
  tuple: the two fields mean different things and the struct documents the
  seam. `above` drops live `Appended` entries at or below `frontier`; the outer
  cursor still advances across cycles and suppresses the replay's duplicates.
  The trailing `Watermark` event is a **`zslg` `Ranged` implementation
  detail**: `StableCycle::range` chains the `HistoricalReplay` output and then
  the `Watermark` item, so it is ordered after the replay and before the chain
  switches to live. The crate only sees a `Channels<Item>`; it has no watermark
  concept.
- Replica: registering the live fan-out before reading the frontier moves into
  `Tail::subscribe`/`Tail::cut`; `open` enforces it.
- Unstable: `above` drops live entries at or below the cut; they are stable,
  and the stable log — not the scan, which reads only above the cut — covers
  them. `WatermarkGate` stays domain-side and still handles the moving level
  (`blend_chain(..).filter_map(gate)`). The `Opened` step is the first item of
  `range`.
- nvzn: `above` drops live entries at or below the cut; `Held` still dedups by
  `DataId` above it. See §6.6.

### 6.6 `nvzn` live

`nvzn` chooses the item type:

```rust
enum FoldItem {
    Live(Data),                  // from the unstable tail
    Scan(Data),                  // from the cold-start scan
    ReplayDone,                  // the scan reached its cut
    TailFailed(zslg::Error),
    ScanFailed(zslg::Error),
    ScanEnded(End),
}
```

`Live`/`Scan` keep `live_scan`'s entry count and the two stop reasons distinct.
`Tail::subscribe` maps `UnstableEvent::Appended` to `Live`, drops unstable
watermarks, and maps an error to `TailFailed(e)`. `Tail::cut` is
`reader.stable_watermark()`; `Tail::above` drops live entries at or below the
cut (the scan covers the stable side; `Held` still dedups by `DataId` above it).

`Ranged::range(cut)` is a `Feed`-backed adapter over
`subscribe_stable_log(from)`: empty when `cut` is `None`; otherwise it stops
at the first entry with `hlc > cut` or the first `Watermark >= cut`, sends
`Scan` entries through the feed, and sends `ReplayDone` only on a clean scan
end. An error or an early end sends `ScanFailed(e)` / `ScanEnded(end)`
instead; cancellation closes the history half cleanly so the live half's
`Cancelled` ends the stream.

`begin` needs the cut to set `Coverage::Scanning` before the consumer starts,
so it opens the seam itself:

```rust
let (tail, until) = seam.open();     // live first, cut second
// Coverage::Scanning/Complete from `until`
let stream = seam.range(until).merge(tail);
spawn_blocking(move || run(&integrator, stream));
```

The ordering guarantee is in `open`; `blend_merge` was removed because it had
no users and hid the cut.

`run` is one loop: `Live`/`Scan` → `arrive` (Scan also counts entries);
`ReplayDone` → `scan_done` (metrics, `finish_scan`); `ScanFailed`/`ScanEnded`
→ halt `LiveScanEnded` + `scan_done`; `TailFailed`/stream end →
halt `UnstableTailEnded` + `finish_scan`; `Cancelled` → return. A drop guard
calls `finish_scan` on every exit, so a cancelled scan still wakes `snapshot()`
waiters. `finish_scan` on `ReplayDone` is what bumps `changed` and notifies
`scanned` for `wait_for`.

`Held`, `Coverage`, the cap/overflow policy and the retire-on-fold-watermark
task are unchanged. The live tail is drained while the scan runs, so the
cold-start overrun does not occur; the scan shares `recv` with the tail, so
tail load can slow the scan but not break it.

On `live-started`, `wait_for` returns only `Seen` or `TimedOut` and waits
through `NotStarted`/`Overflowed`, waking on `changed` or `retired`. The
implementation preserves that: `finish_scan` on `ReplayDone` is still what
bumps `changed` and notifies `scanned`, and `run`'s drop guard calls
`finish_scan` on every exit, so a waiter never sits until its deadline.
`Live::started` no longer exists, so nothing else depends on the start
transition.

## 7. Edge cases and semantics

### 7.1 Close, abort, cancel

`Closed` requires an explicit `Closer::close`/`Feed::close`. `Aborted` covers a
panic, a dropped `Closer`/`Feed`, a discarded task, or an infinite leaf
disconnecting. `Cancelled` is a `Cancellation` firing. Merge and chain
condemn on `Aborted`/`Cancelled`; a clean `Closed` branch is dropped and the
survivor continues.

### 7.2 Range coverage and truncation

`range(cut)` must close only after serving the cut. A truncated range that
closes cleanly is indistinguishable from a short history at the generic layer;
each `Ranged` implementation owns that assertion. A truncated range that
aborts condemns the chain/merge and never emits a domain `ReplayDone`.

### 7.3 Merge/chain behavior

Dropping a dead merge branch is load-bearing: the branch's closures may own the
survivor's sender (the `Replica::write` topology). An abort condemns
immediately; a clean close collapses to the survivor. Chain pops a phase on a
clean close and condemns on an abort.

### 7.4 `Feed` lifecycle

Normal return from the producer body calls `close`; panic/early drop yields
`Aborted`. The data sender outlives the close signal, preserving the
close-before-last-sender-drop rule. `send` returning `false` is the consumer-
gone signal; the producer loop stops.

### 7.5 Cursor and watermark state

Domain state shared between `range` and `above` (stable cursor, watermark
gate) is owned by the seam implementation, not the crate. `above` must be lazy
(read at pull time), because the range may advance the shared cursor before
live items are pulled.

### 7.6 Cancellation and the async bridge

`Cancellation` stays crossbeam-based (one sender, receiver clones) so it can
wake a blocked `Select`. `from_token` is feature-gated. `into_stream` keeps its
liveness guard: a dropped consumer disconnects the guard and stops the drain
thread, so a quiet infinite stream cannot hang `Runtime::drop`.

### 7.7 Empty ranges and missing cuts

An empty range closes cleanly (or emits its domain `ReplayDone`) immediately.
`None` cuts (no stable watermark / no frontier) are valid: the range is empty
and the live side serves everything. `Cut: Copy` makes the repeated uses free.

## 8. Complexity budget

### 8.1 Essential

- The verdict algebra: `Closed` vs `Aborted` vs `Cancelled`, and the rule that
  an abort condemns merge/chain. This is the reason the crate exists.
- The seam ordering: subscribe before cut. `Tail::open` encodes it once.
- The producer close proof: `Feed`/`Closer`. Dropping without close is an
  abort, not a clean finish.

### 8.2 Deferred / accidental

- The `Source` tree and `Select`-per-`recv` routing. They are the current
  performance model, not the design's essence; they move unchanged.
- Per-domain admission (cursor, watermark gate, `Held`) stays out of the crate.
- Performance experiments (~10 strategies) are a separate phase on this
  foundation; they may replace the tree or the wait strategy without changing
  the public API.

## 9. Alternatives considered

### 9.1 Split seam / two consumers

`split()` returning live + cut + range was proposed for `nvzn` so it could
drain the tail on its own thread. Rejected: it is the subscription itself, not
a blend; `blend_merge` gives one consumer with the tail drained, and the
traits still allow a domain to call `open`/`range` directly if it wants two
consumers.

### 9.2 `BlendItem` / `MergeItem` / `Covered(cut)`

A crate-provided tagged item forced both halves to wrap every item even when
the domain did not need a boundary. Rejected: one domain-chosen `Item` type;
`nvzn` puts `ReplayDone` in its own enum, `zslg` has no done variant.

### 9.3 `verify`

A token check was circular: a token appended by the same layer that checks it
proves nothing. A real check would need a keyed assertion on the last range
item, which is domain work. Dropped.

### 9.4 Public `Admit` gate

An `Admit<T>` trait for overlap/watermark was proposed, then folded into
`Tail::above` (pure live filter) and domain gates (`WatermarkGate`, `Held`).
`Admit` is not part of the public seam.

### 9.5 Tokio in the core

Rejected: a crossbeam combinator crate should not pull tokio for a oneshot.
`Closer` becomes crossbeam-backed; tokio/futures are feature-gated.

### 9.6 Wait-model changes now

Rejected for this change. The core is frozen; performance is a later
experiment with ~10 strategies against the stable foundation.

### 9.7 Extraction without migration (F1)

Rejected: the seam API would only be exercised by crate tests, not by the four
real call sites. F2 proves the abstraction by migrating them.

## 10. Testing strategy

### 10.1 Moved tests

The 53 `channel.rs` tests and 6 `fanout.rs` tests move verbatim where possible.
They are the behaviour spec for the core: verdicts, merge/chain/abort, routing,
batch, cancellation, `into_stream` liveness.

### 10.2 New crate tests

- `Tail::open` order with a scripted fake: `subscribe` recorded before `cut`;
  `above` after both; a wrong-order implementation fails.
- `above` is a pure filter: same item type, lazy at pull time.
- `blend_chain`: history then live; empty history; abort in history condemns;
  abort in live is reported after the handoff.
- `blend_merge`: interleaving; a domain done item is ordered after all history
  items; an abort in either branch condemns; no done item on abort.
- `range` contract per domain: the scan closes at the cut; an aborted scan
  yields no domain `ReplayDone`.
- `Feed`: normal close → `Closed`; drop → `Aborted`; panic → `Aborted`;
  consumer drop → `send` false; discarded before running → `Aborted`.
- `Fanout`/`Wakes` lag and level semantics (moved).

### 10.3 Integration

`zslg`'s existing cluster/read/replica/nvzn tests are the integration net. The
migration should not change public behaviour; any stream-order/verdict change
is a bug.

### 10.4 Benchmark

`channel_bench` moves with the crate; run it before and after the extraction
and after migration. The extraction should be within noise. Migration changes
call-site composition, not the core; record both.

Baseline (release, `channel-extraction`, 2026-10-30, ns/item min/median):

```text
  recv  1 leaf   53 / 53
        2 leaves 76 / 76
        4 leaves 115 / 116
        8 leaves 313 / 313
       16 leaves 734 / 736
  recv 2 leaves, half filtered  166 / 166
  ready_batch(256), 2 leaves     29 / 29
```

These are the numbers the later performance experiment compares against.

## 11. Implementation sequence

Dependencies are a DAG; this is a linearisation with two checkpoints.

### Checkpoint A — extraction, behaviour-preserving

1. Create `crates/crossbeam-combinators`: Cargo.toml, lints, `lib.rs` docs;
   add workspace dependency.
2. Move `channel.rs` core + tests; replace the tokio oneshot `Closer` with the
   crossbeam-backed one; gate `into_stream`/`from_token` behind `tokio`.
3. Delete `zslg/src/channel.rs`; replace the 88 `crate::channel::…`
   references in 38 files with direct `crossbeam_combinators::…` imports;
   remove the public re-exports; update `nvzn`'s 20 imports to the crate;
   enable the `tokio` feature in `zslg`. Green: `zslg` and `nvzn` tests pass,
   the crate's tests pass.
4. Move `fanout.rs` + `Wakes` + tests; update `zslg` imports. Green.
5. Add `Feed` + tests; convert one small producer site (`pump_range` or
   `range`) as a proof, or leave for migration. Green.
6. Add `blend.rs` (`Tail`, `Ranged`, `blend_chain`, `blend_merge`) + fake
   tests. Green. **Checkpoint A.**
7. Move/duplicate `channel_bench`; record the baseline.

### Checkpoint B — migration

8. Stable read: `StableCycle` implements `Tail`/`Ranged<StableCut>`; replace
   the hand-written chain; keep the outer `unfold`/cursor/gate logic. Green.
9. Replica: `ReplicaCycle` implements `Tail`/`Ranged<Option<LogIndex>>`;
   `subscribe_from` becomes `blend_chain`. Green.
10. Unstable: `UnstableCycle` implements `Tail`/`Ranged<Option<Hlc>>`;
    `WatermarkGate` stays outside as a joined filter. Green.
11. nvzn: `LiveSeam` implements `Tail`/`Ranged<Option<Hlc>>` with
    `FoldItem::{Live, Scan, ReplayDone, Failed, ScanEnded}`; `begin` opens the
    seam (the cut feeds `Coverage`), then `range(until).merge(tail)` and one
    blocking `run`; `finish_scan` on `ReplayDone`; error paths release waiters.
    The `zslg::{Channels, ChannelEnd, Cancellation, CancelHandle}` imports move
    to `crossbeam-combinators`. Green.
12. Feature/docs cleanup: remove now-unused dependencies (`unused_crate_
    dependencies` enforces), update crate README/lib docs, move the bench.
    **Checkpoint B.**

### Later — performance experiments (separate log)

Not part of this implementation. On the stable foundation: ~10 strategies for
the wait model / graph representation, measured against `channel_bench`, keep
what sticks. The public API and the seam traits should not need to change.

## 12. Operations

- CI: the new crate is a workspace member; `scripts/changed-crates.sh` picks it
  up. Run `cargo test -p crossbeam-combinators --all-targets --all-features`,
  plus `--no-default-features` for the core-only build.
- `cargo deny`: no new licence/source risk (crossbeam/tokio/futures already in
  the graph).
- Docs: crate-level header explains the read/write halves, the verdicts and
  the seam. `zslg`'s module docs point at the crate.
- Downstream: `nlnr` pins `kompakt` by revision; bump after the merge. No
  runtime/migration work.

## 13. Open items

None. Settled: the feature is `tokio`; the module is `blend.rs`; `nvzn` uses
the crate directly; `zslg` has no channel re-exports (`Feed` is crate-internal
too); `StableCut` is a named struct and the trailing watermark is the last
item of `zslg`'s stable `Ranged::range` (domain behavior, not crate support).
