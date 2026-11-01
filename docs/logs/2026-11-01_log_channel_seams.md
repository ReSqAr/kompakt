# Channel seams — decision log

Process: `/home/agent/nlnr/docs/iaic.md`. Branch `channel-extraction`, base
`08a1dd3`. Status: interview; docs only, no code changes.

Input: post-round review feedback on `crossbeam-combinators`, `zslg`, and
`nvzn`, 2026-11-01. The previous design and review are
`2026-10-31_design_channel_polish.md` and
`2026-11-01_post_channel_polish.md`.

## Scope

The crate owns the transport; `zslg` and `nvzn` should declare a data pipeline
(source, cut, keys, metrics) and no channel mechanics. This round removes the
remaining mechanics and makes the important invariants type-checked where the
cost is justified.

## Settled — problems the round must fix

### S1. The split-HLC watermark test is timing-dependent

`stable_log::tests::a_watermark_never_covers_an_hlc_group_split_across_batches`
drains with `try_recv()` after `append(..).await`. With the threaded fan-out the
last events may not have arrived yet. Receive the expected count with a timeout;
keep the "nothing else arrives" assertion only where order guarantees it.

### S2. `HistoricalReplay` is old framing

It uses `tokio::spawn` + `spawn_blocking` + a second hand-closed channel just to
return `range(...).stitch(archive)`. The lazy form is
`OrderedChannel::new(range).stitch(archive).into_channel()` with no task.
The HLC `assert!` should be a second ordered check (`NonDecreasing<Hlc>`) that
ends the stream as an order violation. The fallback's `block_on` then runs on
the consuming thread, so the consumer must not be a tokio worker; a synchronous
archive read would remove that constraint.

### S3. The seam is assembled three ways

`blend_chain` (replica), hand-written `open()` + `range(cut).chain(live)`
(stable and unstable resync), and `range(..).merge(tail)` (nvzn). `above` is
written four times as "drop items whose key is at or below the cut". This is
evidence that `blend_chain` has the wrong shape, not that the callers are
wrong.

### S4. The producer constructor is hand-written nine times

Bounded channel + `Channel::from` + `spawn_blocking` + manual `closer.close()`
at: `stream_above_hlc`, `stream_window_ordered`, `StableLogSnapshot::range`,
archive `stream`, `pump_range`, `read_snapshot`, the merge feeder, the
coordinator prime channel, and `HistoricalReplay`. `Feed` exists for exactly
this and has no production users.

### S5. The fan-out owner is hand-written four times

Each owner stores `(tx, registry, shutdown)` in three fields, writes
`Drop { shutdown.cancel() }`, and repeats
`NonZeroUsize::new(1024).unwrap_or(MIN)`. Producers use
`let _ = tx.send(..)`, which silently ignores a stopped fan-out.

### S6. Resync state is forced into locks and atomics

`resume_on_abort` and the combinators take `Fn`, so the stable cursor is an
`AtomicU64` shared with a `filter_map`, and `WatermarkGate` holds a `Mutex`.
A channel is consumed by one thread; `FnMut` state would remove both.

### S7. Small duplicates

`replica/log.rs::empty_historic()` re-implements `Channel::empty()`.

## Open questions

### Q1. Does the finite/infinite distinction become part of the type?

Candidate: `Channel<T, Kind = Finite>` (or a separate `Infinite<T>` newtype).
`Infinite` has an abort-only read surface; `resume_on_abort` accepts only
`Infinite`; `Ranged::range` promises `Finite`. This makes the previous
`Channel::empty()` bug unrepresentable. The cost is kind propagation through
every combinator and every consumer that matches `End::Complete`.

### Q2. What is the seam type's shape?

Candidate: `Seam` returns a `Segment { live, cut }` with `.chain(range)` /
`.merge(range)`; the cut is readable. If the item implements `Key` and the cut
is `Option<K>`, `above` is crate-provided. `cut()` requires the subscription
proof and `open` is not overridable.

### Q3. What does resync look like?

Candidate: `resync(make_seam, on_resync)` for the skeleton; a resumable ordered
channel for streams with a position, keeping the cursor and dropping at/below
it. This replaces both resyncing tails and the `AtomicU64`.

### Q4. Does the fan-out owner come back?

Earlier rounds explicitly removed a `Fanout` wrapper. A new owner type would
own shutdown-on-drop and default caps, and `send` would report consumer-gone.
This reverses that earlier decision; it is open.

### Q5. Scope and order

The feedback ranks A–F. Do we take the whole set in this round, or split into a
mechanical round (producer constructor, fan-out owner, HistoricalReplay, test)
and a later type-system round (finite/infinite, seam, resync)?

## Decisions

### D1. No default kind; every channel names it explicitly

`Channel<T, Kind>` has no default type parameter. Every construction site,
combinator, signature, and test double names `Finite` or `Infinite`. The
choice is part of the declaration, not an implicit guess; aliases may reduce
noise but must not hide which kind a value is.

### D2. One seam trait; `above` stays required

Adopt the `Seam` shape: `subscribe() -> Live<Item>`, `cut(&Live) -> Cut`,
`above(Live, Cut) -> Live`; a free `open(&seam) -> Segment<Item, Cut>` that
cannot be overridden; and `Segment::cut/chain/merge`. Keep **one** trait and
make `above` required. The crate supplies `filter_above::<K>()` for seams whose
cut is `Option<K>`; unstable and nvzn pass a closure because their cut is `Hlc`
and their key is `DataId`. No second `KeyedSeam`.

### D3. `next(cursor)` owns the resume position; no generic dedup

`Resumable` advances the cursor, but it does not filter items. The factory
receives the last emitted key and starts after it; whether that is inclusive or
exclusive, and how an HLC group is handled, is the seam's rule. The ordered
policy remains the safety net. A crate-level "drop keys <= cursor" would bake in
an assumption that is wrong for HLC groups and custom cuts.

### D4. No dedicated resync hook

The factory receives `(cursor, previous: End)` from the resume loop. zslg can
log or measure inside that closure. No `Resync` value, no required callback; the
hook was not needed.

### D5. Sources implement `Recv`; channels are one implementation; pumps are explicit

`Channel` is already a boxed source, not a crossbeam receiver. A computed source
(`Resumable`, `HistoricalReplay`) should implement the public read interface
directly, not be a receiver plus a producer task. A pump adaptor moves a
blocking source onto a thread chosen by the caller and feeds a `Channel`; the
tokio feature supplies the `spawn_blocking` convenience. `produce_blocking` is
the write-side dual of the same idea. Open: whether the pump preserves the
source's exact `End`.

### D6. `Resumable` is `Recv`, not `Source`

`Source` is the crate-private select/poll leaf used by `Channel`. `Resumable`
owns a cursor and a resync loop, so it implements the public `Recv` surface
directly; it is not a `Source` and is not boxed into a `Channel` implicitly.
`Channel`/`OrderedChannel` are `Recv` implementations that are `Source`-backed.
A caller that needs a `Channel` pumps the `Recv` explicitly.

### D7. Spawn policy is a marker trait

`pump` and the `produce_*` constructors take a zero-sized spawn policy
implementing a `Spawn` trait (`TokioBlocking`, `StdThread`, `CurrentThread` for
tests), not a closure. The tokio policy is feature-gated. The caller chooses the
thread construct at the type level.

### D8. Exact `End` propagation is crate-internal

The close signal widens to `Option<AbortReason>` so the crate's pumps/adapters
can forward `OrderViolation`, `Cancelled`, and the rest. `Closer::abort` and
`Feed::abort` are crate-internal. External users can only `close()` (Complete)
or drop (ProducerGone); they cannot fabricate an abort reason.

### D9. `produce_async` is the async primitive; the archive `stream` becomes sync

Accepted. `produce_async` returns a `Channel` immediately, runs the async body
on a spawn policy, closes on normal completion, and aborts on panic. The archive
`stream` does not need to await before returning: its opens are the async work,
and its item type already carries `Result<_, Error>`, so an open failure is the
first item. `StableArchiveReader::stream` becomes
`fn stream(..) -> ArchiveStream`, and `HistoricalReplay` drops `block_on`.
The only requirement is a runtime handle for the spawned task; the current
`block_on` has the same requirement, just hidden.

## Open questions (continued)

### Q6. How much of the read surface does `Recv` carry?

A `Recv` can be read-only (`recv`/`try_recv`/`end_reason`) with all composition
on `Channel`, or it can carry the single-source lazy combinators
(`map`/`filter_map`/`take_until`/`take_while`/`chain`/`until`) so a
`Resumable` stays lazy through a scan, leaving `merge` and async boundaries as
the explicit pump points.

### D10. `Recv` carries the lazy single-source combinators

`Recv` carries `map`, `filter_map`, `take_until`, `take_while`, `chain`, and
`until`, returning lazy `Recv` adapters. A `Resumable` therefore composes
through a scan without a pump. `merge` stays the multi-source operation that
needs `Source`/a `Channel` (or an explicit pump), because a blocking `recv`
cannot wait on two sources without a thread. `ready_batch` is a `Recv` default
over a generic batch type; that is an implementation detail, not a separate
design fork.

## Open questions (continued)

### Q7. What is the fan-out owner's shape?

Candidate: `let (fanout, registry) = broadcast(what, config)` where `Fanout`
owns `tx` + `Shutdown`, `publish(&self, item) -> bool` reports consumer-gone,
and `Drop` cancels. The reader side is a cloneable `Registry`/`KeyedRegistry`.
A config struct carries the data/register/subscriber caps (with defaults) and
the overflow policy. This reverses the earlier "no `Fanout` wrapper" decision.

### D11. Keep the fan-out split

Construction returns `(fanout, registry)`: the producer-side `Fanout` owns
`tx` + `Shutdown`; the reader side is the independent, cloneable
`Registry`/`KeyedRegistry`. The owner does not expose `subscribe`.

### D12. `OverflowPolicy` is explicit

`FanoutConfig` may default the three capacities, but `OverflowPolicy` is a
required argument. `Block` vs `Shed` is a semantic choice, not sizing.

### D13. Try the producer-side wrapper

Reintroduce a fan-out owner type on the producer side. The earlier "no
`Fanout` wrapper" was about hiding the three parts; this wrapper is the
producer half, with the registry still independent. If it does not earn its
keep, it can be removed again.

## Review round (design restarted after review)

The first design draft was reviewed before sign-off. The review found one
false premise (D6: a `Resumable` that is a `Source` would expose the private
select model — it would not; `Source` stays private either way, and
`ResumeOnAbort` is already a private `Source` holding a `FnMut`) and several
contradictions. The interview restarts on those points; D1–D13 stand unless a
decision below reverses them.

Review concerns, as open questions:

- R1. Drop `Recv` / `pump` / spawn policies (D5–D7, D10)? A lazy `Recv` cannot
  honour `until` while blocked, and nvzn's `Segment::merge` over a `Resumable`
  range would need a pump thread again.
- R2. `until` inside a resumed segment turns a shutdown into a resync.
- R3. `Live` as a proof token is forgeable; `open` is the only real guarantee.
- R4. The resume cursor means "next wanted" on the first call and "last
  emitted" after a resync.
- R5. The archive's own tiling (`expected`/`last_hlc`/`open_failed`) is
  channel logic the design does not move; it needs a deferred phase.
- R6. Missing IAIC sections: constraints, complexity budget, operations,
  `Resumable` state diagram.
- R7. Implementation order: widest churn first vs a thin slice first.
- R8. Scope for the last polish round (Q5 was never answered).

### D14. The seam is opened by `Segment::open`

`open` becomes an associated constructor, `Segment::open(&seam)`, not a free
function. The constructor is the one place subscribe-then-cut happens and is
discoverable on the type it returns. Not `from`/`From`: that names a
conversion, and opening a seam performs I/O and fixes an order.

### D15. Fan-out sends return `Result`, not `bool`

```rust
impl<T> Fanout<T> {
    pub fn publish(&self, item: T) -> Result<(), Stopped<T>>;
    pub fn try_publish(&self, item: T) -> Result<(), TryPublishError<T>>;
    pub fn shutdown(self);
}
pub struct Stopped<T>(pub T);
pub enum TryPublishError<T> { Full(T), Stopped(T) }
```

The error names what actually happened: the distribution thread is gone
(shutdown or panic). It is not "consumer gone" — a departed subscriber is shed
or skipped inside the thread and never reaches the producer. The item is handed
back, as crossbeam's `SendError` does. `#[must_use]` on `Result` replaces the
silent `let _ = tx.send(..)`.

### D16. No `Recv`, no `pump`, no spawn policy (reverses D5, D6, D7, D10)

Threads are needed only where a producer runs (blocking scan, async open,
socket, the fan-out's own thread), never to read or compose. With `Resumable`
as a crate-private `Source`, `until` keeps racing cancellation while blocked and
`Segment::merge` composes without a thread, so `pump` has no caller. Every
producer site runs on tokio, so a spawn-policy type would have one
implementation.

- `Resumable` is a private `Source`; it yields an ordered infinite channel.
- Combinators take `FnMut + Send` (one consumer per channel); this removes the
  `AtomicU64` cursor and the `WatermarkGate` `Mutex`.
- `produce_blocking` / `produce_async` live behind the `tokio` feature with no
  policy argument.
- A public `Recv` or a non-tokio `produce_thread` can be added later without a
  breaking change, if a caller appears.

### D17. Scope: all four slices this round (answers Q5/R8)

Order: (1) mechanical — split-HLC test, `produce_*` + nine producer sites,
fan-out owner; (2) archive — sync `stream`, deferred tiling, lazy
`HistoricalReplay`; (3) kind marker + `Segment::open`, replica path end to end;
(4) `Resumable`/`resync` for stable, unstable and nvzn, delete
`Tail`/`Ranged`/`blend_chain`. Each slice is green before the next. Further
residue spotted on the way may be picked up in this round.

### D18. Resumers resume only on `ProducerGone` (answers R2; narrows D4)

`Resumable` and `resync` reopen a segment only when it ended
`Aborted(ProducerGone)` — the fan-out hang-up / producer death that a resync
repairs. Every other reason (`Cancelled`, `CancelHandleDropped`,
`OrderViolation`, `ConsumerGone`, `RoutingViolation`) ends the resumed stream
with that reason unchanged. The factory still receives the reason (logging,
metrics) and may still decline with `None`, but cannot resume a stop request or
a contract violation. Consequences: `until` is safe inside or outside a
segment; a corrupt log cannot spin a resync loop.

### D19. The resume position is always exclusive: `after: Option<K>` (answers R4)

Every caller already holds a *last-seen* position and converts it to a start:
nvzn's fold and live view compute `LogIndex::next_after(watermark)`, and
`Replica::subscribe_from(Option<LogIndex>)` is already exclusive. So the
cursor is always "after", never "from":

```rust
enum Opening { First, AfterProducerGone { what: &'static str } }

Resumable::<T, P>::new(after: Option<P::Key>,
                       make: impl FnMut(Option<P::Key> /*after*/, Opening) -> Option<S>)
// first open: after = the caller's; resync: after = last emitted key
```

- `None` means "from the beginning"; the first open and a resync use the same
  meaning, so no off-by-one between them.
- No `Successor` needed: `Resumable` works for any order policy
  (`StrictlyIncreasing` and `Contiguous`).
- The ordered check takes the same `after`: `StrictlyIncreasing` requires the
  first key `> after`; `Contiguous` requires it `== Successor::next_after(after)`.
  `Successor` becomes `fn next_after(prev: Option<Self>) -> Self` — exactly
  `LogIndex::next_after` today — so `None` has a defined first key.
- `Reader::subscribe_stable_log(after: Option<LogIndex>)` aligns with
  `subscribe_from`; nvzn passes its watermark directly. The factory converts to
  its own scan bound.
- `Opening` replaces D4's `End` / the draft's `Option<AbortReason>`: with D18
  the factory runs only on the first open or after `ProducerGone`.

### D20. No `Live` proof token (answers R3)

`Seam::subscribe` is implemented outside the crate, so `Live` would have to be
constructible there — and is then forgeable. The guarantee is D14:
`Segment::open` is the only assembly point and fixes subscribe → cut → above,
range. The `Seam` methods are hooks the crate calls; the module docs say so.
`subscribe` returns `Channel<Item, Infinite>`, `cut(&self) -> Cut`. No sealing
trick: it would also stop zslg from implementing the trait.

### D21. The archive is a declaration: `defer_async` + `covers(..until)` (answers R5)

```rust
Channel::chain_all(covering.map(|m| Channel::defer_async(move || async move {
        store.read_snapshot(m.id).await.unwrap_or_else(|e| Channel::once(Err(e)))
    })))
    .filter_map(within(bounds))                 // below → skip, above → end
    .ordered::<Contiguous<LogIndex>>(after)     // tiling
    .ordered::<NonDecreasing<Hlc>>(None)        // replaces the HLC assert!
    .covers(..until)                            // must reach the window's end
```

- **`Channel::defer_async(|| fut)`** (tokio feature): on the first poll, spawn
  the future (runtime handle captured at construction) and delegate to the
  `Channel` it resolves to; the phase is selectable while it waits on the
  hand-off. `chain_all` therefore opens one snapshot at a time.
  `read_snapshot` stays `async` (11 callers unchanged); the open error becomes
  the phase's first item. A synchronous `defer` is not added: no caller.
- **`covers(..until)`** on a `Contiguous` channel: it ends `Complete` only if
  the stream reached `until` (last key's successor `== until`, or an empty
  window `next_after(after) == until`); otherwise
  `Aborted(OrderViolation("short"))`. Named after the `RangeTo` it takes: the
  exclusive upper bound matches every zslg window (`range(from, until)`,
  `IndexExclusive(until)`), so no `until - 1` arithmetic.
- `covers` applies to the archive, the stitch fallback (replacing its hand
  endpoint check) and `HistoricalReplay` — closing the trailing-gap hole: a
  local scan that stops short of `until` now aborts instead of ending early.
- Gone from the archive: `expected`/`last_hlc`/`open_failed`, the nested
  `spawn_blocking`, the HLC `assert!`, the hand-written "not fully tiled" check.
