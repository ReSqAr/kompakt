# Channel seams — internal model

Working document for the IAIC interview, not a design doc. It states my
current model only; how it got here is in the decision log (D1–D36). The design
doc is the spec.

## Where we are

Interview closed; the design doc has been through three external review rounds
and two consistency passes. Waiting for the user's sign-off pass.

## The model in one picture

```text
read side      Channel<T, K>, OrderedChannel<T, P, K>     K = Finite | Infinite, no default
               every read returns K::End: recv, ready_batch; no IntoIterator, no into_stream
               positions only via Pos: Key<T>; closures only for transforms, bounds, bodies

write side     produce_blocking / produce_async / defer_async  — each ::<T, K>, Kind::Proof
               Fanout (publish -> Result<(), Stopped<T>>) + separate Registry
                 (subscribe -> Result<Channel<T, Infinite>, RegistryError>, subscribe_infinite)
               Level<T>: sync core (mutex + private Wakes), changed().await tokio-gated

composition    Segment::open(&seam) → chain / merge     (no Live token; hooks only open calls)
               Resumable(what, after, make) / resync(what, make): one private resume Source
                 resume only on ProducerGone; Opening { First, AfterProducerGone }
                 after: Option<Pos> always exclusive; first-open decline = Aborted, never Complete
               covers(..until, what) on Contiguous Finite; also::<Q> keeps the type
               take_at_least::<Pos>(n) / take_until over FnMut take_while
```

No public `Recv`, `pump` or spawn policy (D16): `Resumable` is a private
`Source`, so `until` and `merge` keep working lazily. `defer_async` takes its
`Handle` explicitly; a dropped deferred phase aborts its task (D26.7).

## Declarations the design produces

- Archive, compaction merge feeder: `chain_all(defer_async(read_snapshot))`,
  ordered, `covers`, `also` (HLC).
- `HistoricalReplay`: scan → `stitch(archive)` → `covers` → `also` (HLC), lazy.
- Snapshot window: plain scan → `take_at_least::<Hlc>(cap)` → peek →
  `write_snapshot` → `SnapshotWrite { count, last, .. }`; watermark rule in
  the job.
- Stable log loader: `dedup_by_key` → `filter_above` → `ordered`.
- nvzn live range: stable subscription → `until` → `take_until` → `filter_map`
  → `chain(once(ReplayDone))`; no sentinel.

## What I expect at sign-off

- Most likely comments: naming (`also`, `covers`, `take_at_least`, `Closed`,
  `Opening`), and whether the kind parameter's verbosity needs aliases. My
  answer: aliases are fine if they don't default the kind (D1).
- Possible pushback: slice 3's breadth (every signature gains `K`). Mitigation
  is already in the order: replica path end to end first.
- Unlikely but consequential: the exact-cap empty iteration (D24) or
  `Subscribers::publish` warning-and-continuing on `Stopped`.

## Open risks for implementation

- The kind parameter touches every test double; slice 3 is the widest churn.
- `Kind::Proof` with async bodies: the `Send`-ness of the body's future must
  be required by the constructor's bounds, not inferred.
- `defer_async`'s hand-off must stay selectable (a crossbeam receiver), or
  `chain_all`/`merge` around it would block.
- `take_at_least` drops the first next-group item, and the producer may have
  read ahead up to one buffer; fine for snapshots, worth a test.
