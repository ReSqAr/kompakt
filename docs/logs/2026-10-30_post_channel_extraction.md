# Post — extracting `crossbeam-combinators`

## 1. Code review

The extraction landed as one squashed commit on `channel-extraction`.

The crate is small and split by role:

```text
  channel.rs   Channels / Source tree / End / Closer / Cancellation / batch
  feed.rs      Feed (Sender + Closer; the caller spawns)
  fanout.rs    Fanout / Wakes (moved; only visibility and tests changed)
  blend.rs     Tail / Ranged / blend_chain
```

### Review follow-up

Before merge: `run` calls `finish_scan` on every exit (a drop guard, with a
cancel-mid-scan test); `Tail::above` filters the live overlap in all four
seams; the resync warning and metric live in one place; `FoldItem` uses
`TailFailed`/`ScanFailed`/`ScanEnded`; `blend_merge` is gone; README,
`Channels::from` and the docs are current.

`Source` and the node states stay private; the public surface is exactly the
read half, the write half, the verdicts and the seam. The moved core is
unchanged apart from the crossbeam-backed `Closer` and feature-gating
`into_stream`/`from_token`; the moved channel and fan-out tests came along
with the new feed and blend tests (68 all-features; 65 without tokio).

`zslg` deletes `channel.rs`, drops all channel re-exports, and updates 88
`crate::channel::…` references in 38 files to direct
`crossbeam_combinators::…` imports. Its seams are now three small impls:

- `ReplicaSeam`: fan-out registration, frontier, `pump_range`.
- `StableSeam`: stable events, `StableCut { frontier, watermark }`,
  `HistoricalReplay` + trailing watermark.
- `UnstableSeam`: unstable events + watermark moves, `end_hlc`, scan + opening.

`WatermarkGate`, `Held`, the cursor filter, the archive replay and the
reconnect loops stay domain-side. That is the intended complexity split: the
crate owns ordering and verdicts, the domain owns what "covered" means.

`nvzn` gets `LiveSeam` and one consumer (`run`) over
`seam.range(until).merge(tail)`. `Held`, `Coverage`, the cap/overflow policy
and the retire-on-F task are untouched. The live tail is drained while the
scan runs, so the cold-start overrun the old two-thread design existed to
avoid does not occur.

The cost is concentrated exactly where the design predicted: the `Source`
tree and `Select`-per-`recv` are untouched, and the seam is a few lines per
call site. The only wide diff is the mechanical import rewrite.

## 2. Implementation vs design

The design held. The deviations are worth recording:

1. **`nvzn` uses `open` + `range` + `merge`, not `blend_merge`.** `begin` must
   know the cut to set `Coverage::Scanning` before the consumer starts, and a
   merge hides it. `open` is still the only ordering-sensitive call;
   `blend_merge` had no users and was removed after review.
2. **`FoldItem` keeps the seam's source and failure mode separate.** It is
   `Live(Data) | Scan(Data) | ReplayDone | TailFailed(Error) |
   ScanFailed(Error) | ScanEnded(End)`. `Live`/`Scan` keep the `live_scan`
   entry count; the failure variants preserve the two stop reasons
   (`UnstableTailEnded` vs `LiveScanEnded`). The crate still knows none of it.
3. **The scan stops on `Watermark >= cut`, not only `hlc > cut`.** The stable
   read emits its trailing watermark after the replay; a range bounded by HLC
   that waits for an entry *above* the cut waits forever when the log is idle.
   The first test run hung on exactly this; the current `scan` had the
   watermark check, and the `Feed`-based adapter needed it too.
4. **`Channels::closed` was added.** `zslg`'s test fixtures used the private
   `closed_for_test`; the public `closed(rx)` is the same constructor without a
   test-only API.
5. **Housekeeping:** the benchmark moved to the crate; `tokio-stream` moved to
   `zslg`'s dev-dependencies; `#[cfg(test)] use tokio_stream as _;` keeps
   `unused_crate_dependencies` quiet in the lib-test target.

Two design questions resolved better than expected:

- The `Tail`/`Ranged` pair with one domain-chosen item type was sufficient.
  No `BlendItem`, no `MergeItem`, no `verify`, no `split`.
- `Feed::channel` + caller-spawned producer is the right shape. The one
  exception is `HistoricalReplay`, which awaits archive reads and still uses
  `Channels::from` + `Closer` manually; an async feed adapter is a later
  convenience, not a blocker.

The `above` hook is a pure filter. It now filters the live overlap at the cut
in every seam (stable frontier, replica index, unstable/nvzn watermark); the
outer cursor, `WatermarkGate` and `Held` are extra nets, not the only
correctness.

## 3. What we learned

- The seam's essential content is the **order** (`subscribe` before `cut`) and
  the **bound** (`range(cut)`), not the overlap predicate. Trying to encode the
  predicate in a crate token (`verify`, `Covered`) was circular or
  order-specific; leaving it to the domain was correct.
- The boundary is best represented as a domain item. `nvzn`'s `ReplayDone` is
  an ordinary enum variant; `zslg` has none. A crate-provided tagged stream
  would have taxed every `zslg` item for a signal only one consumer needs.
- The `Feed`/`Closer` pairing removes the close-before-last-sender-drop trap
  from every producer site without choosing a runtime.
- Removing `zslg`'s re-exports is a large but mechanical diff; one name per
  type is worth it.
- The frozen core made this a safe extraction: all 657 `zslg` lib tests, 154
  `nvzn` lib tests, 93 `btrb` lib tests and the integration suites pass, and
  the benchmark path is untouched.

## 4. What is next

- **Performance experiment** (separate log): ~10 wait-model / graph
  strategies against the recorded baseline. The public API and seam traits
  should not need to change.
- **Crate docs/README**, and decide whether the crate stays `publish = false`.
- **Async feed adapter** for `HistoricalReplay` (tokio feature), so no producer
  site needs the manual `Closer` pairing.
- **OrderedChannel**, later: a checked ordered seam at the API boundary
  (`Reader` returning `OrderedChannel`) while everything below stays
  `Channels`. It catches order regressions, not gaps; keep it as a
  validation/debug layer first.
- **Downstream**: bump `nlnr`'s `kompakt` revision; CI will run the crate and
  the migrated crates through the affected-crates matrix.
