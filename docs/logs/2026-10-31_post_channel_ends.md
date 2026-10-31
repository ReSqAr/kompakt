# Post — channel ends and ordered channels

Implementation commit: `0a52aaf` on `channel-extraction`.

## 1. Code review

The crate now has a clear split:

```text
  channel.rs   End/AbortReason, Channels/Source tree, take_while, resume_on_abort
  ordered.rs   Key, Successor, policies, OrderedChannel, stitch
  feed.rs      Feed
  fanout.rs    Fanout/Wakes
  blend.rs     Tail/Ranged/blend_chain
```

`OrderedChannel` is a thin boundary newtype; the enforcement lives in
`OrderSource`, so `recv`, `try_recv`, `ready_batch` and `end_reason` share one
policy. `into_channels` is a trivial projection because `End` is reused.

`StitchSource` is the most involved piece. It has two phases (primary and
fallback), buffers the gap-triggering item, verifies the fallback endpoint, and
returns a `Gapless` channel. The `ready_batch` path found a real bug during
implementation: after a primary gap, `drain_first` must switch to the fallback
before polling the primary again, or it overwrites the buffered item.

## 2. Design fidelity

Implemented:

- `End = Complete | Aborted { reason }` with the six public reasons.
- `take_while` ends `Complete`; `resume_on_abort` only resumes on `Aborted`.
- `OrderedChannel<T,P>` with `Key<T>`, `Successor`, explicit `start`, and the
  three policy markers.
- `stitch: StrictlyMonotone<K> -> Gapless<K>` with exact coverage validation.
- `Replica::subscribe_from` as `StrictlyMonotone<LogIndex>`; the learner and
  server writer now share the same contract.
- `Reader::subscribe_stable_log` as `Gapless<LogIndex>`; `HistoricalReplay` is
  rebuilt on `OrderedChannel` + `stitch`.

Deviations:

- `LocalStoreView` HLC/window boundaries remain plain `Channels` for now.
- The archive fallback uses `Handle::block_on` on the blocking replay consumer
  rather than a separate `Feed` + spawned task. The crate remains
  runtime-agnostic.
- No-key fallback items pass through without advancing coverage; this carries
  archive error items while the endpoint check still applies.
- Gap metrics are deferred; `subscription_purge_gaps` is retained but unused,
  and `subscription_reorder` still fires on an order violation.
- The old nvzn duplicate-index fold tests and zslg order-classifier tests are
  removed because the new contracts make those inputs unrepresentable.

## 3. What we learned

- The learner/server disagreement was stale. The replica seam already serves
  `(from, cut]` then live `> cut`; duplicates cannot occur. Strict monotonicity
  was the correct contract, and the old lenient duplicate handling hid bugs.
- `take_while` does not need a verdict. A bound is a clean end of the derived
  stream; bound-vs-short-read is a domain completeness question.
- `stitch` is the right expression for a gap. A gap is not an abort; making it
  one would force normal repair through `resume_on_abort`.
- A trait return type is a wide but worthwhile boundary. The zslg/nvzn stubs
  needed mechanical updates, but the consumer loops became simpler.
- The `ready_batch` path is an important oracle. It exposed a phase bug that the
  `recv` tests did not.

## 4. What is next

- Optional `metrics` feature for accepted gaps.
- Extend `OrderedChannel` to HLC/window `LocalStoreView` boundaries.
- Revisit `take_while` use in nvzn's live scan if the short-read distinction can
  be carried by a domain terminal.
- Run the full workspace/clippy matrix before merging.
