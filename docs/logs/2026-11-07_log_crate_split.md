# zslg / nvzn crate split — decision log

Process: `nlnr/docs/iaic.md`. Branch `ccr-3ceece9d-e3tfbt`, base `6aa354b`.
Status: interview; docs only, no code changes.

Input: user question "should zslg + nvzn be broken into small, non-reusable
crates (zslg-api, zslg-local-store, ...), one per subfolder at least?"

## Context established (introspection, grep-based, approximate)

- zslg ~50k lines incl. tests (leader 10.7k, learner 8.8k, tests 8.2k, raft 6.9k,
  network 4.3k, metrics.rs 2.7k); nvzn ~6k lines, depends only on zslg.
- Module `crate::` edges are cyclic: api<->hlc, raft<->network, and the shared
  modules point upward (error->hlc/network/raft, ids->api/hlc/network,
  meta->quorum/raft, metrics->leader/raft). Folders therefore cannot become
  crates as they stand.
- `leader` and `learner` only depend downwards. `database` depends on
  `error` + `unit` only.
- Public API is a small `pub use` list in `zslg/src/lib.rs`; nlnr consumes
  zslg/nvzn by git rev only.

## Settled

### D1. Driver: layering first, build time a welcome side effect

Goal A (enforced layering, reviewability); B (build/test time) only if it falls
out. Target: a correct layering that yields several small, targeted,
non-reusable crates. Moving code and introducing new boundaries/traits to
decouple is explicitly allowed. Rejected: C (separate release/rev pins) — nlnr
pins one rev.

### D2. Foundation is types + api, implementations above, never below

The cycles are four misplaced items, not a redesign: `TypeConfig` (raft/),
`RaftStateError`/`CutoffError` (raft/, hlc/), `CompressionLevel` (object_store/),
`Clock`/`RaftStateView` (api/ implemented by hlc/). `api/` is already a set of
ports. Rule: implementation crates depend on the ports/types, never the reverse.
Rejected: one fat `core` crate (becomes a dumping ground).
User refinement: not one types crate and one api crate but **several**, grouped
logically by domain; the grouping shows up in the import pattern.

### D3. Five foundation groups, each types + ports

`time` (Hlc, Clock), `log` (Data, DataId, LogIndex, events; Reader, Writer,
StableLog, LocalStore*, StableArchive, Replica), `snapshot` (SnapshotId,
SnapshotMetadata, CompressionLevel; SnapshotStore), `consensus` (NodeId,
RaftGeneration, RaftState, ReplicaMembership, Exit, TypeConfig; Raft* ports,
QuorumCheck, SpareSelection), `net` (PeerAddr, HostPort, ServerName; Peers,
Router, ReplicaClient, LivenessProbe, MembershipGate). Dependencies:
time <- log <- snapshot <- consensus <- net (consensus also needs time).
`meta.rs` and `ids.rs` dissolve into the groups that own each item.
`snapshot` stays separate from `log` (it carries CompressionLevel and the
object-store concern). Names are working names.

### D4. Per-group errors; `zslg::Error` composes them

Each foundation group owns its error (`ClockError`, `LogError`,
`SnapshotError`, `ConsensusError`, `NetError`); ports return their group's
error; `zslg::Error` wraps them with `#[from]`. Rejected: one Error crate below
all groups (cycle, or Strings instead of structured variants) and associated
error types on every port (bounds everywhere). Cross-cutting errors
(`Cancelled`, `Shutdown`) may get their own small crate — only if it turns out
to make sense; decided with the cross-cutting utilities.
Check before implementing: how many places in nlnr match on `zslg::Error`
variants (the nesting is a visible API change).

### D5. Metrics: kit crate + vocabulary in the owning crate

`zslg-metrics-kit` holds the mechanism (MetricDef, registry macro,
`label_enum!`, Probe/guards, `describe_all`); each crate declares its own
`METRICS` slice, label enums and emission helpers; the facade concatenates them
in `describe_metrics()`. Exported names and the `zslg_` namespace do not change.
Cross-crate name uniqueness becomes a facade test; label enums used by two
crates live in the lower crate. Rejected: one central metrics crate (any metric
edit rebuilds everything, defeating the build-time side goal).

### D6. Tiny crates are fine: one crate per domain-free primitive

`ByteSize`, `WarnThrottle`, the supervision tree (`shutdown`) and the
`Cancelled`/`Shutdown` errors each get their own crate rather than a shared
support crate. Default for the two borderline files (not yet confirmed):
`framing` is generic (`[record | crc32]`, used by network and object_store) so it
is its own crate; `ordered` names log types so it stays in the `log` group.
Rejected: one `zslg-support` crate; folding into metrics-kit/facade.

## Finding (not a decision): the implementation layer is nearly acyclic

The `learner -> leader` and `learner -> replica` edges are doc links only.
Remaining production back-edges are small: `RaftStateHandle` and
`EvictionStatus` (raft/, used by learner) are state handles and move down into
the `consensus` group; `leader/jobs/snapshot` vs `learner/stable_log/
snapshot_loader` share snapshot-format knowledge that belongs to the `snapshot`
group.

## Open

- Q7: granularity of the implementation layer (learner 8.8k, leader 10.7k,
  raft 6.9k, network 4.3k, replica 3.4k lines): per top-level module or per
  subfolder?
