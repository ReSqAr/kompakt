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

## Open

- Q4: the `Error` enum (33 variants, names Hlc, NodeId, RaftStateError) is
  returned by the ports in every group — where does it live?
