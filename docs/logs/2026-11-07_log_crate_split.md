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

### D7. Implementation granularity: per subfolder, finer where a concern is found

Between "one crate per subfolder that implements a port" and "also split inside
local_store / stable_log / membership": start from the subfolder cut and factor
out any smaller concern the design doc finds. Tiny glue stays with its owner.
User on raft: coupling that forces `startup`/`state`/`tracker` to stay together
is a smell in itself, to be investigated rather than accepted.

## Finding: raft's coupling is a hub of vocabulary, not tangling

`raft_storage`, `rpc`, `startup`, `state`, `tracker` do not import each other.
Each imports the same hub: `raft/types.rs` (TypeConfig, NodeInfo,
RaftStateError, RaftLogIndex, RaftResponse, FencedStateWrite) and, for
`startup`, the concrete `RaftNode`. The hub types are foundation (`consensus`
group) material. `RaftNode` already implements three ports (`RaftConsensus`,
`RaftLeadership`, `StateReplicator`).

### D8. Implementation crates depend only on foundation ports

Layering is two levels: foundation (types + ports per group, errors, metrics
kit, primitives) -> implementation crates (siblings, no edges between them) ->
`zslg` facade, the composition root and the only crate that names concrete
types from several implementation crates. Ports grow where a consumer needs
them (e.g. `RaftNode::proposals`/`snapshot`, used by leader). Rejected:
implementations depending on each other's concrete types (deep chains, any raft
change rebuilds leader/learner/replica, layering only enforced at the
foundation).

### D9. Test support splits by kind

Three kinds, three homes: fakes of ports (own crate(s), foundation-only
deps, dev-dependencies of implementation crates); the 11 fault-injection hooks
(`testing` feature on the owning implementation crate, enabled by facade
tests); the whole-system harness (`tests/common`, integration tests) stays in
the facade. Hooks that no port can express are candidates for new ports in the
design doc. Granularity of the fakes crate(s) left to Q10 (user asked whether a
fake can be its own crate next to the real implementation).

Evidence for shared fakes: `RaftLeadership` is faked twice
(`FakeLeadership`, `StaticLeadership`), `LivenessQuery` twice (both named
`StaticLiveness`), `Replica` twice, `StateReplicator` twice (`FakeRaft`,
`NoopReplicator`), plus nvzn already ships `nvzn::testing`.

### D10. One fake crate per foundation group

`zslg-time-fake`, `zslg-log-fake`, `zslg-snapshot-fake`, `zslg-consensus-fake`,
`zslg-net-fake`: each depends only on its own group and the groups below it, and
is a dev-dependency of implementation crates (and of nvzn). Fault hooks that
need an implementation's internals stay as a `testing` feature on the owning
crate (D9). Rejected: one `zslg-testing` crate (every test pulls every group),
one fake per port (fakes in a group cooperate, e.g. fake log + fake reader).

## Finding: the consumer surface is small

nlnr uses only `zslg::Hlc` and `zslg::ObjectStoreConfig` and never matches on
`zslg::Error`. nvzn uses only ports and foundation types (`Reader` 28x, `Error`
24x, `Writer`, `ByteSize`, `Hlc`, `DataId`, `StableStream`, `UnstableEvent`,
`Data`, `LogIndex`, `PeerAddr`) plus `configure_prometheus`/`describe_metrics`/
`ReplicaNode::start` in btrb. So after D8 nvzn can depend on the foundation
groups alone, without RocksDB or raft; btrb is the composition root.

### D11. The layering is an implementation detail; `zslg` is the only public surface

Consumers (nvzn, btrb, nlnr) import only from the `zslg` facade, which
re-exports the public API under today's paths. Group, implementation and fake
crates are internal (`publish = false`), imported by each other and by the
facade, never by consumers. nvzn keeps one dependency on `zslg`. Consequence for
D4: `zslg::Error` stays the public error type consumers see; the per-group
errors are how it is built. Rejected: nvzn importing group crates directly
(would make the layering a public contract; the Cargo.toml benefit is not worth
it).

## Finding: nvzn's module graph is already acyclic

```text
ids, events, view, projection (types)   no internal deps
store (Store port + rocksdb/inmemory/overlay)  2.4k, uses ids, metrics
resultset (query ops)                   0.7k, no internal deps
dispatcher (log -> store folds)         2.2k, uses projection, store, ids, metrics
live (wait for projections)             1.7k, uses dispatcher, projection, events
reader                                  0.7k, uses dispatcher, live, projection
```

### D12. nvzn gets the same treatment, in this design

Same rules (D1-D11): ports/types below, implementations as siblings, fakes as
their own crates, `nvzn` the facade. Working cut: `nvzn-types` (ids, events,
view, projection), `nvzn-store` (Store port + shared write machinery),
`nvzn-store-rocksdb`, `nvzn-store-fake` (the in-memory store), `nvzn-resultset`,
`nvzn-dispatcher`, `nvzn-live`, `nvzn-reader`, `nvzn` (facade, `testing`).
Two consequences of earlier rules, found while answering "should the in-memory
impl be separate" (yes):

- `store/overlay.rs` (`CommitBatch`, `WriteTransactionImpl`) is not memory
  code: both the RocksDB and in-memory stores build on it. It belongs with the
  Store port crate, not with either implementation.
- `nvzn::testing::TestLog` is an in-memory implementation of zslg's
  `Reader`/`Writer`: a fake of the `log` group (D10). It moves to
  `zslg-log-fake`; `nvzn::testing` keeps the store helpers and re-exports it.

### D13. One PR, one commit per crate, strictly one crate at a time

A single branch and PR; each commit extracts exactly one crate and is green
(fmt, clippy `-D warnings`, tests). Large moves are fine, mixed moves are not:
if a crate needs a prerequisite move (e.g. one of the four misplaced items), the
prerequisite is its own commit before it. Order is bottom-up along the crate DAG.
Rejected: a PR per layer (user is comfortable reviewing large mostly-move
changes) and a PR per crate (many round trips for pieces that only make sense
together).

## Derived, not asked: feature flags

`s3` (object_store/aws), `prometheus`, `tokio-console`, `jemalloc`,
`heap-profiling` each have one or two `cfg` sites. Each feature lives with its
owning crate (`s3` with the snapshot object-store implementation, the others
with observability/process gauges) and the `zslg` facade forwards it, so
consumers' feature names (`zslg/prometheus`, `zslg/s3`, `zslg/jemalloc`) do not
change.

## Finding: raft `startup` is coupled to the openraft handle, not to its siblings

`startup/` calls `raft.inner().initialize / client_write / metrics / wait`: it
drives an `openraft::Raft` through its lifecycle. `raft_storage` and `rpc` are
openraft's own plug points (storage traits, network factory) and never touch
`RaftNode`. openraft's traits are already the ports for those two, and
`RaftNode::new` already takes a ready `Raft`; only `setup_raft` (adopts
storage, wires router, spawns watchers) joins them, i.e. it is composition.

### D14. Split raft startup from RaftNode behind `ClusterBootstrap`

`zslg-raft-startup` holds the decision procedure (bootstrap vs join vs rejoin)
and depends on a narrow port in the `consensus` group
(`ClusterBootstrap { initialize, write_fenced, wait_leader, metrics }`);
`zslg-raft` (`RaftNode`) implements it over openraft; startup is unit-tested
with `zslg-consensus-fake`. `setup_raft` (adopts storage, wires router, spawns
watchers) is composition and goes to the facade. The port's verbs are shaped by
what startup actually calls; the design doc confirms nothing reaches into
openraft beyond them. Rejected: startup + RaftNode in one crate (the riskiest
protocol logic stays testable only against a real raft).

User addition: more crates are welcome whenever a piece splits off cleanly
(reinforces D7). The design doc therefore does a file-level pass over the large
subfolders (local_store 1.7k `store.rs`, hlc clock/cutoff/gate, quorum
coordinator/fanout, network framing vs client, membership change/liveness,
object_store framing/snapshot_store) and proposes every cut whose pieces share
no private state. First impression from a rough import scan: files inside the
big subfolders are mostly independent of each other.

### D15. Flat `crates/`, role readable from the name

`zslg` (facade), `zslg-<group>`, `zslg-<group>-fake`, `zslg-<implementation>`;
`nvzn`, `nvzn-<what>` likewise. `members = ["crates/*"]` is unchanged. A
`crates/README.md` carries the layer diagram as an index (not a public
contract). Rejected: nested directories by family (shows layering as a tree,
contradicts D11; moves the facade path) and one workspace per family.

## Interview closed

Remaining decisions have no wide-reaching consequences; the open items are
listed in the design doc §8.4 for sign-off. Design doc:
`2026-11-07_design_crate_split.md`.

## Design pass 2 (2026-11-07): more crates, import patterns, corners

User asked for a second pass: can we create even more small crates; does the
structure avoid weird import patterns; above all, do not paint ourselves into a
corner where the only move is a bad one. Process instruction for the
implementation phase: **if something does not work out as planned, ask first
instead of Frankenstein-ing the structure** (design doc §8.1).

Corners found by tracing real edges, and the proposed changes (pending
sign-off):

- P1. Port signatures cross groups (`Replica` needs `RaftState`,
  `ReplicaClient` bundles ports of three groups, `SnapshotStore` names a metric
  label). A chain of types+ports crates turns each into an upward edge.
  Proposal: split each group into `zslg-<group>-types` and `zslg-<group>`
  (ports); types form the chain, ports may use any types. Ports re-export
  their types so import sites keep the D3 pattern. Refines D2/D3.
- P2. Errors cross groups (read path raises NotReady/ObjectStore/Lagged; replica
  service raises HlcCutoff/NotAReplica/Shutdown). Ports returning only their
  group's error leave a catch-all or an upward edge. Proposal: `zslg-error`
  between types and ports; ports return the composed error; group errors stay
  structured inside it. **Changes D4.**
- P3. openraft vocabulary in consensus would leak openraft into every crate
  needing `NodeId`. Proposal: `zslg-raft-types`.
- P4. `ReplicaPoller`/`sync_quorum` are used by the reader and the leader's
  snapshot job in production. Proposal: new role *procedure* (shared logic over
  the foundation, implements no port): `zslg-quorum-read`; `zslg-net-wire`
  reclassified as a procedure (its envelope carries Hlc/NodeId). Pure rules
  (`validate_transition`) and shared cells (`RaftStateHandle`,
  `EvictionStatus`) go into types.
- P5. The facade as only composition root becomes a grab bag (4 nodes, setup,
  leader loop, read-side assembly used by 3 nodes). Proposal: new role
  *assembly*: raft-setup, learner, leader, observability, one crate per node;
  the facade only re-exports.
- P6. Leader tests build real sibling stores. Proposal: no dev-dependencies
  between implementations; such tests move to the lowest assembly or onto the
  fakes, decided per test.
- P7. nvzn: `live` and `reader` read dispatcher state (`FoldState`,
  `Subscriptions`); `reader` returns `Live`. Proposal: `FoldState` to
  `nvzn-types`, `reader` is an assembly, `nvzn-store-overlay` a procedure;
  `Subscriptions` left as an open point to decide with the user during the move.
- `zslg-database` is a library (no zslg domain), usable by the four storage
  implementations.

Crate count rises from ~55 to ~70.
