# zslg / nvzn crate split — design

Process: `nlnr/docs/iaic.md`. Decisions D1–D15 are in
`2026-11-07_log_crate_split.md`; this document is the spec. Status: second pass
done (more crates, import patterns, corners), waiting for sign-off. Branch
`ccr-3ceece9d-e3tfbt`, base `6aa354b`.

Reading order: §1 is the whole idea; §2 states the roles and rules; §3 shows the
corners the first draft walked into and how the structure avoids them; §4–§6
deepen the foundation, implementations and assemblies; §7–§8 cover testing and
landing; §9 nvzn; §10.4 lists what to look at.

---

## 1. The idea

zslg (50k lines incl. tests) and nvzn (6k) are one crate each. This change
cuts them into ~70 small, internal crates so that the layering — today a
convention the module graph already violates — is enforced by Cargo.

```text
  consumers (btrb, nlnr, nvzn's users) ──> zslg (facade: re-exports only)
                                              │
  assemblies    nodes (writer, learner, follower, replica) ─> learner, leader,
                raft-setup, observability                     (wiring, no logic)
                                              │
  implementations   database · object-store · raft-* · net-* · stable-log ·
  (siblings)        reader · replica-* · leader-* ...    one port each, no edges
                                              │
  procedures        quorum-read · net-wire     (shared logic over the foundation)
                                              │
  ports             zslg-time · -log · -snapshot · -consensus · -net   (traits;
                                               every crate has its own error union)
                                              │
  types             zslg-<group>-types: lifecycle · time · log · snapshot · consensus · raft · net
                                              │
  libraries         bytesize · warn-throttle · lifecycle · shutdown · framing ·
                    metrics-kit · database · error-union
  fakes             zslg-<group>-fake, next to each ports crate
```

Each layer may depend only on layers below it; inside the implementation layer
there are no edges at all. That one rule is what makes the layering
enforceable, and every other rule below exists to keep it from forcing a bad
move.

### 1.1 Goals, non-goals, success criteria

Goal (D1): layering enforced by the compiler, yielding many small, targeted,
non-reusable crates; more crates whenever a piece splits off cleanly (D7, D14).
Build and test time are welcome side effects. Moving code and introducing new
traits to decouple is allowed.

Non-goals: no behaviour, wire-format or metric-name change, no new features,
no consumer-visible API change other than the error nesting in §4.4. Crates are
`publish = false`.

Success criteria, checked at every commit (§8): fmt, clippy `-D warnings`,
full test suite green; `describe_metrics()` byte-identical to before; btrb,
nvzn and nlnr build against the facades; at the end the layering test (§8.3)
passes.

### 1.2 Current state

The module graph has cycles, but they are shallow: four misplaced items (§4.3)
plus port signatures and errors that already cross concerns (§3). Inside the
big subfolders the files are largely independent. The consumer surface is
small: nlnr uses `zslg::Hlc` and `zslg::ObjectStoreConfig`; nvzn uses ports
and foundation types; btrb is the composition root for `ReplicaNode`.

### 1.3 Constraints

- The workspace denies `wildcard_imports`; every crate carries
  `[lints] workspace = true`; moves rewrite imports explicitly, no shims.
- The orphan rule: a trait impl lives with the trait or the type (§4.2).
- nlnr pins kompakt by git rev and cargo finds packages by name, so crate
  count and layout are invisible to consumers (D11, D15).
- Cross-crate calls lose inlining; hot-path leaf functions get `#[inline]`.

---

## 2. Roles and rules

### 2.1 The roles

Every crate has one role, readable from its name (D15). "May depend on" is for
normal dependencies; dev-dependencies follow §7.3.

| role | names | contents | may depend on |
|---|---|---|---|
| library | unprefixed, workspace-wide (D18): `bytesize`, `warn-throttle`, `shutdown`, `framing`, `metrics-kit`, `error-union`; like `crossbeam-combinators` | code that knows no domain of any family | other libraries; never a `zslg-*`/`nvzn-*` crate |
| types | `zslg-<group>-types` | data, invariants over it, shared cells, the group's leaf error, label enums | libraries, lower types |
| ports | `zslg-<group>` | traits, the crate's error union; re-exports its group's types | types, lower ports, libraries |
| procedure | `zslg-quorum-read`, `zslg-net-wire` | shared logic over the foundation, implements no port | ports, types, libraries |
| fake | `zslg-<group>-fake` | in-memory port implementations | ports, types, libraries |
| implementation | `zslg-<what>` | implements one or a few ports | everything below, nothing beside |
| assembly | `zslg-<role>-node`, `zslg-learner`, `zslg-leader`, `zslg-raft-setup`, `zslg-observability` | wiring of implementations | implementations, lower assemblies, everything below |
| facade | `zslg` | `pub use` only, integration tests, examples | assemblies, ports |

### 2.2 The import rules

Three rules keep the import patterns boring:

1. **One path per item per layer.** Outside the types layer, a type
   is imported through its group's ports crate (`use zslg_log::{Reader, Data,
   LogIndex}`), never from `zslg-log-types` directly. This is the import
   pattern of D3; the types crates exist for the dependency graph, not for
   import sites.
2. **No sideways edges among implementations**, not even as dev-dependencies
   (§7.3). A need for one is a missing port, procedure or assembly.
3. **Assemblies wire, they do not decide.** An assembly holds configuration,
   construction and task spawning. Logic found in an assembly during the move
   goes down into an implementation or procedure.

The layering test (§8.3) checks all three from `cargo metadata` and the crate
names.

---

## 3. Corners: where the first draft would have forced a bad move

The first draft had five group crates, each types *and* ports, in a chain
`time ← log ← snapshot ← consensus ← net`, with per-group errors returned by
ports and the facade as the only composition root. Tracing the real edges shows
five places where that structure leaves only bad moves.

### 3.1 Port signatures cross groups

```text
StableLog, StableArchiveReader (log)   need SnapshotEntry          -> it is {LogIndex, Data}: a log type
Replica (log)                          needs RaftState, ReplicaStatus  (consensus)
ReplicaClient (net)                    bundles LivenessProbe + RaftMembership + RaftTransport + Replica
SnapshotStore (snapshot)               needs ObjectStoreReason, a metric label enum
```

In a chain of types-and-ports crates each of these is an upward edge; the only
moves would be to drag types into the wrong group or to fuse groups. Splitting
each group into a types crate and a ports crate removes the problem: the types
crates form a chain, and a ports crate may use the types of *any* group. The
chain only has to hold for data, where it is natural.

### 3.2 Errors cross groups

Implementations raise errors from several groups today: `learner/read.rs`
raises `NotReady`, `ObjectStore`, `Lagged`; `replica/mod.rs` raises `HlcCutoff`,
`NotAReplica`, `Shutdown`; `snapshot_store.rs` raises `Storage` and
`StreamTruncated`. If a port could return only its own group's error (first
draft of D4), the only moves would be a catch-all `Other(Box<dyn Error>)` or an
upward edge. The types/ports split of §3.1 opens a third move: a crate's error
is a *union* of the leaf errors it can carry, from any group (D16, §4.4). The
union stays flat — it holds leaves, never another crate's union — so the depth
is always `Crate::Leaf::Variant`, and converting one union into a wider one is
generated and checked by the compiler.

### 3.3 openraft would leak into everyone

`TypeConfig`, `NodeInfo`, `RaftResponse`, `FencedStateWrite` are openraft
vocabulary. In the consensus group they would make every crate that needs a
`NodeId` compile openraft's generics. They get their own `zslg-raft-types`,
which only the raft-facing ports (`RaftConsensus`, `RaftPeer`,
`ClusterBootstrap`) and the net messages depend on.

### 3.4 Shared logic used by two implementations

`ReplicaPoller::check` and `sync_quorum` (`learner/quorum.rs`, 0.55k) are used
by the reader *and* by the leader's snapshot job in production. With only
"implementation" available as a role, the moves are a duplicate or a sideways
edge. They are logic over ports (`Router`, `LocalStoreView`, `QuorumCheck`) and
implement none, so they become the first **procedure** crate, `zslg-quorum-read`.
Pure rules over data (`validate_transition`, used by raft-storage, the
integrator and membership) and shared cells (`RaftStateHandle`,
`EvictionStatus`) are not procedures; they go into the types crates.

### 3.5 One composition root becomes the new grab bag

The four node files (2.8k lines) plus `setup_raft`, the leader loop,
`spawn_learner` and observability would all land in the facade, and each node
would import a dozen implementation crates. The read side is assembled the
same way in three nodes (`spawn_learner`, `LearnerService`, `GatedReader`). So
composition gets its own layer: `zslg-learner`, `zslg-leader`,
`zslg-raft-setup`, `zslg-observability`, and one crate per node. The facade is
reduced to `pub use`, integration tests and examples.

---

## 4. The foundation

### 4.1 Types crates

```text
zslg-time-types        Hlc, ClockError, Key impl for Hlc
zslg-log-types         Data, DataId, LogIndex, Stable/UnstableEvent, SnapshotEntry,
                       LogError, Key impls, LogType label
zslg-snapshot-types    SnapshotId, SnapshotMetadata, CompressionLevel, SnapshotError,
                       ObjectStoreReason/Op labels
zslg-consensus-types   NodeId, RaftGeneration, RaftState, ReplicaMembership, Exit,
                       ReplicaStatus, RaftStateHandle, EvictionStatus,
                       validate_transition, is_quorum, ConsensusError, RaftStateError
zslg-raft-types        TypeConfig, NodeInfo, RaftResponse, FencedStateWrite, RaftLogIndex
zslg-net-types         PeerAddr, HostPort, ServerName, RPC messages, NetError
```

The chain is `time ← log ← snapshot ← consensus ← raft ← net`. Each types crate
depends on `metrics-kit` for its label enums (§4.5).

### 4.2 Ports crates

`zslg-time` (`Clock`), `zslg-log` (`Reader`, `Writer`, `StableLog`,
`LocalStoreView/Mut`, `StableArchiveReader`, `Replica`, the replica submit port
of §5.4), `zslg-snapshot` (`SnapshotStore`), `zslg-consensus`
(`RaftConsensus`, `RaftPeer`, `RaftLeadership`, `RaftMembership`,
`StateReplicator`, `RaftStateView`, `QuorumCheck`, `SpareSelection`,
`MembershipGate`, `ClusterBootstrap`), `zslg-net` (`Peers`, `Router`,
`ReplicaClient`, `LivenessProbe`, `LivenessQuery`). Each re-exports its group's
types (§2.2 rule 1).

`ordered.rs` is split by the orphan rule: each `Key`/`Successor` impl goes to
the crate defining the type (`Hlc` → time-types; `LogIndex`, `DataId` →
log-types).

### 4.3 The four misplaced items

Each is a prerequisite commit of the crate that needs it:

```text
TypeConfig & co (raft/types.rs)         -> zslg-raft-types
CompressionLevel (object_store/)        -> zslg-snapshot-types
Clock port vs HlcClock impl (hlc/)      -> port in zslg-time, impl in zslg-hlc-clock
RaftStateHandle, EvictionStatus         -> zslg-consensus-types
```

Fields that were `pub(crate)` (`NodeId(pub(crate) Uuid)`) get the smallest
accessor implementation crates need; nothing new reaches the facade.

### 4.4 Errors: leaves and flat unions (D4, D16)

**Leaves** are `thiserror` enums, one per group, in the types crates:
`ClockError`, `LogError`, `SnapshotError`, `ConsensusError`, `NetError`; plus
`LifecycleError` (`Cancelled`, `Shutdown`, `NotReady`, `StartupCancelled`,
`HardCrashDataLoss`) and `ConfigError` (`InvalidConfiguration`) in the
`zslg-lifecycle-types`, the bottom of the types chain (its `StartupCancelled`
and `HardCrashDataLoss` are zslg semantics, so it is not a library).

**Unions** are declared with `error_union!` from the `error-union`
library (~60 lines of `macro_rules!`). Every crate with fallible functions on
its boundary — ports crates, implementations, procedures, assemblies — has one:

```rust
error_union! {
    pub ReadError { Log(LogError), Snapshot(SnapshotError), Lifecycle(LifecycleError) }
    from ArchiveError { Log, Snapshot }   // From<ArchiveError> for ReadError
}
```

The macro generates the enum, `From<Leaf>`, `Display`, `Error::source` and the
listed union→union `From`s. The compiler enforces the subset rule both ways:
a target without one of the source's leaves fails (E0599), and a `from` list
that omits one of the source's leaves fails (E0004, non-exhaustive match).
Prototyped across three crates before writing this. The convention that makes
it work: a leaf has the same variant name in every union — its group name.

Channel seams (§3.1: ten port signatures carry `Result<_, E>` in channel items)
convert with one `map_err(Into::into)` per source; the generated `From`s make
that the only adaptation.

`zslg::Error` (public, D11) is the union of all leaves, declared in the facade.
The visible change for consumers is the nesting: `Error::NotLeader { .. }`
becomes `Error::Consensus(ConsensusError::NotLeader { .. })`. nlnr does not
match on `zslg::Error`; nvzn's uses are `Result<_, zslg::Error>`.

### 4.5 Metrics

`metrics.rs` (2.7k) is cut at its `// ── section ──` markers (D5).
`metrics-kit` keeps the mechanism (`MetricDef`, `MetricKind`, the registry
macro, `label_enum!`, probes, `describe_all`). It is a workspace-wide library
like `crossbeam-combinators`, not part of zslg's layering, so nvzn uses it too
(D17). Two things make today's mechanism zslg-specific and are moved out when
the kit is extracted: the hard-coded `"zslg_"` prefix becomes the macro's
namespace argument (the internal `metrics_impl!` already takes one), and the
`hlc` row form (`(arg: Hlc)` → `set_hlc_gauge`) becomes a generic
"gauge from a conversion" row, with the `Hlc` → physical-ms conversion living
in `zslg-time-types`. Label enums that appear in port
signatures or are shared by two implementations live in the types crates;
everything else lives with the crate that emits it, as a `METRICS` slice plus
emission helpers. `zslg-observability` concatenates them in
`describe_metrics()` and tests name uniqueness; the `zslg_` namespace is
applied by the macro, so names do not change (§8.2 golden test).

---

## 5. Implementations

### 5.1 The list

Source paths are under `crates/zslg/src`.

| crate | source | lines | implements |
|---|---|---|---|
| `zslg-database` | `database/` | 1.3k | **open** (§5.5) |
| `zslg-object-store` | `object_store/{mod,snapshot_store}.rs` | 1.1k | `SnapshotStore`; owns `s3` |
| `zslg-object-format` | `object_store/framing.rs` | 0.4k | candidate (§5.6) |
| `zslg-hlc-clock` | `hlc/clock.rs` | 0.6k | `Clock` |
| `zslg-hlc-gate` | `hlc/{gate,cutoff}.rs` | 0.7k | write admission |
| `zslg-writer` | `writer.rs`, `quorum/{coordinator,fanout}.rs` | 1.3k | `Writer` |
| `zslg-net-client` | `network/{client,transport}/` | 1.6k | `RaftPeer`, `ReplicaClient` over RPC |
| `zslg-net-server` | `network/server/` | 0.7k | RPC dispatch to ports |
| `zslg-net-router` | `network/router/` | 0.4k | `Router` |
| `zslg-raft` | `raft/{node,membership}.rs` | 0.4k | consensus ports + `ClusterBootstrap` |
| `zslg-raft-startup` | `raft/startup/` | 1.3k | the startup procedure (§5.2) |
| `zslg-raft-storage` | `raft/raft_storage/` | 2.5k | openraft storage traits |
| `zslg-raft-rpc` | `raft/rpc/` | 0.2k | openraft network factory |
| `zslg-raft-state` | `raft/state/state_publisher.rs` | 0.5k | state gauges, leadership events |
| `zslg-raft-tracker` | `raft/tracker/` | 0.5k | `RaftStateTracker` |
| `zslg-local-store` | `learner/local_store/` | 2.5k | `LocalStoreView/Mut` |
| `zslg-stable-log` | `learner/stable_log/` | 2.3k | `StableLog` |
| `zslg-stable-archive` | `learner/stable_archive/` | 0.2k | `StableArchiveReader` |
| `zslg-subscriber` | `learner/subscriber/` | 0.8k | pulls replica logs into `LocalStoreMut` |
| `zslg-reader` | `learner/read.rs` | 2.2k | `Reader` |
| `zslg-replica-log` | `replica/{log,purge}.rs` | 2.4k | replica submit port |
| `zslg-replica-service` | `replica/mod.rs` | 0.9k | `Replica` |
| `zslg-leader-integrator` | `leader/integrator.rs` | 1.1k | the single Raft writer |
| `zslg-leader-membership` | `leader/jobs/{membership/,voters.rs}` | 3.5k | |
| `zslg-leader-snapshot` | `leader/jobs/{snapshot/,cutoff.rs,cleanup.rs}` | 2.9k | |
| `zslg-leader-compaction` | `leader/jobs/compaction/` | 0.9k | |

### 5.2 Raft: decide, then execute (D14)

`zslg-raft-startup` holds the bootstrap/join/rejoin decision procedure and
talks to `ClusterBootstrap` in `zslg-consensus`:

```rust
trait ClusterBootstrap {
    async fn initialize(&self, members: Membership) -> Result<(), Error>;
    async fn write_fenced(&self, w: FencedStateWrite) -> Result<RaftResponse, Error>;
    async fn wait_leader(&self, within: Duration) -> Result<NodeId, Error>;
    fn metrics(&self) -> watch::Receiver<RaftMetricsView>;
}
```

`RaftNode` implements it over openraft. The verbs are what startup calls today;
if the move finds another call into openraft, I stop and ask (§8.1) rather than
adding a back door. `raft-storage` and `raft-rpc` implement openraft's own
traits, which are their ports. `setup_raft` is wiring and goes to
`zslg-raft-setup`.

### 5.3 Leader and learner

Leader jobs depend on consensus, log and snapshot ports, `zslg-quorum-read`
for replica freshness, and `time`. In production they already go through ports
(`LocalStoreView`, `SnapshotStore`, `RaftStateView`); only their tests use the
concrete stores (§7.3). The reader splices `StableLog`, `StableArchiveReader`
and `LocalStoreView` and never names a concrete store.

### 5.4 Network and replica

Client, server and router use `zslg-net-wire` (the RPC codec and TCP layer; a
procedure, since its envelope carries `Hlc` and `NodeId`) and ports, nothing
else concrete. `StaticPeers` and `PermissiveGate`
move to `zslg-net-fake`. `zslg-replica-service` calls the replica log through
a small submit/fence port in `zslg-log`, shaped by what `replica/mod.rs` calls
today.

### 5.5 The node's RocksDB: domain-free engine, domain schema (open)

`database/` is used by raft-storage, stable-log, local-store and replica-log,
so it must be a library or a procedure. The first draft called it a library;
the general rule of D18 (a library knows no domain of any family) shows it is
not: `Subsystem` enumerates zslg's column families (`StableLog`, `ReplicaLog`,
`LocalStoreData`, `LocalStoreCursors`, `RaftLog`, `RaftMeta`, `NodeId`), and
`node_id.rs` is zslg's write-once node identity. The mechanism itself
(`storage.rs`, 1.1k: one explicit open, the caller states root, memory budget
and the complete column-family list — "the descriptor list is the schema") is
domain-free.

So it splits: an unprefixed `rocksdb-kit` (the open, budget, block cache,
typed column-family handles), `zslg-node-id` (an implementation), and the
schema. Where the schema lives is open — a shared `Subsystem` enum in a types
crate, or each storage implementation declaring its own column family with
the assembly listing them (the D5 pattern). To be decided with the user.

The test stays: anything used by two implementations is a library if it knows
no domain, a procedure if it works over the foundation, otherwise a missing
port — and then I ask.

### 5.6 Candidate cuts, confirmed commit by commit

Each is its own commit; if a commit finds shared private state, I stop and
ask (§8.1) instead of forcing it.

```text
local_store/   recent_ids.rs (0.2k, bounded DataId dedup set)  -> library candidate
               key.rs, purge.rs, store.rs (1.7k)
stable_log/    snapshot_loader.rs (0.6k)                       -> own implementation
leader membership   change.rs | liveness.rs
hlc/           gate.rs | cutoff.rs
object_store/  framing.rs | snapshot_store.rs
learner/       read.rs (2.2k) — look for the splice vs. gating split (GatedReader)
replica/       log.rs (2.2k) — flush loop vs. store
```

---

## 6. Assemblies

| crate | source | wires |
|---|---|---|
| `zslg-raft-setup` | `raft/mod.rs` (`setup_raft`) | raft, raft-storage, raft-rpc, raft-state, net-router |
| `zslg-learner` | `learner/mod.rs` (`spawn_learner`, `LearnerService`, `GatedReader` wiring) | local-store, stable-log, stable-archive, subscriber, reader |
| `zslg-leader` | `leader/mod.rs`, `leader/jobs/mod.rs` | integrator and the job crates |
| `zslg-observability` | `observability.rs`, process/jemalloc gauges | metrics aggregation, `prometheus`, `jemalloc`, `tokio-console` features |
| `zslg-writer-node` | `writer_node.rs` | raft-setup, writer, net-* |
| `zslg-learner-node` | `learner_node.rs` | raft-setup, learner |
| `zslg-follower-node` | `follower_node.rs` | raft-setup, learner |
| `zslg-replica-node` | `replica_node.rs` (1.5k) | raft-setup, learner, leader, replica-* |

Node crates depend on the lower assemblies, never on each other. The facade
`pub use`s the node types, their configs, `Error`, the public foundation
types and `describe_metrics`/`configure_prometheus`; the list equals today's
`lib.rs`. Features are forwarded under today's names.

---

## 7. Fakes, tests, hooks

### 7.1 Three kinds of test support (D9, D10)

Fakes of ports in `zslg-<group>-fake`; the 11 `_for_test` fault hooks as a
`testing` feature on the owning implementation; the cluster harness and
integration tests in the facade.

### 7.2 Consolidating the fakes

Duplicated doubles become one each when their fake crate is created:
`FakeLeadership`/`StaticLeadership`, the two `StaticLiveness`,
`FakeReplica`/`FakeUpstreamReplica`, `FakeRaft`/`NoopReplicator`, plus
`StubLocalStore`, `NoOpCheck`, `NoOpRaftViewer`, `NoOpProbe`. `zslg-log-fake`
also takes `nvzn::testing::TestLog`.

### 7.3 Tests that need a real sibling

Some unit tests use real siblings: the leader's snapshot and compaction tests
build a real `LocalStore`, `StableLog` and `RemoteSnapshotStore`. A
dev-dependency from one implementation to another would be the one sideways
edge the structure cannot see, so these tests move to the lowest assembly that
depends on both (`zslg-leader`) or are rewritten against the fakes when they
only need the port. Which one is decided per test in its commit; the default
is to move, because rewriting a test changes what it proves.

---

## 8. Landing the change

### 8.1 One PR, one crate per commit (D13) — and stop when the plan breaks

One branch, one PR, one commit per crate, each green; prerequisite moves are
their own commits before the crate that needs them. **If something does not
split the way this document says — a missing port, shared private state, a
test that cannot move — I stop and ask how to proceed.** No improvised
traits, re-export shims, `pub` back doors or fused crates to get past it.

### 8.2 Gates at every commit

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
describe_metrics() golden       (names, kinds, units, help — identical)
```

At milestones (after §11 step 5, step 12, the end): btrb and nvzn examples build;
nlnr builds against the facade; loadgen before/after; a clean build and an
incremental build of one leaf crate are timed for the post document.

### 8.3 The layering test

A test in the facade reads `cargo metadata`, assigns each workspace crate its
role from the name (§2.1), and fails on any normal or dev dependency the role
table does not allow, and on any crate outside types/ports that depends
on a `-types` crate directly (§2.2 rule 1), and on an `error_union!` that
names another union as a leaf. An unprefixed crate (a library) must not
depend on any `zslg-*` or `nvzn-*` crate; `btrb` is the one application and
is listed explicitly. It is added in the first commit so every later commit
is checked.

---

## 9. nvzn

Same rules (D12). nvzn has one domain and one error, so it has no
types/ports split and no error crate.

| crate | role | source |
|---|---|---|
| `nvzn-types` | types | `ids.rs`, `events.rs`, `view.rs`, `projection.rs` traits, `Error`, `FoldState` |
| `nvzn-store` | ports | `Store`, `ReadTransaction`, `WriteTransaction`, `store/config.rs` |
| `nvzn-store-overlay` | procedure | `store/overlay.rs` (`CommitBatch`, `WriteTransactionImpl`), shared by both stores |
| `nvzn-store-rocksdb` | implementation | `store/rocksdb.rs` |
| `nvzn-store-fake` | fake | `store/inmemory.rs` |
| `nvzn-resultset` | implementation | `resultset/` |
| `nvzn-dispatcher` | implementation | `dispatcher/` |
| `nvzn-live` | implementation | `live.rs` |
| `nvzn-reader` | assembly | `reader.rs` |
| `nvzn` | facade | `lib.rs`, `testing.rs` |

Corner found in this pass: `live` and `reader` both read the dispatcher's
`FoldState` and `Subscriptions`, and `reader` returns `live::Live`. `FoldState`
is a shared cell (69 lines) and goes to `nvzn-types`; `reader` is wiring over
`live`, the store and the dispatcher's subscriptions, so it is an assembly. The
open point is `Subscriptions` (dispatcher's live-subscriber registry, read by
`reader`): it is either a shared cell (types) or a port the dispatcher
implements. I'll decide it with you when the move gets there, not by
improvising (§8.1).

Metrics follow D5 like zslg's (D17): nvzn today hand-writes its own
`MetricKind`/`MetricDef`/`METRIC_PREFIX` and ~40 name constants; these move
onto `metrics-kit` with namespace `"nvzn_"`, the vocabulary goes to the crate
that emits it (dispatcher, live, reader, store-rocksdb), label enums shared by
two crates (`StopReason`, which is also public) go to `nvzn-types`, and the
`nvzn` facade concatenates the slices in `describe_metrics()`. A golden test
pins nvzn's names as §8.2 does for zslg.

---

## 10. Alternatives, edge cases, what to check

### 10.1 Alternatives rejected

One `core` crate; a single error crate below everything; associated error
types on ports; a central metrics crate; a `zslg-support` crate; nvzn
importing group crates; nested directories; a PR per layer or per crate;
implementations depending on each other. From this pass: types and ports in one
crate per group (§3.1); ports returning only their group's error (§3.2);
a composed `zslg-error` crate below the ports (superseded by D16);
`error_set` (cannot aggregate sets across crates — verified) and `terrors`
(works across crates, but type-level sets replace `match` at ~140 sites);
the facade as the only composition root (§3.5); dev-dependencies between
implementations (§7.3).

### 10.2 Edge cases

- `RUST_LOG=zslg=debug` must still match `zslg_raft::…`. `EnvFilter` target
  directives match by prefix, so it should; the first extraction commit
  verifies it.
- Features (`s3`, `prometheus`, `tokio-console`, `jemalloc`,
  `heap-profiling`) live in the owning crate; the facade forwards them.
- `pub(crate)` ports become `pub` in internal crates; the facade's `pub use`
  list is unchanged.
- Hot-path functions on `Hlc`, `LogIndex`, `DataId` get `#[inline]`; loadgen
  before/after catches a regression.

### 10.3 Complexity

Essential: a port set that actually decouples (`ClusterBootstrap`, the
replica submit port, a few verbs on existing ports) and a role table that
says where any new piece goes. Accepted: ~70 crates, more `Cargo.toml` files,
public-in-internal visibility. Avoided: shims, wildcard re-exports, duplicate
fakes, sideways edges.

### 10.4 Please look at

Changed in this pass:
1. §3.1/§4.1–4.2: each group is a types crate and a ports crate (+6 crates,
   incl. `zslg-raft-types`). Ports re-export their types (§2.2 rule 1).
2. §3.2/§4.4: errors are `thiserror` leaves per group plus a flat
   `error_union!` per crate (D16); no global error crate. D6's `zslg-cancelled`
   becomes `zslg-lifecycle-types`, since it now holds the lifecycle leaf.
3. §2.1/§3.4: new role *procedure* (`zslg-quorum-read`, `zslg-net-wire`,
   `nvzn-store-overlay`).
4. §3.5/§6: new role *assembly* (8 crates); the facade only re-exports.
5. §7.3: no dev-dependencies between implementations; such tests move up.
6. §9: nvzn's `reader` is an assembly; `Subscriptions` is an open point.
7. §4.5/§9: `metrics-kit` is workspace-wide (D17); nvzn applies D5 and the
   central `nvzn-metrics` crate is gone.

Carried over: `ordered.rs` split by the orphan rule (§4.2); libraries usable
by implementations (§5.5); the nested `Error` match
(§4.4).

---

## 11. Implementation order

A linearisation of the DAG; each line is one or more commits, prerequisites
first.

```text
 1  layering test (roles from names; passes trivially on today's two crates)
 2  libraries: metrics-kit (namespace argument, generic conversion row), bytesize,
    warn-throttle, shutdown, framing, error-union, rocksdb-kit (pending §5.5)
 3  types: lifecycle, time, log, snapshot, consensus, raft, net  (+ the four moves of §4.3)
 4  ports: time, log, snapshot, consensus (+ ClusterBootstrap), net
 5  fakes: time, log (+ TestLog), snapshot, consensus, net
 ── checkpoint: foundation enforced; one big implementation crate left; all green
 6  procedures: quorum-read, net-wire
 7  object-store, hlc-clock, hlc-gate
 8  net-client, net-server, net-router
 9  raft-storage, raft-rpc, raft-state, raft-tracker, raft, raft-startup
10  local-store, stable-log, stable-archive, subscriber, reader
11  replica-log, replica-service, writer
12  leader-integrator, leader-membership, leader-snapshot, leader-compaction
 ── checkpoint: no implementation code left in zslg
13  assemblies: raft-setup, learner, leader, observability, the four nodes
14  facade reduced to pub use + integration tests + examples
15  nvzn: metrics onto metrics-kit (names pinned by golden test), types, store,
    store-overlay, store-rocksdb, store-fake, resultset, dispatcher, live,
    reader, facade
```

After step 15 a post document records the review, surprises and build-time
numbers.
