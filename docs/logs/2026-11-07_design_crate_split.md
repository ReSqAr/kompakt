# zslg / nvzn crate split — design

Process: `nlnr/docs/iaic.md`. Decisions D1–D20 are in
`2026-11-07_log_crate_split.md`; this document is the spec. Status: pass 3 —
all decisions integrated and checked against each other; waiting for sign-off.
Branch `ccr-3ceece9d-e3tfbt`, base `6aa354b`.

Reading order: §1 is the whole idea; §2 states the roles and rules; §3 shows the
corners an earlier draft walked into and how the structure avoids them; §4–§6
deepen the foundation, implementations and assemblies; §7–§8 cover testing and
landing; §9 nvzn; §10.4 lists what to look at.

---

## 1. The idea

zslg (50k lines incl. tests) and nvzn (6k) are one crate each. This change
cuts them into ~75 small, internal crates so that the layering — today a
convention the module graph already violates — is enforced by Cargo.

```text
  consumers (btrb, nlnr, nvzn's users) ──> zslg  (facade: pub use, the public Error
                                              │     union, integration tests, examples)
  assemblies        nodes (writer, learner, follower, replica) ─> learner, leader,
                    raft-setup, observability          (wiring, no logic)
                                              │
  implementations   node-id · object-store · raft-* · net-* · stable-log · reader ·
  (siblings)        replica-* · leader-* ...            no edges between them
                                              │
  procedures        quorum-read · net-wire              (shared logic over the foundation)
                                              │
  ports             zslg-time · -log · -snapshot · -consensus · -raft · -net
                                              │         (traits + the crate's error union)
  types             zslg-<group>-types: lifecycle · time · log · snapshot · consensus ·
                                              │         raft · net   (data + leaf errors)
  libraries         bytesize · warn-throttle · shutdown · framing · metrics-kit ·
                    error-union · rocksdb-kit           (no domain; usable by any family)

  fakes             zslg-<group>-fake, one per ports crate; dev-dependencies only
```

Each layer may depend only on layers below it; inside the implementation layer
there are no edges at all. That one rule is what makes the layering
enforceable; every other rule below exists to keep it from forcing a bad move.

### 1.1 Goals, non-goals, success criteria

Goal (D1): layering enforced by the compiler, yielding many small, targeted,
non-reusable crates; more crates whenever a piece splits off cleanly (D7, D14).
Build and test time are welcome side effects. Moving code and introducing new
traits to decouple is allowed.

Non-goals: no behaviour, wire-format, on-disk or metric-name change, no new
features, no consumer-visible API change other than the error nesting in §4.4.
Crates are `publish = false`.

Success criteria, checked at every commit (§8): fmt, clippy `-D warnings`,
full test suite green; zslg's and nvzn's `describe_metrics()` byte-identical
to before; the node column-family lists identical to today's (§5.5); btrb, nvzn
and nlnr build against the facades; the layering test (§8.3) passes.

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

Every crate has one role. The name suggests it (D15); the authoritative value
is `[package.metadata.layer] role = "..."` in its `Cargo.toml`, which the
layering test reads (§8.3). Names alone cannot carry it: `zslg-leader` (an
assembly) and `zslg-reader` (an implementation) have the same shape.

| role | crates | contents | may depend on |
|---|---|---|---|
| library | unprefixed (D18): `bytesize`, `warn-throttle`, `shutdown`, `framing`, `metrics-kit`, `error-union`, `rocksdb-kit`, like `crossbeam-combinators` | code that knows no domain of any family | other libraries; never a `zslg-*`/`nvzn-*` crate |
| types | `zslg-<group>-types` | data, invariants over it, shared cells, the group's leaf error, label enums | libraries, other types (a DAG, §4.1) |
| ports | `zslg-<group>` | traits, the crate's error union; re-exports its own group's types | types, libraries; other ports only to bundle traits (§4.2) |
| procedure | `zslg-quorum-read`, `zslg-net-wire` | shared logic over the foundation, implements no port | ports, types, libraries |
| fake | `zslg-<group>-fake` | in-memory port implementations | ports, types, libraries |
| implementation | `zslg-<what>` | implements one or a few ports | procedures, ports, types, libraries — nothing beside |
| assembly | `zslg-<role>-node`, `zslg-learner`, `zslg-leader`, `zslg-raft-setup`, `zslg-observability` | wiring of implementations | implementations, lower assemblies, everything below |
| facade | `zslg` | `pub use`, the public `Error` union, integration tests, examples, layering test | everything |

### 2.2 The import rules

Three rules keep the import patterns boring:

1. **One path per item per layer.** Above the ports layer, a type is imported
   through its group's ports crate (`use zslg_log::{Reader, Data, LogIndex}`),
   never from `zslg-log-types`. Ports crates import other groups' types from
   their `-types` crates (that layer is where re-exporting happens). The one
   group without a ports crate, `lifecycle`, is imported from
   `zslg-lifecycle-types` everywhere.
2. **No sideways edges among implementations**, not even as dev-dependencies
   (§7.3). A need for one is a missing port, procedure or assembly — and I ask
   (§8.1).
3. **Assemblies wire, they do not decide.** An assembly holds configuration,
   construction and task spawning. Logic found in an assembly during the move
   goes down into an implementation or procedure.

The layering test (§8.3) checks all three.

---

## 3. Corners: where an earlier draft would have forced a bad move

The first draft had five group crates, each types *and* ports, in a chain
`time ← log ← snapshot ← consensus ← net`, with per-group errors returned by
ports, the facade as the only composition root, and the database as a library.
Tracing the real edges showed where that leaves only bad moves.

### 3.1 Port signatures cross groups

```text
StableLog, StableArchiveReader (log)   need SnapshotEntry       -> it is {LogIndex, Data}: a log type
Replica (log)                          needs RaftState, ReplicaStatus  (consensus)
RaftMembership, MembershipGate,        need PeerAddr, ServerName       (net)
RaftLeadership (consensus)
ReplicaClient (net)                    bundles RaftTransport + RaftMembership + LivenessProbe + Replica
SnapshotStore (snapshot)               needs ObjectStoreReason, a metric label enum
```

In a chain of types-and-ports crates each of these is an upward edge; the only
moves would be to drag types into the wrong group or to fuse groups. Splitting
each group into a types crate and a ports crate removes the problem: the types
crates form a DAG of data, and a ports crate may use the types of any group.

### 3.2 Errors cross groups

Implementations raise errors from several groups today: `learner/read.rs`
raises `NotReady`, `ObjectStore`, `Lagged`; `replica/mod.rs` raises `HlcCutoff`,
`NotAReplica`, `Shutdown`; `snapshot_store.rs` raises `Storage` and
`StreamTruncated`. If a port could return only its own group's error, the only
moves would be a catch-all `Other(Box<dyn Error>)` or an upward edge. With the
types/ports split, a crate's error can be a flat *union* of the leaf errors it
carries, from any group (D16, §4.4).

### 3.3 openraft would leak into everyone

`TypeConfig`, `NodeInfo`, `RaftResponse`, `FencedStateWrite` are openraft
vocabulary. If they sat in the consensus group, every crate needing a `NodeId`
would compile openraft's generics — and by import rule 1 it would reach them
through whichever ports crate re-exports them. So raft is a group of its own
with both crates: `zslg-raft-types` and `zslg-raft` (ports: `RaftConsensus`,
`RaftPeer`, `RaftTransport`, `ClusterBootstrap`). The RPC messages, which embed
openraft requests, live in `zslg-net-wire`, not in `zslg-net-types`. A crate
that only needs `NodeId` or `PeerAddr` never sees openraft.

### 3.4 Shared logic used by two implementations

`ReplicaPoller::check` and `sync_quorum` (`learner/quorum.rs`, 0.55k) are used
by the reader *and* by the leader's snapshot job in production. With only
"implementation" available, the moves are a duplicate or a sideways edge. They
are logic over ports (`Router`, `LocalStoreView`, `QuorumCheck`) and implement
none, so they become a **procedure**, `zslg-quorum-read`. Pure rules over data
(`validate_transition`, used by raft-storage, the integrator and membership)
and shared cells (`RaftStateHandle`, `EvictionStatus`) are not procedures; they
go into the types crates.

### 3.5 One composition root becomes the new grab bag

The four node files (2.8k lines) plus `setup_raft`, the leader loop,
`spawn_learner` and observability would all land in the facade, each node
importing a dozen implementation crates; the read side is assembled the same
way in three nodes. So composition gets its own layer (§6) and the facade
keeps only what is public.

### 3.6 The database knew zslg's schema

`database/` is used by four storage implementations, so it looked like a
library — but its `Subsystem` enum is zslg's column-family schema and
`node_id.rs` is zslg's node identity. Calling it a library would have hidden a
central vocabulary under four siblings. It splits into a domain-free
`rocksdb-kit`, per-owner column families and `zslg-node-id` (D19, §5.5).

---

## 4. The foundation

### 4.1 Types crates

```text
zslg-lifecycle-types   LifecycleError (Cancelled, Shutdown, NotReady, StartupCancelled,
                       HardCrashDataLoss), ConfigError (InvalidConfiguration)
zslg-time-types        Hlc, ClockError, Key impl for Hlc, Hlc -> physical-ms gauge conversion
zslg-log-types         Data, DataId, LogIndex, Stable/UnstableEvent, SnapshotEntry,
                       LogError, Key impls, LogType label
zslg-snapshot-types    SnapshotId, SnapshotMetadata, CompressionLevel, SnapshotError,
                       ObjectStoreReason/Op labels
zslg-net-types         PeerAddr, HostPort, ServerName, NetError
zslg-consensus-types   NodeId, RaftGeneration, RaftState, ReplicaMembership, Exit,
                       ReplicaStatus, RaftStateHandle, EvictionStatus, SpareSelectionStrategy,
                       validate_transition, is_quorum, ConsensusError, RaftStateError
zslg-raft-types        TypeConfig, NodeInfo, RaftResponse, FencedStateWrite, RaftLogIndex
```

The types crates form a DAG: `lifecycle` at the bottom; `time ← log ←
snapshot ← consensus ← raft`; `net` beside them (it needs only `lifecycle`).
Each depends on `metrics-kit` for its label enums (§4.5).

### 4.2 Ports crates

```text
zslg-time        Clock
zslg-log         Reader, Writer, StableLog, LocalStoreView/Mut, StableArchiveReader,
                 Replica, the replica submit port (§5.4)
zslg-snapshot    SnapshotStore
zslg-consensus   RaftLeadership, RaftMembership, StateReplicator, RaftStateView,
                 QuorumCheck, SpareSelection, MembershipGate
zslg-raft        RaftConsensus, RaftPeer, RaftTransport, ClusterBootstrap (§5.2)
zslg-net         Peers, Router, LivenessProbe, LivenessQuery, ReplicaClient
```

`ReplicaClient` bundles traits of four groups, so `zslg-net` depends on the
log, consensus and raft ports crates; it is the only ports→ports edge, and the
layering test pins it. Each ports crate re-exports its own group's types.

`ordered.rs` is split by the orphan rule: each `Key`/`Successor` impl goes to
the crate defining the type (`Hlc` → time-types; `LogIndex`, `DataId` →
log-types).

### 4.3 The four misplaced items

Each is a prerequisite commit of the crate that needs it:

```text
TypeConfig & co (raft/types.rs)          -> zslg-raft-types
CompressionLevel (object_store/)         -> zslg-snapshot-types
Clock port vs HlcClock impl (hlc/)       -> port in zslg-time, impl in zslg-hlc-clock
RaftStateHandle, EvictionStatus,
SpareSelectionStrategy (raft/)           -> zslg-consensus-types
```

Fields that were `pub(crate)` (`NodeId(pub(crate) Uuid)`) get the smallest
accessor implementation crates need; nothing new reaches the facade.

### 4.4 Errors: leaves and flat unions (D4, D16)

**Leaves** are `thiserror` enums, one per group, in the types crates (§4.1).

**Unions** are declared with `error_union!` from the `error-union` library
(~60 lines of `macro_rules!`). Every crate with fallible functions on its
boundary — ports crates, implementations, procedures, assemblies — has one:

```rust
error_union! {
    pub ReadError { Log(LogError), Snapshot(SnapshotError), Lifecycle(LifecycleError) }
    from ArchiveError { Log, Snapshot }   // From<ArchiveError> for ReadError
}
```

The macro generates the enum, `From<Leaf>`, `Display`, `Error::source` and the
listed union→union `From`s. The compiler enforces the subset rule both ways: a
target without one of the source's leaves fails (E0599), and a `from` list
that omits one of the source's leaves fails (E0004, non-exhaustive match) —
prototyped across three crates. Conventions: a union holds leaves only, never
another union (depth is always `Crate::Leaf::Variant`); a leaf has the same
variant name in every union, its group name.

Channel seams (ten port signatures carry `Result<_, E>` in channel items)
convert with one `map_err(Into::into)` per source.

`zslg::Error` (public, D11) is the union of all leaves, declared in the facade.
The visible change for consumers is the nesting: `Error::NotLeader { .. }`
becomes `Error::Consensus(ConsensusError::NotLeader { .. })`. nlnr does not
match on `zslg::Error`; nvzn's uses are `Result<_, zslg::Error>`.

### 4.5 Metrics (D5, D17)

`metrics.rs` (2.7k) is cut at its `// ── section ──` markers. `metrics-kit`
keeps the mechanism (`MetricDef`, `MetricKind`, the registry macro,
`label_enum!`, probes, `describe_all`); it is a library, so nvzn uses it too.
Two zslg-specific bits move out when it is extracted: the hard-coded `"zslg_"`
prefix becomes the macro's namespace argument (the internal `metrics_impl!`
already takes one), and the `hlc` row form becomes a generic "gauge from a
conversion" row, the `Hlc` conversion living in `zslg-time-types`.

Label enums that appear in port signatures or are shared by two crates live in
the types crates; everything else lives with the crate that emits it, as a
`METRICS` slice plus emission helpers. `zslg-observability` concatenates them
in `describe_metrics()` and tests name uniqueness. Names do not change (§8.2
golden test).

---

## 5. Implementations

### 5.1 The list

Source paths are under `crates/zslg/src`.

| crate | source | lines | implements |
|---|---|---|---|
| `zslg-node-id` | `database/node_id.rs` | 0.1k | write-once node identity (§5.5) |
| `zslg-object-store` | `object_store/{mod,snapshot_store}.rs` | 1.1k | `SnapshotStore`; owns `s3`; `ObjectStoreConfig` |
| `zslg-object-format` | `object_store/framing.rs` | 0.4k | candidate (§5.6) |
| `zslg-hlc-clock` | `hlc/clock.rs` | 0.6k | `Clock` |
| `zslg-hlc-gate` | `hlc/{gate,cutoff}.rs` | 0.7k | write admission |
| `zslg-writer` | `writer.rs`, `quorum/{coordinator,fanout}.rs` | 1.3k | `Writer` |
| `zslg-net-client` | `network/{client,transport}/` | 1.6k | `RaftPeer`, `RaftTransport`, `ReplicaClient` over RPC |
| `zslg-net-server` | `network/server/` | 0.7k | RPC dispatch to ports |
| `zslg-net-router` | `network/router/` | 0.4k | `Router` |
| `zslg-raft-node` | `raft/{node,membership}.rs` | 0.4k | `RaftNode`: raft and consensus ports, `ClusterBootstrap` |
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
| `zslg-leader-compaction` | `leader/jobs/compaction/` | 0.9k | `CompactionTier` |

### 5.2 Raft: decide, then execute (D14)

`zslg-raft-startup` holds the bootstrap/join/rejoin decision procedure and
talks to `ClusterBootstrap` in `zslg-raft`:

```rust
trait ClusterBootstrap {
    async fn initialize(&self, members: Membership) -> Result<(), BootstrapError>;
    async fn write_fenced(&self, w: FencedStateWrite) -> Result<RaftResponse, BootstrapError>;
    async fn wait_leader(&self, within: Duration) -> Result<NodeId, BootstrapError>;
    fn metrics(&self) -> watch::Receiver<RaftMetricsView>;
}
```

`RaftNode` (`zslg-raft-node`) implements it over openraft; startup is tested
against `zslg-raft-fake`. The verbs are what startup calls today; if the move
finds another call into openraft, I stop and ask (§8.1). `raft-storage` and
`raft-rpc` implement openraft's own traits, which are their ports. `setup_raft`
is wiring and goes to `zslg-raft-setup`.

### 5.3 Leader and learner

Leader jobs depend on consensus, log and snapshot ports, `zslg-quorum-read`
for replica freshness, and `time`. In production they already go through ports
(`LocalStoreView`, `SnapshotStore`, `RaftStateView`); only their tests use the
concrete stores (§7.3). The reader splices `StableLog`, `StableArchiveReader`
and `LocalStoreView` and never names a concrete store.

### 5.4 Network and replica

`zslg-net-wire` (procedure) holds the RPC messages (`network/messages.rs`), the
wire codec (`network/framing/`) and the TCP layer (`network/tcp.rs`). Client,
server and router use it and ports, nothing else concrete. `StaticPeers` and
`PermissiveGate` move to `zslg-net-fake`. `zslg-replica-service` calls the
replica log through a small submit/fence port in `zslg-log`, shaped by what
`replica/mod.rs` calls today.

### 5.5 The node's RocksDB (D19, D20)

```text
rocksdb-kit       storage.rs + create_storage_root: one explicit open; the caller
(library)         states root, memory budget and the complete CF list; the
                  per-CF engine gauges (label `cf`); an engine-neutral API:
                  Db, ColumnFamily, Batch (put, delete, delete_range), durable
                  write, Snapshot, Scan (ordered, bounded), get
zslg-node-id      node_id.rs: the write-once identity row
owners            each declares its own column families (today's names):
                    zslg-stable-log      stable_log
                    zslg-replica-log     replica_log
                    zslg-local-store     local_store_data, local_store_cursors
                    zslg-raft-storage    raft_log, raft_meta
                    zslg-node-id         node_id
node assembly     passes the list of what it wires to rocksdb-kit's one open
```

"The descriptor list is the schema" stays true; the list lives in the node
assembly, where the open already happens, and each node has a golden test of
it. The five adapters never name `rust_rocksdb`. There is no storage trait
yet: their tests run on a real RocksDB in a temp dir because what they prove is
durability, which an in-memory KV would not model. If a second engine or a KV
fake is ever needed, the trait is introduced inside `rocksdb-kit` and the
adapters do not move.

The general test: anything used by two implementations is a library if it
knows no domain, a procedure if it works over the foundation, otherwise a
missing port — and then I ask.

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
learner/       read.rs (2.2k) — the splice vs. the gating (GatedReader)
replica/       log.rs (2.2k) — flush loop vs. store
```

---

## 6. Assemblies

| crate | source | wires |
|---|---|---|
| `zslg-raft-setup` | `raft/mod.rs` (`setup_raft`) | raft-node, raft-storage, raft-rpc, raft-state, net-router |
| `zslg-learner` | `learner/mod.rs` (`spawn_learner`, `LearnerService`, `GatedReader` wiring) | local-store, stable-log, stable-archive, subscriber, reader |
| `zslg-leader` | `leader/mod.rs`, `leader/jobs/mod.rs` | integrator and the job crates |
| `zslg-observability` | `observability.rs`, process/jemalloc gauges | `describe_metrics`; `prometheus`, `jemalloc`, `tokio-console` features |
| `zslg-writer-node` | `writer_node.rs` | raft-setup, writer, net-* |
| `zslg-learner-node` | `learner_node.rs` | raft-setup, learner |
| `zslg-follower-node` | `follower_node.rs` | raft-setup, learner |
| `zslg-replica-node` | `replica_node.rs` (1.5k) | raft-setup, learner, leader, replica-* |

Node crates depend on the lower assemblies, never on each other, and own the
column-family list of what they wire (§5.5). The facade `pub use`s exactly
today's `lib.rs` list, wherever the items now live (node types and configs,
`ObjectStoreConfig`, `CompactionTier`, `RaftStateHandle`, `Hlc`, `ByteSize`,
…), declares `zslg::Error`, and forwards features under today's names.
Behind its `testing` feature it re-exports the log fake's `TestLog` as
`zslg::testing::TestLog`, so `nvzn::testing` keeps reaching it through the
facade (D11).

---

## 7. Fakes, tests, hooks

### 7.1 Three kinds of test support (D9, D10)

Fakes of ports in `zslg-<group>-fake` (time, log, snapshot, consensus, raft,
net); the 11 `_for_test` fault hooks as a `testing` feature on the owning
implementation; the cluster harness and integration tests in the facade.

### 7.2 Consolidating the fakes

Duplicated doubles become one each when their fake crate is created:
`FakeLeadership`/`StaticLeadership`, the two `StaticLiveness`,
`FakeReplica`/`FakeUpstreamReplica`, `FakeRaft`/`NoopReplicator`, plus
`StubLocalStore`, `NoOpCheck`, `NoOpRaftViewer`, `NoOpProbe`. `zslg-log-fake`
also takes `nvzn::testing::TestLog`.

### 7.3 Tests that need a real sibling

The leader's snapshot and compaction tests build a real `LocalStore`,
`StableLog` and `RemoteSnapshotStore`. A dev-dependency between implementations
would be a sideways edge, so these tests move to the lowest assembly that
depends on both (`zslg-leader`) or are rewritten against the fakes when they
only need the port. Decided per test in its commit; the default is to move,
because rewriting a test changes what it proves.

---

## 8. Landing the change

### 8.1 One PR, one crate per commit (D13) — and stop when the plan breaks

One branch, one PR, one commit per crate, each green; prerequisite moves are
their own commits before the crate that needs them. **If something does not
split the way this document says — a missing port, shared private state, a
test that cannot move, an adapter needing more than `rocksdb-kit` offers — I
stop and ask how to proceed.** No improvised traits, re-export shims, `pub`
back doors or fused crates to get past it.

### 8.2 Gates at every commit

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
golden tests: zslg and nvzn describe_metrics(); per-node column-family lists
```

At milestones (after §11 step 5, after step 12, at the end): btrb and nvzn
examples build; nlnr builds against the facade; loadgen before/after; a clean
build and an incremental build of one leaf crate are timed for the post
document.

### 8.3 The layering test

A test in the facade reads `cargo metadata` and each crate's
`[package.metadata.layer] role`, then fails on:

- a crate without a role (`btrb`, the application, has role `application`);
- any normal or dev dependency the role table (§2.1) does not allow;
- a ports→ports edge other than `zslg-net`'s (§4.2);
- a dependency on a `-types` crate from above the ports layer, except
  `zslg-lifecycle-types` (§2.2 rule 1);
- an `error_union!` that names another union as a leaf.

It lands in the first commit, so every later commit is checked. A
`crates/README.md` with the layer diagram is the index for readers (D15).

---

## 9. nvzn

Same rules (D12). nvzn has one domain and one error leaf, so it has no
types/ports split and no unions; its `Error` lives in `nvzn-types`. nvzn depends
on zslg only through the `zslg` facade (D11) and may use any library.

| crate | role | source |
|---|---|---|
| `nvzn-types` | types | `ids.rs`, `events.rs`, `view.rs`, `projection.rs` traits, `Error`, `FoldState`, `StopReason` |
| `nvzn-store` | ports | `Store`, `ReadTransaction`, `WriteTransaction`, `store/config.rs` |
| `nvzn-store-overlay` | procedure | `store/overlay.rs` (`CommitBatch`, `WriteTransactionImpl`), shared by both stores |
| `nvzn-store-rocksdb` | implementation | `store/rocksdb.rs` |
| `nvzn-store-fake` | fake | `store/inmemory.rs` |
| `nvzn-resultset` | implementation | `resultset/` |
| `nvzn-dispatcher` | implementation | `dispatcher/` |
| `nvzn-live` | implementation | `live.rs` |
| `nvzn-reader` | assembly | `reader.rs` |
| `nvzn` | facade | `lib.rs`, `testing.rs` |

`live` and `reader` both read the dispatcher's `FoldState` and `Subscriptions`,
and `reader` returns `live::Live`. `FoldState` is a shared cell (69 lines) and
goes to `nvzn-types`; `reader` is wiring over `live`, the store and the
dispatcher's subscriptions, so it is an assembly. Open: `Subscriptions`
(dispatcher's live-subscriber registry, read by `reader`) is either a shared
cell (types) or a port the dispatcher implements; decided with you when the
move gets there (§8.1).

Metrics (D17): nvzn's hand-written `MetricKind`/`MetricDef`/`METRIC_PREFIX` and
~40 name constants move onto `metrics-kit` with namespace `"nvzn_"`; the
vocabulary goes to the emitting crate (dispatcher, live, reader,
store-rocksdb); shared label enums (`StopReason`, also public) go to
`nvzn-types`; the facade concatenates the slices; a golden test pins the names.

Follow-up, not in scope: `nvzn-store-rocksdb` onto `rocksdb-kit` (D20).

---

## 10. Alternatives, edge cases, what to check

### 10.1 Alternatives rejected

One `core` crate; a single error crate below everything; associated error
types on ports; a central metrics crate; a `zslg-support` crate; nvzn
importing group crates; nested directories; a PR per layer or per crate;
implementations depending on each other. From the later passes: types and
ports in one crate per group (§3.1); ports returning only their group's error
(§3.2); a composed `zslg-error` crate below the ports (D16); `error_set`
(cannot aggregate sets across crates, verified) and `terrors` (type-level sets
replace `match` at ~140 sites); the facade as the only composition root
(§3.5); dev-dependencies between implementations (§7.3); a central `nvzn-metrics`
(D17); a shared `Subsystem` enum (D19); a storage trait now (D20); roles from
names alone (§2.1).

### 10.2 Edge cases

- `RUST_LOG=zslg=debug` must still match `zslg_raft_node::…`. `EnvFilter`
  target directives match by prefix, so it should; the first extraction commit
  verifies it.
- Features (`s3`, `prometheus`, `tokio-console`, `jemalloc`,
  `heap-profiling`) live in the owning crate; the facade forwards them.
- `pub(crate)` ports become `pub` in internal crates; the facade's `pub use`
  list is unchanged.
- `rocksdb-kit`'s API must cover what the adapters use today (batch put/delete/
  delete_range, fenced write, snapshot, bounded raw scans, pinned gets, the
  engine-property gauges).
- Unprefixed library names (`bytesize`, `framing`, `shutdown`) match no
  package in either lockfile today; a crates.io dependency of the same name
  added later would clash, so the layering test also fails on a lockfile
  package that shares a workspace library's name.
- Hot-path functions on `Hlc`, `LogIndex`, `DataId` get `#[inline]`; loadgen
  before/after catches a regression.

### 10.3 Complexity

Essential: a port set that actually decouples (`ClusterBootstrap`, the
replica submit port, a few verbs on existing ports) and a role table that
says where any new piece goes. Accepted: ~75 crates, more `Cargo.toml` files,
public-in-internal visibility, one `from` line per union conversion. Avoided:
shims, wildcard re-exports, duplicate fakes, central vocabularies, sideways
edges.

### 10.4 Please look at

Changed in pass 3 (consistency across D1–D20):
1. §3.3/§4.2: raft is a full group (`zslg-raft-types` + `zslg-raft` ports);
   otherwise rule 1 would leak openraft through `zslg-consensus` again. The
   implementation crate is therefore `zslg-raft-node`, and there is a
   `zslg-raft-fake` for startup's tests. RPC messages move to `zslg-net-wire`.
2. §4.1: the types crates form a DAG, not a chain — consensus ports need
   `PeerAddr`/`ServerName`, and `net-types` needs nothing but lifecycle.
3. §2.1/§8.3: roles come from `[package.metadata.layer]`, not from names —
   `zslg-leader` (assembly) and `zslg-reader` (implementation) look alike.
4. §2.2: lifecycle has no ports crate, so it is imported directly; it is the
   one exception to rule 1.
5. §6: the facade may depend on implementations (it re-exports
   `ObjectStoreConfig`, `CompactionTier`), and re-exports `TestLog` so nvzn
   does not reach into `zslg-log-fake` (D11).
6. §10.2: lockfile name-clash check for unprefixed libraries.

Earlier passes, still open for sign-off: types/ports split; procedures;
assemblies; no dev-dependencies between implementations; nvzn `reader` as an
assembly and `Subscriptions` open. Settled since: D16–D20.

---

## 11. Implementation order

A linearisation of the DAG; each line is one or more commits, prerequisites
first.

```text
 1  layering test + role metadata on today's crates
 2  libraries: metrics-kit (namespace argument, generic conversion row), bytesize,
    warn-throttle, shutdown, framing, error-union, rocksdb-kit (engine-neutral API)
 3  types: lifecycle, time, log, snapshot, net, consensus, raft  (+ the moves of §4.3)
 4  ports: time, log, snapshot, consensus, raft (+ ClusterBootstrap), net
 5  fakes: time, log (+ TestLog), snapshot, consensus, raft, net
 ── checkpoint: foundation enforced; one big implementation crate left; all green
 6  procedures: quorum-read, net-wire (+ messages)
 7  node-id, object-store, hlc-clock, hlc-gate
 8  net-client, net-server, net-router
 9  raft-storage, raft-rpc, raft-state, raft-tracker, raft-node, raft-startup
10  local-store, stable-log, stable-archive, subscriber, reader
11  replica-log, replica-service, writer
12  leader-integrator, leader-membership, leader-snapshot, leader-compaction
 ── checkpoint: no implementation code left in zslg
13  assemblies: raft-setup, learner, leader, observability, the four nodes
14  facade reduced to pub use + Error + integration tests + examples; README
15  nvzn: metrics onto metrics-kit (names pinned by golden test), types, store,
    store-overlay, store-rocksdb, store-fake, resultset, dispatcher, live,
    reader, facade
```

Column families move with their owners in steps 9–11; until the node
assemblies exist (step 13), the remaining `zslg` crate composes the list.
After step 15 a post document records the review, surprises and build-time
numbers.
