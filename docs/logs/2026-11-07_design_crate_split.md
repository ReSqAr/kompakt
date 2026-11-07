# zslg / nvzn crate split — design

Process: `nlnr/docs/iaic.md`. Decisions D1–D15 are in
`2026-11-07_log_crate_split.md`; this document is the spec. Status: waiting for
sign-off. Branch `ccr-3ceece9d-e3tfbt`, base `6aa354b`.

Reading order: §1 is the whole idea; §2–§4 deepen it layer by layer; §5–§6
cover testing and landing the change; §7 applies the same rules to nvzn; §8.4 lists what I want you to look at.

---

## 1. The idea

zslg (50k lines incl. tests) and nvzn (6k) are one crate each. This change
cuts them into ~55 small, internal crates so that the layering — today a
convention that the module graph already violates — is enforced by Cargo.

```text
                         consumers (btrb, nlnr, nvzn's users)
                                      │ only ever see
                                      ▼
  ┌───────────────────────────  zslg  (facade) ───────────────────────────┐
  │  composition root: nodes, setup_raft, leader loop, observability      │
  └───▲────────────▲────────────▲────────────▲────────────▲───────────────┘
      │            │            │            │            │
  implementations (siblings, no edges between them; ports only)
  database · object-store · raft-* · net-* · stable-log · reader · leader-* ...
      │            │            │            │            │
  ┌───┴────────────┴────────────┴────────────┴────────────┴───────────────┐
  │ foundation: five groups, each = types + ports + its error + its metrics│
  │   time ◄── log ◄── snapshot ◄── consensus ◄── net                      │
  │ libraries: bytesize · warn-throttle · cancelled · shutdown · framing · │
  │            metrics-kit                                                 │
  └────────────────────────────────────────────────────────────────────────┘
  fakes: one per group (zslg-<group>-fake), dev-dependencies of the layer above
```

Three layers and one rule that does the work: an
implementation crate depends on foundation and libraries, never on another
implementation. The facade is the only crate that names concrete types from
several implementations.

### 1.1 Goals, non-goals, success criteria

Goal (D1): layering enforced by the compiler, yielding many small, targeted,
non-reusable crates. Build and test time are welcome side effects, not targets.
Moving code and introducing new traits to decouple is allowed.

Non-goals: no behaviour change, no wire-format change, no metric-name change,
no new features, no public-API change for consumers other than what §3.3
states for errors. Crates are `publish = false`; reuse outside this repository
is not a goal.

Success criteria, all checked at every commit (§6):
- fmt, clippy `-D warnings`, and the full test suite green;
- `describe_metrics()` output byte-identical to before the change;
- btrb, nvzn and a nlnr checkout build against the facade without edits other
  than the error nesting in §3.3;
- at the end, `cargo tree` shows no implementation→implementation edge (§6.3).

### 1.2 Current state

The module graph has cycles that make the folders unusable as crates, but they
are shallow. Real compile-time back-edges are four misplaced items (§3.2); most
other upward edges are doc links and test imports. Inside the big subfolders
the files are largely independent of each other. The consumer surface is small:
nlnr uses `zslg::Hlc` and `zslg::ObjectStoreConfig`; nvzn uses ports and
foundation types; btrb is the composition root for `ReplicaNode`.

### 1.3 Constraints and assumptions

- The workspace denies `wildcard_imports` and many other lints; every new crate
  carries `[lints] workspace = true`. Moves rewrite imports explicitly.
- The orphan rule applies: a trait impl lives in the crate of the trait or of
  the type. §3.1 shows where this bites (`ordered.rs`).
- nlnr pins kompakt by git rev and cargo finds a package by name, so
  directory layout and crate count are invisible to consumers (D11, D15).
- Cross-crate calls lose inlining; hot-path leaf functions get `#[inline]`
  (§5.4).

---

## 2. The layers

### 2.1 Roles and the one rule

Every crate has exactly one role, readable from its name (D15):

| role | name | may depend on |
|---|---|---|
| library | `zslg-bytesize`, `-warn-throttle`, `-cancelled`, `-shutdown`, `-framing`, `-metrics-kit`, `-net-wire` | nothing in zslg (net-wire: net group) |
| foundation group | `zslg-<group>` | libraries, lower groups |
| fake | `zslg-<group>-fake` | its group, lower groups, libraries |
| implementation | `zslg-<what>` | foundation, libraries |
| facade | `zslg` | everything |

A library is code that implements no port (a codec, a newtype, a macro kit);
implementations may use it. This refines D8: "ports only between
implementations" is "no edge between two crates that each implement a port".

### 2.2 The five foundation groups

Each group is a crate with the types its ports mention, the ports, the group's
error (D4) and its metric vocabulary (D5). Dependencies point left:

```text
time        Hlc                           Clock, HlcCutoff view
log         Data, DataId, LogIndex,       Reader, Writer, StableLog, LocalStoreView/Mut,
            Stable/UnstableEvent          StableArchiveReader, Replica
snapshot    SnapshotId, SnapshotMetadata, SnapshotStore
            CompressionLevel
consensus   NodeId, RaftGeneration,       RaftConsensus, RaftPeer, RaftLeadership,
            RaftState(Handle), TypeConfig, RaftMembership, StateReplicator, RaftStateView,
            NodeInfo, FencedStateWrite,    QuorumCheck, SpareSelection, MembershipGate,
            RaftStateError, is_quorum      ClusterBootstrap (new, §4.2)
net         PeerAddr, HostPort,           Peers, Router, ReplicaClient, LivenessProbe,
            ServerName, RPC messages      LivenessQuery
```

`is_quorum` is pure arithmetic over membership types and moves into
`consensus`; the coordinator that collects acks stays an implementation (§4.1).

### 2.3 Libraries

`zslg-bytesize` (`ByteSize`, config newtype; its parse error becomes its own),
`zslg-warn-throttle` (118 lines, no deps), `zslg-cancelled` (`Cancelled`,
`Shutdown`), `zslg-shutdown` (the supervision tree, generic over the task
result), `zslg-framing` (`[record | crc32]`, `Corrupt` error). `zslg-metrics-kit`
holds the mechanism of `metrics.rs` (§3.4). `zslg-net-wire` holds the RPC wire
codec and TCP layer (`network/framing`, `network/tcp.rs`, 1.2k lines); it
implements no port, it is a codec the client, server and router share.

---

## 3. The foundation in detail

### 3.1 Where today's files go

`api/` is already a set of ports; its files follow the table in §2.2. The two
grab-bag files dissolve: `ids.rs` item by item (`NodeId`/`RaftGeneration` →
consensus, `LogIndex`/`DataId` → log, `PeerAddr`/`HostPort`/`ServerName` → net,
`SnapshotId` → snapshot), and `meta.rs` the same way (`Data`, events → log;
`SnapshotMetadata` → snapshot; `RaftState`, `ReplicaMembership`, `Exit` →
consensus).

`ordered.rs` (the `Key`/`Successor` impls for `OrderedChannel`) cannot stay one
file: the orphan rule puts each impl in the crate that defines the type. `Hlc`'s
goes to `zslg-time`, `LogIndex`'s and `DataId`'s to `zslg-log`; both depend on
`crossbeam-combinators`. This refines D3, which kept `ordered` whole in `log`.

Fields that were `pub(crate)` (`NodeId(pub(crate) Uuid)`, `LogIndex(pub(crate)
u64)`) become reachable from implementation crates, so each gets the smallest
accessor it needs (`new`, `raw`). Nothing new is exposed through the facade.

### 3.2 The four misplaced items

These are the real cycles; each is a prerequisite commit of the crate that
needs it (§6):

```text
TypeConfig, NodeInfo, FencedStateWrite, RaftLogIndex, RaftResponse,
RaftStateError          raft/types.rs        ->  consensus   (api + network used them)
CompressionLevel        object_store/        ->  snapshot    (api/snapshot_store used it)
Clock impl vs port      hlc implements api   ->  port in time, impl in zslg-hlc-clock
RaftStateHandle,
EvictionStatus          raft/state, startup  ->  consensus   (learner read path used them)
```

### 3.3 Errors

Each group owns its error: `ClockError` (HlcCutoff, DriftTooLarge,
ClockSkewTooLarge), `LogError` (StableLogGap, StableLogBehindManifest,
StreamTruncated, Storage, Codec), `SnapshotError` (ObjectStore, Compression),
`ConsensusError` (NotLeader, QuorumUnreachable, StaleStateWrite,
InvalidRaftState, …), `NetError` (Rpc, UnknownPeer, MembershipDenied).
Ports return their group's error.

`zslg::Error` stays the public type (D11) and is built from the group errors
with `#[from]` plus the cross-cutting and startup variants (`Cancelled`,
`Shutdown`, `InvalidConfiguration`, `Observability`, `StartupCancelled`,
`HardCrashDataLoss`). This is the one visible API change: code matching
`Error::NotLeader { .. }` matches `Error::Consensus(ConsensusError::NotLeader
{ .. })`. nlnr does not match on `zslg::Error`; nvzn's 24 uses are mostly
`Result<_, zslg::Error>` and need no edit. Each group error has a `From` into
`zslg::Error`, so `?` keeps working across groups.

### 3.4 Metrics

`metrics.rs` (2.7k lines) is cut at its existing `// ── section ──` markers
(D5). `zslg-metrics-kit` keeps `MetricDef`, the registry macro, `label_enum!`,
probes and `describe_all`. Each crate that emits metrics owns a
`pub const METRICS: &[MetricDef]`, its label enums and its emission helpers; a
label enum used by two crates lives in the lower one.

The facade concatenates every crate's `METRICS` in `describe_metrics()` and has
a test that name uniqueness holds across crates. The `zslg_` namespace is applied
by the registry macro, so exported names do not change; the golden test in §6.2
proves it.

---

## 4. The implementations in detail

### 4.1 The crate list

Source column is today's path under `crates/zslg/src`. Sizes in lines.

| crate | source | lines | notes |
|---|---|---|---|
| `zslg-database` | `database/` | 1.3k | RocksDB `Storage`, node id |
| `zslg-object-store` | `object_store/` (config, snapshot_store) | 1.1k | `SnapshotStore` impl; owns the `s3` feature |
| `zslg-object-format` | `object_store/framing.rs` | 0.4k | candidate cut (§4.5) |
| `zslg-hlc-clock` | `hlc/clock.rs` | 0.6k | `HlcClock` implements `Clock` |
| `zslg-hlc-gate` | `hlc/gate.rs`, `cutoff.rs` | 0.7k | write-admission generation barrier |
| `zslg-writer` | `writer.rs`, `quorum/{coordinator,fanout}.rs` | 1.3k | `Writer` + ack collection |
| `zslg-net-client` | `network/{client,transport}/` | 1.6k | ports spoken over the wire |
| `zslg-net-server` | `network/server/` | 0.7k | |
| `zslg-net-router` | `network/router/` | 0.4k | |
| `zslg-raft` | `raft/{node,membership}.rs` | 0.4k | `RaftNode`; three port impls + `ClusterBootstrap` |
| `zslg-raft-startup` | `raft/startup/` | 1.3k | decision procedure (§4.2) |
| `zslg-raft-storage` | `raft/raft_storage/` | 2.5k | openraft storage traits |
| `zslg-raft-rpc` | `raft/rpc/` | 0.2k | openraft network factory, peer registration |
| `zslg-raft-state` | `raft/state/` (publisher) | 0.5k | gauges, leadership events |
| `zslg-raft-tracker` | `raft/tracker/` | 0.5k | |
| `zslg-local-store` | `learner/local_store/` | 2.5k | candidate cut (§4.5) |
| `zslg-stable-log` | `learner/stable_log/` | 2.3k | snapshot loader candidate cut |
| `zslg-stable-archive` | `learner/stable_archive/` | 0.2k | |
| `zslg-subscriber` | `learner/subscriber/` | 0.8k | |
| `zslg-reader` | `learner/{read,quorum}.rs` | 2.8k | `Reader`; `sync_quorum` |
| `zslg-replica-log` | `replica/{log,purge}.rs` | 2.4k | |
| `zslg-replica-service` | `replica/mod.rs` | 0.9k | RPC service |
| `zslg-leader-integrator` | `leader/integrator.rs` | 1.1k | the single Raft writer |
| `zslg-leader-membership` | `leader/jobs/{membership/,voters.rs}` | 3.5k | |
| `zslg-leader-snapshot` | `leader/jobs/{snapshot/,cutoff.rs,cleanup.rs}` | 2.9k | |
| `zslg-leader-compaction` | `leader/jobs/compaction/` | 0.9k | |

### 4.2 Raft: decide, then execute

`startup/` drives an `openraft::Raft` (`initialize`, `client_write`, `metrics`,
`wait`) through bootstrap, join and rejoin (D14). The decision procedure moves
to `zslg-raft-startup` and talks to a port in `consensus`:

```rust
trait ClusterBootstrap {
    async fn initialize(&self, members: Membership) -> Result<(), ConsensusError>;
    async fn write_fenced(&self, w: FencedStateWrite) -> Result<RaftResponse, ConsensusError>;
    async fn wait_leader(&self, within: Duration) -> Result<NodeId, ConsensusError>;
    fn metrics(&self) -> watch::Receiver<RaftMetricsView>;
}
```

`RaftNode` implements it over openraft. The verbs are what startup calls today;
the implementation phase confirms nothing else reaches into openraft, and adds a
verb rather than a back-door if something does. `setup_raft` — adopt storage,
wire the router, spawn the watchers — is composition and moves to the facade.
`raft-storage` and `raft-rpc` implement openraft's own traits (`RaftLogStorage`,
`RaftNetworkFactory`), which are the ports for those two.

### 4.3 Leader and learner

The leader's jobs run as concurrent tasks and write to Raft only through the
integrator's intents. Each job crate depends on `consensus` ports
(`RaftLeadership`, `StateReplicator`, `RaftStateView`), `log`/`snapshot` ports
and `time`. The leader loop that spawns and supervises the jobs
(`leader/mod.rs`, `jobs/mod.rs`, 220 lines) is composition and moves to the
facade. `zslg-leader-snapshot` and `zslg-stable-log` both know the snapshot
object layout; that knowledge belongs to `zslg-snapshot` and moves there as a
prerequisite commit.

The reader splices three sources (local stable log, stable archive, unstable
store) through the `StableLog`, `StableArchiveReader` and `LocalStoreView`
ports; it no longer names the concrete stores. `zslg-subscriber` writes through
`LocalStoreMut`.

### 4.4 Network and replica

`zslg-net-client` implements `RaftPeer`, `ReplicaClient` and friends by speaking
RPC; `zslg-net-server` accepts connections and dispatches to `RaftConsensus`,
`Replica` and `StateReplicator` ports; `zslg-net-router` routes by node id.
All three use `zslg-net-wire` and nothing else concrete. `StaticPeers` and
`PermissiveGate` (test and example doubles today) move to `zslg-net-fake`.

`zslg-replica-service` currently calls the concrete replica log directly; it gets a
small submit/fence port in `log` (named in the implementation commit, shaped by
what `replica/mod.rs` calls).

### 4.5 Candidate cuts, to be confirmed commit by commit

D7/D14: split whenever the pieces share no private state. The file-level
import scan suggests these; each is its own commit, and if a commit finds shared
private state it is merged back and recorded in the log, not forced.

```text
local_store/    key.rs, recent_ids.rs, purge.rs, store.rs (1.7k)  -> store + keys
stable_log/     purge.rs, snapshot_loader.rs (0.6k)               -> loader separate
leader membership   change.rs (0.7k), liveness.rs (0.7k, uses change)
hlc/            clock | gate | cutoff   (cutoff may fold into gate)
object_store/   framing.rs (0.4k) | snapshot_store.rs (0.9k)
```

---

## 5. Fakes, tests, hooks

### 5.1 Three kinds of test support (D9, D10)

```text
fakes of ports       zslg-<group>-fake   time, log, snapshot, consensus, net
fault hooks          `testing` feature on the owning implementation crate
whole-system harness tests/common + integration tests, in the facade
```

The 11 `_for_test` hooks (`demote_to_learner_for_test`,
`seed_foreign_membership_for_test`, `integrate_manifest_for_test`, …) become
`pub` behind `testing`, enabled only by the facade's dev-dependencies. A hook
that no port can express is listed in the commit message as a candidate for a
real port.

### 5.2 The fakes

Duplicated test doubles are consolidated when their group's fake crate is
created: `FakeLeadership`/`StaticLeadership` → one fake `RaftLeadership`; both
`StaticLiveness`; `FakeReplica`/`FakeUpstreamReplica`; `FakeRaft`/
`NoopReplicator` → one `StateReplicator`; `StubLocalStore`, `NoOpCheck`,
`NoOpRaftViewer`, `NoOpProbe`. `zslg-log-fake` also takes `nvzn::testing::
TestLog` (an in-memory `Reader`/`Writer`, 140 lines of channel plumbing), which
today lives in the wrong crate.

### 5.3 What tests move where

Unit tests move with their source into the new crate. The 5k lines of
integration tests and the cluster harness stay in the facade and keep using
the facade's public items plus `testing` hooks. Tests that today reach into
module internals without a hook are moved next to the code they test.

### 5.4 Performance guard

Hot-path leaf functions in `time` and `log` types (`Hlc` ops, `LogIndex`
arithmetic, `DataId` comparison) get `#[inline]`. The loadgen example is run
before and after the series and compared (§6.2). If a regression shows, the
fix is `#[inline]` or merging the two crates, not a revert of the split.

---

## 6. Landing the change

### 6.1 One PR, one crate per commit (D13)

One branch, one PR, one commit per crate, each green. A prerequisite move (one
of §3.2, an error split, an accessor) is its own commit before the crate that
needs it. Imports are rewritten in the extraction commit; no re-export shim is
left behind. Large moves are expected; mixed moves are not.

### 6.2 Gates at every commit

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
describe_metrics() golden       (names, kinds, units, help — identical)
```

Plus, on the milestones (after the foundation, after the implementations, at the
end): btrb and nvzn examples build; a nlnr checkout builds against the facade;
loadgen before/after; a clean build and an incremental build of one leaf crate,
timed, recorded in the post document as the B data point.

### 6.3 Enforcing the layering

Cargo already rejects cycles. The rule "no implementation→implementation edge"
is enforced by one test in the facade that reads `cargo metadata` and checks
each crate's workspace dependencies against its role (§2.1), the role being
derived from the name. A new crate with a wrong dependency fails the test.

---

## 7. nvzn

Same rules, in this design (D12). nvzn is already acyclic.

| crate | source | notes |
|---|---|---|
| `nvzn-types` | `ids.rs`, `events.rs`, `view.rs`, `projection.rs` | no internal deps |
| `nvzn-store` | `store/{mod,config,overlay}.rs` | the `Store` port and the shared write machinery (`CommitBatch`, `WriteTransactionImpl`) |
| `nvzn-store-rocksdb` | `store/rocksdb.rs` | |
| `nvzn-store-fake` | `store/inmemory.rs` | the in-memory store, a fake of `Store` |
| `nvzn-resultset` | `resultset/` | query ops |
| `nvzn-dispatcher` | `dispatcher/` | log → store fold |
| `nvzn-live` | `live.rs` | wait for projections |
| `nvzn-reader` | `reader.rs` | |
| `nvzn-metrics` | `metrics.rs` | central vocabulary, see below |
| `nvzn` | `lib.rs`, `testing.rs` | facade; `TestLog` moves to `zslg-log-fake` |

`overlay.rs` is shared by the RocksDB and in-memory stores, so it lives with
the port. nvzn is a consumer of zslg and may not import `zslg-metrics-kit`
(D11); with ~850 lines of vocabulary used by four crates, nvzn keeps one
`nvzn-metrics` crate instead of D5's per-crate split. This is the one place D5
is not applied, and it is flagged in §8.

---

## 8. Alternatives, risks, what to check

### 8.1 Alternatives rejected (see the log for reasons)

One `core` crate; a single Error crate; associated error types on ports; a
central metrics crate; a `zslg-support` crate; nvzn importing group crates;
nested directory layout; a PR per layer or per crate; implementations
depending on each other.

### 8.2 Edge cases

- Orphan rule: `Key` impls and any `From` between group errors must sit in the
  crate of the trait or the type (§3.1).
- `tracing` targets default to the module path, so `RUST_LOG=zslg=debug` must
  still match `zslg_raft::…`. `EnvFilter` target directives match by prefix, so
  it should; the first extraction commit verifies it and the observability doc
  is updated if not.
- Features: `s3`, `prometheus`, `tokio-console`, `jemalloc`, `heap-profiling`
  live in the owning crate; the facade forwards them under the same names.
- Workspace dependency versions stay in `[workspace.dependencies]`; each crate
  lists only what it uses, which will surface unused dependencies.
- `pub(crate)` ports become `pub` in internal crates; the facade's `pub use`
  list is unchanged, so the consumer-visible API is the same.

### 8.3 Complexity: essential and accepted

Essential: a port set that actually decouples (new: `ClusterBootstrap`, the
replica submit port, a few verbs on existing ports). Accepted: ~60 crates
instead of 2 (+ the facades), more `Cargo.toml` files, public-in-internal visibility. Avoided:
shims, wildcard re-exports, duplicate fakes.

### 8.4 Please look at

1. §3.1: `ordered.rs` is split by the orphan rule — a refinement of D3.
2. §2.1: libraries may be used by implementations — a refinement of D8.
3. §7: `nvzn-metrics` is central, unlike D5.
4. §4.1/§4.5: the crate list is a proposal; the candidate cuts are confirmed
   per commit, and anything that shares private state is merged back.
5. §3.3: the nested `Error` match is the one visible API change.

---

## 9. Implementation order

A linearisation of the DAG. Each line is one or more commits (prerequisites
first, then the extraction).

```text
 1  libraries      bytesize, warn-throttle, cancelled, shutdown, framing, metrics-kit
 2  time           (Hlc, Clock, ClockError, Key impl) + time-fake
 3  log            (ids, events, ports, LogError, Key impls) + log-fake (+ TestLog)
 4  snapshot       (+ snapshot layout moved from leader/learner) + snapshot-fake
 5  consensus      (the four misplaced items, ports, ClusterBootstrap) + consensus-fake
 6  net            (PeerAddr..., messages, ports) + net-wire + net-fake
 ── thin vertical slice: facade still holds all implementations; everything builds
 7  database, object-store, hlc-clock, hlc-gate
 8  net-client, net-server, net-router
 9  raft-storage, raft-rpc, raft-state, raft-tracker, raft (RaftNode), raft-startup
10  local-store, stable-log, stable-archive, subscriber, reader
11  replica-log, replica-service, writer
12  leader-integrator, leader-membership, leader-snapshot, leader-compaction
13  facade finalised: nodes, setup_raft, leader loop, layering test, README
14  nvzn: types, store, store-rocksdb, store-fake, resultset, dispatcher, live,
    reader, metrics, facade
```

Steps 1–6 are the foundation and leave one big implementation crate — that is
the checkpoint: everything below it is enforced, everything above it is still
the old code, and the whole workspace builds and passes. Steps 7–12 then peel
implementations off bottom-up. After step 14 a post document records the review,
the surprises and the build-time numbers.
