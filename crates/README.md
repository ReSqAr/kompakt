# Crates

Every directory here is a crate; each declares its layer in `[package.metadata.layer] role` and the test layering test (in `crates/kompakt-workspace-checks/tests/layering.rs`) rejects a dependency its role does not allow. Design: `docs/logs/2026-11-07_design_crate_split.md`, decisions: `docs/logs/2026-11-07_log_crate_split.md`, what changed on the way: `docs/logs/2026-11-07_post_crate_split.md`.

```text
library < types < ports < procedure < implementation < assembly < facade
               \ config /      fake (beside ports, test-only)
```

- **library** — knows no domain; usable by anything (and by nvzn).
- **types** — plain data and leaf error enums of one group.
- **ports** — the traits of one group; only `zslg-net` bundles other ports.
- **procedure** — logic over the ports, no I/O of its own.
- **config** — public configuration shared with the facade.
- **fake** — in-memory stand-ins for ports; dev-dependencies, or behind a `testing` feature.
- **implementation** — one adapter; depends on ports, never on another implementation.
- **assembly** — wires implementations (node crates, `zslg-leader`, `zslg-learner`, `nvzn-dispatcher`, …).
- **facade** — `zslg` and `nvzn`: `pub use`, examples. Never an implementation, and no unit tests: the multi-node ones are `zslg-cluster-tests`, the rest live in the crates that own the code. The one exception is `nvzn`, whose integration tests drive its public API against a real cluster.

The `nvzn*` crates are built on the `zslg` facade (D11) and may depend on it; nothing else may.

## library

| crate | purpose |
|---|---|
| `backoff-kit` | Jitter for retry delays |
| `bytesize` | Byte-size and non-zero quantity newtypes for configuration |
| `crossbeam-combinators` | Blocking channel pipelines: a composable read/write pair, verdicts, and a subscribe/cut/range seam |
| `testing-kit` | Deadline-bounded waits, collision-free ports and log setup for async tests |
| `error-union` | Flat unions of leaf errors, with compiler-checked conversions between unions |
| `framing` | CRC32 frame integrity: [record \| crc32(4)] |
| `metrics-kit` | Metric registry mechanism: definitions, row macros, label enums, probes |
| `objectstorage-cache` | Disk cache and fetch scheduler for object stores |
| `pace-kit` | A floor on a loop's iteration rate |
| `raft-kit` | The one place that names openraft |
| `rocksdb-kit` | One explicit RocksDB open per node: memory budget, column families, batches, scans, fence |
| `shutdown` | Supervision tree: any background task exiting unrequested stops the node |
| `svg-charts` | A server-side SVG line chart: grouping, viewport math and the renderer |
| `warn-throttle` | Rate limiter for hot-path warn! sites |

## types

| crate | purpose |
|---|---|
| `btrb-types` | The vocabulary the btrb platform's capabilities share: Error, Shutdown, Stopped, Job, SigningKey |
| `nvzn-types` | Identifiers, event sets, the error and the shared cells of nvzn |
| `zslg-consensus-types` | The cluster's vocabulary: who is in it, what state it replicates, and the rules that state must obey |
| `zslg-error` | The public error union that every crate returning it converts into |
| `zslg-lifecycle-types` | How a node's lifecycle and configuration can fail |
| `zslg-log-types` | The replicated log's vocabulary: records, positions, events and the errors of reading and writing them |
| `zslg-net-types` | Where nodes are and how they name each other |
| `zslg-node-types` | A node's identity |
| `zslg-raft-types` | openraft's vocabulary for the cluster: the replicated write, its response, node info and the type configuration |
| `zslg-snapshot-types` | Snapshots: identity, metadata, compression and compaction vocabulary, and the errors of keeping them |
| `zslg-time-types` | Hybrid logical clocks: the value type, its order, and the errors of crossing a bound |

## ports

| crate | purpose |
|---|---|
| `nvzn-projection` | The nvzn projection model (`Projection`, `Write`, `Ref`, `Index`, `View`) and the read ports it is written against |
| `nvzn-store` | The nvzn store ports: write side, staging overlay and store configuration |
| `zslg-consensus` | The consensus group's ports |
| `zslg-log` | The log group's ports |
| `zslg-net` | The net group's ports |
| `zslg-raft` | The raft group's ports |
| `zslg-snapshot` | The snapshot group's ports |
| `zslg-time` | The clock port |

## procedure

| crate | purpose |
|---|---|
| `nvzn-projector` | The nvzn projector: folds events into projection rows and indices |
| `zslg-net-wire` | The RPC wire: messages, frame codec and TCP setup |
| `zslg-quorum-read` | Quorum fan-out and replica polling, shared by readers and leaders |

## config

| crate | purpose |
|---|---|
| `btrb-config` | The value grammar and the consumable environment every btrb binary and app reads its settings through |
| `zslg-object-store-config` | Object-store backend configuration |

## fake

| crate | purpose |
|---|---|
| `nvzn-fixtures` | Test events and projections shared by the nvzn crates' tests |
| `nvzn-store-fake` | An in-memory nvzn Store |
| `zslg-consensus-fake` | In-memory consensus ports for tests |
| `zslg-log-fake` | In-memory log ports for tests |
| `zslg-net-fake` | In-memory net ports for tests, with replica fault-injection decorators |
| `zslg-raft-fake` | An in-process Raft network with pairwise partitions, for tests |
| `zslg-time-fake` | In-memory Clock for tests, and a controllable wall-clock source |

## implementation

| crate | purpose |
|---|---|
| `btrb-background` | The btrb background capability: jobs that end only when told to, and the process's stop signals |
| `btrb-cli` | The btrb CLI capability: the worker's admin socket and the console a command talks to |
| `btrb-db` | The btrb db capability: the zslg node and nvzn dispatcher bring-up, the fold supervisor and the cache |
| `btrb-metrics` | The btrb metrics capability: the Prometheus recorder, process gauges and the /metrics listener |
| `btrb-web` | The btrb web capability: HTTP mechanisms, response layers and the deployment facts an app is built with |
| `nvzn-resultset` | Lazy, tenant-scoped query results and `Snapshot` over one nvzn read transaction |
| `nvzn-store-rocksdb` | The nvzn Store on RocksDB |
| `zslg-hlc-clock` | The hybrid logical clock |
| `zslg-hlc-gate` | Write admission against the HLC cutoff |
| `zslg-leader-compaction` | The leader compaction job: merges snapshot objects and splices the manifest |
| `zslg-leader-integrator` | The leader integrator: folds job intents into one Raft proposal |
| `zslg-leader-membership` | The leader membership jobs: liveness, replica-set changes and voter eviction |
| `zslg-leader-snapshot` | The leader snapshot jobs: cutoff, snapshot build/commit and object cleanup |
| `zslg-local-store` | The learner's local store: integrated replica data in RocksDB |
| `zslg-net-client` | RPC client: unary pool, streams, and the per-peer ReplicaClient |
| `zslg-net-router` | Router with an in-process fast path for self |
| `zslg-net-server` | RPC server: accept, dispatch and the streaming handlers |
| `zslg-node-id` | The write-once node identity |
| `zslg-object-store` | SnapshotStore over an object-store backend |
| `zslg-peers` | The configured peer set and the join admission gate over it |
| `zslg-raft-node` | The raft node over the consensus engine |
| `zslg-raft-rpc` | openraft network factory and peer registration |
| `zslg-raft-startup` | Raft startup: bootstrap, join and rejoin |
| `zslg-raft-state` | Raft state gauges and leadership events |
| `zslg-raft-storage` | openraft storage over RocksDB column families |
| `zslg-raft-tracker` | Raft state tracking from peer observations |
| `zslg-reader` | The learner's reader: stable/unstable splice and quorum-read freshness |
| `zslg-replica-log` | The replica log in RocksDB with its flush loop |
| `zslg-replica-service` | The replica service: write admission, status and subscription over the replica log |
| `zslg-stable-archive` | The stable archive: HLC-ordered reads from object-store snapshots |
| `zslg-stable-log` | The stable log: snapshot-fed, HLC-ordered log in RocksDB |
| `zslg-subscriber` | Pulls replica logs into the local store |
| `zslg-writer` | The writer: fans client writes out to the replicas and acknowledges them at quorum |

## assembly

| crate | purpose |
|---|---|
| `btrb-app` | The seam between the btrb platform and an app: the App trait, the app list and its boot |
| `btrb-host` | The btrb process roles: serve, work, standalone and the CLI, composed from the capabilities |
| `btrb-live` | Read-after-write for btrb apps: the interactive write handle and the cookie layer that carries a write's position |
| `nvzn-dispatcher` | The nvzn runtime: the fold, its supervisor and readers |
| `nvzn-live` | Live views: the folded state plus the few events received above it, readable at once |
| `zslg-follower-node` | Follower node assembly: learner plus writer on one node |
| `zslg-leader` | Leader assembly: hands every leader job its handles and config slice |
| `zslg-learner` | Learner assembly: wires the local store, stable log, archive, subscriber and reader |
| `zslg-learner-node` | Learner node assembly: read-only node over the learner assembly |
| `zslg-net-setup` | Network assembly: builds the outbound router and binds the RPC server |
| `zslg-observability` | Observability assembly: the metric registry, histogram buckets, process gauges |
| `zslg-raft-setup` | Raft assembly: wires storage, RPC, state publishing and the node for one openraft instance |
| `zslg-replica-node` | Replica node assembly: raft voter, replica log, learner, leader loop and writer on one node |
| `zslg-writer-node` | Writer node assembly: tracks the cluster over RPC and fans writes to a quorum |

## facade

| crate | purpose |
|---|---|
| `btrb` | The btrb platform for apps: one dependency that re-exports the crates behind it |
| `nvzn` | Projections folded from an append-only log |
| `zslg` | A replicated, eventually-stable HLC-ordered log |

## application

| crate | purpose |
|---|---|
| `kompakt-workspace-checks` | Checks over the whole workspace (crate layering) |
| `zslg-cluster-tests` | End-to-end tests: real multi-node zslg clusters over TCP |
