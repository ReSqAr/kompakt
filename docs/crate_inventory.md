# Crate inventory

Workspace-internal dependencies only; "dev" lists edges that exist only for tests.


## library

| crate | depends on | dev only |
|---|---|---|
| `backoff-kit` | — | — |
| `bytesize` | — | — |
| `crossbeam-combinators` | — | — |
| `error-union` | — | — |
| `framing` | — | — |
| `metrics-kit` | — | — |
| `objectstorage-cache` | — | — |
| `pace-kit` | — | — |
| `raft-kit` | — | — |
| `rocksdb-kit` | bytesize | — |
| `shutdown` | — | — |
| `svg-charts` | — | — |
| `warn-throttle` | — | — |

## types

| crate | depends on | dev only |
|---|---|---|
| `nvzn-types` | crossbeam-combinators, metrics-kit, zslg | — |
| `zslg-consensus-types` | metrics-kit, zslg-log-types, zslg-net-types, zslg-node-types, zslg-snapshot-types, zslg-time-types | — |
| `zslg-error` | bytesize, error-union, framing, rocksdb-kit, shutdown, zslg-consensus-types, zslg-lifecycle-types, zslg-log-types, zslg-net-types, zslg-snapshot-types, zslg-time-types | zslg-node-types |
| `zslg-lifecycle-types` | — | — |
| `zslg-log-types` | crossbeam-combinators, metrics-kit, zslg-node-types, zslg-time-types | — |
| `zslg-net-types` | — | — |
| `zslg-node-types` | — | — |
| `zslg-raft-types` | raft-kit, zslg-consensus-types, zslg-net-types, zslg-node-types | — |
| `zslg-snapshot-types` | metrics-kit, zslg-lifecycle-types, zslg-log-types, zslg-time-types | — |
| `zslg-time-types` | crossbeam-combinators | — |

## ports

| crate | depends on | dev only |
|---|---|---|
| `nvzn-store` | nvzn-projection, nvzn-types, zslg | — |
| `nvzn-projection` | nvzn-types, zslg | — |
| `zslg-consensus` | error-union, zslg-consensus-types, zslg-error, zslg-lifecycle-types, zslg-log-types, zslg-net-types, zslg-node-types, zslg-snapshot-types, zslg-time-types | — |
| `zslg-log` | crossbeam-combinators, error-union, zslg-consensus-types, zslg-error, zslg-lifecycle-types, zslg-log-types, zslg-net-types, zslg-node-types, zslg-snapshot-types, zslg-time-types | — |
| `zslg-net` | error-union, zslg-consensus, zslg-consensus-types, zslg-error, zslg-lifecycle-types, zslg-log, zslg-log-types, zslg-net-types, zslg-node-types, zslg-raft, zslg-snapshot-types, zslg-time-types | — |
| `zslg-raft` | error-union, raft-kit, zslg-consensus-types, zslg-error, zslg-lifecycle-types, zslg-log-types, zslg-net-types, zslg-node-types, zslg-raft-types, zslg-snapshot-types, zslg-time-types | — |
| `zslg-snapshot` | crossbeam-combinators, error-union, zslg-consensus-types, zslg-error, zslg-lifecycle-types, zslg-log-types, zslg-net-types, zslg-snapshot-types, zslg-time-types | — |
| `zslg-time` | error-union, zslg-consensus-types, zslg-error, zslg-lifecycle-types, zslg-log-types, zslg-net-types, zslg-snapshot-types, zslg-time-types | — |

## procedure

| crate | depends on | dev only |
|---|---|---|
| `nvzn-projector` | metrics-kit, nvzn-projection, nvzn-store, nvzn-types | — |
| `zslg-net-wire` | bytesize, error-union, framing, metrics-kit, raft-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-quorum-read` | error-union, metrics-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | — |

## config

| crate | depends on | dev only |
|---|---|---|
| `zslg-object-store-config` | error-union, zslg-error, zslg-snapshot-types | — |

## fake

| crate | depends on | dev only |
|---|---|---|
| `nvzn-fixtures` | crossbeam-combinators, nvzn-projection, nvzn-store, nvzn-types, zslg | — |
| `nvzn-store-fake` | nvzn-projection, nvzn-store, nvzn-types | — |
| `zslg-consensus-fake` | zslg-consensus, zslg-net | — |
| `zslg-log-fake` | crossbeam-combinators, zslg-consensus, zslg-log, zslg-time | — |
| `zslg-net-fake` | crossbeam-combinators, error-union, raft-kit, zslg-consensus, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-time-fake` | zslg-time | — |

## implementation

| crate | depends on | dev only |
|---|---|---|
| `nvzn-resultset` | nvzn-projection, nvzn-types | — |
| `nvzn-store-rocksdb` | metrics-kit, nvzn-projection, nvzn-store, nvzn-types, zslg | nvzn-fixtures |
| `zslg-hlc-clock` | metrics-kit, zslg-time | — |
| `zslg-hlc-gate` | metrics-kit, zslg-consensus, zslg-time | — |
| `zslg-leader-compaction` | crossbeam-combinators, error-union, metrics-kit, shutdown, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-leader-integrator` | error-union, metrics-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | zslg-consensus-fake |
| `zslg-leader-membership` | error-union, metrics-kit, pace-kit, shutdown, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | zslg-consensus-fake, zslg-net-fake |
| `zslg-leader-snapshot` | crossbeam-combinators, error-union, metrics-kit, pace-kit, shutdown, warn-throttle, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-local-store` | bytesize, crossbeam-combinators, error-union, metrics-kit, rocksdb-kit, shutdown, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | zslg-consensus-fake, zslg-log-fake |
| `zslg-net-client` | bytesize, crossbeam-combinators, error-union, metrics-kit, raft-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-net-wire, zslg-raft, zslg-snapshot, zslg-time | zslg-time-fake |
| `zslg-net-router` | crossbeam-combinators, raft-kit, zslg-consensus, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft | — |
| `zslg-net-server` | crossbeam-combinators, error-union, metrics-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-net-wire, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-node-id` | error-union, rocksdb-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log | bytesize |
| `zslg-object-store` | crossbeam-combinators, framing, metrics-kit, zslg-log, zslg-snapshot | zslg-consensus, zslg-time |
| `zslg-peers` | zslg-consensus, zslg-net | — |
| `zslg-raft-node` | error-union, metrics-kit, raft-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-raft-rpc` | raft-kit, zslg-consensus, zslg-net, zslg-raft | — |
| `zslg-raft-startup` | backoff-kit, error-union, metrics-kit, shutdown, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-raft-state` | metrics-kit, raft-kit, zslg-consensus, zslg-raft, zslg-time | — |
| `zslg-raft-storage` | error-union, metrics-kit, raft-kit, rocksdb-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-snapshot, zslg-time | bytesize |
| `zslg-raft-tracker` | backoff-kit, error-union, shutdown, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-snapshot, zslg-time | — |
| `zslg-reader` | crossbeam-combinators, error-union, metrics-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | zslg-consensus-fake |
| `zslg-replica-log` | bytesize, crossbeam-combinators, error-union, metrics-kit, rocksdb-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | zslg-log-fake |
| `zslg-replica-service` | crossbeam-combinators, error-union, metrics-kit, warn-throttle, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-stable-archive` | crossbeam-combinators, error-union, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | — |
| `zslg-stable-log` | bytesize, crossbeam-combinators, error-union, metrics-kit, rocksdb-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | zslg-log-fake |
| `zslg-subscriber` | backoff-kit, error-union, metrics-kit, warn-throttle, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | crossbeam-combinators, zslg-consensus-fake, zslg-log-fake, zslg-time-fake |
| `zslg-writer` | backoff-kit, crossbeam-combinators, error-union, metrics-kit, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-quorum-read, zslg-raft, zslg-snapshot, zslg-time | zslg-consensus-fake, zslg-log-fake, zslg-net-fake |

## assembly

| crate | depends on | dev only |
|---|---|---|
| `nvzn-dispatcher` | crossbeam-combinators, metrics-kit, nvzn-projection, nvzn-projector, nvzn-resultset, nvzn-store, nvzn-store-fake, nvzn-store-rocksdb, nvzn-types, zslg | nvzn-fixtures |
| `zslg-follower-node` | bytesize, rocksdb-kit, shutdown, zslg-consensus, zslg-error, zslg-hlc-clock, zslg-learner, zslg-lifecycle-types, zslg-log, zslg-net, zslg-net-setup, zslg-node-id, zslg-object-store, zslg-object-store-config, zslg-observability, zslg-peers, zslg-raft-tracker, zslg-snapshot, zslg-writer | — |
| `zslg-leader` | error-union, shutdown, zslg-consensus, zslg-error, zslg-leader-compaction, zslg-leader-integrator, zslg-leader-membership, zslg-leader-snapshot, zslg-lifecycle-types, zslg-log, zslg-net, zslg-snapshot, zslg-time | crossbeam-combinators, pace-kit, warn-throttle, zslg-net-fake |
| `zslg-learner` | bytesize, crossbeam-combinators, error-union, rocksdb-kit, shutdown, zslg-consensus, zslg-error, zslg-hlc-clock, zslg-lifecycle-types, zslg-local-store, zslg-log, zslg-log-fake, zslg-net, zslg-quorum-read, zslg-raft, zslg-reader, zslg-snapshot, zslg-stable-archive, zslg-stable-log, zslg-subscriber, zslg-time | zslg-consensus-fake, zslg-object-store |
| `zslg-learner-node` | bytesize, rocksdb-kit, shutdown, zslg-consensus, zslg-error, zslg-hlc-clock, zslg-learner, zslg-lifecycle-types, zslg-log, zslg-net, zslg-net-setup, zslg-node-id, zslg-object-store, zslg-object-store-config, zslg-observability, zslg-peers, zslg-raft-tracker, zslg-snapshot | — |
| `zslg-net-setup` | error-union, shutdown, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-net-client, zslg-net-router, zslg-net-server, zslg-net-wire, zslg-snapshot, zslg-time | — |
| `zslg-observability` | error-union, metrics-kit, rocksdb-kit, zslg-consensus, zslg-error, zslg-hlc-clock, zslg-hlc-gate, zslg-leader-compaction, zslg-leader-integrator, zslg-leader-membership, zslg-leader-snapshot, zslg-lifecycle-types, zslg-local-store, zslg-log, zslg-net, zslg-net-client, zslg-net-server, zslg-net-wire, zslg-object-store, zslg-quorum-read, zslg-raft-node, zslg-raft-startup, zslg-raft-state, zslg-raft-storage, zslg-reader, zslg-replica-log, zslg-replica-service, zslg-snapshot, zslg-stable-log, zslg-subscriber, zslg-time, zslg-writer | — |
| `zslg-raft-setup` | error-union, raft-kit, rocksdb-kit, shutdown, zslg-consensus, zslg-error, zslg-lifecycle-types, zslg-log, zslg-net, zslg-raft, zslg-raft-node, zslg-raft-rpc, zslg-raft-startup, zslg-raft-state, zslg-raft-storage, zslg-snapshot, zslg-time | — |
| `zslg-replica-node` | bytesize, rocksdb-kit, shutdown, zslg-consensus, zslg-error, zslg-hlc-clock, zslg-hlc-gate, zslg-leader, zslg-leader-membership, zslg-learner, zslg-lifecycle-types, zslg-log, zslg-net, zslg-net-setup, zslg-node-id, zslg-object-store, zslg-object-store-config, zslg-observability, zslg-peers, zslg-raft-node, zslg-raft-setup, zslg-raft-storage, zslg-raft-tracker, zslg-replica-log, zslg-replica-service, zslg-snapshot, zslg-time, zslg-writer | — |
| `zslg-writer-node` | bytesize, shutdown, zslg-consensus, zslg-error, zslg-hlc-clock, zslg-log, zslg-net, zslg-net-setup, zslg-observability, zslg-peers, zslg-raft-tracker, zslg-writer | — |

## facade

| crate | depends on | dev only |
|---|---|---|
| `nvzn` | metrics-kit, nvzn-dispatcher, nvzn-projection, nvzn-store, zslg | crossbeam-combinators, nvzn-fixtures, nvzn-projector, nvzn-store-rocksdb, nvzn-types |
| `zslg` | bytesize, zslg-consensus, zslg-follower-node, zslg-learner-node, zslg-log, zslg-log-fake, zslg-net, zslg-object-store-config, zslg-observability, zslg-replica-node, zslg-snapshot, zslg-time, zslg-writer-node | crossbeam-combinators, raft-kit, rocksdb-kit, shutdown, zslg-consensus-fake, zslg-error, zslg-hlc-clock, zslg-hlc-gate, zslg-leader, zslg-leader-compaction, zslg-leader-integrator, zslg-leader-membership, zslg-leader-snapshot, zslg-learner, zslg-local-store, zslg-net-client, zslg-net-fake, zslg-net-setup, zslg-net-wire, zslg-node-id, zslg-object-store, zslg-peers, zslg-raft, zslg-raft-node, zslg-raft-rpc, zslg-raft-setup, zslg-raft-storage, zslg-reader, zslg-replica-log, zslg-replica-service, zslg-time-fake |

## application

| crate | depends on | dev only |
|---|---|---|
| `btrb` | nvzn, objectstorage-cache, zslg | — |
| `kompakt-workspace-checks` | — | — |
