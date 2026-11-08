# zslg crate split — what changed from the design

Branch `ccr-3ceece9d-e3tfbt`. Design: `2026-11-07_design_crate_split.md`;
decisions: `2026-11-07_log_crate_split.md`. This note records where the
implementation (zslg side, steps 1–14) departed from the design, and why. The
nvzn split (step 15) is tracked separately below.

## Result

zslg is 73 workspace crates by role (`crates/README.md`). The `zslg` facade is
`pub use`, the public `Error`, `zslg::testing`, the integration tests and the
examples; it depends on no type crate and no implementation. The layering test
(`zslg::layering`) enforces the role table, the ports→ports exception for
`zslg-net`, and "no `-types` import above the ports layer" (except
`zslg-lifecycle-types`).

## Deviations

| area | design | as built | why |
|---|---|---|---|
| node ids | with `NodeId` in consensus | `zslg-node-types` | shared by every group |
| `HasHlc` | n/a | trait in `zslg-time` | keeps the clock port free of log types |
| facade role | — | transitional `monolith` role until step 14 | allowed the split to land crate by crate; removed in step 14 |
| error size bar | `size_of::<Error>() <= size_of::<DataId>() + 8` | bar kept at 40 bytes | measured; user-approved |
| `Home` / `err_into` | — | `error_home!` (`.home()` on Result and channels) | boundary conversions without per-site `map_err` |
| spare selection | `SpareSelector` trait | `SpareSelectionStrategy::rank` + `CandidateInfo` in consensus-types | trait had one implementer |
| extra ports | — | `RaftMetricsSource`, `ClusterBootstrap` (raft); `WriteGate`/`Admission` (time); `ReplicaLog` (log); `IntentSubmitter` (consensus) | each removed an implementation→implementation edge |
| shared metrics | one crate per emitter | `log_writes`, `log_index_frontier`, `purged_entries` in `zslg-log-types`; leader-job heartbeat in `zslg-consensus-types` | emitted by several implementations |
| fakes | `net-fake` for peers | `zslg-peers` (static peers), `zslg-consensus-fake`, `zslg-net-fake`, `zslg-time-fake`, `zslg-log-fake` | actual doubles needed |
| raft | `openraft` used directly | `raft-kit` is the only crate depending on openraft | one place to track the dependency |
| leader | integrator + three job crates | plus `pace-kit` (library) and `RaftIntent`/`IntentSubmitter` moved to the ports | jobs share `Pace` and submit intents; sideways edges otherwise |
| assemblies | raft-setup, learner, leader, observability, 4 nodes | plus `zslg-net-setup` (shared by 3 nodes) and **`zslg-error`** | see below |
| public error | `zslg::Error` declared in the facade | `zslg-error` declares it; node crates return it; the facade re-exports | nodes live below the facade, so the union cannot; user-approved |
| `ConfigError`/`LifecycleError` | via ports | re-exported from `zslg-error` | no ports crate owns them |
| `HlcClock` | removed from the facade API | removed | §5.6 |
| tests needing two implementations | move to the lowest assembly | learner tests moved to `zslg-learner`; pacing tests to `zslg-leader`; those needing a *cluster* (leader snapshot/compaction/membership, replica service, raft cluster tests) stay in the facade as end-to-end tests | the cluster harness serves the e2e suites too; decided per test with the user |
| `testing` features | on owning implementation | also on assemblies (`zslg-learner`, `zslg-raft-setup`, `zslg-replica-node`); the layering test allows an assembly an *optional* fake dependency | assemblies export real-engine test helpers |
| `error_home!` | `pub(crate)` trait | takes a visibility | `zslg-error` exports it |

## Workflow notes

- One commit per crate, each green on `clippy --workspace --all-targets
  --all-features --profile lint -D warnings`; the full workspace test run was
  executed at the milestones. Two tests are load-sensitive and pass alone:
  `cluster_proptest::cluster_upholds_invariants_under_generated_faults` and
  `objectstorage-cache` evictor timing; decommission tests flake under a full
  parallel run and pass in isolation.
- `[profile.lint] incremental = false` keeps `target/` from growing with the
  number of crates; stale test executables in `target/lint/deps` can be deleted
  when the disk fills.

## Not done yet

- nvzn split (design §9): `nvzn-types`, `nvzn-store`, `-overlay`, `-rocksdb`,
  `-fake`, `-resultset`, `-dispatcher`, `-live`, `-reader`, facade; metrics onto
  `metrics-kit` with a golden test. `Subscriptions` goes to `nvzn-types` as a
  shared cell (decided).
- CI / `cargo deny` verification on the final tree, and a before/after loadgen
  comparison (design §8).
- Revisit the error model once everything has landed (user asked to do this
  last).
