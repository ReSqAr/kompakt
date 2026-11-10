# btrb decomposition — design

Process: `nlnr/docs/iaic.md`. Decisions D1–D7 are in `2026-11-09_log_btrb_decomposition.md`; this document is the
spec. Status: **awaiting sign-off**. Predecessor and pattern: `2026-11-07_design_crate_split.md`.

Reading order: §1 is the whole idea; §2–§4 frame it; §5 is the crate map; §6 the interfaces that carry the
decisions; §7–§10 edge cases and alternatives; §11–§12 how it is tested and built. Decisions I took beyond the
ones we settled are collected in §4.2, so they can be struck there.

## 1. The idea

btrb is an app **platform**. It gives every app a db layer, a web layer, a background-process layer, a CLI and
metrics, and every app mounted on it runs on all five. The decomposition follows that: one crate per
capability, the five independent of each other; a small crate of shared vocabulary beneath them and a config
library beside it; and above them the things that *compose* capabilities.

```text
  btrb                                  facade: every path consumers use today, unchanged
   │
   ├─ btrb-host                         the process roles: serve | work | standalone | cli
   │    └─ btrb-app                     the seam: App, Entry, Handles, Nvzn, boot
   │         ├─ btrb-live               read-after-write: a cookie that carries a write's position
   │         ├─ btrb-db  btrb-web  btrb-background  btrb-cli  btrb-metrics     the five capabilities
   │         └─ btrb-types  btrb-config                                         vocabulary · env grammar
   └─ zslg · nvzn                        libraries, from btrb's point of view
```

Each capability crate owns two things: what an app is given (`Buckets`, `Live`, `Console`, `Histogram`, …) and
what the host runs for it (the HTTP layers, the fold supervisor, the admin socket, the recorder). The
roles only decide which capabilities a process carries.

## 2. Goals, non-goals, success criteria

**Goals.** One crate per capability with its own tests, settings and header; no cycle between the seam and the
admin socket; no static shared between the web layer and the db layer; the zslg/nvzn knob reader next to the code
that uses it; the layering test enforcing all of it, as it does for zslg and nvzn.

**Non-goals.** No behaviour change: every environment variable, metric name, route, header and exit code stays.
No change to nlnr beyond a rev bump. No new abstraction that has one user (§10 lists the ones declined).
No narrowed dependency for consumers: everyone runs everything (D1).

**Success.** `cargo clippy --workspace --all-targets --all-features` and `cargo test --workspace` stay green at
every commit; nlnr builds against the branch unchanged; the golden dumps of §11 (env names, metric names, routes)
are identical before and after; `btrb`'s own source is re-exports and integration tests only.

## 3. Current state

One crate, role `application`, 6.9k lines. Inside it the modules form tiers, with two defects in them:

```text
  leaves      assets  base_path  buckets  floor  console  config(env, parse)
  mid         error→config   live→base_path   metrics→app   admin⇄app   ← cycle
  top         host/*  (serve, work, standalone, cli, db, fold, routes, security, metrics, paths, settings)
```

The cycle: `Shutdown` and `Stopped` are defined in `app` and used by `admin`, while `app` uses `admin`'s
`Handler`/`Remote`. The static: `host::routes` answers `/health`, `/ready` and the startup gate from
`fold::current()`, a `OnceLock` installed by the db supervisor, so the web layer reaches into the db layer.
Also: `host::settings::replica_node` (~400 lines) is the ZSLG_*/NVZN_* reader and is not btrb-specific, and the
`lib.rs` header describes a contract-only crate, which `btrb` stopped being when `host` arrived.

Consumers: nlnr through a git dependency pinned by rev. Almost all uses are the app-author surface; `btrb::host`
appears at four call sites, all binaries.

## 4. Constraints and assumptions

### 4.1 Constraints (settled)

The facade is the contract and the split is internal (D1). Capabilities are the cut (D2); settings are
per-capability slices (D3); read-after-write is its own crate (D4); the seam and the roles are two assemblies
(D5); one opaque `Error` (D6); the roles own health (D7). The layering test (`kompakt-workspace-checks`) is the
enforcement: every new crate declares a role, and implementation crates may depend only on library, types, ports,
procedure and config crates.

### 4.2 Assumptions I took — strike any of them

**A1.** From btrb's point of view the `zslg` and `nvzn` facades are *libraries* (the rule the nvzn family already
uses for zslg); `zslg`/`nvzn` never depend on btrb. This is what lets implementation-role crates use them.
**A2.** `AppConfig`, `CookiePolicy` and `SigningKey` go to `btrb-web`: the cookie layer and HSTS both hang off
`CookiePolicy`, and the key signs web tokens. **A3.** `Shutdown`, `Stopped` and `Job` (with `Error`) are the
shared vocabulary of the seam and go to a types crate, `btrb-types`: the five capabilities are independent
siblings, so anything two of them need sits beneath them. **A4.** `btrb` becomes role `facade` (it is `application`
today). **A5.** Each capability exposes the histograms it records as data (`HISTOGRAMS`), and `btrb-metrics`
installs a list it is handed; it knows no other capability. **A6.** The integration test `tests/live_write.rs`
stays in the facade, as nvzn's integration tests did.

## 5. Architecture

### 5.1 The crate map

| crate | role | owns | depends on (btrb family) | ≈ lines |
|---|---|---|---|---|
| `btrb-config` | config | `Env`, `Scope`, `parse`, `ConfigError` | — | 830 |
| `btrb-types` | types | `Error`, `Shutdown`, `Stopped`, `Job` | `btrb-config` | 100 |
| `btrb-web` | implementation | `floor`, `buckets`, `assets`, `BasePath`, `AppConfig`, `CookiePolicy`, `SigningKey`; limits, tracing, security headers, canonical paths; its settings | config, types | 1,100 |
| `btrb-db` | implementation | the zslg node and nvzn dispatcher bring-up, `FoldHealth` and the fold supervisor, `Cache`; the ZSLG_*/NVZN_*/cache/store settings | config, types | 1,300 |
| `btrb-background` | implementation | `run_background`, signal handling, the worker's settings | config, types | 150 |
| `btrb-cli` | implementation | `Console`, the admin socket (`AdminSocket`, `Remote`, `Handler`), the CLI socket setting | config, types | 580 |
| `btrb-metrics` | implementation | `Histogram`, the Prometheus recorder, process gauges, the `/metrics` listener, its settings | config, types | 290 |
| `btrb-live` | assembly | `Live`, `LiveSettings`, `Written`, the `ReadAfterWrite` layer, `BTRB_READ_AFTER_WRITE_WAIT`, its histogram | web, db, types | 300 |
| `btrb-app` | assembly | `App`, `Entry`, `Handles`, `Nvzn`, boot, the list of histograms | all of the above | 740 |
| `btrb-host` | assembly | the four roles, `/health`, `/ready`, the startup gate, the `Settings` aggregate, role defaults | `btrb-app` and below | 1,100 |
| `btrb` | facade | re-exports only | `btrb-host`, `btrb-app` and below | — |

### 5.2 Why the capabilities are siblings

An implementation crate may not depend on another implementation crate, so db, web, background, cli and metrics
cannot see each other. That constraint is the point: today's two couplings (the cycle, the static) were exactly
the places where one capability reached into another. Anything two of them need is either vocabulary (`btrb-types`)
or data a composer passes in (a `FoldHealth` handle, a `HISTOGRAMS` list). The composers — `btrb-live` for the one
protocol that spans db and web, `btrb-app` for the seam, `btrb-host` for the processes — are assemblies and may
depend on the implementation crates and on each other, strictly upward.

### 5.3 The layering test

`kompakt-workspace-checks` learns a third family. `btrb-*` may depend on `zslg` and `nvzn` (A1) and on its own
lower layers; `zslg-*` and `nvzn-*` never on `btrb-*`. The role table is unchanged: the new crates fit the
existing roles (`config`, `types`, `implementation`, `assembly`, `facade`).

## 6. Interfaces, data model, data flow

### 6.1 The shared vocabulary

`btrb-types` holds the four things the seam passes around: `Error` (a boxed source that carries an app's error
across the seam; it implements `From<ConfigError>` and `From<nvzn::Error>`), `Shutdown` and `Stopped` (a stop
request and the proof one was honoured), and `Job` (`BoxFuture<Result<Stopped, Error>>`). `ConfigError` stays in
`btrb-config` because callers match it and its messages name the variable. This is what removes the `app`⇄`admin`
cycle: `btrb-cli` and `btrb-app` both depend on `btrb-types`, and neither on the other.

### 6.2 Settings are slices

Each capability crate has a reader that takes the shared `Env` and the role's defaults and returns its slice;
nothing else in the crate touches the environment. The host assembles them and calls `Env::finish()` once, so a
mistyped variable still fails before any store opens.

```text
  btrb-host:   env = Env::from_process(prefixes)                      ZSLG_ NVZN_ BTRB_ + each app's
               db   = btrb_db::settings::read(&mut env, role_defaults)       (32 | 64 | 16 MiB budgets)
               web  = btrb_web::settings::read(&mut env)              listen addr, cookie policy
               …    background, cli, metrics, live (read-after-write wait)
               apps = configure_all(&mut env, …)
               env.finish()?                                           unread variable ⇒ error, exit
               Settings { db, web, … , temp_dirs }
```

The `:temp:` directory guards travel in the slices that created them and are collected by `Settings`, exactly
as now: the host holds them for as long as the stores are open.

### 6.3 Metrics are data

An app already says what it records as data (`App::histograms()`). Every capability does the same: it exposes a
`HISTOGRAMS: &[Histogram]` beside the series it emits (`btrb-live` its wait histogram; `btrb-db` the cache's).
`btrb-app` builds the process's list (the apps', then every capability's) and `btrb-metrics` installs a recorder
for the list it is handed, then calls zslg's and nvzn's `describe_metrics`. Today `host::metrics` hard-codes the
read-after-write buckets; after the change nothing in `btrb-metrics` names another capability.

### 6.4 Health without a global

```text
  before   web routes ──reads──▶ fold::current()  (OnceLock, installed by db)
  after    btrb-db:    FoldHealth { is_ready(), stop_reason() }, built by the host, fed by catch_up()
           btrb-host:  routes::platform(health.clone())   /health  /ready      ─┐ the handle is
                       routes::startup_gate(health)       503 until replayed   ─┘ captured, not global
```

`catch_up`/`until_stop` keep their signatures. `btrb-web` keeps only the generic layers (body and time limits,
tracing, security headers, canonical paths); the host composes them with the platform routes and the gate in the
same order as today. The `btrb_fold_up` gauge stays with the db supervisor that sets it.

### 6.5 The seam and read-after-write

`btrb-app` keeps `App`, `Entry`, `Mounted`, `Handles`, `Nvzn` and boot as they are; the `Nvzn` constructor is the
one place that builds a `Live` (from `btrb-live`). `btrb-live` exports what the seam and the host need and
nothing else: `Live`, `LiveSettings`, `Written`, the `ReadAfterWrite` layer, `DEFAULT_WAIT`, `HISTOGRAMS`. The
layer reaches the db only through the `Waits` trait it already has.

### 6.6 The facade

`btrb` re-exports so that every path nlnr uses resolves unchanged (`btrb::floor`, `btrb::buckets`,
`btrb::config`, `btrb::host`, `btrb::Error`, `btrb::Live`, …). Module-valued paths are re-exported as modules
(`pub use btrb_web::{assets, buckets, floor};`). The facade forwards the `jemalloc` feature and the `prometheus`
and `s3` features of the crates it re-exports. After the change `btrb/src/lib.rs` contains no logic and its
header says so.

## 7. State transitions

No state machine is introduced or changed. The process lifecycle (joining → replaying → caught up → draining; a
stopped fold exits non-zero) stays in `btrb-db` and `btrb-host` and is only re-wired (§6.4). The one new
dependency edge that matters is the order in which the host builds things, unchanged from `host::serve` today:

```text
  settings ─▶ metrics recorder ─▶ db (node) ─▶ boot apps ─▶ assemble routes (+ health, gate) ─▶ serve
                                                    └─ the fold catches up while the server listens
```

## 8. Complexity

**Essential.** Five independent capabilities composed by three assemblies; the settings slices (each capability's
variables stay with it, the host owns the role-dependent defaults and the single `finish`); the `:temp:` guard
lifetime, which is a real constraint on where a slice's value lives.

**Accidental, accepted.** Eleven crates for ~7k lines, two of them (`btrb-background`, `btrb-types`) under 200
lines; the layering test needs the third family. **Accidental, removed.** The `app`⇄`admin` cycle, the
web→db static, the hard-coded read-after-write buckets in the recorder, and the zslg knob reader's wrong home.

## 9. Edge cases

**Unread variables.** `Env::finish` must still see every slice read before it runs, including each app's; the
host test that a typo fails before anything opens moves with the aggregate. **Defaults depend on the role.** The
role is a parameter of the db slice's reader, not a global, so a standalone process cannot inherit the server's
budgets. **The global's other readers.** Anything besides `/health`, `/ready` and the gate that reads
`fold::current()` is found before the static is removed (§12, step 6). **Doc and feature paths.** Intra-doc
links across the old module boundaries become cross-crate and are checked by `cargo doc`; feature flags are
forwarded by the facade. **Facade visibility.** An item consumers use that is `pub(crate)` today (for instance
`Booted::wrap`) becomes `pub` at the crate boundary and is `#[doc(hidden)]` in the facade.

## 10. Alternatives considered

Narrow SDK crates for apps (D1: nobody wants a subset). Cut by role first (D2). One settings module in the
assembly (D3). Read-after-write as a module of the web crate or split across db and web (D4). One assembly for
the seam and the roles (D5). Per-capability error unions (D6). A `Health` handle every capability feeds (D7).
**Declined here:** a trait for "a capability" (five implementations, no caller that is generic over them), and
merging `btrb-background` into `btrb-types` (it would hide that the runner and signals are a capability).

## 11. Testing strategy

Tests move with their owners: `assets`, `buckets`, `floor`, `base_path` and `security` to `btrb-web`; `fold` and
the db settings tests to `btrb-db`; `console` and the admin socket to `btrb-cli`; `env` and `parse` to
`btrb-config`; the live tests to `btrb-live`; `app` to `btrb-app`; routes, canonical paths, the standalone role
and the settings aggregate to `btrb-host`. Inline modules over ~300 lines go to sibling `tests.rs` files
(the rule used in zslg).

Three checks run outside the crates, each as a before/after comparison: (1) a **golden dump** of every
environment variable the process reads (`Env`'s read set), of every metric name and bucket list, and of the
routes with their headers; (2) the layering test; (3) **nlnr builds** against the branch through a `[patch]` of
its git dependency, with no source change. The facade keeps `tests/live_write.rs`.

## 12. Implementation sequence

A linearisation of the dependency DAG. Every step is one or more commits that satisfy clippy and the tests; the
facade re-exports what has moved, so nothing downstream changes until the last step.

1. **Layering first.** Teach `kompakt-workspace-checks` the btrb family and the new roles (no code moves).
   Capture the golden dumps of §11 and the nlnr `[patch]` build as the baseline.
2. **`btrb-config`, `btrb-types`.** Move `config/*`, `Error`, `Shutdown`, `Stopped`, `Job`; facade re-exports.
3. **`btrb-web`.** `floor`, `buckets`, `assets`, `base_path`, `AppConfig`/`CookiePolicy`/`SigningKey`, security
   headers, canonical paths, the generic layers of `routes`; its settings slice.
4. **`btrb-metrics`, `btrb-cli`, `btrb-background`.** `Histogram` and the recorder (taking a list); `Console` and
   the admin socket; `run_background` and signals. Each with its slice and tests.
5. **`btrb-db`.** Bring-up, `Cache`, the fold supervisor with an explicit `FoldHealth`, the ZSLG_*/NVZN_* reader.
6. **Health into the host.** Find every reader of `fold::current()`; move `/health`, `/ready` and the gate into
   `btrb-host` capturing the handle; delete the static.
7. **`btrb-live`**, then **`btrb-app`**, then **`btrb-host`** (roles and the `Settings` aggregate).
8. **Close.** `btrb` reduced to re-exports, header rewritten, `crates/README.md`, `docs/crate_inventory.md`, a
   post-split note (`…_post_btrb_decomposition.md`), the golden dumps and the nlnr build compared with the baseline.

## 13. Operational concerns

Nothing changes at run time: the same variables, metrics, routes, headers and exit codes (§11 checks it).
Rollout is a rev bump in nlnr. The one observable difference a deployer could notice is none by design; if
the golden dumps differ, that is a bug in the split, not a decision. Observability of the split itself is the
layering test and the nlnr build.
