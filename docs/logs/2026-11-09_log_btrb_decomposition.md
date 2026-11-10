# btrb decomposition — decision log

Process: `nlnr/docs/iaic.md`. Append-only: settled questions, decisions, why, rejected alternatives.
Predecessor: the zslg/nvzn crate split (`2026-11-07_*_crate_split.md`), whose pattern this round reuses.

## Starting facts (not decisions)

- `btrb` is one crate, role `application`, ~6.9k lines, "the host runtime for btrb applications".
- Consumers are the nlnr crates (`robin*`, `nlnr*`), through a git dependency pinned by rev. Almost all of them
  use the *app-author* surface (`Error`, `App`, `Written`, `Buckets`, `floor`, `SigningKey`, `AppConfig`,
  `CookiePolicy`, `Shutdown`, `Job`, `Entry`, `Handler`, `Live`); `btrb::host` appears at 4 call sites (binaries).
- Module dependencies inside `btrb` (from `use crate::…`):

```text
  leaves, no deps    assets  base_path  buckets  console  floor  config(env, parse)
  error          ──▶ config
  live           ──▶ base_path                         (+ nvzn)
  app            ──▶ admin  base_path  config  console  live  metrics
  admin          ──▶ app          ◀── cycle with app (Shutdown, Stopped live in app)
  metrics        ──▶ app
  host/*         ──▶ admin  config  live  (+ app items via the crate root)
```

## Decisions

### D1 — the consumer contract is the facade; the split is internal (settled)

`btrb` stays the one crate consumers name and re-exports everything; nlnr changes by a rev bump only.

Why: btrb is an app *platform* — it provides the db layer, the web layer, the background-process layer, CLIs
and metrics, and every app mounted on it runs on all of them. There is no consumer that wants a subset, so a
narrow "SDK" crate has nobody to serve. The first framing (apps are library customers who might want less)
was wrong; the platform framing is the user's.

Rejected: B (apps depend on a narrow SDK crate, binaries on the host crate) and C (A now, narrow crates as
the documented way in): both rest on consumers using part of the platform.

Consequence: the layering inside btrb follows the platform's capabilities, not an SDK/host line.

### D2 — the primary cut is by capability (settled)

One crate per capability — db, web, background, cli, metrics — each owning what apps use *and* what the host
runs for it. Config and `Error` are libraries beneath them; `App`/`Entry`/`Handles` and the process roles
(server, worker, standalone, cli) are the assembly over them; `btrb` is the facade.

Why: every app runs on all five layers, so the capabilities are the stable units; the roles only choose which
capabilities a process carries.

Rejected: cutting by role first with capabilities as modules inside each. It repeats the shared layers per
role and hides that they are shared.

### D3 — settings are per-capability slices (settled)

Each capability crate owns the environment variables of its layer: names, parsing, defaults. The assembly reads
every slice from the one `Env`, passes the role-dependent defaults down (server 32 MiB, worker 64 MiB,
standalone 16 MiB RocksDB budgets), and calls `Env::finish()` once so a typo still fails before any store
opens. The ZSLG_*/NVZN_* knob reader (`host::settings::replica_node`, `fields`) therefore lives in the db
capability.

Why: each capability can be understood and tested alone, and a new capability brings its own knobs.

Rejected: one settings module in the assembly. It keeps the names in one grep-able place, but the assembly
would know every layer's variables and no capability could be configured on its own.

### D4 — read-after-write is its own crate above db and web (settled)

`live.rs` (~300 production lines, its own setting `BTRB_READ_AFTER_WRITE_WAIT` and metric
`btrb_read_after_write_wait_seconds`) becomes one crate depending on db and web; the assembly mounts it.

Why: one protocol — a cookie carrying a write's position, and the wait for the view to show it — stays in one
place, and `Live::write` returns a web response part (`Written`), so splitting it would make db depend on web.

Rejected: a module in the web crate behind the `Waits` trait (web would carry a db dependency for one seam),
and a split across db and web. Accepted cost: the smallest crate of the set.

### D5 — the app seam and the process roles are two assemblies, seam below roles (settled)

`btrb-app` (App, Entry, Handles, Nvzn, boot) sits over the capability crates and knows nothing about
processes. `btrb-host` (the serve, work, standalone and cli roles) sits over `btrb-app`. The `app` ↔ `admin`
cycle disappears because the shutdown types live in the background crate beneath both.

Why: the seam is the contract an app is written against and must not be able to grow process logic.

Rejected: one assembly containing both — fewer boundaries, but nothing would stop `App` from mentioning roles.

### D6 — one shared opaque `Error`; `ConfigError` is the only typed error (settled)

`btrb::Error` (a boxed source that carries an app's error across the seam) lives in a base crate every
capability returns. `ConfigError` stays typed in the config crate because callers match it and its messages
name the variable. No per-capability error unions.

Why: nobody matches on btrb errors — consumers print a chain; the zslg unions paid off only because callers
matched and the compiler could prove which leaves a crate raises.

Rejected: per-capability error types converting into `Error` at the seam — more machinery for the same output.

