# btrb decomposition — my mental model

Updated as the interview goes. Open questions come with the answer I expect, so a wrong model shows early.

## What I think we are doing

Split `btrb` into layered crates the way zslg/nvzn were split: each crate one role, a layering test that
enforces it, tests with their owners, a thin facade so consumers' paths do not move.

## The shape I currently see (revised after D1)

btrb is a platform, not a library: five capabilities, and every app runs on all of them.

```text
  capability     app-facing piece                          host machinery
  db             Nvzn, AppCache, Live/ReadAfterWrite       host::db, fold, settings (zslg/nvzn/cache knobs)
  web            floor, buckets, assets, base_path         host::routes, security, serve
  background     Job, Shutdown                             host::work, signals, run_background
  cli            Console, Handler, Remote, Entry::command  admin socket, host::cli
  metrics        Histogram                                 host::metrics (recorder, gauges, listener)
  ─ cross-cutting: config (Env, Scope, parse), Error
  ─ composition:  App/Entry/Handles (touches all five) and the roles serve | work | standalone | cli
```

Candidate: one crate per capability owning *both* columns, config/error as libraries, `App` + the roles as the
assembly over them, `btrb` the facade. Obstacles: `app` ↔ `admin` cycle; `live` straddles db and web; the
~400-line zslg-knob reader inside `host::settings` is not btrb's.

## Open questions → expected answer

| # | question | I expect |
|---|---|---|
| 1 | ~~facade vs narrow crates~~ | settled: D1, facade |
| 2 | ~~capability cut~~ | settled: D2 |
| 2b | ~~settings ownership~~ | settled: D3, slices |
| 2c | ~~`live.rs` placement~~ | settled: D4, own crate |
| 2d | ~~seam vs roles~~ | settled: D5, two crates |
| 2f | ~~error layering~~ | settled: D6, one shared opaque error |
| 2h | ~~who owns health~~ | settled: D7, the roles |
| 2g | from btrb's point of view are the `zslg` and `nvzn` facades libraries (as zslg was for nvzn)? | yes |
| 2e | `AppConfig`, `CookiePolicy`, `SigningKey`: web? | web |
| 3 | does the zslg-knob reader belong in btrb at all? | no: it belongs next to `ReplicaNodeConfig` |
| 4 | tests: integration tests (`tests/live_write.rs`) stay in the facade? | yes, like nvzn |
| 5 | scope: only btrb, or also the nlnr side (git rev bump, Cargo.toml changes)? | btrb only; nlnr follows with a rev bump |

## Status

Design doc written: `2026-11-10_design_btrb_decomposition.md`, awaiting sign-off. Assumptions taken without asking
are in its §4.2 (A1–A6); I expect the user to accept A1, A2, A4, A6 as written and might push on A3 (a types
crate for `Shutdown`/`Stopped`/`Job`) and on the crate count (eleven crates for ~7k lines).
