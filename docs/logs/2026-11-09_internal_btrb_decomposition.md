# btrb decomposition — my mental model

Updated as the interview goes. Open questions come with the answer I expect, so a wrong model shows early.

## What I think we are doing

Split `btrb` into layered crates the way zslg/nvzn were split: each crate one role, a layering test that
enforces it, tests with their owners, a thin facade so consumers' paths do not move.

## The shape I currently see

```text
  http mechanisms   floor  buckets  assets  base_path      no deps among themselves, axum/blake3 only
  config            Env, Scope, parse, ConfigError         a library
  app SDK           App, Entry, Handles, Job, Shutdown, Live/ReadAfterWrite, Handler/Remote, Console, Histogram
  host runtime      settings, db, fold, routes, security, metrics, serve, work, standalone, cli
```

Real obstacles: `app` ↔ `admin` is a cycle (Shutdown/Stopped sit in `app`); `host::settings` mixes a
~400-line zslg-knob reader (nothing btrb-specific) with btrb's own settings; the lib.rs header describes a
contract-only crate, which is no longer what it is.

## Open questions → expected answer

| # | question | I expect |
|---|---|---|
| 1 | is `btrb::…` fixed for nlnr (internal split only), or may apps depend on a narrower crate? | facade stays; narrow crates exist for later |
| 2 | is the cut app-SDK vs host-runtime the axis, or something finer inside the SDK? | SDK vs host first, then split the leaves |
| 3 | does the zslg-knob reader belong in btrb at all? | no: it belongs next to `ReplicaNodeConfig` |
| 4 | tests: integration tests (`tests/live_write.rs`) stay in the facade? | yes, like nvzn |
| 5 | scope: only btrb, or also the nlnr side (git rev bump, Cargo.toml changes)? | btrb only; nlnr follows with a rev bump |
