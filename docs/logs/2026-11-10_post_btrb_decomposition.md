# btrb decomposition — what changed from the design

Branch `ccr-3ceece9d-e3tfbt`. Design: `2026-11-10_design_btrb_decomposition.md`;
decisions and implementation notes: `2026-11-09_log_btrb_decomposition.md`.

## Result

`btrb` was one crate of about 2,200 lines holding the seam, the process roles, five capabilities and
read-after-write. It is now a facade over eleven crates:

```text
  btrb (facade, re-exports only)
    ├─ btrb-host        roles serve · work · standalone · cli, /health, /ready, the Settings aggregate
    ├─ btrb-app         the seam: App, Entry, boot, the metrics list
    ├─ btrb-live        read-after-write
    ├─ btrb-db  btrb-web  btrb-background  btrb-cli  btrb-metrics     the capabilities
    └─ btrb-types  btrb-config                                         vocabulary · env grammar
```

Every path nlnr uses resolves unchanged; nlnr's `cargo check --all-targets` builds against the patched
workspace without a source change.

## Deviations

| area | design | as built | why |
|---|---|---|---|
| `Histogram` | in `btrb-metrics` | in `btrb-types` | capabilities may not depend on each other (N4) |
| `until_stop` | calls the signal handler | takes the shutdown future | `btrb-db` may not depend on `btrb-background` (N5) |
| facade dependencies | `btrb-host`, `btrb-app` and below | `btrb-host`, `btrb-app`, `btrb-live`, `btrb-config` | the role table admits assemblies only; `btrb-app` re-exports the capability items (N6) |
| metrics address | read in the host | `btrb-metrics::settings::listen_addr(env, default)` | each capability owns its variable; the host passes the role default |

## Verification

- Golden dumps of env var names, `btrb_*` metric names, routes and headers (static greps over the `btrb*`
  sources) are identical before and after.
- `cargo clippy --workspace --all-targets --all-features` is clean; the btrb crates' tests and the layering
  test pass.
- Tests moved with their code: web 22, db 27, cli 10, config 17, live 4, app 3, host 5.
