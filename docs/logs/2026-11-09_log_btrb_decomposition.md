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

(none yet)
