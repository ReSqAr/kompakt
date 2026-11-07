# zslg / nvzn crate split — decision log

Process: `nlnr/docs/iaic.md`. Branch `ccr-3ceece9d-e3tfbt`, base `6aa354b`.
Status: interview; docs only, no code changes.

Input: user question "should zslg + nvzn be broken into small, non-reusable
crates (zslg-api, zslg-local-store, ...), one per subfolder at least?"

## Context established (introspection, grep-based, approximate)

- zslg ~50k lines incl. tests (leader 10.7k, learner 8.8k, tests 8.2k, raft 6.9k,
  network 4.3k, metrics.rs 2.7k); nvzn ~6k lines, depends only on zslg.
- Module `crate::` edges are cyclic: api<->hlc, raft<->network, and the shared
  modules point upward (error->hlc/network/raft, ids->api/hlc/network,
  meta->quorum/raft, metrics->leader/raft). Folders therefore cannot become
  crates as they stand.
- `leader` and `learner` only depend downwards. `database` depends on
  `error` + `unit` only.
- Public API is a small `pub use` list in `zslg/src/lib.rs`; nlnr consumes
  zslg/nvzn by git rev only.

## Settled

(none yet)

## Open

- Q1: what is the split for (goal / driver)?
