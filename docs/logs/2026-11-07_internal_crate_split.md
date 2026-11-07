# zslg / nvzn crate split — internal model

Working document for the IAIC interview. Current model only.

## Where we are

Interview closed (D1-D15). Design doc written; waiting for sign-off. Prediction track record so far: wrong on Q6 (user wants more crates), Q7 (finer), Q11 (facade only), Q13 (one PR). Right on the rest — so the user leans towards more crates, smaller steps, layering hidden from consumers. Then the implementation layer cut (database, object-store, quorum, clock impl, net, raft, learner, leader, replica, facade). No design doc yet.

## Model of the problem

```text
today      zslg (one crate)  <- nvzn
             module graph has cycles; layering is by convention only

candidate  foundation = several domain crates, each types+traits (time, log, consensus, snapshot, net)
           step 0: break cycles inside zslg (no new crates), enforce layering
           step 1: cut bottom-up: core -> database/object-store -> raft+network
                   -> learner -> leader -> zslg facade (re-exports, *_node, tests)
```

## Open questions and expected answers

| # | Question | Expected answer |
|---|---|---|
| Q1 | Driver: enforced layering / cognitive load vs build+test time | layering and reviewability; build time secondary |
| Q2 | Facade: keep `zslg` name and public API byte-compatible | yes, nlnr must not change |
| Q3 | Fate of `metrics.rs` (2.7k) and `error.rs` | per-crate definitions, registered by facade |
| Q4 | Where do the 8k lines of integration tests live | facade crate, unchanged |
| Q5 | Land as one PR per crate, or one big PR | one per crate, cycles first |
| Q6 | nvzn | stays one crate |

## What I expect at sign-off

- Likeliest comments: crate granularity in §4.1 (too fine or not fine enough),
  `nvzn-metrics` being central, the nested `Error` match.
- Possible pushback: the libraries-may-be-used-by-implementations refinement of
  D8 (§2.1), and whether `zslg-net-wire` is really a library.
- Biggest implementation risk: the `ClusterBootstrap` port not covering
  everything `startup` does, and fault hooks that need new ports.
