# zslg / nvzn crate split — internal model

Working document for the IAIC interview. Current model only.

## Where we are

Q1-Q10 settled (D1-D10). Asked Q11 (consumer imports: groups vs facade). Still to cover: nvzn split, public API / nlnr compatibility, migration order, then design doc. Then the implementation layer cut (database, object-store, quorum, clock impl, net, raft, learner, leader, replica, facade). No design doc yet.

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
