//! The fault vocabulary and the strategies that generate scripts of it.

use super::GENERATED_RECOVERY_BUDGET_MS;
use proptest::prelude::{Just, Strategy, prop_oneof};

/// One step of a generated script.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Op {
    /// `n` writes of `len` bytes each, issued concurrently to the node at
    /// `target` (resolved modulo the live node list) and joined. `n = 1` is a
    /// plain single write, and is what this op shrinks toward — so a minimal
    /// counterexample is the simple case.
    ///
    /// Each write's `DataId` is recorded only on success.
    ///
    /// The target is part of the op rather than derived from a seed so it
    /// shrinks with the rest of the script and appears in the failing input.
    ///
    /// # Why a burst, and why against one node
    ///
    /// The capacity dials in [`config_for`] are per-node queue depths, so they
    /// only bite when more than one write is in flight against the same node. A
    /// burst against a single target is what fills them; spreading it across
    /// nodes would divide the pressure by three and reproduce the concurrency-1
    /// case on each. See [`capacity_ranges`] for what that measurably changed.
    ///
    /// This is also the only op that exercises CLAUDE.md §8's write-batch
    /// drain, which folds additional queued items into one append + fsync via
    /// `try_recv`. At concurrency 1 that drain always batches exactly one item.
    WriteBurst { n: usize, len: usize, target: usize },
    /// Issue single writes against node 0 until one is acked, or until
    /// `within_ms` elapses. Records whether it ever got one, how many attempts
    /// it took, and how long — see [`CaseResult::recovery`].
    ///
    /// # Why this is a separate op and not a `WriteBurst { n: 1 }`
    ///
    /// A burst asks "does the cluster ack *right now*", which is a question
    /// about the moment it lands. This asks "does the cluster ack *eventually*",
    /// which is a question about whether it is stressed or broken — and those
    /// are different claims with different answers under load. A burst issued
    /// into an election in flight can legitimately ack nothing (CLAUDE.md §20:
    /// a failed write licenses no claim); a cluster that cannot ack a single
    /// write in a whole minute of retries is wedged.
    ///
    /// It keeps its own accounting and deliberately does **not** touch
    /// [`Tally`]: its retries would inflate `attempted` with attempts made
    /// *after* the stress the tally is measuring, and the ratio printed by the
    /// stressed-floor test would stop meaning what it says. The one write that
    /// does ack is still pushed onto the case's `acked` ground truth, so the
    /// durability chain covers it like any other.
    ///
    /// Never generated: [`script_strategy`] does not emit it, because the
    /// question it asks is about a script's end state rather than about a step
    /// that composes with faults. Hand-written scripts use it.
    WriteUntilAcked { within_ms: u64 },
    /// Take the node at this index down **gracefully** (resolved modulo the
    /// live node list) and **leave it down**. Every op that follows, until the
    /// matching [`Op::Up`], runs against a two-node cluster — still a quorum of
    /// three, so writes must continue to be acked.
    Down(usize),
    /// Take the node at this index down the way a crash does: drop it without
    /// cancelling its shutdown token and without awaiting `run()`, so its
    /// `JoinSet` aborts its tasks mid-flight and whatever was not yet fsynced
    /// stays that way. Opens the same down window as [`Op::Down`] and is closed
    /// by the same [`Op::Up`], so the quorum floor covers both identically.
    ///
    /// The difference from `Down` is what the *relaunch* then has to do: a
    /// graceful stop leaves a clean store, a kill leaves the crash debris
    /// CLAUDE.md §8's recovery rules exist for (a torn WAL tail, a batch with no
    /// commit marker), and the node has to repair it before it can rejoin and
    /// serve. See the module header for why this needs no production surface and
    /// what was measured before it was added.
    Kill(usize),
    /// Bring the downed node back on its original address and db-path, so it
    /// resumes its persisted raft state and its `NodeId` — the same thing an
    /// operator's rolling bounce does. That the `NodeId` is unchanged is
    /// asserted, and is what distinguishes this pair from [`Op::Replace`].
    ///
    /// Closes a window opened by either [`Op::Down`] or [`Op::Kill`]; the driver
    /// does not distinguish, because a node that resumes from crash debris must
    /// come back the same way one that stopped cleanly does.
    ///
    /// Takes no index: [`script_strategy`] guarantees at most one node is down,
    /// so there is only ever one node to bring back.
    Up,
    /// Replace the node at this index with a fresh one on the same address.
    /// The new node has a new `NodeId`, so this drives a real membership
    /// change: joint config, drain, collapse.
    ///
    /// The proptest accumulates `acked` across the whole script and checks the
    /// durability chain at the end, so a `Replace` with writes on both sides of
    /// it *is* the test of CLAUDE.md §8's drain invariant — every write acked
    /// before the change must still be readable after the outgoing node is
    /// gone. No separate assertion is needed.
    Replace(usize),
    /// A bounded pause, letting timer-driven work (snapshots, cutoff advances,
    /// compaction) interleave with the faults.
    Settle(u64),
}

/// Node indices are generated in `0..3` and resolved against the live node
/// list at execution time, so every script is well-formed by construction
/// rather than filtered.
///
/// Snapshots and cutoff advances are deliberately **not** ops: no trigger
/// affordance exists for them, and the short `snapshot_interval` makes them
/// interleave on their own. That they interleave *safely* is the property
/// under test; their exact timing is not.
pub(crate) fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Writes are weighted up: they are what the durability chain is keyed
        // on, and a script of nothing but restarts asserts very little.
        6 => (1usize..=32, 1usize..=256, 0usize..3)
            .prop_map(|(n, len, target)| Op::WriteBurst { n, len, target }),
        // The two window-openers weigh 1 each and `Up` weighs 2, so openers and
        // closers are drawn about equally often. The imbalance a given script
        // draws is what produces the interesting shapes: an `Up` with no open
        // window becomes a `Settle`, and a window left open at the end is closed
        // by an appended `Up` — so a surplus of openers lengthens the degraded
        // window rather than breaking the script. Weighting `Up` at 1 against
        // two openers would instead spend most of the vocabulary on openers
        // rewritten to `Settle`, thinning the fault coverage.
        1 => (0usize..3).prop_map(Op::Down),
        1 => (0usize..3).prop_map(Op::Kill),
        2 => Just(Op::Up),
        // The most expensive op in the vocabulary: a whole node teardown plus a
        // fresh launch plus a two-phase membership change. Weighted down to 1
        // accordingly, and capped at two per script by `script_strategy`.
        1 => (0usize..3).prop_map(Op::Replace),
        3 => (50u64..=300).prop_map(Op::Settle),
    ]
}

/// A script, guaranteed to contain at least one [`Op::WriteBurst`].
///
/// The guarantee is structural rather than a filter, and it is load-bearing
/// for the assertions: every write claim in the proptest body is about `acked`,
/// which only means something if the script tried to write at all. `op_strategy`
/// picks a non-write with probability 0.4, so an all-restart-and-settle script
/// of 8 ops arises about once per 1500 cases — rare enough to survive review and
/// frequent enough to fire spuriously on a nightly 256-case triple run.
/// Appending a write when none was generated removes that case entirely,
/// which is preferable to guarding the claim with `attempted > 0` — a guard
/// would silently skip those cases rather than test them. The appended burst is
/// `n = 1`: the guarantee is "the script writes at all", and adding pressure
/// that the generator did not ask for would make the fallback path behave
/// differently from the generated one.
pub(crate) fn script_strategy() -> impl Strategy<Value = Vec<Op>> {
    (
        proptest::collection::vec(op_strategy(), 8..=20),
        1usize..=256,
        0usize..3,
    )
        .prop_map(|(mut ops, len, target)| {
            if !ops.iter().any(|op| matches!(op, Op::WriteBurst { .. })) {
                ops.push(Op::WriteBurst { n: 1, len, target });
            }
            enforce_script_constraints(ops)
        })
}

/// The two script-level constraints, in one walk. Split out of
/// [`script_strategy`] so [`script_constraints_hold_for_every_short_script`] can
/// check them exhaustively — a violation here does not fail an assertion, it
/// wedges a case below quorum for the full 30 s deadline.
///
/// Both belong here and not in [`run_case`]: `tally.attempted` is incremented
/// before a burst is issued, so any runtime `continue` over a generated op
/// silently breaks the `acked == attempted` claim. Constraints on the script are
/// the strategy's job.
///
/// 1. **The quorum floor.** At most one of three nodes is down at a time. A
///    second window-opener (`Down` *or* `Kill` — the walk does not distinguish
///    them, because a killed node is exactly as absent as a stopped one), or an
///    `Up` with no window open, becomes a `Settle` — a rewrite rather than a
///    filter, so no sample is rejected and shrinking stays clean. A window still
///    open at the end is closed by an appended `Up`.
/// 2. **The replace cap.** A script that replaces every node repeatedly is
///    `rolling_replacement.rs`, which already exists as a named scenario.
///
/// `Replace` interacts with the floor because it *is* a node-down window: it
/// stops the victim before launching its successor, so a `Replace` inside a
/// `Down` window leaves one live node of three and the successor's raft join
/// blocks forever. The window is therefore **closed ahead of it** rather than
/// the `Replace` being dropped — dropping it would silently thin out the
/// membership coverage this module exists for, in proportion to how much of a
/// script is spent degraded.
///
/// The two rewrites are ordered: the cap is applied **first**, so a `Replace`
/// that becomes a `Settle` does not also close an open window. Closing first
/// would spend a degraded window — the thing the `Down`/`Up` split exists to
/// produce — on an op that then never runs.
pub(crate) fn enforce_script_constraints(ops: Vec<Op>) -> Vec<Op> {
    let mut replaces = 0;
    let mut down = false;
    let mut out: Vec<Op> = Vec::with_capacity(ops.len().saturating_add(2));
    for op in ops {
        match op {
            Op::Down(_) | Op::Kill(_) if down => out.push(Op::Settle(50)),
            Op::Down(idx) => {
                down = true;
                out.push(Op::Down(idx));
            }
            Op::Kill(idx) => {
                down = true;
                out.push(Op::Kill(idx));
            }
            Op::Up if !down => out.push(Op::Settle(50)),
            Op::Up => {
                down = false;
                out.push(Op::Up);
            }
            Op::Replace(_) if replaces >= 2 => out.push(Op::Settle(50)),
            Op::Replace(idx) => {
                if down {
                    down = false;
                    out.push(Op::Up);
                }
                replaces += 1;
                out.push(Op::Replace(idx));
            }
            other => out.push(other),
        }
    }
    if down {
        out.push(Op::Up);
    }
    // 3. **The recovery probe**, appended last — after the `Up` above, so it
    //    always runs against a whole cluster rather than a degraded one. This is
    //    the liveness claim every case carries (see the `match (regime,
    //    faulted)` in the proptest body): the script's own bursts answer "did
    //    writes land *while* this was happening", which is a question about how
    //    fast the host is, and this answers "does it admit writes once the
    //    script stops pushing", which is a question about the system. The settle
    //    is not the recovery window — the probe carries its own — it just keeps
    //    the first attempt out of the tail of the last op.
    out.push(Op::Settle(500));
    out.push(Op::WriteUntilAcked {
        within_ms: GENERATED_RECOVERY_BUDGET_MS,
    });
    out
}
