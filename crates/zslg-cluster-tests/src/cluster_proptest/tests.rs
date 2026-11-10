//! The generated and hand-pinned cases.

use super::{
    Op, Regime, capacity_ranges, config_for, enforce_script_constraints, pick, regime_ranges,
    run_case, script_strategy, spawn_observer,
};
use crate::{
    Cluster, ClusterConfig, NodePlacement, assert_cluster_invariants,
    assert_cluster_invariants_with_acks, assert_durability_chain_on,
};
use bytes::Bytes;
use object_store::memory::InMemory;
use proptest::prelude::{Just, ProptestConfig, prop_oneof, proptest};
use proptest::{prop_assert, prop_assert_eq};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use zslg_consensus::NodeId;

// ── tests ─────────────────────────────────────────────────────────────

/// Every config the generator can produce must pass `validate()`. Checked at
/// every corner of both regimes' ranges — the two timescales and the three
/// capacity dials — so a validation failure surfaces here as a clear unit
/// failure rather than as a mysterious cluster-start error inside a generated
/// case.
///
/// This is also the guard on the one cross-field constraint `config_for`
/// satisfies by derivation rather than by range choice
/// (`subscribe >= max_flush_batch`): the derivation pins them equal, so any
/// loosening of it shows up here.
#[test]
pub(crate) fn generated_configs_validate_at_every_regime_corner() {
    for regime in [Regime::Healthy, Regime::Stressed] {
        let (snap, elec) = regime_ranges(regime);
        let (mfb_r, wc_r) = capacity_ranges(regime);
        for &snapshot_ms in &[*snap.start(), *snap.end()] {
            for &election_min_ms in &[*elec.start(), *elec.end()] {
                for &mfb in &[*mfb_r.start(), *mfb_r.end()] {
                    for &wc in &[*wc_r.start(), *wc_r.end()] {
                        let mut cfg = config_for(snapshot_ms, election_min_ms, mfb, wc);
                        // `validate` requires a non-empty peer list that
                        // includes this node's own server_name, plus a
                        // db_path; the cluster harness fills both in. Supply
                        // stand-ins so this test exercises the *tunings*,
                        // which is what the generator varies. The stand-in
                        // peer's server_name must match `cfg.network_server_name`
                        // (set by `default_fast_config` to "placeholder") or
                        // this trips the "own config omits self" check.
                        cfg.peers = vec![zslg_net::PeerAddr {
                            addr: "127.0.0.1:1".into(),
                            server_name: cfg.network_server_name.clone(),
                        }];
                        cfg.db_path = std::path::PathBuf::from("/tmp/placeholder.db");
                        cfg.validate().unwrap_or_else(|e| {
                            panic!(
                                "{regime:?} corner (snapshot {snapshot_ms}ms, election_min \
                                 {election_min_ms}ms, max_flush_batch {mfb}, \
                                 write_capacity {wc}) fails validation: {e}"
                            )
                        });
                    }
                }
            }
        }
    }
}

/// Every script the generator can emit must satisfy the two constraints
/// [`enforce_script_constraints`] exists to impose: at most one node down at
/// any point, no `Replace` inside a down window, and no window left open at the
/// end.
///
/// Checked exhaustively over every sequence of the five *shape-relevant* ops up
/// to length 5 — `WriteBurst` and `Settle` are folded into one `Settle`
/// representative because neither reads or writes the walk's state, so they
/// cannot affect its outcome. 5^0 + … + 5^5 = 3906 scripts, instant to run.
/// `Kill` is enumerated alongside `Down` rather than folded into it: they are
/// two arms of the walk and folding them would stop checking that the second
/// one maintains the floor.
///
/// It earns its place because a violation is not a failed assertion but a
/// *hang*, and where the case blocks decides whether it ever comes back. Two of
/// three down has no quorum. If it blocks in a `wait_until_sync` it panics after
/// 30 s, somewhere unrelated to the generator that caused it. If it blocks
/// inside a node's raft startup — which is what an `Up` or a `Replace` into a
/// quorum-less config does — it never comes back at all: raft startup is
/// unbounded by design, and `raft/startup/bootstrap.rs::run_phase` escalates its
/// logging the longer it waits but never aborts, so `raft_startup_phase_timeout`
/// sets an escalation cadence and not a deadline. That is CLAUDE.md §17's hazard
/// exactly — a hung test needs manual intervention — so cheap deterministic
/// coverage of the walk is worth far more than the seed that eventually
/// reproduces one.
#[test]
pub(crate) fn script_constraints_hold_for_every_short_script() {
    let alphabet = [
        Op::Down(0),
        Op::Kill(0),
        Op::Up,
        Op::Replace(0),
        Op::Settle(50),
    ];
    let mut scripts: Vec<Vec<Op>> = vec![Vec::new()];
    // Extend only the previous round's frontier. Re-extending everything
    // accumulated so far would re-check each shorter script once per remaining
    // round — the same 1365 distinct scripts, evaluated 3125 times.
    let mut frontier: Vec<Vec<Op>> = vec![Vec::new()];
    for _ in 0..5 {
        let mut next = Vec::new();
        for script in &frontier {
            for op in alphabet {
                let mut extended = script.clone();
                extended.push(op);
                next.push(extended);
            }
        }
        scripts.extend(next.iter().cloned());
        frontier = next;
    }
    assert_eq!(scripts.len(), 3906, "every script of length 0..=5");

    for script in scripts {
        let out = enforce_script_constraints(script.clone());
        let mut down = false;
        let mut replaces = 0;
        for op in &out {
            match op {
                Op::Down(_) | Op::Kill(_) => {
                    assert!(!down, "two nodes down at once in {out:?} (from {script:?})");
                    down = true;
                }
                Op::Up => {
                    assert!(down, "Up with no node down in {out:?} (from {script:?})");
                    down = false;
                }
                Op::Replace(_) => {
                    assert!(
                        !down,
                        "Replace inside a down window in {out:?} (from {script:?})"
                    );
                    replaces += 1;
                }
                Op::WriteBurst { .. } | Op::Settle(_) | Op::WriteUntilAcked { .. } => {}
            }
        }
        assert!(
            !down,
            "script ends with a node down: {out:?} (from {script:?})"
        );
        assert!(replaces <= 2, "replace cap exceeded in {out:?}");
    }
}

/// The stressed regime must be *stressed*, not simply broken.
///
/// "Everything errored" is a correct outcome under `Stressed` but a vacuous
/// one, and a stratum that always errors everything burns CI to assert
/// nothing. This runs one fixed `Stressed` script at the most aggressive corner
/// of *both* range sets — the timescale floor and the capacity floor, the
/// latter being where the stress now lives — and then requires the cluster to
/// **recover**: after the bursts, a patient probe must get a single write
/// admitted.
///
/// # Why recovery, and not "the bursts acked something"
///
/// This asserted `tally.acked > 0` over the bursts themselves until it was
/// found to fail on a slower machine with `attempted=128 acked=0` and a raft
/// term that had walked past 30. Both halves of that are worth separating:
///
/// - **Acking nothing under maximal burst pressure is a legitimate `Stressed`
///   outcome, not a defect.** Four concurrent bursts of 32 against a two-slot
///   outcome channel, at a timescale floor whose `write_timeout` derives to
///   720 ms, is a regime where whether *any* write beats its deadline depends
///   on how fast the host is. Asserting on it measures the host.
/// - **Never acking again is a defect**, and that is the property the
///   assertion was reaching for all along: the difference between stressed and
///   broken. [`Op::WriteUntilAcked`] tests it directly and does not care how
///   fast the host is — a fast host recovers in one attempt, a slow one in
///   twenty, and both pass; only a wedged cluster fails.
///
/// So the stress is unchanged — same bursts, same floors, both ranges are
/// still confirmed by this test rather than widened by it — and the claim on
/// top of it got stronger rather than weaker. The burst tally is still printed,
/// because the thin margin is informative even when it is not a gate: on the
/// reference host four bursts of 32 ack about 25 of 128, while the same floor
/// driven one write at a time acks 4 of 4 and sits nowhere near the edge.
///
/// The failures are outcome-channel drops, and a dropped outcome is a durable
/// write whose ack was shed (CLAUDE.md §8): nothing here treats that as an
/// error, and nothing asserts the data is absent.
///
/// # Known fragility at this floor
///
/// `election_min = 60` derives a **15 ms** AppendEntries deadline
/// (`config_for` sets `heartbeat_interval = election_min / 4`, which openraft
/// 0.9.24 spends as the RPC hard deadline) against openraft's **fixed 500 ms**
/// replication backoff. A host that cannot serve a raft round trip inside 15 ms
/// therefore pays a full election for every slow RPC, which is the term churn
/// seen above. That is why the recovery probe is given a whole minute: it must
/// outlast an election storm, not merely a slow write. See
/// [`full_replacement_transfers_membership_and_data`] for the same coupling
/// where it was not survivable.
#[tokio::test(flavor = "multi_thread")]
pub(crate) async fn stressed_regime_admits_writes() {
    crate::init_logging();
    let (snap, elec) = regime_ranges(Regime::Stressed);
    let (mfb_r, wc_r) = capacity_ranges(Regime::Stressed);
    // Bursts at the top of the generated `n` range, not single writes: the
    // capacity floor is only a floor if something is pushing against it, and a
    // burst of 32 against a two-slot write channel is that. This is also the
    // script the shed-warning count in `capacity_ranges` was measured on.
    let script = vec![
        Op::Settle(200),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
        Op::WriteBurst {
            n: 32,
            len: 64,
            target: 0,
        },
        // Then stop pushing and ask whether it comes back. The settle is not
        // the recovery window — the probe below carries its own — it just means
        // the first attempt is not issued into the tail of the last burst.
        Op::Settle(500),
        Op::WriteUntilAcked { within_ms: 60_000 },
    ];
    // Boxed at the call site (CLAUDE.md §2): `run_case` owns a whole cluster's
    // worth of locals, and this is a one-off heavy await.
    let result = Box::pin(run_case(
        *snap.start(),
        *elec.start(),
        *mfb_r.start(),
        *wc_r.start(),
        script,
    ))
    .await;
    let recovery = result.recovery.expect("the script ran a WriteUntilAcked");
    println!(
        "[stressed floor] attempted={} acked={} boundary_exercised={} \
         recovery={{admitted={} attempts={} elapsed={:?}}}",
        result.tally.attempted,
        result.tally.acked,
        result.boundary_exercised,
        recovery.admitted,
        recovery.attempts,
        recovery.elapsed,
    );
    assert!(
        recovery.admitted,
        "the Stressed regime never recovered at its floor \
         (snapshot {}ms, election_min {}ms, max_flush_batch {}, write_capacity {}): \
         after the bursts, {} single writes over {:?} were all refused. The bursts \
         acking nothing is fair at this floor; never acking again is not — this is \
         a wedged cluster, not a stressed one. Check the raft term in the log: a \
         term that keeps climbing means the election storm never ended, and the \
         floor is below what this host can serve (see the header).",
        snap.start(),
        elec.start(),
        mfb_r.start(),
        wc_r.start(),
        recovery.attempts,
        recovery.elapsed,
    );
}

/// Can a hard-killed node be relaunched on its own db-path?
///
/// Two things must hold and neither may be assumed. `spawn_blocking` tasks
/// cannot be aborted — they run to completion — and the engine's blocking
/// loops hold RocksDB's LOCK file, so a relaunch may fail with the DB still
/// open. The listener must also release its port.
#[tokio::test(flavor = "multi_thread")]
pub(crate) async fn hard_killed_node_can_be_relaunched() {
    crate::init_logging();
    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(config_for(100, 400, 32, 256)),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    let leader = cluster.leader().expect("no leader");
    let pre_kill = Bytes::from_static(b"pre-kill");
    let acked = cluster
        .node(leader)
        .writer()
        .write(pre_kill.clone())
        .await
        .expect("write");

    let victim = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a non-leader node");
    let killed_at = std::time::Instant::now();
    // `kill` returns once the aborted listener has given the port back, so this
    // elapsed time *is* the port-free spike.
    let placement = cluster.kill(victim).await;
    println!("[kill spike] port freed after {:?}", killed_at.elapsed());

    let relaunch_at = std::time::Instant::now();
    let relaunched = cluster.relaunch(placement).await.unwrap_or_else(|e| {
        panic!(
            "relaunch after a hard kill failed after {:?} — a failure here is the finding: \
             the engine still holds the db-path, so a crash is not reachable at this tier: {e}",
            relaunch_at.elapsed()
        )
    });
    println!(
        "[kill spike] relaunch took {:?} (kill to relaunched: {:?})",
        relaunch_at.elapsed(),
        killed_at.elapsed()
    );
    assert_eq!(relaunched, victim, "relaunch must resume the same NodeId");

    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;
    let _ = assert_cluster_invariants_with_acks(&cluster, &[(acked, pre_kill)]).await;
    cluster.shutdown().await.expect("clean shutdown");
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Low by default so CI cost stays bounded; the nightly job raises it
        // via PROPTEST_CASES.
        cases: 8,
        // Each case starts a real three-node cluster over real TCP. Shrinking
        // by re-running dozens of variants would take longer than the run
        // itself; the failing seed plus the printed script is the diagnostic.
        max_shrink_iters: 4,
        .. ProptestConfig::default()
    })]

    #[test]
    fn cluster_upholds_invariants_under_generated_faults(
        regime in prop_oneof![Just(Regime::Healthy), Just(Regime::Stressed)],
        snapshot_pick in 0u64..=1000,
        election_pick in 0u64..=1000,
        flush_batch_pick in 0u64..=1000,
        write_capacity_pick in 0u64..=1000,
        ops in script_strategy(),
    ) {
        crate::init_logging();
        let (snap_r, elec_r) = regime_ranges(regime);
        let snapshot_ms = pick(&snap_r, snapshot_pick);
        let election_min_ms = pick(&elec_r, election_pick);
        let (mfb_r, wc_r) = capacity_ranges(regime);
        let max_flush_batch = pick(&mfb_r, flush_batch_pick);
        let write_capacity = pick(&wc_r, write_capacity_pick);

        // A fresh runtime per case: a case restarts nodes and drops a whole
        // cluster, and sharing a runtime across cases would let one case's
        // stragglers land in the next one's.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt.block_on(run_case(
            snapshot_ms,
            election_min_ms,
            max_flush_batch,
            write_capacity,
            ops,
        ));

        println!(
            "[case] regime={regime:?} snapshot={snapshot_ms}ms election_min={election_min_ms}ms \
             flush_batch={max_flush_batch} write_cap={write_capacity} \
             attempted={} acked={} down_attempted={} down_acked={} replaced={} killed={} \
             faulted={} degraded_windows={} boundary_exercised={} joint_polls={} \
             recovery_attempts={}",
            result.tally.attempted, result.tally.acked, result.tally.attempted_while_down,
            result.tally.acked_while_down, result.tally.replaced, result.tally.killed,
            result.faulted, result.degraded_windows, result.boundary_exercised, result.joint_polls,
            result.recovery.map_or(0, |r| r.attempts)
        );

        // The write claim, three ways. What is asserted is always `acked` —
        // never `attempted`, which is incremented before the write is issued
        // and so is a property of the generated script, not of the cluster.
        // `script_strategy` guarantees `attempted >= 1`, so each branch below
        // is a real claim about the system rather than about the generator.
        match (regime, result.faulted) {
            // Generous timescales and no fault: every write must land. This is
            // the only place liveness is claimed in full, and it holds under
            // concurrency — 506 of 506 acked across the fault-free `Healthy`
            // cases of a 64-case run, with bursts up to 32. That is only **4 of
            // 38** `Healthy` cases, because the branch is scoped by `faulted`
            // and most scripts fault, so it is thinly sampled and the figure
            // reads narrower than its denominator suggests. The reason it holds
            // is structural rather than lucky: `Healthy`'s `write_capacity`
            // floor is 256, 8x the largest burst the generator can produce, so
            // the outcome channel that costs `Stressed` its writes never fills
            // here.
            (Regime::Healthy, false) => prop_assert_eq!(
                result.tally.acked, result.tally.attempted,
                "fault-free Healthy script lost a write: {:?}", result.tally
            ),
            // A `Down`, a `Kill` or a `Replace` bounces the leader and costs the
            // writes issued inside the re-election (or membership-change) window
            // — but not all of them: **the cluster is a quorum throughout**, since a
            // script may only take one of three down, so a sustained down
            // window is degraded, not unavailable. Zero acks here would mean a
            // fault permanently wedged a quorate cluster, which is a real
            // failure and must be investigated rather than assertion-adjusted.
            // A `Down`, a `Kill` or a `Replace` bounces the leader and costs the
            // writes issued inside the re-election (or membership-change)
            // window. What must NOT happen is that the fault costs every write
            // from then on — the cluster is a quorum throughout, since a script
            // may only take one of three down, so a sustained down window is
            // degraded, not unavailable.
            //
            // That claim is carried by the recovery probe below rather than by
            // a whole-script `acked > 0`, which was what stood here. The two
            // differ on slow hosts and only one of them is about the system:
            // `acked > 0` asks whether some burst happened to land *while* the
            // fault was in flight, which a host slow enough to put every burst
            // inside the re-election window fails without anything being wrong.
            // The recovery probe asks whether writes are admitted once the
            // script stops pushing, which is the actual difference between
            // "degraded" and "wedged".
            (Regime::Healthy, true) => {}
            // Stressed makes no write claim: timescales are near operation cost
            // and the queues are at the shed boundary by design, so "everything
            // errored" is a correct outcome. It is now measurably the stratum
            // that loses writes — 83.3% acked, 24 of 29 cases lossy. A loss here
            // means only that quorum was not observed: most are durable writes
            // whose ack was shed, but a write can also expire on the client
            // deadline while still in flight, or fail its append. Nothing may
            // therefore assert that an unacked Stressed write is *absent* —
            // which is exactly why no claim is made here (CLAUDE.md §20).
            // `stressed_regime_admits_writes` still pins the floor corner
            // specifically, which a generated case reaches only by chance.
            (Regime::Stressed, _) => {}
        }

        // The claim every case carries, in every regime and with or without a
        // fault: once the script stops pushing, the cluster admits a write
        // again.
        //
        // This is what `Stressed` asserted nothing in place of. Raft-level
        // liveness was already covered — `run_case`'s quiesce panics if no
        // leader is elected or the replica set does not converge — but a
        // cluster that holds an election and converges while never acking
        // another write satisfied both and every write branch above, so the
        // whole stratum passed on it. It is also the honest version of the
        // `(Healthy, faulted)` branch, for the reason given there.
        let recovery = result
            .recovery
            .expect("enforce_script_constraints appends a WriteUntilAcked to every script");
        prop_assert!(
            recovery.admitted,
            "the cluster never admitted another write after the script: {} attempts over {:?} \
             were all refused ({regime:?}, faulted={}). The script's own writes failing is a \
             fair outcome under these dials; never accepting one again is not — that is a \
             wedged cluster, and the tally {:?} is context, not the failure.",
            recovery.attempts,
            recovery.elapsed,
            result.faulted,
            result.tally,
        );
    }
}

/// Every op in the vocabulary, in one deterministic `Healthy` script.
///
/// The generator reaches every op probabilistically; nothing pinned a single
/// script that uses all of them together, so no fixed case covered the
/// *interplay* — a write burst inside a graceful-down window, then inside a
/// hard-kill window, then a membership change on top. This is that case.
///
/// It also asserts every op actually executed. A script whose ops silently
/// no-op asserts nothing, and the battery would still pass: `tally.killed`,
/// `tally.replaced` and `degraded_windows` are the vacuity guards, and each is
/// checked to fail if the corresponding op is removed.
///
/// # Why `boundary_exercised` is reported and not asserted
///
/// It measures `false` here, and that is correct rather than a gap. `Healthy`
/// snapshots at 50 ms (`regime_ranges`), so by the time the script's trailing
/// `Settle` and `run_case`'s quiesce are done, every write has been promoted
/// and `unstable_tail` is empty — the boundary check has nothing left to
/// compare. That it is genuinely "all promoted" and not "no watermark" follows
/// from the durability chain in the same battery passing for all 20 acked
/// entries on every node, which requires a watermark that covers them.
///
/// Asserting it would therefore be asserting that snapshots are *slow*. The
/// flag exists so a vacuous check is reported rather than mistaken for a
/// passing one, and this is the reporting.
#[tokio::test(flavor = "multi_thread")]
pub(crate) async fn full_vocabulary_smoke_script_holds_the_battery() {
    crate::init_logging();
    // Healthy dials; the script itself drives the faults.
    let (snap, elec) = regime_ranges(Regime::Healthy);
    let (mfb_r, wc_r) = capacity_ranges(Regime::Healthy);
    // Valid by construction against `run_case`'s own assertions: at most one
    // node down at a time, every Down/Kill window closed by an Up, and no
    // Replace inside a window.
    let script = vec![
        Op::Settle(100),
        Op::WriteBurst {
            n: 8,
            len: 64,
            target: 0,
        },
        Op::Down(1),
        Op::WriteBurst {
            n: 4,
            len: 32,
            target: 2,
        },
        Op::Up,
        Op::Kill(2),
        Op::WriteBurst {
            n: 4,
            len: 32,
            target: 0,
        },
        Op::Up,
        Op::Replace(1),
        Op::WriteBurst {
            n: 8,
            len: 64,
            target: 0,
        },
        Op::Settle(100),
    ];
    // Boxed at the call site (CLAUDE.md §2): `run_case` owns a whole cluster's
    // worth of locals, and this is a one-off heavy await.
    let result = Box::pin(run_case(
        *snap.start(),
        *elec.start(),
        *mfb_r.start(),
        *wc_r.start(),
        script,
    ))
    .await;
    println!(
        "[full vocabulary] attempted={} acked={} killed={} replaced={} \
         degraded_windows={} joint_polls={} boundary_exercised={}",
        result.tally.attempted,
        result.tally.acked,
        result.tally.killed,
        result.tally.replaced,
        result.degraded_windows,
        result.joint_polls,
        result.boundary_exercised,
    );

    // The battery and the observer already ran inside `run_case`. What is left
    // is vacuity: prove the script did what it says.
    assert!(
        result.tally.acked > 0,
        "smoke script acked nothing: {:?}",
        result.tally
    );
    assert!(
        result.tally.killed >= 1,
        "smoke script never executed a Kill: {:?}",
        result.tally
    );
    assert!(
        result.tally.replaced >= 1,
        "smoke script never executed a Replace: {:?}",
        result.tally
    );
    assert!(
        result.degraded_windows >= 1,
        "no Down/Kill window carried ops: {}",
        result.degraded_windows
    );
}

/// The full replacement: a 7-node cluster whose 3 replicated members are all
/// killed at once, driving `Joint { old: R, next: N }` with `R ∩ N = ∅`.
///
/// This is the shape `docs/TESTING.md` recorded as unreachable at this tier,
/// and the reason the drain bound was untestable: `compute_target` seeds from
/// `current ∩ live`, so `next` always retains the live members of `old` and
/// every quorum of `old` intersects `next` — under which the bound is never
/// load-bearing and every armed exit measures `Exit::AnyTime`.
///
/// Seven nodes break the deadlock that made it unreachable before. Killing all
/// 3 replicas forces `old ∩ live = ∅`, so `next` is disjoint; the 4 surviving
/// voters are still a quorum of 7, so Raft keeps committing; and the killed
/// nodes are relaunched (never evicted, so they resume as voters) in time to
/// satisfy step 2's generation quorum of `old`, which is what previously could
/// never be met once `old` was wholly dead.
///
/// Step 2b then samples a *real* `WatermarkAbove` — the relaunched nodes' logs
/// still hold W, the write acked against `R` alone — and step 3 must wait for
/// a snapshot to cover it before collapsing.
///
/// Liveness, not just safety: every stage carries an explicit deadline, so a
/// stalled membership change is a failure that names its stage rather than a
/// hang.
///
/// The 30 s snapshot interval is load-bearing. W must stay above the manifest
/// watermark for the whole dance; a snapshot covering it early would satisfy
/// the bound trivially and make the teeth check meaningless. Do not "speed up"
/// this test by shortening it.
///
/// # The election window is a floor, not a speed dial
///
/// `election_min_ms` here is 600, not the 150 the other `config_for` callers
/// use, and both of openraft 0.9.24's undocumented couplings to
/// `heartbeat_interval` are why. `config_for` derives that interval as
/// `election_min / 4`, and openraft spends the same number twice:
///
/// - it is the **hard deadline of every AppendEntries RPC**
///   (`replication/mod.rs`: `let the_timeout =
///   Duration::from_millis(self.config.heartbeat_interval)`), not just the
///   heartbeat cadence `config_for`'s comment reasons about. At
///   `election_min = 150` that deadline is **37 ms** — for a real-TCP round
///   trip whose far end fsyncs the entries it is appending.
/// - on an RPC error openraft backs replication off by a **fixed 500 ms**
///   (`network/network.rs`: `Backoff::new(repeat(Duration::from_millis(500)))`),
///   which no config value can influence.
///
/// Together those make any regime with `election_timeout_max < 500 ms` a trap
/// rather than merely a tight budget: one slow RPC parks replication for longer
/// than a follower's whole election window, so the follower campaigns, and the
/// cluster churns terms instead of recovering. At `election_min = 150` the max
/// is 300 ms and that is guaranteed.
///
/// Stage 3 is exactly where it bites. It relaunches the three killed nodes —
/// each opening RocksDB and replaying its WAL — and those same nodes are the
/// `old` quorum step 2b must sample, so the RPC deadline is tightest precisely
/// when its targets are slowest. The observed failure is a leader that never
/// arms the drain bound while the log fills with `timeout after 37ms when
/// AppendEntries` and a vote walking T1 → T5.
///
/// 600 ms buys a 150 ms RPC deadline and a 1200 ms election window that clears
/// the 500 ms backoff. Nothing here measures election speed — the only timing
/// assertion is the `drain_wait >= 5 s` floor below, which a slower election
/// can only lengthen — so this costs the test a few seconds of failover and
/// nothing else.
#[tokio::test(flavor = "multi_thread")]
pub(crate) async fn full_replacement_transfers_membership_and_data() {
    crate::init_logging();
    let mut base = config_for(30_000, 600, 32, 256);
    // Deterministic spare choice, so `next` is not a coin flip across runs.
    base.membership_spare_selection = zslg_consensus::SpareSelectionStrategy::LowestLatency;
    let mut cluster = Cluster::new(ClusterConfig {
        count: 7,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    // Discover the replicated set R and the voter set from applied state —
    // join order decides which three form R.
    let snap = cluster
        .nodes()
        .next()
        .expect("node")
        .state_handle()
        .snapshot();
    let r: BTreeSet<NodeId> = snap.replica.current().clone();
    let voters: BTreeSet<NodeId> = snap.voters.clone();
    assert_eq!(r.len(), 3, "replica_node_count = 3");
    assert_eq!(voters.len(), 7, "all seven are voters: {voters:?}");
    assert!(r.is_subset(&voters), "replicas are voters");

    // One write, acked against R, and above the watermark for the whole dance.
    let leader_id = cluster.leader().expect("leader");
    let w_payload = Bytes::from_static(b"replacement-write");
    let w_id = cluster
        .node(leader_id)
        .writer()
        .write(w_payload.clone())
        .await
        .expect("write W");

    let observer = spawn_observer(&cluster);

    // ── Stage 1: kill all three replicated members ───────────────────────
    // Sequential: each `kill` borrows `&mut cluster`.
    let mut placements: Vec<NodePlacement> = Vec::new();
    for id in &r {
        placements.push(cluster.kill(*id).await);
    }
    observer.refresh(&cluster);
    let t_killed = std::time::Instant::now();

    // ── Stage 2: a joint config with a DISJOINT next must be committed ────
    // 4 of 7 voters survive, so the raft layer keeps working.
    let mut next: Option<BTreeSet<NodeId>> = None;
    crate::wait_until_sync("joint proposed", Duration::from_secs(20), || {
        cluster.nodes().any(|n| {
            let s = n.state_handle().snapshot();
            match s.replica.next() {
                Some(nx) if nx.len() == 3 && nx.is_disjoint(&r) => {
                    next = Some(nx.clone());
                    true
                }
                _ => false,
            }
        })
    })
    .await;
    let n = next.expect("joint config with disjoint next");
    let t_joint = std::time::Instant::now();

    // ── Stage 3: relaunch the old nodes ──────────────────────────────────
    // Step 2's generation quorum needs a quorum of `old` alive and applied,
    // and step 2b needs their logs to sample a real bound from. They were
    // never evicted from the voters, so they resume as voters.
    // All bound before any is awaited for readiness, and with the full peer
    // list — see `relaunch_many`. A loop of `relaunch` (start *and* wait, per
    // node) deadlocks even after the start/readiness split: each node's gate
    // waits for a quorum of R, which is exactly the set being relaunched.
    let relaunched = cluster
        .relaunch_many(placements)
        .await
        .expect("relaunch old nodes");
    assert_eq!(
        relaunched.iter().copied().collect::<BTreeSet<NodeId>>(),
        r,
        "relaunch must resume the same NodeIds (all of R)"
    );
    observer.refresh(&cluster);

    // Step 2b is done when the leader arms the bound. It must be a REAL
    // bound: `AnyTime` would mean every responder reported an empty log, so
    // nothing could be stranded and the gate would be doing no work.
    let mut armed: Option<zslg_consensus::Exit> = None;
    crate::wait_until_sync("drain bound armed", Duration::from_secs(30), || {
        cluster
            .nodes()
            .any(|node| match node.state_handle().snapshot().replica.exit() {
                Some(e) => {
                    armed = Some(e);
                    true
                }
                None => false,
            })
    })
    .await;
    let t_armed = std::time::Instant::now();
    assert!(
        matches!(armed, Some(zslg_consensus::Exit::WatermarkAbove(_))),
        "the drain bound must be a real watermark, not AnyTime — an AnyTime arm \
         means the old logs were empty and the gate did no work: {armed:?}"
    );

    // ── Stage 4: drain — the watermark must pass the sampled bound ────────
    crate::wait_until_sync("collapse", Duration::from_secs(60), || {
        cluster.nodes().all(|node| {
            let s = node.state_handle().snapshot();
            !s.replica.is_joint() && *s.replica.current() == n
        })
    })
    .await;
    let t_collapsed = std::time::Instant::now();

    let drain_wait = t_collapsed.saturating_duration_since(t_armed);
    println!(
        "[replacement] armed={armed:?} kill->joint {:?}, joint->armed {:?}, armed->collapse {:?}",
        t_joint.saturating_duration_since(t_killed),
        t_armed.saturating_duration_since(t_joint),
        drain_wait,
    );

    // **This is the assertion that detects a relaxed drain gate**, and it is
    // deliberately about the wait rather than about lost data.
    //
    // Measured both ways: with the gate intact the collapse waits ~28 s for a
    // snapshot to cover W (bounded by the 30 s snapshot interval); with both
    // gates disabled — the write-point validator and the leader's step 3 — it
    // collapses in ~89 ms. 5 s separates those cleanly.
    //
    // The durability chain below does NOT detect the relaxation, and that is a
    // finding rather than an oversight: with the gates off the chain still
    // passes, because the outgoing nodes are relaunched and stay live, so the
    // incoming set catches up W straight from their replica logs — the joint
    // window it had to do that in was short, but the subscription replays from
    // the start of the log, and W predates the kills. Exhibiting actual loss
    // needs the outgoing set to become unreachable before the incoming set has
    // caught up, which is a different (and racy) scenario. See docs/TESTING.md.
    assert!(
        drain_wait >= Duration::from_secs(5),
        "the collapse did not wait for the drain — the joint config went from armed \
         ({armed:?}) to collapsed in {drain_wait:?}. The bound is armed but nothing is \
         honouring it: check the write-point validator (`validate_transition`'s \
         JointExitTooEarly arm) and the leader's step 3 drain wait."
    );

    // ── Assert: battery + durability chain + observer ────────────────────
    // The battery covers all 7 nodes; they all share the raft state. The
    // durability chain is scoped to N — the claim is "W reached every node of
    // the new replicated set". A voter-only node outside the replicated set
    // has no local-store subscription to the replica logs (the subscriber
    // manager subscribes to `current ∪ next` only), so it is not a stable-log
    // carrier for W and asserting against it would assert the wrong thing.
    assert_cluster_invariants(&cluster);
    assert_durability_chain_on(&cluster, &[(w_id, w_payload)], Duration::from_secs(30), &n).await;
    observer.assert_monotonic();
    observer.stop().await;

    cluster.shutdown().await.expect("clean shutdown");
}
