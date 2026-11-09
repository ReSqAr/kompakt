use super::*;

fn input(
    has_local_state: bool,
    fence_present: bool,
    is_bootstrap_node: bool,
    in_membership: bool,
) -> BootstrapInputs {
    BootstrapInputs {
        has_local_state,
        fence_present,
        is_bootstrap_node,
        in_membership,
    }
}

#[test]
fn rejoin_when_local_state_and_in_membership() {
    assert_eq!(
        bootstrap_decision(input(true, false, false, true)),
        BootstrapDecision::Rejoin,
    );
    assert_eq!(
        bootstrap_decision(input(true, true, true, true)),
        BootstrapDecision::Rejoin,
    );
}

#[test]
fn hard_crash_when_local_state_but_not_in_membership() {
    assert_eq!(
        bootstrap_decision(input(true, true, true, false)),
        BootstrapDecision::HardCrashRestart,
    );
}

#[test]
fn join_existing_when_fence_present_and_no_local_state() {
    assert_eq!(
        bootstrap_decision(input(false, true, true, false)),
        BootstrapDecision::JoinExisting,
    );
    assert_eq!(
        bootstrap_decision(input(false, true, false, false)),
        BootstrapDecision::JoinExisting,
    );
}

#[test]
fn bootstrap_when_first_member_and_no_state_no_fence() {
    assert_eq!(
        bootstrap_decision(input(false, false, true, false)),
        BootstrapDecision::Bootstrap,
    );
}

#[test]
fn join_when_not_first_member_and_no_state_no_fence() {
    assert_eq!(
        bootstrap_decision(input(false, false, false, false)),
        BootstrapDecision::JoinExisting,
    );
}

#[test]
fn local_state_dominates_fence_and_bootstrap_flags() {
    // With local state present, the fence / bootstrap flags are irrelevant:
    // in-membership decides Rejoin vs HardCrashRestart.
    for fence in [false, true] {
        for boot in [false, true] {
            assert_eq!(
                bootstrap_decision(input(true, fence, boot, true)),
                BootstrapDecision::Rejoin,
            );
            assert_eq!(
                bootstrap_decision(input(true, fence, boot, false)),
                BootstrapDecision::HardCrashRestart,
            );
        }
    }
}

#[test]
fn join_attempt_is_copy_eq() {
    let a = JoinAttempt::Accepted;
    let b = a; // Copy
    assert_eq!(a, b);
    assert_ne!(JoinAttempt::Accepted, JoinAttempt::Denied);
    assert_ne!(JoinAttempt::Denied, JoinAttempt::Retry);
}

#[tokio::test(start_paused = true)]
async fn run_phase_passes_through_inner_result() {
    // A completed phase returns its own result, Ok or Err, untouched.
    let ok: Result<u32, Error> = run_phase("p", Duration::from_secs(30), async { Ok(7u32) }).await;
    assert!(matches!(ok, Ok(7)));

    let err: Result<u32, Error> = run_phase("p", Duration::from_secs(30), async {
        Err(Error::Consensus(ConsensusError::QuorumUnreachable))
    })
    .await;
    assert!(matches!(
        err,
        Err(Error::Consensus(ConsensusError::QuorumUnreachable))
    ));
}

#[tokio::test(start_paused = true)]
async fn run_phase_waits_past_escalation_without_aborting() {
    // A phase that only completes AFTER several escalation intervals must
    // still return its inner `Ok` — `run_phase` escalates logging but never
    // aborts; startup is intentionally unbounded (header).
    let escalate = Duration::from_millis(10);
    let r: Result<u32, Error> = run_phase("slow_phase", escalate, async move {
        // Five escalation intervals of blocked-but-progressing work.
        tokio::time::sleep(escalate * 5).await;
        Ok(42u32)
    })
    .await;
    assert!(
        matches!(r, Ok(42)),
        "run_phase must wait for a slow phase, not abort it: {r:?}"
    );
}
