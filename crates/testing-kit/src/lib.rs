//! Helpers every async test in the workspace wants: deadline-bounded waits, a
//! hard cap for a whole test body, collision-free local ports and log output.
//!
//! Test-only by intent; it panics on failure the way a test assertion does.

#![allow(
    clippy::panic,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    reason = "test helpers fail by panicking"
)]

/// Poll `pred` every 20ms until it returns true; panic with `label` after
/// `timeout`.
pub async fn wait_until<F, Fut>(label: &str, timeout: std::time::Duration, mut pred: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("Instant + timeout does not overflow");
    loop {
        if pred().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("wait_until timed out: {label}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Run a whole test body under a hard cap, failing when it expires.
///
/// A test that neither passes nor fails is worse than one that fails: the
/// harness reports it as "still running" and the run never terminates, so
/// there is no failure to read and no stack to look at. Every wait inside a
/// cluster test is supposed to carry its own deadline, which makes reaching
/// this cap a bug report in itself — one that names the test and the cap
/// instead of leaving a hung process behind.
///
/// The cap is the outer backstop, not the diagnosis: a wait that can fail for
/// a knowable reason should still say so itself (see
/// `leader::leadership_churn_tests::await_stall`), because "did not finish in
/// 120 s" localises nothing.
pub async fn within<F>(what: &str, cap: std::time::Duration, body: F) -> F::Output
where
    F: std::future::Future,
{
    match tokio::time::timeout(cap, body).await {
        Ok(out) => out,
        Err(_) => panic!("{what}: did not finish within {cap:?}"),
    }
}


/// Reserve an OS-assigned TCP port on localhost (the RPC server binds TCP).
/// Returns the bound address after releasing the listener.
///
/// # The race this closes, and the one it does not
///
/// Asking the OS for a port, dropping the listener and handing the address to a
/// caller that binds it later is a time-of-check/time-of-use gap by
/// construction: nothing holds the port in between. It cannot be closed here —
/// keeping the listener alive would mean handing it to `ReplicaNode::start`,
/// which is production surface this tier does not add.
///
/// What *is* closable is the dominant case. Tests run many nodes across
/// parallel threads of one process, and the OS will happily hand the same
/// freed port to two callers; each then races to bind it and one loses with
/// `Address already in use`. Remembering every port handed out in this process
/// and refusing to return one twice removes that entirely. A cross-process
/// collision remains possible and is rare enough to live with.
pub fn reserve_port() -> std::net::SocketAddr {
    /// Ports already handed out in this process. Never pruned: a test binary is
    /// short-lived, and reusing a port whose node has since shut down is the
    /// very thing this exists to prevent.
    static HANDED_OUT: std::sync::Mutex<Option<std::collections::HashSet<u16>>> =
        std::sync::Mutex::new(None);

    // Bounded: each attempt that loses is a port already claimed, and the OS
    // has ~64k to draw from, so exhausting this many means something else is
    // wrong and a clear panic beats an infinite loop.
    for _ in 0..64 {
        let sock = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = sock.local_addr().expect("addr");
        drop(sock);
        let mut guard = HANDED_OUT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard
            .get_or_insert_with(Default::default)
            .insert(addr.port())
        {
            return addr;
        }
    }
    panic!("reserve_port: 64 consecutive attempts all returned an already-issued port");
}


/// Convenience wrapper around [`wait_until`] for sync `FnMut() -> bool`
/// predicates (no async needed).
pub async fn wait_until_sync(
    label: &str,
    deadline: std::time::Duration,
    mut predicate: impl FnMut() -> bool,
) {
    let stop = tokio::time::Instant::now()
        .checked_add(deadline)
        .expect("Instant + deadline");
    loop {
        if predicate() {
            return;
        }
        if tokio::time::Instant::now() >= stop {
            panic!("{label}: deadline elapsed without predicate becoming true");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Initialise `tracing_subscriber` for test output (no-op if already set).
pub fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

