//! The RPC wire's own metrics: connection failures, and DNS time inside a dial.
//! `METRICS` joins the node's registry.

use metrics_kit::{MetricDef, label_enum, metrics};

const LABEL_REASON: &str = "reason";

metrics!(pub "zslg_", rpc_wire; {
    labeled_counter rpc_connect_failed RPC_CONNECT_FAILED_TOTAL
        "rpc_connect_failed_total"
        "Failed outbound dials by reason. \"refused\" = the host answered \
         but nothing is listening (peer down, or its accept loop is \
         dead). \"local_fd_exhausted\" = EMFILE/ENFILE — OUR resource \
         exhaustion, not a peer fault, and every dial fails until fds are \
         released. \"reset\" = established then torn down, typically the \
         peer's accept backlog overflowing. \"timeout\" = no answer \
         within connect_timeout (wedged peer or blackholed route). \
         \"dns\" = resolution failed, no socket was ever attempted. \
         \"unreachable\" = no route. A sustained non-zero rate with a \
         live peer means connections cannot be re-established at all — \
         distinct from, and far more serious than, streams that end."
        (reason: ConnectFailure) [LABEL_REASON => reason.as_str()];
    labeled_counter rpc_accept_failed RPC_ACCEPT_FAILED_TOTAL
        "rpc_accept_failed_total"
        "Failed accept() calls on the RPC server, same `reason` \
         vocabulary as rpc_connect_failed_total. The server-side half of \
         a connectivity failure: peers reporting \"refused\"/\"reset\" \
         while this counter climbs locates the fault on THIS node. \
         \"local_fd_exhausted\" additionally means the accept loop is \
         hot-spinning, since it retries immediately on error."
        (reason: ConnectFailure) [LABEL_REASON => reason.as_str()];
    probe_unlabeled rpc_dns_resolve_probe RPC_DNS_RESOLVE_SECONDS "rpc_dns_resolve_seconds"
        "DNS resolution time inside a dial. Called on every connect / \
         reconnect; slow = resolver issue or DNS server unreachable.";});

/// Every metric this crate emits.
pub const METRICS: &[MetricDef] = &rpc_wire!();

label_enum!(#[doc = "`reason` label of [`RPC_CONNECT_FAILED_TOTAL`]. Bounded by \
construction: every `std::io::Error` maps into exactly one of these."] pub ConnectFailure {
    /// The connect did not complete within `connect_timeout`. Peer alive but
    /// wedged, or a blackholed route (no RST comes back).
    Timeout => "timeout",
    /// DNS resolution failed — we never got as far as a socket.
    Dns => "dns",
    /// `ECONNREFUSED`: the host answered, nothing is listening on the port.
    /// The peer's accept loop is dead or the process is down.
    Refused => "refused",
    /// `EMFILE`/`ENFILE`: **we** ran out of file descriptors. A local resource
    /// exhaustion, not a peer fault — every dial fails until fds are released.
    LocalFdExhausted => "local_fd_exhausted",
    /// `ECONNRESET`/`ECONNABORTED`: the connection was established and then
    /// torn down. Typically the peer's accept backlog overflowed.
    Reset => "reset",
    /// `EHOSTUNREACH`/`ENETUNREACH`/`ENETDOWN`: no route.
    Unreachable => "unreachable",
    /// Anything else. A non-trivial rate here means this enum needs another arm.
    Other => "other",
});

impl ConnectFailure {
    /// Classify a connect `io::Error`. `ErrorKind` covers most cases on stable;
    /// fd exhaustion still has no stable `ErrorKind`, so it is matched on the
    /// raw errno (EMFILE 24 / ENFILE 23 on Linux and macOS alike) — and it is
    /// worth the special case, because it is the one failure that is *ours*.
    #[must_use]
    pub fn classify(e: &std::io::Error) -> Self {
        use std::io::ErrorKind as K;
        const EMFILE: i32 = 24;
        const ENFILE: i32 = 23;
        if matches!(e.raw_os_error(), Some(EMFILE | ENFILE)) {
            return Self::LocalFdExhausted;
        }
        match e.kind() {
            K::ConnectionRefused => Self::Refused,
            K::ConnectionReset | K::ConnectionAborted => Self::Reset,
            K::HostUnreachable | K::NetworkUnreachable | K::NetworkDown => Self::Unreachable,
            K::TimedOut => Self::Timeout,
            _ => Self::Other,
        }
    }
}
