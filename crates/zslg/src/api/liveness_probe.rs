//! Is a peer reachable? A facet of the per-peer
//! [`ReplicaClient`](crate::api::ReplicaClient) — the client is already bound to
//! one peer, so there is no `peer` argument and no way to probe the wrong node.
//!
//! Deliberately the cheapest round-trip there is: the leader's liveness worker
//! probes every peer on an interval to decide replica-set membership, so the
//! probe must not depend on log state, storage, or Raft leadership — anything it
//! touched would turn that subsystem's slowness into a false failure.

#[async_trait::async_trait]
pub(crate) trait LivenessProbe: Send + Sync + 'static {
    /// `Ok(())` iff the peer answered within the caller's deadline. Timeout and
    /// error are the same signal to the caller: a failure.
    async fn probe(&self) -> Result<(), crate::Error>;
}
