#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments,
    clippy::result_large_err,
    reason = "test code"
)]
mod cluster;
pub(crate) use cluster::{Cluster, ClusterConfig, NodePlacement, default_fast_config};
mod invariants;
pub(crate) use invariants::{
    assert_cluster_invariants, assert_cluster_invariants_with_acks, assert_durability_chain_on,
};
mod faults;
pub(crate) use faults::{
    FaultyReplica, FixedClock, MapPeerRouter, ReadyReplica, ReplicaServiceHarness,
    SharedClockController, SkewedClock, skewed_clock_pair,
};
mod snapshot;
pub(crate) use snapshot::{SnapshotHarness, boot_snapshot_harness, commit_snapshot_result};
mod cutoff_observer;
pub(crate) use cutoff_observer::CutoffObserver;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::Error;
use crate::api::{
    AddMemberOutcome, LivenessProbe, RaftMembership, RaftTransport, Replica, ReplicaClient, Router,
    StampedWrite, WriteOutcome,
};
use crate::ids::{DataId, LogIndex, NodeId, PeerAddr};
use crate::learner::LocalStore;
use crate::meta::{Data, RaftState, ReplicaStatus};
use crate::raft::RaftStateHandle;
use crate::raft::build_raft_for_test;
use crate::raft::{NodeInfo, Raft, TypeConfig};
use crossbeam_combinators::Channels;
use dashmap::DashMap;
use futures::stream::BoxStream;
use openraft::Config;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
// ── fault injection: pairwise partitions ─────────────────────────────

/// Shared handle that cuts and heals in-process network links. Cloned into
/// every node's `MemRouter`; cutting a pair makes both directions of raft
/// traffic between those nodes fail with `Unreachable` until healed.
#[derive(Clone, Default)]
pub(crate) struct Partition {
    cut: Arc<DashMap<(NodeId, NodeId), ()>>,
}

impl Partition {
    /// Cut the link between `a` and `b` (both directions).
    pub(crate) fn cut(&self, a: NodeId, b: NodeId) {
        self.cut.insert((a, b), ());
        self.cut.insert((b, a), ());
    }

    /// Restore the link between `a` and `b`.
    pub(crate) fn heal(&self, a: NodeId, b: NodeId) {
        self.cut.remove(&(a, b));
        self.cut.remove(&(b, a));
    }

    /// Cut `node` off from every node in `others`.
    pub(crate) fn isolate(&self, node: NodeId, others: &[NodeId]) {
        for &other in others {
            if other != node {
                self.cut(node, other);
            }
        }
    }

    /// Restore all links.
    pub(crate) fn heal_all(&self) {
        self.cut.clear();
    }

    fn is_cut(&self, from: NodeId, to: NodeId) -> bool {
        self.cut.contains_key(&(from, to))
    }
}

// ── in-memory Raft transport ──────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct MemRouter {
    nodes: Arc<DashMap<NodeId, Raft>>,
    source: NodeId,
    partition: Partition,
}

impl RaftNetworkFactory<TypeConfig> for MemRouter {
    type Network = MemNetwork;
    async fn new_client(&mut self, target: NodeId, _node: &NodeInfo) -> Self::Network {
        MemNetwork {
            router: self.nodes.clone(),
            source: self.source,
            target,
            partition: self.partition.clone(),
        }
    }
}

pub(crate) struct MemNetwork {
    router: Arc<DashMap<NodeId, Raft>>,
    source: NodeId,
    target: NodeId,
    partition: Partition,
}

impl MemNetwork {
    // mirrors the RaftNetwork method signatures, whose error type is openraft's
    fn check_link<E: std::error::Error>(&self) -> Result<(), RPCError<NodeId, NodeInfo, E>> {
        if self.partition.is_cut(self.source, self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "partitioned".into(),
            ))));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct UnreachableMsg(String);
impl std::fmt::Display for UnreachableMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for UnreachableMsg {}

impl RaftNetwork<TypeConfig> for MemNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.check_link()?;
        match self.router.get(&self.target) {
            Some(raft) => raft
                .append_entries(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e))),
            None => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "not registered".into(),
            )))),
        }
    }
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeInfo, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.check_link()?;
        match self.router.get(&self.target) {
            Some(raft) => raft
                .install_snapshot(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e))),
            None => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "not registered".into(),
            )))),
        }
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.check_link()?;
        match self.router.get(&self.target) {
            Some(raft) => raft
                .vote(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e))),
            None => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "not registered".into(),
            )))),
        }
    }
}

// ── cluster helpers ───────────────────────────────────────────────────

pub(crate) struct TestNode {
    pub(crate) raft: Raft,
    pub(crate) raft_state: RaftStateHandle,
    pub(crate) _tempdir: tempfile::TempDir,
}

pub(crate) fn test_membership_map(ids: &[NodeId]) -> BTreeMap<NodeId, NodeInfo> {
    ids.iter()
        .map(|id| {
            (
                *id,
                NodeInfo(Some(PeerAddr {
                    addr: "127.0.0.1:0".into(),
                    server_name: "localhost".into(),
                })),
            )
        })
        .collect()
}

pub(crate) async fn start_cluster(
    ids: &[NodeId],
    cfg: Arc<Config>,
) -> Result<BTreeMap<NodeId, TestNode>, Box<dyn std::error::Error>> {
    let (nodes, _partition) = start_cluster_with_partition(ids, cfg).await?;
    Ok(nodes)
}

/// `start_cluster` plus a [`Partition`] handle for cutting/healing the
/// in-process raft links between the nodes.
pub(crate) async fn start_cluster_with_partition(
    ids: &[NodeId],
    cfg: Arc<Config>,
) -> Result<(BTreeMap<NodeId, TestNode>, Partition), Box<dyn std::error::Error>> {
    let shared_nodes: Arc<DashMap<NodeId, Raft>> = Arc::new(DashMap::new());
    let partition = Partition::default();
    let mut nodes = BTreeMap::new();
    for &id in ids {
        let router = MemRouter {
            nodes: shared_nodes.clone(),
            source: id,
            partition: partition.clone(),
        };
        let tempdir = tempfile::tempdir()?;
        let raft_state = RaftStateHandle::new();
        let raft = build_raft_for_test(id, cfg.clone(), router, tempdir.path(), raft_state.clone())
            .await
            .map_err(|e| format!("Raft::new: {e}"))?;
        shared_nodes.insert(id, raft.clone());
        nodes.insert(
            id,
            TestNode {
                raft,
                raft_state,
                _tempdir: tempdir,
            },
        );
    }
    Ok((nodes, partition))
}

// ── deadline-bounded wait helpers ─────────────────────────────────────

/// Poll `pred` every 20ms until it returns true; panic with `label` after
/// `timeout`.
pub(crate) async fn wait_until<F, Fut>(label: &str, timeout: std::time::Duration, mut pred: F)
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
pub(crate) async fn within<F>(what: &str, cap: std::time::Duration, body: F) -> F::Output
where
    F: std::future::Future,
{
    match tokio::time::timeout(cap, body).await {
        Ok(out) => out,
        Err(_) => panic!("{what}: did not finish within {cap:?}"),
    }
}

/// Wait until `raft` observes `expected` as the leader.
pub(crate) async fn wait_for_leader(raft: &Raft, expected: NodeId) {
    wait_until(
        "expected node to become leader",
        std::time::Duration::from_secs(10),
        || async { raft.current_leader().await == Some(expected) },
    )
    .await;
}

/// Wait until some node in the cluster is observed as leader; returns it.
pub(crate) async fn wait_for_any_leader(
    nodes: &BTreeMap<NodeId, TestNode>,
    timeout: std::time::Duration,
) -> NodeId {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("Instant + timeout does not overflow");
    loop {
        for sys in nodes.values() {
            if let Some(id) = sys.raft.current_leader().await
                && nodes.contains_key(&id)
            {
                return id;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("no leader elected within deadline");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Write one `Data` to a `Replica` and await its outcome, mapped to the
/// pre-stream `write` semantics: `Accepted → Ok(raft_state)`, `Rejected`/`Failed`
/// → `Err(error)`. Opens a single-write [`Replica::write`] for tests that hold one
/// write per call; production callers hold one connection per peer.
pub(crate) async fn replica_write<R: Replica + ?Sized>(
    replica: &R,
    data: Data,
) -> Result<RaftState, Error> {
    let mut outcomes = replica.write(Channels::from_vec(vec![StampedWrite::now(data)]));
    // The channel blocks on the replica's pipeline, so it must not be pulled on the
    // reactor.
    let first = tokio::task::spawn_blocking(move || outcomes.recv())
        .await
        .map_err(|e| Error::Rpc(format!("write stream join: {e}")))?;
    match first {
        Ok(Ok(WriteOutcome::Accepted { raft_state, .. })) => Ok(raft_state),
        Ok(Ok(WriteOutcome::Rejected { error, .. } | WriteOutcome::Failed { error, .. }))
        | Ok(Err(error)) => Err(error),
        Err(_) => Err(Error::Rpc("replica dropped write outcome".into())),
    }
}

pub(crate) async fn write_state_fenced(
    raft: &Raft,
    raft_state: &RaftStateHandle,
    state: crate::api::ReplicatedRaftState,
) -> Result<(), String> {
    use crate::raft::{FencedStateWrite, RaftResponse};
    // The transient outcomes keep different clocks, and only one of them is
    // this handle's to wait out:
    //
    // - `RejectedStaleGeneration` is a CAS race against a **live** leader —
    //   someone applied a newer generation between this handle's snapshot and
    //   the write. The leader exists, so retrying is cheap and an attempt
    //   count bounds it (the outer 16).
    // - `ForwardToLeader` with **no** leader id is an election in flight: the
    //   write was not applied, and this node may yet win that election, so
    //   retrying the same handle is the one wait that can succeed here. Under
    //   a parallel test run the whole machine's CPUs are contended and an
    //   election can take seconds, so this side waits on wall-clock at
    //   `wait_for_leader`'s scale, not on an attempt count.
    // - `ForwardToLeader` **with** a leader id means this handle is a follower
    //   that knows its leader: every retry on this handle forwards to the same
    //   node (in the in-memory clusters that address is the `127.0.0.1:0`
    //   placeholder, unreachable by construction), so no wait here can ever
    //   succeed. Return immediately — the caller's move is to re-observe who
    //   leads and write through that node
    //   ([`write_state_fenced_via_leader`]).
    let election_deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(5))
        .expect("Instant + 5 s does not overflow");
    for _ in 0u8..16 {
        let expected_generation = raft_state.snapshot().generation;
        let response = loop {
            match raft
                .client_write(FencedStateWrite {
                    expected_generation,
                    state: state.clone(),
                })
                .await
            {
                Ok(response) => break response,
                Err(e) => {
                    if e.forward_to_leader()
                        .is_some_and(|forward| forward.leader_id.is_none())
                    {
                        if tokio::time::Instant::now() >= election_deadline {
                            return Err(format!(
                                "client_write: no leader stabilized within 5 s (last: {e})"
                            ));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    return Err(format!("client_write: {e}"));
                }
            }
        };
        match response.data {
            RaftResponse::Applied => return Ok(()),
            RaftResponse::RejectedStaleGeneration { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            RaftResponse::RejectedInvalidState { reason } => {
                return Err(format!("fenced state write: invalid raft state: {reason}"));
            }
        }
    }
    Err("fenced state write: lost the generation CAS 16 times".to_owned())
}

/// [`write_state_fenced`] plus the cluster-level patience a leadership flap
/// needs: write through whoever leads **now**, and when that write reports a
/// flap — a forwarded write, an election in flight, a lost CAS — ask again
/// rather than pinning to whichever node the last observation returned.
///
/// This is the loop `raft::tests::partition` has always run after a heal; it
/// lives here because a flap is not partition-specific — any test whose state
/// write lands while a snapshot commit or whole-machine load has stirred an
/// election would otherwise assert the race rather than the write. Every
/// failure class still terminates: at the deadline with the last error intact,
/// or immediately on a non-transient one.
pub(crate) async fn write_state_fenced_via_leader(
    nodes: &BTreeMap<NodeId, TestNode>,
    state: crate::api::ReplicatedRaftState,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(30))
        .expect("Instant + 30 s does not overflow");
    loop {
        let leader_id = wait_for_any_leader(nodes, std::time::Duration::from_secs(10)).await;
        let sys = nodes.get(&leader_id).expect("leader in cluster");
        match write_state_fenced(&sys.raft, &sys.raft_state, state.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

// ── test transport helpers ───────────────────────────────────────────

/// Wrap a [`Replica`] as a full [`ReplicaClient`]. The non-replica facets are
/// `unimplemented!()` because the consumers that route through `Router::remote`
/// only invoke `Replica` methods.
pub(crate) fn replica_only(replica: Arc<dyn Replica>) -> Arc<dyn ReplicaClient> {
    Arc::new(ReplicaOnly(replica))
}

struct ReplicaOnly(Arc<dyn Replica>);

#[async_trait::async_trait]
impl Replica for ReplicaOnly {
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        self.0.write(input)
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> Channels<Result<(LogIndex, Data), Error>> {
        self.0.subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
        self.0.fetch_status().await
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
        self.0.subscribe_status().await
    }
}

#[async_trait::async_trait]
impl RaftTransport for ReplicaOnly {
    async fn append_entries(
        &self,
        _rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ReplicaOnly: raft transport not used")
    }

    async fn vote(
        &self,
        _rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ReplicaOnly: raft transport not used")
    }

    async fn install_snapshot(
        &self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        Error,
    > {
        unimplemented!("ReplicaOnly: raft transport not used")
    }
}

#[async_trait::async_trait]
impl RaftMembership for ReplicaOnly {
    async fn add_member(
        &self,
        _node_id: NodeId,
        _peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, Error> {
        unimplemented!("ReplicaOnly: membership not used")
    }
}

#[async_trait::async_trait]
impl LivenessProbe for ReplicaOnly {
    async fn probe(&self) -> Result<(), Error> {
        unimplemented!("ReplicaOnly: liveness not used")
    }
}

/// A [`Router`] whose `remote(id)` defers to a closure (wrap
/// `Replica` fakes with [`replica_only`]). The direct-dial and liveness paths
/// are `unimplemented!()` — unused by the tests that build this.
pub(crate) fn fn_remote(
    f: impl Fn(NodeId) -> Arc<dyn ReplicaClient> + Send + Sync + 'static,
) -> Arc<dyn Router> {
    Arc::new(FnRemote(Box::new(f)))
}

struct FnRemote(Box<dyn Fn(NodeId) -> Arc<dyn ReplicaClient> + Send + Sync>);

impl Router for FnRemote {
    fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

    fn remote(&self, node_id: NodeId) -> Arc<dyn ReplicaClient> {
        (self.0)(node_id)
    }

    fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn ReplicaClient> {
        unimplemented!("FnRemote: direct dial not used")
    }
}

/// A [`Router`] whose `remote(_)` returns a probe-only [`ReplicaClient`] backed by
/// `probe` for any peer — for the leader liveness-worker tests. Only the
/// `LivenessProbe` facet is real; the rest is `unimplemented!()`.
pub(crate) fn probe_router(probe: Arc<dyn LivenessProbe>) -> Arc<dyn Router> {
    Arc::new(ProbeRouter(probe))
}

struct ProbeRouter(Arc<dyn LivenessProbe>);

impl Router for ProbeRouter {
    fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

    fn remote(&self, _node_id: NodeId) -> Arc<dyn ReplicaClient> {
        Arc::new(ProbeOnly(self.0.clone()))
    }

    fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn ReplicaClient> {
        unimplemented!("ProbeRouter: direct dial not used")
    }
}

/// A [`ReplicaClient`] that delegates only `probe`; the other facets are
/// `unimplemented!()` (the liveness worker calls nothing else).
struct ProbeOnly(Arc<dyn LivenessProbe>);

#[async_trait::async_trait]
impl LivenessProbe for ProbeOnly {
    async fn probe(&self) -> Result<(), Error> {
        self.0.probe().await
    }
}

#[async_trait::async_trait]
impl Replica for ProbeOnly {
    fn write(&self, _: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        unimplemented!("ProbeOnly: replica not used")
    }

    fn subscribe_from(&self, _from: Option<LogIndex>) -> Channels<Result<(LogIndex, Data), Error>> {
        unimplemented!("ProbeOnly: replica not used")
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
        unimplemented!("ProbeOnly: replica not used")
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
        unimplemented!("ProbeOnly: replica not used")
    }
}

#[async_trait::async_trait]
impl RaftTransport for ProbeOnly {
    async fn append_entries(
        &self,
        _rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ProbeOnly: raft transport not used")
    }

    async fn vote(
        &self,
        _rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ProbeOnly: raft transport not used")
    }

    async fn install_snapshot(
        &self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        Error,
    > {
        unimplemented!("ProbeOnly: raft transport not used")
    }
}

#[async_trait::async_trait]
impl RaftMembership for ProbeOnly {
    async fn add_member(
        &self,
        _node_id: NodeId,
        _peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, Error> {
        unimplemented!("ProbeOnly: membership not used")
    }
}

// ── shared test utilities ─────────────────────────────────────────────

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
pub(crate) fn reserve_port() -> std::net::SocketAddr {
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

/// Build a deterministic [`NodeId`] from a single byte seed.
pub(crate) fn nid(byte: u8) -> crate::NodeId {
    let mut bytes = [0u8; 16];
    bytes[15] = byte;
    crate::NodeId::from_uuid(uuid::Uuid::from_bytes(bytes))
}

/// Convenience wrapper around [`wait_until`] for sync `FnMut() -> bool`
/// predicates (no async needed).
pub(crate) async fn wait_until_sync(
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
pub(crate) fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("crate=debug,openraft=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Fast raft config suitable for in-memory tests:
/// 50 ms heartbeat, 200–400 ms election timeout.
pub(crate) fn fast_raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    )
}

/// Open a test stable log beside a store path (mirrors
/// `StorageConfig::stable_log_dir`'s sibling convention) with default sizing.
pub(crate) async fn stable_log_at(
    store_path: &std::path::Path,
) -> std::sync::Arc<crate::learner::StableLog> {
    std::sync::Arc::new(
        crate::learner::StableLog::open_engine(store_path.with_extension("stable"))
            .await
            .expect("open stable log"),
    )
}

pub(crate) fn make_data(seed: u8, hlc: u64, blob: &[u8]) -> Data {
    Data {
        data_id: make_meta(seed, hlc),
        blob: bytes::Bytes::copy_from_slice(blob),
    }
}

/// Deterministic "upstream" [`NodeId`] with `0xAA` in byte 0 and `seed`
/// in byte 15 — used for testing replication data sources.
pub(crate) fn upstream(seed: u8) -> NodeId {
    let mut b = [0u8; 16];
    b[0] = 0xAA;
    b[15] = seed;
    NodeId::from_uuid(uuid::Uuid::from_bytes(b))
}

/// Build a deterministic [`DataId`]: nil creator node, HLC-seeded, with `seed`
/// as the random tiebreaker. (Formerly returned a `Meta`; the id is now
/// self-describing.) Prefer [`make_data`] when a full [`Data`] with blob is
/// needed.
pub(crate) fn make_meta(seed: u8, hlc: u64) -> DataId {
    DataId::from_parts(
        crate::Hlc(hlc),
        NodeId::from_uuid(uuid::Uuid::nil()),
        u64::from(seed),
    )
}

/// Create a fresh tempdir-backed [`crate::learner::LocalStore`] and return
/// the directory (for cleanup) and the store handle.
pub(crate) async fn fresh_local() -> (
    tempfile::TempDir,
    std::sync::Arc<crate::learner::LocalStore>,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.db");
    let store = LocalStore::simple_at(path.with_extension("stable"))
        .await
        .expect("from_db");
    (dir, store)
}
