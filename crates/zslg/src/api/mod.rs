//! The trait surface of the node: every seam across which a component is
//! substitutable (real vs. in-process vs. test double), one trait per file.
//!
//! The traits mirror design doc §"Public API" verbatim — changing a signature
//! here requires a doc change first (CLAUDE.md §13). Two are `pub` because they
//! are the client's whole view of the cluster ([`Writer`], [`Reader`]); the rest
//! are `pub(crate)` seams.
//!
//! Roles, and the module that owns the production implementation:
//!
//! ```text
//!   client        Writer, Reader                     writer/, learner/
//!   peer client   ReplicaClient = Replica            network/  (TcpPeerClient)
//!                   + RaftTransport + RaftMembership + LivenessProbe
//!                 Router hands one out per NodeId    network/  (PeerRouter)
//!   raft node     RaftConsensus, RaftLeadership,     raft/
//!                 RaftState{View,Tracker}, StateReplicator, MembershipGate
//!   node-local    LocalStoreView/Mut, StableLogSnapshot   learner/, replica/
//!   remote-durable SnapshotStore, StableArchiveReader     object_store/
//! ```
//!
//! Facet traits are deliberately narrow: a consumer names the smallest surface
//! it needs, so a leader job that only reads leadership cannot reach the log.
//! Composite surfaces are assembled by blanket impls rather than wrappers —
//! `ReplicaClient` over its four facets, `RaftTransport`/`RaftMembership` over a
//! local `RaftConsensus` — which is what lets the self/loopback path present the
//! same client surface as a remote peer with no hand-written forwarding.

mod liveness_probe;
mod peers;
mod raft_consensus;
mod raft_peer;
mod replica_client;
mod router;

pub(crate) use peers::PeersWatcher;

pub(crate) use liveness_probe::LivenessProbe;
pub(crate) use raft_consensus::RaftConsensus;
pub(crate) use raft_peer::RaftTransport;
pub(crate) use replica_client::ReplicaClient;
pub(crate) use router::Router;
