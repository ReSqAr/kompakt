//! A `Router` with an in-process fast path for this node itself; every other
//! node is delegated to an inner `Router`.

mod local;
mod peer;

pub use local::LocalReplicaClient;
pub use peer::PeerRouter;
