//! Feeds the raft gauges and the leadership-transition logs from the engine's
//! metrics stream plus the applied `RaftStateHandle`.

pub mod metrics;
mod state_publisher;

pub use state_publisher::RaftStatePublisher;
