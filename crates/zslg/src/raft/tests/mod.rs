//! In-crate tests that must see `pub(in crate::raft)` internals (the openraft
//! `NetworkFactory`, `RocksRaftStorage`) and therefore live inside the `raft`
//! module tree rather than under `crate::tests`.

mod cluster_tcp_test;
mod multi_node_writes;
mod partition;
mod replicated_state;
