//! Raft clusters, in-process and over real TCP: partition and heal, replicated
//! state, and the RPC layer end to end.

mod cluster_tcp_test;
mod partition;
mod replicated_state;
