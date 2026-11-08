//! One `ReplicaNode` at a time: bootstrap, restart, startup routes and the
//! membership admission gate, over real sockets and a real database.

mod membership_gate;
mod single_node;
mod startup_routes;
