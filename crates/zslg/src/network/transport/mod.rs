//! The `api` traits, spoken over the wire — see [`peer`].

mod peer;

pub(in crate::network) use peer::TcpPeerClient;
