//! openraft's network side: the factory that turns a membership entry into a
//! per-peer client over the `Router`, and the watcher that keeps the router's
//! peer table in step with the membership.

mod network;
mod peer_registration;

pub use network::NetworkFactory;
pub use peer_registration::PeerRegistrationWatcher;
