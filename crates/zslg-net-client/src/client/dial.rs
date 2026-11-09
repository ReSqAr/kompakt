//! Finding and dialing a peer: the address book and the dialer.

use super::Target;
use crate::Error;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use zslg_consensus::ConsensusError;
use zslg_consensus::NodeId;
use zslg_net::PeerAddr;
use zslg_net_wire::tcp::{self, SocketOpts};
use zslg_net_wire::{AsyncFrameSink, AsyncFrameStream};
use zslg_time::Clock;

pub(crate) struct AddrBook {
    addrs: DashMap<NodeId, PeerAddr>,
}

impl AddrBook {
    pub(crate) fn new() -> Self {
        Self {
            addrs: DashMap::new(),
        }
    }

    pub(crate) fn register(&self, node_id: NodeId, addr: PeerAddr) {
        self.addrs.insert(node_id, addr);
    }

    pub(crate) fn resolve(&self, target: &Target) -> Result<(PeerAddr, Option<NodeId>), Error> {
        match target {
            Target::Registered(peer) => {
                let addr = self
                    .addrs
                    .get(peer)
                    .map(|e| e.clone())
                    .ok_or(Error::Consensus(ConsensusError::UnknownPeer(*peer)))?;
                Ok((addr, Some(*peer)))
            }
            Target::Direct(addr) => Ok((addr.clone(), None)),
        }
    }
}

pub(crate) struct Dialer {
    clock: Arc<dyn Clock>,
    socket_opts: SocketOpts,
    connect_timeout: Duration,
}

impl Dialer {
    pub(crate) fn new(
        clock: Arc<dyn Clock>,
        socket_opts: SocketOpts,
        connect_timeout: Duration,
    ) -> Self {
        Self {
            clock,
            socket_opts,
            connect_timeout,
        }
    }

    pub(crate) async fn dial(
        &self,
        peer_addr: &PeerAddr,
        expected: Option<NodeId>,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (write, read) = tcp::dial(peer_addr, self.connect_timeout, &self.socket_opts).await?;
        let sink = AsyncFrameSink::new(write, self.clock.clone(), expected);
        let stream = AsyncFrameStream::new(read, self.clock.clone(), expected);
        Ok((sink, stream))
    }
}
