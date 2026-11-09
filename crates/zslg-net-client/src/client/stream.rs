//! Opening the long-lived write and subscribe streams.

use super::subscribe_stream::open_subscribe;
use super::write_stream::open_write;
use super::{Dialer, RpcClient, Target};
use crate::Error;
use crossbeam_combinators::Channel;
use metrics_kit::OpState;
use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use tokio_util::sync::CancellationToken;
use zslg_consensus::NodeId;
use zslg_log::LogIndex;
use zslg_log::{StampedWrite, WriteOutcome};
use zslg_net::PeerAddr;
use zslg_net_wire::messages::{RpcRequest, RpcStreamKind};
use zslg_net_wire::{AsyncFrameSink, AsyncFrameStream};

pub(crate) struct StreamOpener {
    rpc_weak: Weak<RpcClient>,
    dialer: Arc<Dialer>,
    pool_channel_cap: usize,
    subscribe_channel_cap: NonZeroUsize,
    /// The node's shutdown token — see [`open_subscribe`] for what it is for.
    cancel: CancellationToken,
}

impl StreamOpener {
    pub(crate) fn new(
        rpc_weak: Weak<RpcClient>,
        dialer: Arc<Dialer>,
        pool_channel_cap: usize,
        subscribe_channel_cap: NonZeroUsize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            rpc_weak,
            dialer,
            pool_channel_cap,
            subscribe_channel_cap,
            cancel,
        }
    }

    pub(crate) async fn open(
        &self,
        addr: PeerAddr,
        expected: Option<NodeId>,
        request: RpcRequest,
    ) -> Result<AsyncFrameStream, Error> {
        let probe = crate::metrics::rpc_open_stream_probe();
        let (mut sink, stream) = self.dialer.dial(&addr, expected).await?;
        sink.send(request).await?;
        drop(sink);
        probe.finish(OpState::Success);
        Ok(stream)
    }

    pub(crate) async fn dial_write_stream(
        &self,
        addr: PeerAddr,
        expected: Option<NodeId>,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (mut sink, stream) = self.dialer.dial(&addr, expected).await?;
        sink.send(RpcRequest::Stream(RpcStreamKind::ReplicaWrite))
            .await?;
        Ok((sink, stream))
    }

    pub(crate) fn write(
        &self,
        node_id: NodeId,
        input: Channel<StampedWrite, crossbeam_combinators::Finite>,
    ) -> Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> {
        open_write(
            self.rpc_weak.clone(),
            Target::Registered(node_id),
            input,
            self.pool_channel_cap,
        )
    }

    pub(crate) async fn dial_subscribe_stream(
        &self,
        addr: PeerAddr,
        expected: Option<NodeId>,
        from: Option<LogIndex>,
    ) -> Result<(AsyncFrameSink, AsyncFrameStream), Error> {
        let (mut sink, stream) = self.dialer.dial(&addr, expected).await?;
        sink.send(RpcRequest::Stream(RpcStreamKind::ReplicaSubscribe { from }))
            .await?;
        Ok((sink, stream))
    }

    pub(crate) fn subscribe_from(
        &self,
        node_id: NodeId,
        from: Option<LogIndex>,
    ) -> zslg_log::ReplicaStream {
        (open_subscribe(
            self.rpc_weak.clone(),
            Target::Registered(node_id),
            from,
            self.subscribe_channel_cap,
            self.cancel.clone(),
        )
        .ordered(from, "replica subscription"))
        .err_into()
    }
}
