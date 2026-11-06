//! The verdict of a stream: [`End`], its [`AbortReason`], and the producer's [`Closer`].

/// Why a [`Channel`](crate::Channel) stopped yielding items.
///
/// The top level is only “clean” or “not clean”. The detail lives in
/// [`AbortReason`], so a producer close, a consumer bound, and a cancellation
/// share one small verdict type without sharing one meaning.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum End {
    /// The stream ended cleanly: the producer declared completion, every
    /// phase/branch completed, or a consumer bound was reached.
    Complete,
    /// The stream did not complete cleanly. **Items may be missing** unless the
    /// reason says otherwise.
    Aborted { reason: AbortReason },
}

/// Why an [`End::Aborted`] stream stopped.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AbortReason {
    /// A data channel disconnected without a close: the producer panicked, was
    /// discarded before it ran, simply forgot, or an infinite source ended.
    ProducerGone { what: &'static str },
    /// A [`Cancellation`](crate::Cancellation) bound with [`Channel::until`](crate::Channel::until) fired. Items still
    /// buffered when it fired may or may not have been delivered.
    Cancelled,
    /// A cancellation handle was dropped without firing. This is teardown, not
    /// a stop request.
    CancelHandleDropped,
    /// An internal routing invariant was violated; the stream fails closed.
    RoutingViolation { what: &'static str },
    /// An ordered source saw a position that violated its policy.
    OrderViolation { what: &'static str },
}

impl AbortReason {
    /// The static label for this reason: the producer/guard/source name, or a
    /// fixed string for the reasons without one.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::ProducerGone { what }
            | Self::RoutingViolation { what }
            | Self::OrderViolation { what } => what,
            Self::Cancelled => "cancelled",
            Self::CancelHandleDropped => "cancellation handle dropped",
        }
    }
}

impl std::fmt::Display for AbortReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProducerGone { what } => write!(f, "stream {what} ended"),
            Self::Cancelled => f.write_str("stream cancelled"),
            Self::CancelHandleDropped => f.write_str("cancellation handle dropped"),
            Self::RoutingViolation { what } => write!(f, "routing violation in {what}"),
            Self::OrderViolation { what } => write!(f, "order violation in {what}"),
        }
    }
}

/// The producer's half of an explicit close. Handed out by
/// [`Channel::from`](crate::Channel::from) so a construction site cannot forget it exists.
///
/// Dropping this without calling [`close`](Self::close) marks the stream
/// [`End::Aborted`] — that is the whole point, so the "I forgot" case and the
/// "I panicked" case land in the same place.
pub struct Closer(pub(super) crossbeam_channel::Sender<()>);

impl Closer {
    /// Declare the stream complete. Everything the producer meant to send has
    /// been sent.
    ///
    /// **Close before the last `Sender` drops.** The consumer forms its verdict
    /// the moment the data channel disconnects, so if the sender goes first the
    /// consumer can read "disconnected, not yet closed" — an abort — for a stream
    /// that was about to close cleanly.
    ///
    /// Keep the `Closer` in the same scope as the sender and this is free: the
    /// sender drops at the end of that scope, after the `close()` that precedes
    /// it. Producers here are written with a single exit for that reason —
    /// `break` out to one unconditional `close()` rather than closing on each
    /// path. Do not move the sender into a nested `async` block or closure while
    /// closing outside it; the sender would then drop first.
    pub fn close(self) {
        let _ = self.0.try_send(());
    }
}
