//! Ordered channels: a position-keyed stream with an explicit policy.
//!
//! `OrderedChannel<T, P>` wraps a [`Channel<T>`] with a policy marker `P`
//! (`Contiguous<K>`, `StrictlyIncreasing<K>`, `NonDecreasing<K>`). `K: Key<T>`
//! extracts the position from an item; `None` means the item does not
//! participate in the order (a watermark or an error item).
//!
//! The policy is enforced in the wrapped source, so `recv`, `try_recv` and
//! `ready_batch` all see the same check. A violation ends the stream with
//! [`AbortReason::OrderViolation`]; a permitted forward gap is passed through.
//!
//! Every start is exclusive: a channel is checked from `after: Option<K>`, the
//! last position already delivered (`None` = from the beginning). `covers`
//! checks the end of a finite contiguous stream; `also` adds a second policy as
//! a runtime guard without changing the type.
//!
//! `stitch` is the one transition that strengthens a policy: it fills gaps from
//! a fallback and returns a `Contiguous` channel.

mod check;
mod covers;
mod key;
mod ordered_channel;
mod policy;
mod stitch;

pub use key::{Key, Successor};
pub use ordered_channel::OrderedChannel;
pub use policy::{Contiguous, NonDecreasing, OrderPolicy, StrictlyIncreasing};
pub use stitch::Gap;

use policy::OrderState;

#[cfg(test)]
mod tests;
