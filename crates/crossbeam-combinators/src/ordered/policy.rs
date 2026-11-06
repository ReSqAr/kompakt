//! Order policies and the state they advance over.

use std::marker::PhantomData;

use super::Successor;

/// The internal state a policy advances over.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OrderState<K> {
    pub(crate) after: Option<K>,
    pub(crate) last: Option<K>,
}

impl<K: Copy> OrderState<K> {
    /// The last position this stream has delivered, or started after.
    fn bound(&self) -> Option<K> {
        self.last.or(self.after)
    }
}

/// The closed set of order policies.
///
/// Sealed on purpose: the markers below are the whole set.
pub trait OrderPolicy: sealed::Sealed + Send + 'static {
    type Key: Copy + Ord + Send + 'static;

    #[doc(hidden)]
    const LABEL: &'static str;

    #[doc(hidden)]
    fn check(state: &mut OrderState<Self::Key>, key: Self::Key) -> bool;
}

mod sealed {
    pub trait Sealed {}
}

/// Every key must be the successor of the previous one.
pub struct Contiguous<K>(PhantomData<K>);

/// Keys are strictly increasing; gaps are allowed.
pub struct StrictlyIncreasing<K>(PhantomData<K>);

/// Keys are non-decreasing; gaps and duplicates are allowed.
pub struct NonDecreasing<K>(PhantomData<K>);

impl<K: Successor + Ord + Send + 'static> sealed::Sealed for Contiguous<K> {}
impl<K: Ord + Copy + Send + 'static> sealed::Sealed for StrictlyIncreasing<K> {}
impl<K: Ord + Copy + Send + 'static> sealed::Sealed for NonDecreasing<K> {}

impl<K: Successor + Ord + Send + 'static> OrderPolicy for Contiguous<K> {
    type Key = K;
    const LABEL: &'static str = "contiguous";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        if key != K::next_after(state.bound()) {
            return false;
        }
        state.last = Some(key);
        true
    }
}

impl<K: Ord + Copy + Send + 'static> OrderPolicy for StrictlyIncreasing<K> {
    type Key = K;
    const LABEL: &'static str = "strictly increasing";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        if state.bound().is_some_and(|bound| key <= bound) {
            return false;
        }
        state.last = Some(key);
        true
    }
}

impl<K: Ord + Copy + Send + 'static> OrderPolicy for NonDecreasing<K> {
    type Key = K;
    const LABEL: &'static str = "non-decreasing";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        let ok = match state.last {
            // Duplicates are allowed between items...
            Some(last) => key >= last,
            // ...but the first key is still strictly after the start.
            None => state.after.is_none_or(|after| key > after),
        };
        if ok {
            state.last = Some(key);
        }
        ok
    }
}
