//! Positions: [`Key`] reads one from an item, [`Successor`] names the next.

/// The position of an item in an ordered stream.
///
/// `None` means the item is not positional and passes through without advancing
/// the order. Implement this for the key type, not for the item, because the
/// same item type can be ordered by different keys.
pub trait Key<T>: Copy + Ord + Send + 'static {
    fn key(item: &T) -> Option<Self>;
}

/// The first position after a gapless key.
///
/// `next_after(None)` is the first position of all, so an exclusive `after`
/// of `None` ("from the beginning") has a defined first key. `Hlc` is ordered
/// but not gapless; it implements `Key`/`Ord`, not this.
pub trait Successor: Copy {
    fn next_after(prev: Option<Self>) -> Self;
}
