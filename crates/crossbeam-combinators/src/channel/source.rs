//! The private source tree every [`Channel`](super::Channel) is built from.

use crossbeam_channel::Select;

use super::End;

pub(crate) enum Poll<T> {
    Got(T),
    Filtered,
    Empty,
    Dead,
}

pub(crate) trait Source<T>: Send {
    /// Register this source's live leaves with `sel`, depth-first. The index
    /// `Select::ready` later returns is the leaf's position in that order, so
    /// `poll` can route by it directly — no path is stored.
    ///
    /// **Register exactly [`leaf_count()`](Source::leaf_count) operations, in
    /// that order.** `register` and `leaf_count` are one contract split over
    /// two methods: composite sources cut the ordinal space by the counts of
    /// their children (`MergeState::poll`, `Guarded::poll`), so a source that
    /// registers a different number than it counts misroutes every ordinal to
    /// its right — including in *sibling* subtrees it cannot see.
    ///
    /// Registering under a condition `leaf_count` does not also test is the
    /// way to break it. `Guarded` is the worked case: either side gone means
    /// nothing registers **and** the count is zero, tested by the same call.
    ///
    /// The failure does not look like a routing bug. A misrouted poll lands on
    /// a live-but-empty leaf, returns `Poll::Empty`, and `recv` loops — so the
    /// symptom is a hang or a spin, never a wrong value. (Reaching the wrong
    /// leaf is harmless in itself: every leaf feeds the same output stream.
    /// What breaks is finding *nothing* there, and mistaking whose death it
    /// was.)
    ///
    /// Returns the number of operations pushed. `recv` debug-asserts it equals
    /// [`leaf_count()`](Source::leaf_count); returning it makes that equality
    /// checkable rather than assumed.
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize;
    /// Poll the leaf at `ordinal`, the depth-first leaf index from `register`.
    fn poll(&mut self, ordinal: usize) -> Poll<T>;
    fn drain_first(&mut self) -> Option<T>;
    /// How many leaves [`register`](Source::register) will register — see there,
    /// the equality is load-bearing.
    ///
    /// It must also hold *steady* from `register` until the `poll` that
    /// follows it, because the splits are computed at poll time while the
    /// registration was built before `Select::ready` blocked. Nothing
    /// threatens that today — every mutating method takes `&mut self` and the
    /// consumer holds `&mut Channel`, so no leaf can die while `ready` is
    /// parked — but a source that mutated itself from elsewhere would break
    /// routing without touching this file.
    fn leaf_count(&self) -> usize;
    /// Why this source finished. Only meaningful once `leaf_count() == 0`.
    ///
    /// Deliberately separate from `leaf_count`, which is *arithmetic*
    /// (`MergeState::poll` uses it to route an ordinal, where an error has no
    /// meaningful handling). The check still happens where deadness is decided
    /// — at the leaf.
    fn end_reason(&mut self) -> End;
}
