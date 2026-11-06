//! Sequential chain of phases.

use std::collections::VecDeque;

use crossbeam_channel::Select;

use super::{End, Poll, Source};

/// Sequential first-then-second. An aborted phase **condemns the chain
/// immediately** — remaining phases are discarded and the chain reports
/// `Done(Aborted)`. This stops a truncated historical replay from laundering
/// itself behind live items that would make the stream look complete.
pub(super) enum ChainState<T> {
    Active {
        phases: VecDeque<Box<dyn Source<T>>>,
    },
    Done(End),
}

impl<T: Send + 'static> Source<T> for ChainState<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        match self {
            ChainState::Active { phases } => match phases.front_mut() {
                Some(front) => front.register(sel),
                None => 0,
            },
            ChainState::Done(_) => 0,
        }
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        match self {
            ChainState::Active { phases } => match phases.front_mut() {
                None => {
                    *self = ChainState::Done(End::Complete);
                    Poll::Dead
                }
                Some(front) => {
                    debug_assert!(
                        ordinal < front.leaf_count(),
                        "ordinal {ordinal} out of range for chain front with {} leaves",
                        front.leaf_count()
                    );
                    match front.poll(ordinal) {
                        Poll::Dead => {
                            let reason = front.end_reason();
                            phases.pop_front();
                            match reason {
                                End::Complete if phases.is_empty() => {
                                    *self = ChainState::Done(End::Complete);
                                    Poll::Dead
                                }
                                End::Complete => Poll::Filtered,
                                aborted @ End::Aborted { .. } => {
                                    *self = ChainState::Done(aborted);
                                    Poll::Dead
                                }
                            }
                        }
                        other => other,
                    }
                }
            },
            ChainState::Done(_) => Poll::Dead,
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        loop {
            return match self {
                ChainState::Active { phases } => {
                    let front = match phases.front_mut() {
                        Some(f) => f,
                        None => {
                            *self = ChainState::Done(End::Complete);
                            return None;
                        }
                    };
                    match front.drain_first() {
                        Some(v) => Some(v),
                        None => {
                            if front.leaf_count() == 0 {
                                let reason = front.end_reason();
                                phases.pop_front();
                                match reason {
                                    End::Complete if phases.is_empty() => {
                                        *self = ChainState::Done(End::Complete);
                                        return None;
                                    }
                                    End::Complete => continue,
                                    aborted @ End::Aborted { .. } => {
                                        *self = ChainState::Done(aborted);
                                        return None;
                                    }
                                }
                            }
                            None
                        }
                    }
                }
                ChainState::Done(_) => None,
            };
        }
    }
    fn leaf_count(&self) -> usize {
        match self {
            ChainState::Active { phases } => phases.front().map(|f| f.leaf_count()).unwrap_or(0),
            ChainState::Done(_) => 0,
        }
    }
    fn end_reason(&mut self) -> End {
        match self {
            ChainState::Done(end) => *end,
            ChainState::Active { phases } => match phases.front_mut() {
                Some(front) => front.end_reason(),
                None => End::Complete,
            },
        }
    }
}
