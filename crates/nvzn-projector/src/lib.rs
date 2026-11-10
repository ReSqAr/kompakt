//! Folds events into projections: one `Projector` per projection type, and the
//! manager that registers them.

pub mod metrics;

#[cfg(test)]
mod projection_tests;

mod projector;

pub use projector::{
    Apply, BoxedProjectorLike, Projector, ProjectorLike, ProjectorManager, Registered, fold_event,
};
