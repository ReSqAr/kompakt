//! The read side: assembled by `zslg-learner`; this module re-exports what the
//! rest of the crate names.

pub(crate) use zslg_learner::StableLog;
pub(crate) use zslg_learner::{LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF, STABLE_LOG_CF};

#[cfg(test)]
pub(crate) use zslg_learner::testing::integrate_manifest_for_test;

#[cfg(test)]
pub(crate) use zslg_learner::LocalStore;
