//! The dispatcher's live-subscriber registry, shared by the reader side.

use crate::{ProjectionId, ProjectionTypeId, TenantId};
use crossbeam_combinators::{CancelHandle, Cancellation, KeyedRegistry};
use std::sync::Arc;

/// A registration key.
pub type SubscriptionKey = (TenantId, ProjectionTypeId);
/// A written ref: the key plus the projection id.
pub type SubscriptionItem = (TenantId, ProjectionTypeId, ProjectionId);
/// The reader side: the subscribe registry plus the live-view cancellation.
/// The cancellation is not the fan-out's; it stops live views when the fold
/// stops. The armed handle keeps the cancellation from firing during teardown.
#[derive(Clone)]
pub struct Subscriptions {
    pub registry: KeyedRegistry<SubscriptionKey, SubscriptionItem>,
    pub cancelled: Cancellation,
    _keep_armed: Arc<CancelHandle>,
}

impl Subscriptions {
    pub fn new(
        registry: KeyedRegistry<SubscriptionKey, SubscriptionItem>,
        cancelled: Cancellation,
        keep_armed: Arc<CancelHandle>,
    ) -> Self {
        Self {
            registry,
            cancelled,
            _keep_armed: keep_armed,
        }
    }
}
