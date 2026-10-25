use std::sync::Arc;

use tokio::sync::watch;

use crate::api::{RaftStateTracker, RaftStateView};
use crate::meta::RaftState;

pub(crate) struct NoOpRaftViewer(Arc<dyn RaftStateView>);

impl NoOpRaftViewer {
    #[must_use]
    pub(crate) fn new(inner: Arc<dyn RaftStateView>) -> Self {
        Self(inner)
    }
}

impl RaftStateView for NoOpRaftViewer {
    fn snapshot(&self) -> Arc<RaftState> {
        self.0.snapshot()
    }

    fn subscribe(&self) -> watch::Receiver<Arc<RaftState>> {
        self.0.subscribe()
    }
}

impl RaftStateTracker for NoOpRaftViewer {
    fn integrate(&self, _observed: &RaftState) {}
}
