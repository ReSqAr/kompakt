//! `Writer` — the public write entry point.
//!
//! ```text
//!   write(blob):
//!       data_id = (hlc_now, node_id, 64 random bits)
//!       Coordinator::submit ─┬─> quorum acked      Ok(data_id)
//!                            ├─> replica error     Err(e)
//!                            ├─> channel dropped   Err(QuorumUnreachable)
//!                            └─> write_timeout     Err(WriteTimeout)
//! ```
//!
//! Fan-out, quorum evaluation, reconnect and resend are all
//! [`crate::quorum::Coordinator`]'s. What is left here is the identity and the
//! deadline.
//!
//! Minting identity — rather than deriving it from the payload — is what makes
//! a `data_id` impossible to semantically reuse (CLAUDE.md §8), so the
//! coordinator's resends dedup downstream instead of conflicting.
//!
//! `write_timeout` caps the whole wait; it is not a retry bound, since the
//! coordinator retries on its own. Giving up drops the receiver, which is how
//! the coordinator learns to reap the entry and free its permit — so every
//! per-write resource here is released by `Drop`, not by the success path.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::warn;

use crate::api::{Clock, RaftStateTracker, Router};
use crate::error::Error;
use crate::ids::NodeId;
use crate::meta::Data;
use crate::metrics::{self, OpState};
use crate::quorum::Coordinator;

pub(crate) struct WriterConfig {
    pub(crate) node_id: NodeId,
    pub(crate) write_timeout: Duration,
}

impl WriterConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.write_timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "write_timeout must be > 0".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct WriterService {
    coord: Arc<Coordinator>,
    clock: Arc<dyn Clock>,
    node_id: NodeId,
    write_timeout: Duration,
}

impl WriterService {
    #[must_use]
    pub(crate) fn new(
        router: Arc<dyn Router>,
        raft_state: Arc<dyn RaftStateTracker>,
        clock: Arc<dyn Clock>,
        WriterConfig {
            node_id,
            write_timeout,
        }: WriterConfig,
    ) -> Self {
        Self {
            coord: Coordinator::new(router, raft_state),
            clock,
            node_id,
            write_timeout,
        }
    }
}

#[async_trait::async_trait]
impl crate::api::Writer for WriterService {
    async fn write(&self, blob: Bytes) -> Result<crate::ids::DataId, Error> {
        let creation_hlc = self.clock.now();
        let data_id = crate::ids::DataId::from_parts(creation_hlc, self.node_id, rand::random());

        let _in_flight = metrics::writes_in_flight_guard();
        let probe = metrics::write_duration_probe();

        let rx = self.coord.submit(Data { data_id, blob });
        let out = match tokio::time::timeout(self.write_timeout, rx).await {
            Ok(Ok(Ok(()))) => Ok(data_id),
            Ok(Ok(Err(e))) => Err(e),
            // A dropped sender is either backpressure at submit or a departed
            // coordinator; neither is distinguishable here, and both mean the
            // write never reached a quorum.
            Ok(Err(_)) => Err(Error::QuorumUnreachable),
            Err(_) => {
                warn!(
                    node_id = ?self.node_id,
                    limit_s = self.write_timeout.as_secs(),
                    "write exceeded its hard deadline (quorum stalled)"
                );
                Err(Error::WriteTimeout {
                    elapsed: self.write_timeout,
                })
            }
        };
        probe.finish(OpState::from_ok(out.is_ok()));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_config_rejects_zero_write_timeout() {
        let c = WriterConfig {
            node_id: crate::ids::NodeId::new_v4(),
            write_timeout: Duration::ZERO,
        };
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }
}
