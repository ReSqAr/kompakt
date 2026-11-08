//! The read path: `LocalStoreView` — scans pushed into channels, and the
//! unstable fan-out.

use std::ops::Bound;

use crossbeam_combinators::{Channel, Infinite};
use tokio::sync::watch;

use super::engine::{above_hlc, read_cursor, stream_scan};
use super::{LocalStore, STREAM_CHANNEL_CAPACITY};
use zslg_consensus::NodeId;
use zslg_log::Data;
use zslg_log::ToStorage;
use zslg_log::{DataId, LogIndex};
use zslg_time::Hlc;

#[async_trait::async_trait]
impl zslg_log::LocalStoreView for LocalStore {
    fn stream_above_hlc(&self, threshold: Option<Hlc>) -> zslg_log::LocalStoreStream {
        let storage = self.storage.clone();
        let start = above_hlc(threshold);
        let ch =
            Channel::produce_blocking(STREAM_CHANNEL_CAPACITY, "stream_above_hlc", move |feed| {
                stream_scan(&storage, (start, Bound::Unbounded), feed)
            });
        ch.ordered(None, "local store above HLC").err_into()
    }

    fn stream_window(&self, above: Option<Hlc>, upto: Hlc) -> zslg_log::LocalStoreStream {
        let storage = self.storage.clone();
        let start = above_hlc(above);
        let end = Bound::Included(DataId::from_parts(upto, NodeId::MAX, u64::MAX));
        let ch = Channel::produce_blocking(STREAM_CHANNEL_CAPACITY, "stream_window", move |feed| {
            stream_scan(&storage, (start, end), feed)
        });
        ch.ordered(None, "local store window").err_into()
    }

    fn subscribe_integrated(&self) -> watch::Receiver<()> {
        self.integrated.subscribe()
    }

    fn subscribe_unstable(&self) -> Channel<Data, Infinite> {
        self.unstable_registry.subscribe_infinite()
    }

    async fn subscription_cursor(
        &self,
        upstream: NodeId,
    ) -> Result<Option<LogIndex>, zslg_log::Error> {
        async {
            let storage = self.storage.clone();
            tokio::task::spawn_blocking(move || read_cursor(&storage, upstream))
                .await
                .to_storage("subscription_cursor spawn_blocking panicked")?
        }
        .await
        .map_err(Into::into)
    }
}
