//! Installing the recorder and serving `/metrics`, for the role that was started.
//!
//! The recorder and the listener are `btrb-metrics`'s; this composes the histograms every capability and app
//! declares, and has each describe its own series after the install.

use std::error::Error;
use std::net::SocketAddr;

/// HTTP and read-after-write buckets are the web capability's and read-after-write's; the apps' and the
/// cache's come from the seam.
pub(super) fn start(addr: Option<SocketAddr>, apps: &[crate::Entry]) -> Result<(), Box<dyn Error>> {
    match addr {
        Some(addr) => {
            let mut histograms = crate::histograms(apps);
            histograms.extend_from_slice(btrb_web::HISTOGRAMS);
            histograms.extend_from_slice(crate::live::HISTOGRAMS);
            let handle = btrb_metrics::install(&histograms)?;
            btrb_web::describe_metrics();
            crate::live::describe_metrics();
            btrb_db::describe_metrics();
            crate::describe_metrics(apps);
            tokio::spawn(async move {
                if let Err(e) = btrb_metrics::serve(addr, handle).await {
                    tracing::error!(%e, "the metrics listener stopped");
                }
            });
        }
        None => tracing::info!("metrics export is off"),
    }
    Ok(())
}
