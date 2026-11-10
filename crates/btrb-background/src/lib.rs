//! The background capability: jobs that end only when told to, and the signals that tell them.
//!
//! ```text
//!   Job = Future<Result<Stopped, Error>>       Stopped is built only by Shutdown::wait, so a job cannot end Ok early
//!   run_background(stop, jobs)                 all together; the first failure cancels `stop`, the rest drain
//!   shutdown_signal()                          Ctrl-C or SIGTERM: what a container runtime sends
//! ```

mod signals;

pub use signals::shutdown_signal;

use btrb_types::{Error, Job, Shutdown};
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;

/// Run every app's background jobs together. The first job to fail cancels
/// `stop`; the rest drain, and that first error is returned. With no jobs this
/// waits for shutdown, so a worker of job-less apps is still a worker.
pub async fn run_background(stop: &Shutdown, jobs: Vec<Job>) -> Result<(), Error> {
    if jobs.is_empty() {
        stop.wait().await;
        return Ok(());
    }
    let mut running: FuturesUnordered<_> = jobs.into_iter().collect();
    let mut first = None;
    while let Some(done) = running.next().await {
        if let Err(e) = done {
            stop.cancel();
            first.get_or_insert(e);
        }
    }
    first.map_or(Ok(()), Err)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use btrb_types::Stopped;
    use futures::future::BoxFuture;

    #[tokio::test]
    async fn the_first_failure_stops_the_others_and_is_the_result() {
        let stop = Shutdown::new();
        let patient: BoxFuture<'static, Result<Stopped, Error>> = {
            let stop = stop.clone();
            Box::pin(async move { Ok(stop.wait().await) })
        };
        let failing: BoxFuture<'static, Result<Stopped, Error>> =
            Box::pin(async { Err(Error::new("boom")) });
        let result = run_background(&stop, vec![patient, failing]).await;
        assert_eq!(result.unwrap_err().to_string(), "boom");
        assert!(stop.is_stopping());
    }

    #[tokio::test]
    async fn no_jobs_waits_for_shutdown() {
        let stop = Shutdown::new();
        let waiting = tokio::spawn({
            let stop = stop.clone();
            async move { run_background(&stop, Vec::new()).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        stop.cancel();
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_job_ends_only_when_told() {
        let stop = Shutdown::new();
        let waiting = tokio::spawn({
            let stop = stop.clone();
            async move { stop.wait().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        stop.cancel();
        waiting.await.unwrap();
        assert!(stop.is_stopping());
    }
}
