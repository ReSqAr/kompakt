//! Creating a live view, and starting it once the fold has caught up.

use super::{Live, LiveLimits};
use crate::FoldState;
use crate::{ArcStore, Error, ProjectionTypeId, Tenant, WithWireFormat};
use crossbeam_combinators::Segment;
use nvzn_projector::BoxedProjectorLike;
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};
use zslg::Hlc;

use super::consumer::{LiveSeam, run};
use super::coverage::{Coverage, Held, Inner, State};

/// A live view not started yet: it serves the stable snapshot until [`begin`]
/// starts it.
pub fn not_started<Ev: WithWireFormat>(
    tenant: Tenant,
    store: ArcStore,
    fold_state: FoldState,
    projectors: BTreeMap<ProjectionTypeId, BoxedProjectorLike<Ev>>,
    events: &'static [u32],
    limits: LiveLimits,
) -> Result<Live<Ev>, Error> {
    let f = store.read()?.watermark()?;
    let (changed, _) = watch::channel(0);
    let (retired, _) = watch::channel(0);
    let inner = Arc::new(Inner {
        tenant,
        store,
        fold_state,
        projectors,
        events,
        limits,
        state: Mutex::new(State {
            coverage: Coverage::NotStarted,
            folded: f,
        }),
        scanned: Condvar::new(),
        changed,
        retired,
    });
    inner.publish(&inner.lock());
    Ok(Live { inner })
}

/// Start `live`: open the unstable tail, scan the stable log from the store's
/// watermark up to the stable one, and integrate from then on.
pub fn begin<Ev: WithWireFormat>(
    live: &Live<Ev>,
    reader: &Arc<dyn zslg::Reader>,
    cancelled: &crossbeam_combinators::Cancellation,
    mut folded: watch::Receiver<Option<Hlc>>,
) -> Result<(), Error> {
    let inner = &live.inner;
    let f = inner.store.read()?.watermark()?;
    let after = f.index;
    let seam = LiveSeam {
        reader: Arc::clone(reader),
        after,
        cancelled: cancelled.clone(),
    };
    // The seam opens the live tail first and reads the stable watermark
    // second; nothing falls between them. `f`/`from` bound the scan only, so
    // reading them first is safe.
    let segment = Segment::open(&seam);
    let until = segment.cut();
    {
        let mut state = inner.lock();
        state.coverage = match until {
            Some(_) => Coverage::Scanning {
                held: Held::new(),
                bytes: 0,
            },
            None => Coverage::Complete {
                held: Held::new(),
                bytes: 0,
            },
        };
        state.folded = f;
        inner.publish(&state);
    }
    info!(
        tenant = inner.tenant.name(),
        live_projections = inner.projectors.len(),
        scan_until = ?until,
        "live view started"
    );

    let stream = segment.merge();
    let integrator = Arc::clone(inner);
    tokio::task::spawn_blocking(move || run(&integrator, stream));

    let retirer = Arc::clone(inner);
    tokio::spawn(async move {
        // Ends with the fold (its sender goes) or the runtime.
        while folded.changed().await.is_ok() {
            let retirer = Arc::clone(&retirer);
            match tokio::task::spawn_blocking(move || retirer.retire_now()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(error = %e, "live view: reading the store's watermark failed"),
                Err(e) => warn!(error = %e, "live view: the retirement task failed"),
            }
        }
    });

    // Started: a reader of the stable state until now looks again.
    inner.bump();
    Ok(())
}

/// The first wait between two attempts to start, doubling up to [`START_BACKOFF_MAX`].
const START_BACKOFF: Duration = Duration::from_millis(250);
const START_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Start `live` once the fold has caught up (`Syncer::sync`): the dispatcher's
/// finalise may reset the store and fold the log again, and a view started
/// before that would hold events above a watermark the store no longer has.
/// A failed sync is retried with backoff; the view stays `NotStarted` (and
/// serves the stable snapshot) only if the fold stops.
pub async fn begin_when_caught_up<Ev: WithWireFormat>(
    live: Live<Ev>,
    syncer: nvzn_types::Syncer,
    reader: Arc<dyn zslg::Reader>,
    cancelled: crossbeam_combinators::Cancellation,
) {
    let tenant = live.inner.tenant;
    let mut backoff = START_BACKOFF;
    loop {
        let attempt = match syncer.sync().await {
            Ok(()) => begin(&live, &reader, &cancelled, syncer.watermark()),
            Err(e) => Err(e),
        };
        let Err(e) = attempt else { return };
        if live.inner.fold_state.check().is_err() {
            warn!(tenant = tenant.name(), error = %e, "live view: not started, the fold stopped");
            return;
        }
        warn!(
            tenant = tenant.name(),
            error = %e,
            retry_in_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
            "live view: not started yet, the fold has not caught up"
        );
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2).min(START_BACKOFF_MAX);
    }
}
