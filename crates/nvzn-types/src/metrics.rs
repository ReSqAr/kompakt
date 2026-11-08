//! Metrics of the event writer handle.

use crate::Tenant;
use metrics_kit::{MetricDef, metrics};

const LABEL_TENANT: &str = "tenant";

metrics!(pub "nvzn_", writes; {
    custom writes_total WRITES_TOTAL "writes_total" Counter None
        "Event batches written to the log, by tenant.";
    custom write_events_total WRITE_EVENTS_TOTAL "write_events_total" Counter None
        "Events written to the log, by tenant. Against nvzn_writes_total, the \
               batch size a caller is actually achieving.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &writes!();

pub fn write(tenant: Tenant, events: usize) {
    metrics_kit::metrics_crate::counter!(WRITES_TOTAL, LABEL_TENANT => tenant.name()).increment(1);
    metrics_kit::metrics_crate::counter!(WRITE_EVENTS_TOTAL, LABEL_TENANT => tenant.name())
        .increment(u64::try_from(events).unwrap_or(0));
}
