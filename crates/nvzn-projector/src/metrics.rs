//! Metrics of the projector: the churn each projection sees.

use metrics_kit::{MetricDef, label_enum, metrics};
use nvzn_types::Tenant;

const LABEL_TENANT: &str = "tenant";
const LABEL_PROJECTION: &str = "projection";
const LABEL_OP: &str = "op";

metrics!(pub "nvzn_", projector; {
    custom projection_rows_total PROJECTION_ROWS_TOTAL "projection_rows_total" Counter None
        "Projection rows written by the fold loop, by tenant, projection type and \
               operation. The churn each projection actually sees.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &projector!();

label_enum!(pub RowOp {
    Upsert => "upsert",
    Delete => "delete",
});

pub fn projection_row(tenant: Tenant, projection: &'static str, op: RowOp) {
    metrics_kit::metrics_crate::counter!(
        PROJECTION_ROWS_TOTAL,
        LABEL_TENANT => tenant.name(),
        LABEL_PROJECTION => projection,
        LABEL_OP => op.as_str(),
    )
    .increment(1);
}

/// The last `::` segment of a type's path — `Todo`, not
/// `my_app::projections::Todo`. Label values, so readability beats fidelity;
/// a generic projection would render its last parameter, which none of them
/// are.
#[must_use]
pub fn short_type_name<P: ?Sized>() -> &'static str {
    let full = std::any::type_name::<P>();
    full.rsplit("::").next().unwrap_or(full)
}
