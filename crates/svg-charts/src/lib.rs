//! A server-side line chart: grouping, viewport math, projection and the
//! complete SVG renderer.
//!
//! Points arrive flat — every point of every series, unordered — and leave as
//! one frame whose width the caller measured:
//!
//! ```text
//!   (id, label, at, value)
//!        │
//!        ├── Chart::builder(width) ──> Chart { series, x, y }
//!        │                                  │
//!        │                                  ├── view()   -> ChartView (escape hatch)
//!        │                                  └── render() -> RenderedChart
//!        │                                       { svg, tooltips, meta, view }
//!        └── hidden: BTreeSet<SeriesId>       (stable identity, never an index)
//! ```
//!
//! The crate owns the SVG, the stylesheet ([`STYLESHEET`]) and the explicit
//! behavior ([`SCRIPT`]). Rust formats every value and tooltip row; JS only
//! chooses the nearest rendered sample and positions it. Consumers may use the
//! public [`ChartView`] instead when their page needs custom markup.

mod render;

pub use render::RenderedChart;

/// The default chart stylesheet. Consumers may serve this as-is or override
/// its custom properties/classes.
pub const STYLESHEET: &str = include_str!("../static/chart.css");

/// The default, explicit chart behavior. See its file header for the tiny DOM
/// contract; it handles tooltip positioning and optional htmx reinitialization.
pub const SCRIPT: &str = include_str!("../static/chart.js");

mod builder;
mod chart;
mod layout;
mod scale;
mod types;
mod view;

pub use self::builder::{ChartBuilder, ChartSpec};
pub(crate) use self::scale::{
    align_span, auto_bounds, color, fmt_value, legend_layout, pad_span, project_x, project_y,
    x_label, x_ticks, y_ticks,
};
pub use self::types::{
    Bounds, Chart, ChartMeta, ChartView, Coord, Legend, LegendItem, Plot, Series, SeriesId,
    SeriesMeta, SeriesView, Tick, XAlignment, YAxis,
};
pub(crate) use self::types::{Point, SeriesData};

/// Used by [`Chart::from_points`]; callers that know their rendered size pass
/// one to [`Chart::from_series`] so 1 unit is 1 CSS pixel.
const DEFAULT_WIDTH: f64 = 960.0;

const MARGIN_LEFT: f64 = 56.0;

const MARGIN_RIGHT: f64 = 40.0;

const MARGIN_TOP: f64 = 16.0;

const PLOT_HEIGHT: f64 = 240.0;

const X_LABEL_GAP: f64 = 20.0;

const LEGEND_GAP: f64 = 26.0;

const LEGEND_ROW: f64 = 18.0;

const LEGEND_ITEM_GAP: f64 = 18.0;

const SWATCH: f64 = 9.0;

const SWATCH_GAP: f64 = 8.0;

const LEGEND_CHAR: f64 = 7.0;

const BOTTOM_PAD: f64 = 12.0;

const X_PAD_SECONDS: i64 = 60;

const TARGET_TICKS: i32 = 5;

const MAX_TICKS: usize = 16;

/// X tick steps in seconds: seconds, minutes, hours, days.
const X_STEPS: [i64; 15] = [
    1, 5, 15, 30, 60, 300, 900, 1800, 3600, 10800, 21600, 43200, 86400, 604_800, 2_592_000,
];

const PALETTE: [&str; 10] = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1", "#ff9da7",
    "#9c755f", "#bab0ac",
];

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests;
