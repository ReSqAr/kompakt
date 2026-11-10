//! The chart vocabulary: ids, axes, bounds, and the views a chart renders from.

use jiff::{SignedDuration, Timestamp};
use std::collections::BTreeSet;

#[allow(dead_code, reason = "completeness")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XAlignment {
    Hour,
    Day,
}

impl XAlignment {
    pub(crate) fn seconds(self) -> i64 {
        match self {
            Self::Hour => 3_600,
            Self::Day => 86_400,
        }
    }
}

/// A stable series identity. Opaque to the renderer; callers use it to carry
/// visibility and tooltip state across requests and DOM updates.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeriesId(String);

impl SeriesId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SeriesId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for SeriesId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for SeriesId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Optional y-axis bounds, Prometheus-style.
///
/// * `soft_min`/`soft_max` extend the data's range to include them but never
///   clip: the axis expands further if the data does.
/// * `hard_min`/`hard_max` fix the bound exactly; the line is clipped there.
///
/// Hard wins over soft.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Bounds {
    pub soft_min: Option<f64>,
    pub soft_max: Option<f64>,
    pub hard_min: Option<f64>,
    pub hard_max: Option<f64>,
}

/// How the legend is rendered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Legend {
    #[default]
    Inline,
    External,
    None,
}

/// The y-axis configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct YAxis {
    pub bounds: Bounds,
}

impl YAxis {
    #[must_use]
    pub fn auto() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn soft_min(mut self, value: f64) -> Self {
        self.bounds.soft_min = Some(value);
        self
    }

    #[must_use]
    pub fn soft_max(mut self, value: f64) -> Self {
        self.bounds.soft_max = Some(value);
        self
    }

    #[must_use]
    pub fn hard_min(mut self, value: f64) -> Self {
        self.bounds.hard_min = Some(value);
        self
    }

    #[must_use]
    pub fn hard_max(mut self, value: f64) -> Self {
        self.bounds.hard_max = Some(value);
        self
    }
}

#[derive(Debug)]
pub struct Chart {
    pub(crate) series: Vec<SeriesData>,
    pub(crate) x_min: Timestamp,
    pub(crate) x_max: Timestamp,
    pub(crate) width: f64,
    pub(crate) height: f64,
    pub(crate) legend: Vec<LegendItem>,
    /// Series hidden by the caller; the legend keeps them, the plot and the
    /// y-range do not.
    pub(crate) hidden: BTreeSet<SeriesId>,
    pub(crate) bounds: Bounds,
    pub(crate) legend_mode: Legend,
    pub(crate) unit: String,
    pub(crate) aria_label: String,
    pub(crate) plot_height: f64,
    /// Consecutive points further apart than this are not joined: the line
    /// breaks there (no data is not a straight line). `None`: always joined.
    pub(crate) max_step: Option<SignedDuration>,
}

#[derive(Debug)]
pub(crate) struct SeriesData {
    pub(crate) id: SeriesId,
    pub(crate) name: String,
    pub(crate) points: Vec<Point>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Point {
    pub(crate) at: Timestamp,
    pub(crate) value: f32,
}

pub struct ChartView {
    pub width: f64,
    pub height: f64,
    pub plot: Plot,
    pub x_ticks: Vec<Tick>,
    pub y_ticks: Vec<Tick>,
    pub series: Vec<SeriesView>,
    pub legend: Vec<LegendItem>,
}

/// Plot rectangle and label anchors, in viewBox units.
pub struct Plot {
    pub left: f64,
    pub right: f64,
    pub top: f64,
    pub bottom: f64,
    pub y_label_x: f64,
    pub x_label_y: f64,
    pub caption_y: f64,
    pub unit_x: f64,
    pub unit_y: f64,
}

#[derive(Debug, Clone)]
pub struct Tick {
    pub pos: f64,
    pub label: String,
}

/// A stable series summary for consumers that build their own controls.
#[derive(Debug, Clone)]
pub struct SeriesMeta {
    pub id: SeriesId,
    pub name: String,
    pub color: &'static str,
    pub hidden: bool,
}

/// What the renderer knows besides the SVG itself. The view remains the escape
/// hatch for custom markup; this is the small, stable metadata surface.
#[derive(Debug, Clone)]
pub struct ChartMeta {
    pub series: Vec<SeriesMeta>,
    pub x_ticks: Vec<Tick>,
    pub y_ticks: Vec<Tick>,
    pub unit: String,
}

pub struct SeriesView {
    pub id: SeriesId,
    pub name: String,
    pub color: &'static str,
    pub is_line: bool,
    pub hidden: bool,
    pub points: Vec<Coord>,
    /// The line, as runs of joined points (`x,y x,y …`, SVG polyline
    /// syntax): one run unless a gap wider than the maximum step breaks it.
    pub runs: Vec<String>,
    /// Points alone in their run, drawn as dots so they still show.
    pub dots: Vec<(f64, f64)>,
    pub latest: String,
    pub latest_at: String,
}

pub struct Coord {
    pub x: f64,
    pub y: f64,
    /// The y value as text (already scaled by the caller), for a tooltip.
    pub value: String,
    /// The point's timestamp as text, for a tooltip.
    pub at: String,
}

/// One legend entry, positioned inside the SVG.
#[derive(Debug, Clone)]
pub struct LegendItem {
    pub id: SeriesId,
    pub name: String,
    pub color: &'static str,
    pub swatch_x: f64,
    pub swatch_y: f64,
    pub text_x: f64,
    pub text_y: f64,
}

/// One series in the builder API.
#[derive(Debug, Clone)]
pub struct Series {
    pub(crate) id: SeriesId,
    pub(crate) label: String,
    pub(crate) points: Vec<(Timestamp, f32)>,
}

impl Series {
    #[must_use]
    pub fn new(id: impl Into<SeriesId>, label: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            points: Vec::new(),
        }
    }

    #[must_use]
    pub fn point(mut self, at: Timestamp, value: f32) -> Self {
        self.points.push((at, value));
        self
    }

    #[must_use]
    pub fn points(mut self, points: impl IntoIterator<Item = (Timestamp, f32)>) -> Self {
        self.points.extend(points);
        self
    }
}
