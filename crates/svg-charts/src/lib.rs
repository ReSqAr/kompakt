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

use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, Unit};
use std::collections::{BTreeMap, BTreeSet};

mod render;

pub use render::RenderedChart;

/// The default chart stylesheet. Consumers may serve this as-is or override
/// its custom properties/classes.
pub const STYLESHEET: &str = include_str!("../static/chart.css");

/// The default, explicit chart behavior. See its file header for the tiny DOM
/// contract; it handles tooltip positioning and optional htmx reinitialization.
pub const SCRIPT: &str = include_str!("../static/chart.js");

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

#[allow(dead_code, reason = "completeness")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XAlignment {
    Hour,
    Day,
}

impl XAlignment {
    fn seconds(self) -> i64 {
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
    series: Vec<SeriesData>,
    x_min: Timestamp,
    x_max: Timestamp,
    width: f64,
    height: f64,
    legend: Vec<LegendItem>,
    /// Series hidden by the caller; the legend keeps them, the plot and the
    /// y-range do not.
    hidden: BTreeSet<SeriesId>,
    bounds: Bounds,
    legend_mode: Legend,
    unit: String,
    aria_label: String,
    plot_height: f64,
    /// Consecutive points further apart than this are not joined: the line
    /// breaks there (no data is not a straight line). `None`: always joined.
    max_step: Option<SignedDuration>,
}

#[derive(Debug)]
struct SeriesData {
    id: SeriesId,
    name: String,
    points: Vec<Point>,
}

#[derive(Debug, Clone, Copy)]
struct Point {
    at: Timestamp,
    value: f32,
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
    id: SeriesId,
    label: String,
    points: Vec<(Timestamp, f32)>,
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

/// A validated chart builder.
#[derive(Debug, Clone)]
pub struct ChartBuilder {
    width: f64,
    alignment: XAlignment,
    domain: Option<(Timestamp, Timestamp)>,
    series: Vec<Series>,
    y_axis: YAxis,
    max_step: Option<SignedDuration>,
    hidden: BTreeSet<SeriesId>,
    legend: Legend,
    unit: String,
    aria_label: String,
    plot_height: Option<f64>,
}

impl ChartBuilder {
    #[must_use]
    pub fn alignment(mut self, alignment: XAlignment) -> Self {
        self.alignment = alignment;
        self
    }

    #[must_use]
    pub fn domain(mut self, start: Timestamp, end: Timestamp) -> Self {
        self.domain = Some((start, end));
        self
    }

    #[must_use]
    pub fn series(mut self, series: impl IntoIterator<Item = Series>) -> Self {
        self.series.extend(series);
        self
    }

    #[must_use]
    pub fn y_axis(mut self, y_axis: YAxis) -> Self {
        self.y_axis = y_axis;
        self
    }

    #[must_use]
    pub fn max_step(mut self, step: SignedDuration) -> Self {
        self.max_step = Some(step);
        self
    }

    #[must_use]
    pub fn legend(mut self, legend: Legend) -> Self {
        self.legend = legend;
        self
    }

    #[must_use]
    pub fn hidden(mut self, hidden: impl IntoIterator<Item = SeriesId>) -> Self {
        self.hidden = hidden.into_iter().collect();
        self
    }

    #[must_use]
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = unit.into();
        self
    }

    #[must_use]
    pub fn aria_label(mut self, label: impl Into<String>) -> Self {
        self.aria_label = label.into();
        self
    }

    #[must_use]
    pub fn plot_height(mut self, height: f64) -> Self {
        self.plot_height = Some(height);
        self
    }

    #[must_use]
    pub fn build(self) -> Option<Chart> {
        let points = self.series.into_iter().flat_map(|series| {
            series
                .points
                .into_iter()
                .map(move |(at, value)| (series.id.to_string(), series.label.clone(), at, value))
        });
        let mut chart = Chart::from_series(points, self.alignment, self.domain, self.width)?;
        chart = chart
            .with_bounds(self.y_axis.bounds)
            .with_legend(self.legend)
            .with_unit(self.unit)
            .with_aria_label(self.aria_label);
        if let Some(step) = self.max_step {
            chart = chart.with_max_step(step);
        }
        if let Some(height) = self.plot_height {
            chart = chart.with_plot_height(height);
        }
        chart.hide(self.hidden);
        Some(chart)
    }
}

/// The serializable/test-friendly shape of a chart. The builder is the normal
/// fluent surface; a spec is useful when a caller constructs a chart from data.
#[derive(Debug, Clone)]
pub struct ChartSpec {
    pub width: f64,
    pub alignment: XAlignment,
    pub domain: Option<(Timestamp, Timestamp)>,
    pub series: Vec<Series>,
    pub y_axis: YAxis,
    pub max_step: Option<SignedDuration>,
    pub hidden: BTreeSet<SeriesId>,
    pub legend: Legend,
    pub unit: String,
    pub aria_label: String,
    pub plot_height: Option<f64>,
}

impl ChartSpec {
    #[must_use]
    pub fn build(self) -> Option<Chart> {
        let mut chart = Chart::builder(self.width)
            .alignment(self.alignment)
            .series(self.series)
            .y_axis(self.y_axis)
            .legend(self.legend)
            .unit(self.unit)
            .aria_label(self.aria_label)
            .hidden(self.hidden);
        if let Some((start, end)) = self.domain {
            chart = chart.domain(start, end);
        }
        if let Some(step) = self.max_step {
            chart = chart.max_step(step);
        }
        if let Some(height) = self.plot_height {
            chart = chart.plot_height(height);
        }
        chart.build()
    }
}

impl Chart {
    #[must_use]
    pub fn from_spec(spec: ChartSpec) -> Option<Self> {
        spec.build()
    }

    #[must_use]
    pub fn builder(width: f64) -> ChartBuilder {
        ChartBuilder {
            width,
            alignment: XAlignment::Hour,
            domain: None,
            series: Vec::new(),
            y_axis: YAxis::default(),
            max_step: None,
            hidden: BTreeSet::new(),
            legend: Legend::Inline,
            unit: String::new(),
            aria_label: "Line chart".to_owned(),
            plot_height: None,
        }
    }

    /// Group `(series name, timestamp, value)` points, sort each series by
    /// time, and fit one shared viewport to the data. `None` when there are no
    /// points.
    #[must_use]
    pub fn from_points(
        points: impl IntoIterator<Item = (String, Timestamp, f32)>,
        alignment: XAlignment,
    ) -> Option<Self> {
        Self::from_series(
            points
                .into_iter()
                .map(|(name, at, value)| (name.clone(), name, at, value)),
            alignment,
            None,
            DEFAULT_WIDTH,
        )
        // Keep the historical look (the axis includes zero) for existing callers.
        .map(|chart| {
            chart.with_bounds(Bounds {
                soft_min: Some(0.0),
                ..Bounds::default()
            })
        })
    }

    /// Like [`from_points`](Self::from_points), but a point carries a *grouping
    /// key* and a *display label* separately, and the caller passes the chart's
    /// intended rendered `width` so `viewBox="0 0 width height"` maps 1 unit to
    /// 1 CSS pixel. Points sharing a key form one series; the label is what the
    /// legend and tooltips show, so two sources that both emit `GET` keep two
    /// lines. A domain that is not a positive span is ignored.
    #[must_use]
    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub fn from_series(
        points: impl IntoIterator<Item = (String, String, Timestamp, f32)>,
        alignment: XAlignment,
        domain: Option<(Timestamp, Timestamp)>,
        width: f64,
    ) -> Option<Self> {
        let mut grouped: BTreeMap<SeriesId, (String, Vec<Point>)> = BTreeMap::new();
        for (key, label, at, value) in points {
            grouped
                .entry(SeriesId::new(key))
                .or_insert_with(|| (label, Vec::new()))
                .1
                .push(Point { at, value });
        }

        let series: Vec<SeriesData> = grouped
            .into_iter()
            .map(|(id, (name, mut points))| {
                points.sort_by_key(|point| point.at);
                SeriesData { id, name, points }
            })
            .collect();
        if series.is_empty() {
            return None;
        }

        let (x_min, x_max) = match domain {
            Some((start, end)) if start < end => (start, end),
            _ => {
                let points = series.iter().flat_map(|series| series.points.iter());
                let x_min = points.clone().map(|point| point.at).min()?;
                let x_max = points.map(|point| point.at).max()?;
                let (x_min, x_max) = pad_span(x_min, x_max);
                align_span(x_min, x_max, alignment)
            }
        };

        let width = if width.is_finite() && width > MARGIN_LEFT + MARGIN_RIGHT + 1.0 {
            width
        } else {
            DEFAULT_WIDTH
        };
        let legend_top = MARGIN_TOP + PLOT_HEIGHT + X_LABEL_GAP + LEGEND_GAP;
        let (legend, rows) = legend_layout(&series, width, legend_top);
        let height = legend_top + (rows - 1.0) * LEGEND_ROW + BOTTOM_PAD;

        Some(Self {
            series,
            x_min,
            x_max,
            width,
            height,
            legend,
            hidden: BTreeSet::new(),
            bounds: Bounds::default(),
            legend_mode: Legend::Inline,
            unit: String::new(),
            aria_label: "Line chart".to_owned(),
            plot_height: PLOT_HEIGHT,
            max_step: None,
        })
    }

    /// A chart with no data: the caller's x-domain, a `0..1` y-range and no
    /// series. It keeps the plot's dimensions so an empty panel does not
    /// collapse, and it draws the same frame (axes, grid, unit) as a filled one.
    #[must_use]
    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub fn empty(x_min: Timestamp, x_max: Timestamp, width: f64) -> Self {
        let (x_min, x_max) = pad_span(x_min, x_max);
        let width = if width.is_finite() && width > MARGIN_LEFT + MARGIN_RIGHT + 1.0 {
            width
        } else {
            DEFAULT_WIDTH
        };
        let legend_top = MARGIN_TOP + PLOT_HEIGHT + X_LABEL_GAP + LEGEND_GAP;
        let (legend, rows) = legend_layout(&[], width, legend_top);
        let height = legend_top + (rows - 1.0) * LEGEND_ROW + BOTTOM_PAD;
        Self {
            series: Vec::new(),
            x_min,
            x_max,
            width,
            height,
            legend,
            hidden: BTreeSet::new(),
            bounds: Bounds::default(),
            legend_mode: Legend::Inline,
            unit: String::new(),
            aria_label: "Line chart".to_owned(),
            plot_height: PLOT_HEIGHT,
            max_step: None,
        }
    }

    /// The y-axis bounds to use (see [`Bounds`]). Data outside a hard bound is
    /// clipped by the projection; a soft bound only extends the axis.
    #[must_use]
    pub fn with_bounds(mut self, bounds: Bounds) -> Self {
        self.bounds = bounds;
        self
    }

    /// Break a series' line where consecutive points are further apart than
    /// `step` (a missing bucket), instead of drawing a straight line across.
    #[must_use]
    pub fn with_max_step(mut self, step: SignedDuration) -> Self {
        self.max_step = Some(step);
        self
    }

    /// Hide these stable series ids (the legend still lists them); the plot
    /// and the y-range re-fit to the series that remain. Unknown ids are
    /// ignored when the view is built.
    pub fn hide(&mut self, hidden: BTreeSet<SeriesId>) {
        self.hidden = hidden;
    }

    #[must_use]
    pub fn with_legend(mut self, legend: Legend) -> Self {
        self.legend_mode = legend;
        self.relayout_legend();
        self
    }

    #[must_use]
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = unit.into();
        self
    }

    #[must_use]
    pub fn with_aria_label(mut self, label: impl Into<String>) -> Self {
        self.aria_label = label.into();
        self
    }

    #[must_use]
    pub fn with_plot_height(mut self, height: f64) -> Self {
        if !height.is_finite() || height <= 0.0 {
            return self;
        }
        self.plot_height = height;
        self.relayout_legend();
        self
    }

    /// Rebuild legend positions and the total height after a layout option
    /// changes. External legends keep their entries but do not reserve inline
    /// space; `None` discards them.
    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    fn relayout_legend(&mut self) {
        let legend_top = self.plot_bottom() + X_LABEL_GAP + LEGEND_GAP;
        let (legend, rows) = match self.legend_mode {
            Legend::Inline => legend_layout(&self.series, self.width, legend_top),
            Legend::External => {
                let (items, _) = legend_layout(&self.series, self.width, legend_top);
                (items, 1.0)
            }
            Legend::None => (Vec::new(), 1.0),
        };
        self.legend = legend;
        self.height = if self.legend_mode == Legend::Inline {
            legend_top + (rows - 1.0) * LEGEND_ROW + BOTTOM_PAD
        } else {
            self.plot_bottom() + X_LABEL_GAP + BOTTOM_PAD
        };
    }

    /// The effective y-range: the visible data's range, extended by any soft
    /// bound and finally fixed by any hard bound.
    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    fn y_range(&self) -> (f64, f64) {
        let mut data_min = f64::INFINITY;
        let mut data_max = f64::NEG_INFINITY;
        for series in &self.series {
            if self.hidden.contains(&series.id) {
                continue;
            }
            for point in &series.points {
                data_min = data_min.min(f64::from(point.value));
                data_max = data_max.max(f64::from(point.value));
            }
        }

        let (mut min, mut max) = if data_min <= data_max {
            auto_bounds(data_min, data_max)
        } else {
            (0.0, 1.0)
        };
        if let Some(soft) = self.bounds.soft_min {
            // No data below the soft minimum: the axis starts there, even
            // when padding a flat line would reach below it (all-zero rates
            // read 0 to 1, not -1 to 1).
            min = if data_min >= soft {
                soft
            } else {
                min.min(soft)
            };
        }
        if let Some(soft) = self.bounds.soft_max {
            max = max.max(soft);
        }
        if let Some(hard) = self.bounds.hard_min {
            min = hard;
        }
        if let Some(hard) = self.bounds.hard_max {
            max = hard;
        }
        if min >= max {
            max = min + 1.0;
        }
        (min, max)
    }

    fn plot_left(&self) -> f64 {
        MARGIN_LEFT
    }

    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    fn plot_right(&self) -> f64 {
        self.width - MARGIN_RIGHT
    }

    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    fn plot_width(&self) -> f64 {
        self.plot_right() - self.plot_left()
    }

    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    fn plot_bottom(&self) -> f64 {
        MARGIN_TOP + self.plot_height
    }

    /// A series' line as runs of joined points and lone dots, broken where
    /// two points are further apart than the maximum step.
    fn runs(&self, series: &SeriesData, y_min: f64, y_max: f64) -> (Vec<String>, Vec<(f64, f64)>) {
        let mut groups: Vec<Vec<&Point>> = Vec::new();
        for point in &series.points {
            let joined = groups
                .last()
                .and_then(|g| g.last())
                .is_some_and(|previous| {
                    self.max_step
                        .is_none_or(|step| point.at.duration_since(previous.at) <= step)
                });
            match groups.last_mut() {
                Some(group) if joined => group.push(point),
                _ => groups.push(vec![point]),
            }
        }
        let xy = |point: &Point| {
            (
                project_x(point.at, self),
                project_y(f64::from(point.value), self, y_min, y_max),
            )
        };
        let mut runs = Vec::new();
        let mut dots = Vec::new();
        for group in groups {
            if let [only] = group.as_slice() {
                dots.push(xy(only));
            } else {
                let coords: Vec<String> = group
                    .iter()
                    .map(|point| {
                        let (x, y) = xy(point);
                        format!("{x:.2},{y:.2}")
                    })
                    .collect();
                runs.push(coords.join(" "));
            }
        }
        (runs, dots)
    }

    #[must_use]
    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub fn render(&self) -> RenderedChart {
        render::render(self, self.view())
    }

    #[must_use]
    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub fn view(&self) -> ChartView {
        let span_secs = self.x_max.duration_since(self.x_min).as_secs();
        let (y_min, y_max) = self.y_range();

        let x_ticks = x_ticks(self)
            .into_iter()
            .map(|at| Tick {
                pos: project_x(at, self),
                label: x_label(at, span_secs),
            })
            .collect();

        let y_ticks = y_ticks(y_min, y_max)
            .into_iter()
            .map(|value| Tick {
                pos: project_y(value, self, y_min, y_max),
                label: fmt_value(value),
            })
            .collect();

        let series = self
            .series
            .iter()
            .enumerate()
            .map(|(index, series)| {
                let latest = series.points.last();
                let (runs, dots) = self.runs(series, y_min, y_max);
                SeriesView {
                    id: series.id.clone(),
                    name: series.name.clone(),
                    color: color(index),
                    is_line: series.points.len() >= 2,
                    hidden: self.hidden.contains(&series.id),
                    runs,
                    dots,
                    points: series
                        .points
                        .iter()
                        .map(|point| Coord {
                            x: project_x(point.at, self),
                            y: project_y(f64::from(point.value), self, y_min, y_max),
                            value: fmt_value(f64::from(point.value)),
                            at: point.at.strftime("%Y-%m-%d %H:%M").to_string(),
                        })
                        .collect(),
                    latest: latest
                        .map_or_else(|| fmt_value(0.0), |point| fmt_value(f64::from(point.value))),
                    latest_at: latest.map_or_else(String::new, |point| {
                        point.at.strftime("%Y-%m-%d %H:%M").to_string()
                    }),
                }
            })
            .collect();

        ChartView {
            width: self.width,
            height: self.height,
            plot: Plot {
                left: self.plot_left(),
                right: self.plot_right(),
                top: MARGIN_TOP,
                bottom: self.plot_bottom(),
                y_label_x: self.plot_left() - 10.0,
                x_label_y: self.plot_bottom() + X_LABEL_GAP,
                caption_y: self.height - 6.0,
                unit_x: 14.0,
                unit_y: MARGIN_TOP + self.plot_height / 2.0,
            },
            x_ticks,
            y_ticks,
            series,
            legend: self.legend.clone(),
        }
    }
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
fn legend_layout(series: &[SeriesData], width: f64, legend_top: f64) -> (Vec<LegendItem>, f64) {
    let right = width - MARGIN_RIGHT;
    let mut items = Vec::with_capacity(series.len());
    let mut x = MARGIN_LEFT;
    let mut row = 0.0_f64;
    for (index, series) in series.iter().enumerate() {
        let text_width = f64_of(series.name.chars().count()) * LEGEND_CHAR;
        let item_width = SWATCH + SWATCH_GAP + text_width + LEGEND_ITEM_GAP;
        if x > MARGIN_LEFT && x + item_width > right {
            x = MARGIN_LEFT;
            row += 1.0;
        }
        let baseline = legend_top + row * LEGEND_ROW;
        items.push(LegendItem {
            id: series.id.clone(),
            name: series.name.clone(),
            color: color(index),
            swatch_x: x,
            swatch_y: baseline - SWATCH + 1.0,
            text_x: x + SWATCH + SWATCH_GAP,
            text_y: baseline,
        });
        x += item_width;
    }
    (items, row + 1.0)
}

#[allow(clippy::as_conversions, reason = "counts and indices are small")]
fn f64_of(value: usize) -> f64 {
    value as f64
}

fn pad_span(x_min: Timestamp, x_max: Timestamp) -> (Timestamp, Timestamp) {
    if x_min < x_max {
        return (x_min, x_max);
    }

    let pad = SignedDuration::from_secs(X_PAD_SECONDS);
    (
        x_min.checked_sub(pad).unwrap_or(x_min),
        x_max.checked_add(pad).unwrap_or(x_max),
    )
}

/// The range auto-fitted to the visible data: the data's span with 10% headroom
/// on top (both sides when the line is flat). Soft and hard bounds are applied
/// on top of this.
#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
fn auto_bounds(data_min: f64, data_max: f64) -> (f64, f64) {
    if data_min == data_max {
        let pad = (data_max.abs() * 0.1).max(1.0);
        (data_min - pad, data_max + pad)
    } else {
        (data_min, data_max + (data_max - data_min) * 0.1)
    }
}

fn color(index: usize) -> &'static str {
    let palette_index = index.checked_rem(PALETTE.len()).unwrap_or_default();
    PALETTE.get(palette_index).copied().unwrap_or(PALETTE[0])
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
fn project_x(at: Timestamp, chart: &Chart) -> f64 {
    let span = chart.x_max.duration_since(chart.x_min).as_secs_f64();
    let offset = at.duration_since(chart.x_min).as_secs_f64();
    chart.plot_left() + (chart.plot_right() - chart.plot_left()) * (offset / span)
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
fn project_y(value: f64, chart: &Chart, min: f64, max: f64) -> f64 {
    chart.plot_bottom() - (chart.plot_bottom() - MARGIN_TOP) * ((value - min) / (max - min))
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
fn nice_step(rough: f64) -> f64 {
    if rough <= 0.0 {
        return 1.0;
    }

    let magnitude = 10f64.powf(rough.log10().floor());
    for factor in [1.0, 2.0, 5.0] {
        let step = magnitude * factor;
        if step >= rough {
            return step;
        }
    }
    magnitude * 10.0
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
fn y_ticks(min: f64, max: f64) -> Vec<f64> {
    let step = nice_step((max - min) / f64::from(TARGET_TICKS));
    let first = (min / step).ceil() * step;

    let mut ticks = Vec::new();
    let mut index = 0i32;
    while ticks.len() < MAX_TICKS {
        let value = first + step * f64::from(index);
        if value > max {
            break;
        }
        ticks.push(value);
        index = index.saturating_add(1);
    }
    ticks
}

/// The smallest step that puts at most `ticks` labels on the span.
fn x_step(span_secs: i64, ticks: i32) -> i64 {
    X_STEPS
        .iter()
        .copied()
        .find(|step| span_secs <= step.saturating_mul(i64::from(ticks)))
        .unwrap_or(86_400)
}

/// How many x labels fit side by side: the plot's width over a label's (its
/// characters at about 7 px, and a gap), at least 2, at most the target.
#[allow(
    clippy::float_arithmetic,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "a count of labels from a width in pixels"
)]
fn x_labels_that_fit(plot_width: f64, span_secs: i64) -> i32 {
    let chars = x_label(Timestamp::UNIX_EPOCH, span_secs).chars().count();
    let label = 7.0 * chars as f64 + 18.0;
    ((plot_width / label).floor() as i32).clamp(2, TARGET_TICKS)
}

/// Snaps the domain outward to the alignment period, so charts start and end
/// on period boundaries (e.g. UTC midnight) and no tick falls outside the plot.
fn align_span(x_min: Timestamp, x_max: Timestamp, alignment: XAlignment) -> (Timestamp, Timestamp) {
    let period = alignment.seconds();
    let floor = TimestampRound::new()
        .smallest(Unit::Second)
        .increment(period)
        .mode(RoundMode::Floor);
    let ceil = TimestampRound::new()
        .smallest(Unit::Second)
        .increment(period)
        .mode(RoundMode::Ceil);
    (
        x_min.round(floor).unwrap_or(x_min),
        x_max.round(ceil).unwrap_or(x_max),
    )
}

fn x_ticks(chart: &Chart) -> Vec<Timestamp> {
    let span = chart.x_max.duration_since(chart.x_min).as_secs();
    let step = x_step(span, x_labels_that_fit(chart.plot_width(), span));

    let mut ticks = Vec::new();
    let mut tick = chart.x_min;
    while tick <= chart.x_max && ticks.len() < MAX_TICKS {
        ticks.push(tick);
        match tick.checked_add(SignedDuration::from_secs(step)) {
            Ok(next) => tick = next,
            Err(_) => break,
        }
    }
    ticks
}

fn x_label(tick: Timestamp, span_secs: i64) -> String {
    let format = if span_secs <= 21_600 {
        "%H:%M"
    } else if span_secs <= 259_200 {
        "%m-%d %H:%M"
    } else {
        "%Y-%m-%d"
    };
    tick.strftime(format).to_string()
}

fn fmt_value(value: f64) -> String {
    // Three places keep a narrow counter (9.999, 10.001) distinct; trailing
    // zeros are still removed, so ordinary integers stay compact.
    let mut text = format!("{value:.3}");
    while text.ends_with('0') {
        let _ = text.pop();
    }
    if text.ends_with('.') {
        let _ = text.pop();
    }
    text
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn built_chart() -> Chart {
        let start = Timestamp::from_second(0).unwrap();
        let a = Series::new("a", "Alpha")
            .point(start, 1.0)
            .point(Timestamp::from_second(60).unwrap(), 2.0)
            .point(Timestamp::from_second(120).unwrap(), 3.0);
        let b = Series::new("b", "Beta")
            .point(start, 4.0)
            .point(Timestamp::from_second(60).unwrap(), 5.0)
            .point(Timestamp::from_second(120).unwrap(), 6.0);
        Chart::builder(640.0)
            .alignment(XAlignment::Day)
            .series([a, b])
            .legend(Legend::None)
            .unit("ms")
            .aria_label("test chart")
            .plot_height(220.0)
            .build()
            .unwrap()
    }

    #[test]
    fn the_builder_renders_one_complete_svg_and_shared_tooltips() {
        let rendered = built_chart().render();
        assert_eq!(
            rendered.tooltips.matches("chart-tooltip-template").count(),
            3
        );
        assert_eq!(rendered.tooltips.matches("chart-tooltip-row").count(), 6);
        assert!(rendered.svg.contains(r#"data-series-id="a""#));
        assert!(rendered.svg.contains(r#"data-series-id="b""#));
        assert!(rendered.svg.contains("chart-crosshair"));
        assert!(rendered.svg.contains("chart-unit"));
        assert!(rendered.svg.contains("test chart"));
        assert_eq!(rendered.view.series.len(), 2);
        assert_eq!(
            rendered
                .tooltips
                .matches(r#"chart-tooltip-unit">ms"#)
                .count(),
            6
        );
    }

    #[test]
    fn hiding_by_stable_id_drops_the_line_and_its_tooltip_row() {
        let mut chart = built_chart();
        chart.hide(BTreeSet::from([SeriesId::from("a")]));
        let rendered = chart.render();
        assert!(!rendered.svg.contains(r#"data-series-id="a""#));
        assert!(rendered.svg.contains(r#"data-series-id="b""#));
        assert_eq!(rendered.tooltips.matches("chart-tooltip-row").count(), 3);
        assert!(!rendered.tooltips.contains(">Alpha<"));
        assert!(rendered.tooltips.contains(">Beta<"));
    }

    #[test]
    fn a_spec_builds_the_same_chart_as_the_builder() {
        let start = Timestamp::from_second(0).unwrap();
        let spec = ChartSpec {
            width: 640.0,
            alignment: XAlignment::Day,
            domain: None,
            series: vec![Series::new("a", "Alpha").point(start, 1.0)],
            y_axis: YAxis::auto(),
            max_step: None,
            hidden: BTreeSet::new(),
            legend: Legend::None,
            unit: "ms".to_owned(),
            aria_label: "spec".to_owned(),
            plot_height: Some(220.0),
        };
        let chart = Chart::from_spec(spec).unwrap();
        assert_eq!(chart.view().series.len(), 1);
        assert!(chart.render().svg.contains("spec"));
    }

    #[test]
    fn all_hidden_series_keep_the_frame_and_say_so() {
        let mut chart = built_chart();
        chart.hide(BTreeSet::from([SeriesId::from("a"), SeriesId::from("b")]));
        let rendered = chart.render();
        assert!(rendered.svg.contains("All series hidden"));
        assert!(!rendered.svg.contains(r#"data-series-id="a""#));
        assert!(!rendered.svg.contains(r#"data-series-id="b""#));
        assert!(rendered.tooltips.is_empty());
    }

    #[test]
    fn legend_modes_separate_entries_from_inline_space() {
        let start = Timestamp::from_second(0).unwrap();
        let series = Series::new("a", "Alpha")
            .point(start, 1.0)
            .point(Timestamp::from_second(60).unwrap(), 2.0);
        let build = |legend| {
            Chart::builder(640.0)
                .alignment(XAlignment::Day)
                .series([series.clone()])
                .legend(legend)
                .build()
                .unwrap()
        };
        let inline = build(Legend::Inline);
        let external = build(Legend::External);
        let none = build(Legend::None);
        assert_eq!(inline.view().legend.len(), 1);
        assert_eq!(external.view().legend.len(), 1);
        assert!(none.view().legend.is_empty());
        assert!(inline.render().svg.contains("chart-legend"));
        assert!(!external.render().svg.contains("chart-legend"));
        assert!(!none.render().svg.contains("chart-legend"));
        assert!(none.view().height < inline.view().height);
    }

    /// Points further apart than the maximum step are not joined: two runs,
    /// and a point alone between gaps is a dot. Without a step, one line.
    #[test]
    fn a_gap_wider_than_the_step_breaks_the_line() {
        let points = [
            ("s", 0, 1.0),
            ("s", 60, 2.0),
            ("s", 600, 3.0),
            ("s", 1_200, 4.0),
            ("s", 1_260, 5.0),
        ];
        let chart = day_chart(&points).unwrap();
        let joined = chart.view();
        assert_eq!(
            (joined.series[0].runs.len(), joined.series[0].dots.len()),
            (1, 0)
        );
        let broken = chart.with_max_step(SignedDuration::from_secs(90)).view();
        assert_eq!(broken.series[0].runs.len(), 2, "0–60 and 1200–1260");
        assert_eq!(broken.series[0].dots.len(), 1, "600 alone");
        assert_eq!(broken.series[0].runs[0].split(' ').count(), 2);
    }

    fn day_chart(points: &[(&str, i64, f32)]) -> Option<Chart> {
        Chart::from_points(
            points.iter().map(|(name, at, value)| {
                (
                    (*name).to_owned(),
                    Timestamp::from_second(*at).unwrap(),
                    *value,
                )
            }),
            XAlignment::Day,
        )
    }

    #[test]
    fn groups_by_name_and_sorts_points() {
        let chart = day_chart(&[("b", 20, 2.0), ("a", 30, 1.0), ("a", 10, 3.0)]).unwrap();

        let names: Vec<&str> = chart.series.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(
            chart.series[0].points[0].at,
            Timestamp::from_second(10).unwrap()
        );
        assert_eq!(
            chart.series[0].points[1].at,
            Timestamp::from_second(30).unwrap()
        );
    }

    #[test]
    fn empty_points_have_no_chart() {
        assert!(day_chart(&[]).is_none());
    }

    #[test]
    fn bounds_use_headroom_and_soft_zero() {
        let chart = day_chart(&[("t", 0, 20.0)]).unwrap();
        let (min, max) = chart.y_range();
        assert_eq!(min, 0.0);
        assert!((max - 22.0).abs() < f64::EPSILON);
    }

    #[test]
    fn bounds_keep_floor_for_zero_and_extend_for_negative() {
        let (_, max) = day_chart(&[("t", 0, 0.0)]).unwrap().y_range();
        assert_eq!(max, 1.0);
        let (min, _) = day_chart(&[("t", 0, -5.0)]).unwrap().y_range();
        assert!(min <= -5.0);
    }

    #[test]
    fn soft_bounds_extend_but_never_clip() {
        let chart = day_chart(&[("t", 0, 100.0)]).unwrap().with_bounds(Bounds {
            soft_max: Some(50.0),
            soft_min: Some(-10.0),
            ..Bounds::default()
        });
        let (min, max) = chart.y_range();
        assert_eq!(min, -10.0);
        assert!(max >= 100.0, "soft max must not clip the data: {max}");
    }

    #[test]
    fn hard_bounds_fix_the_range() {
        let chart = day_chart(&[("t", 0, 100.0)]).unwrap().with_bounds(Bounds {
            hard_min: Some(0.0),
            hard_max: Some(10.0),
            ..Bounds::default()
        });
        assert_eq!(chart.y_range(), (0.0, 10.0));
    }

    #[test]
    fn a_single_timestamp_gets_a_padded_day_aligned_span() {
        let chart = day_chart(&[("t", 100, 1.0)]).unwrap();
        assert_eq!(chart.x_min, Timestamp::UNIX_EPOCH);
        assert_eq!(chart.x_max, Timestamp::from_second(86_400).unwrap());
    }

    #[test]
    fn day_alignment_snaps_the_domain_to_midnight() {
        let chart = day_chart(&[("t", 3_700, 1.0), ("t", 90_000, 2.0)]).unwrap();
        assert_eq!(chart.x_min, Timestamp::UNIX_EPOCH);
        assert_eq!(chart.x_max, Timestamp::from_second(172_800).unwrap());
    }

    #[test]
    fn hour_alignment_snaps_the_domain_to_the_hour() {
        let chart = Chart::from_points(
            [("t".to_owned(), Timestamp::from_second(3_700).unwrap(), 1.0)],
            XAlignment::Hour,
        )
        .unwrap();
        assert_eq!(chart.x_min, Timestamp::from_second(3_600).unwrap());
        assert_eq!(chart.x_max, Timestamp::from_second(7_200).unwrap());
    }

    #[test]
    fn an_explicit_domain_sets_the_x_range() {
        let domain = (
            Timestamp::from_second(50).unwrap(),
            Timestamp::from_second(150).unwrap(),
        );
        let chart = Chart::from_series(
            [(
                "a".to_owned(),
                "a".to_owned(),
                Timestamp::from_second(100).unwrap(),
                1.0,
            )],
            XAlignment::Hour,
            Some(domain),
            600.0,
        )
        .unwrap();
        assert_eq!(chart.x_min, domain.0);
        assert_eq!(chart.x_max, domain.1);
    }

    #[test]
    fn a_useless_domain_falls_back_to_the_data() {
        let chart = Chart::from_series(
            [(
                "a".to_owned(),
                "a".to_owned(),
                Timestamp::from_second(100).unwrap(),
                1.0,
            )],
            XAlignment::Hour,
            Some((
                Timestamp::from_second(200).unwrap(),
                Timestamp::from_second(200).unwrap(),
            )),
            600.0,
        )
        .unwrap();
        assert!(chart.x_min < chart.x_max);
    }

    #[test]
    fn the_same_label_from_two_keys_stays_two_series() {
        let chart = Chart::from_series(
            [
                (
                    "0\u{1}GET".to_owned(),
                    "GET".to_owned(),
                    Timestamp::from_second(0).unwrap(),
                    1.0,
                ),
                (
                    "1\u{1}GET".to_owned(),
                    "GET".to_owned(),
                    Timestamp::from_second(0).unwrap(),
                    2.0,
                ),
            ],
            XAlignment::Day,
            None,
            600.0,
        )
        .unwrap();
        assert_eq!(chart.series.len(), 2);
        let names: Vec<&str> = chart.series.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["GET", "GET"]);
        let view = chart.view();
        assert_eq!(view.legend.len(), 2);
        assert_ne!(view.legend[0].color, view.legend[1].color);
    }

    /// All zero with a soft minimum of 0: the axis is 0 to 1, not -1 to 1.
    #[test]
    fn a_flat_zero_line_does_not_reach_below_a_soft_minimum() {
        let view = day_chart(&[("a", 0, 0.0), ("a", 600, 0.0)]).unwrap().view();
        let lowest = view
            .y_ticks
            .iter()
            .map(|t| t.label.clone())
            .collect::<Vec<_>>();
        assert!(lowest.iter().all(|l| !l.starts_with('-')), "{lowest:?}");
    }

    /// A day on a phone (360 px): only as many labels as fit side by side,
    /// none overlapping; a wide chart keeps its five.
    #[test]
    fn x_labels_fit_the_width() {
        let day = |width: f64| {
            let (start, end) = (
                Timestamp::from_second(0).unwrap(),
                Timestamp::from_second(86_400).unwrap(),
            );
            Chart::from_series(
                [
                    ("a".to_owned(), "a".to_owned(), start, 1.0_f32),
                    ("a".to_owned(), "a".to_owned(), end, 2.0),
                ],
                XAlignment::Hour,
                Some((start, end)),
                width,
            )
            .unwrap()
            .view()
        };
        let phone = day(360.0);
        assert!(
            phone.x_ticks.len() >= 2 && phone.x_ticks.len() < 5,
            "{}",
            phone.x_ticks.len()
        );
        for pair in phone.x_ticks.windows(2) {
            let room = pair[1].pos - pair[0].pos;
            let needs = 7.0 * f64::from(u32::try_from(pair[0].label.chars().count()).unwrap());
            assert!(
                room > needs,
                "{:?} and {:?}: {room} px for {needs}",
                pair[0].label,
                pair[1].label
            );
        }
        assert_eq!(day(1200.0).x_ticks.len(), 5);
    }

    #[test]
    fn x_ticks_start_at_the_plot_edge() {
        let view = day_chart(&[("#1", 1_000, 1.0), ("#2", 200_000, 2.0)])
            .unwrap()
            .view();
        assert_eq!(view.x_ticks[0].pos, view.plot.left);
        for tick in &view.x_ticks {
            assert!(
                tick.pos >= view.plot.left && tick.pos <= view.plot.right,
                "x label {:?} outside the plot at {}",
                tick.label,
                tick.pos
            );
        }
    }

    #[test]
    fn view_flags_lines_and_single_points() {
        let view = day_chart(&[("a", 0, 1.0), ("a", 10, 2.0), ("b", 5, 3.0)])
            .unwrap()
            .view();
        assert_eq!(view.series.len(), 2);
        assert!(view.series[0].is_line);
        assert!(!view.series[1].is_line);
        assert_eq!(view.series[0].points.len(), 2);
        assert_eq!(view.series[1].points.len(), 1);
        assert_eq!(view.series[0].latest, "2");
        assert_eq!(view.series[0].latest_at, "1970-01-01 00:00");
    }

    #[test]
    fn an_empty_frame_keeps_the_plot_dimensions() {
        let start = Timestamp::from_second(0).unwrap();
        let end = Timestamp::from_second(3600).unwrap();
        let empty = Chart::empty(start, end, 644.0).view();
        let filled = day_chart(&[("a", 0, 1.0)]).unwrap().view();
        assert!(empty.series.is_empty());
        assert_eq!(empty.height, filled.height);
        assert_eq!(
            empty.plot.bottom - empty.plot.top,
            filled.plot.bottom - filled.plot.top
        );
    }

    #[test]
    fn hiding_a_series_rescales_but_keeps_the_legend_entry() {
        let mut chart = Chart::from_points(
            [
                ("a".to_owned(), Timestamp::from_second(0).unwrap(), 100.0),
                (
                    "a".to_owned(),
                    Timestamp::from_second(3_600).unwrap(),
                    100.0,
                ),
                ("b".to_owned(), Timestamp::from_second(0).unwrap(), 1.0),
                ("b".to_owned(), Timestamp::from_second(3_600).unwrap(), 2.0),
            ],
            XAlignment::Day,
        )
        .unwrap();
        let together = chart.view();
        chart.hide(BTreeSet::from([SeriesId::from("a")]));
        let hidden = chart.view();

        assert!(hidden.series[0].hidden);
        assert!(!hidden.series[1].hidden);
        assert_eq!(hidden.legend.len(), 2, "the legend keeps the hidden entry");
        // With `a` (100) gone the range fits `b` (1..2), so its line moves.
        assert_ne!(together.series[1].points[0].y, hidden.series[1].points[0].y);
    }

    #[test]
    fn colors_are_stable_per_name() {
        let first = day_chart(&[("a", 0, 1.0), ("b", 1, 2.0)]).unwrap().view();
        let second = day_chart(&[("b", 2, 3.0), ("a", 3, 4.0)]).unwrap().view();
        assert_eq!(first.legend[0].color, second.legend[0].color);
        assert_eq!(first.legend[1].color, second.legend[1].color);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn projected_points_stay_inside_the_plot(
            points in prop::collection::vec(("[a-z]{1,4}", 0.0f32..1_000.0, 0i64..1_000_000), 1..40),
        ) {
            let chart = Chart::from_points(
                points.iter().map(|(name, value, at)| {
                    ((*name).to_owned(), Timestamp::from_second(*at).unwrap(), *value)
                }),
                XAlignment::Day,
            )
            .expect("non-empty points");
            let view = chart.view();

            for series in &view.series {
                for point in &series.points {
                    prop_assert!(point.x.is_finite() && point.y.is_finite());
                    prop_assert!(point.x >= view.plot.left && point.x <= view.plot.right);
                    prop_assert!(point.y >= view.plot.top && point.y <= view.plot.bottom);
                }
            }
            for tick in &view.x_ticks {
                prop_assert!(tick.pos >= view.plot.left && tick.pos <= view.plot.right);
            }
            for tick in &view.y_ticks {
                prop_assert!(tick.pos >= view.plot.top && tick.pos <= view.plot.bottom);
            }
        }
    }
}
