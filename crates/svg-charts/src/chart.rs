//! Chart constructors and the layout options set after construction.

use super::{
    BOTTOM_PAD, Bounds, Chart, ChartBuilder, ChartSpec, DEFAULT_WIDTH, LEGEND_GAP, LEGEND_ROW,
    Legend, MARGIN_LEFT, MARGIN_RIGHT, MARGIN_TOP, PLOT_HEIGHT, Point, SeriesData, SeriesId,
    X_LABEL_GAP, XAlignment, align_span, legend_layout, pad_span,
};
use jiff::{SignedDuration, Timestamp};
use std::collections::{BTreeMap, BTreeSet};

impl Chart {
    #[must_use]
    pub fn from_spec(spec: ChartSpec) -> Option<Self> {
        spec.build()
    }

    #[must_use]
    pub fn builder(width: f64) -> ChartBuilder {
        ChartBuilder::new(width)
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
}
