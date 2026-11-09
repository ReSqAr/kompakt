//! Building a chart from flat series.

use super::{Chart, Legend, Series, SeriesId, XAlignment, YAxis};
use jiff::{SignedDuration, Timestamp};
use std::collections::BTreeSet;

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
    /// An empty builder for a chart `width` units wide.
    #[must_use]
    pub(crate) fn new(width: f64) -> Self {
        Self {
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
