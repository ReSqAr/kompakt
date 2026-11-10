//! A chart as the data its renderer draws from.

use super::{
    Chart, ChartView, Coord, MARGIN_TOP, Plot, SeriesView, Tick, X_LABEL_GAP, color, fmt_value,
    project_x, project_y, x_label, x_ticks, y_ticks,
};
use super::{RenderedChart, render};

impl Chart {
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
