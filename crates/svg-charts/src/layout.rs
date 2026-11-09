//! Chart geometry: the legend layout, y range, plot box and line runs.

use super::{
    BOTTOM_PAD, Chart, LEGEND_GAP, LEGEND_ROW, Legend, MARGIN_LEFT, MARGIN_RIGHT, MARGIN_TOP,
    Point, SeriesData, X_LABEL_GAP, auto_bounds, legend_layout, project_x, project_y,
};

impl Chart {
    /// Rebuild legend positions and the total height after a layout option
    /// changes. External legends keep their entries but do not reserve inline
    /// space; `None` discards them.
    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub(crate) fn relayout_legend(&mut self) {
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
    pub(crate) fn y_range(&self) -> (f64, f64) {
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

    pub(crate) fn plot_left(&self) -> f64 {
        MARGIN_LEFT
    }

    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub(crate) fn plot_right(&self) -> f64 {
        self.width - MARGIN_RIGHT
    }

    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub(crate) fn plot_width(&self) -> f64 {
        self.plot_right() - self.plot_left()
    }

    #[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
    pub(crate) fn plot_bottom(&self) -> f64 {
        MARGIN_TOP + self.plot_height
    }

    /// A series' line as runs of joined points and lone dots, broken where
    /// two points are further apart than the maximum step.
    pub(crate) fn runs(
        &self,
        series: &SeriesData,
        y_min: f64,
        y_max: f64,
    ) -> (Vec<String>, Vec<(f64, f64)>) {
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
}
