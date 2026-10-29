//! Serialize a [`ChartView`] into the crate's default SVG.
//!
//! The view is already laid out; this module owns only the markup and the
//! stable class/data contract. Consumers that need different markup use the
//! public `ChartView` escape hatch instead.

use super::{Chart, ChartMeta, ChartView, Legend, SeriesMeta};
use std::fmt::Write as _;

/// The rendered product: complete SVG plus the view it came from.
pub struct RenderedChart {
    /// Complete, escaped SVG fragment.
    pub svg: String,
    /// Hidden server-rendered tooltip templates, one per rendered x sample.
    pub tooltips: String,
    /// Stable series/tick metadata for app controls.
    pub meta: ChartMeta,
    /// The layout data, for consumers that need custom markup.
    pub view: ChartView,
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
pub(crate) fn render(chart: &Chart, view: ChartView) -> RenderedChart {
    let mut svg = String::with_capacity(8 * 1024);
    let _ = write!(
        svg,
        r#"<svg viewBox="0 0 {width:.2} {height:.2}" class="chart" role="img" aria-label="{label}">"#,
        width = view.width,
        height = view.height,
        label = escape(&chart.aria_label),
    );
    let _ = write!(svg, "<title>{}</title>", escape(&chart.aria_label));
    svg.push_str(r#"<g class="chart-grid">"#);
    for tick in &view.y_ticks {
        let _ = write!(
            svg,
            r#"<line x1="{left:.2}" y1="{y:.2}" x2="{right:.2}" y2="{y:.2}"/>"#,
            left = view.plot.left,
            right = view.plot.right,
            y = tick.pos,
        );
    }
    svg.push_str("</g><g class=\"chart-axis\">");
    let _ = write!(
        svg,
        r#"<line x1="{left:.2}" y1="{top:.2}" x2="{left:.2}" y2="{bottom:.2}"/>"#,
        left = view.plot.left,
        top = view.plot.top,
        bottom = view.plot.bottom,
    );
    let _ = write!(
        svg,
        r#"<line x1="{left:.2}" y1="{bottom:.2}" x2="{right:.2}" y2="{bottom:.2}"/>"#,
        left = view.plot.left,
        right = view.plot.right,
        bottom = view.plot.bottom,
    );
    svg.push_str("</g><g class=\"chart-labels\">");
    for tick in &view.y_ticks {
        let _ = write!(
            svg,
            r#"<text x="{x:.2}" y="{y:.2}" text-anchor="end" dominant-baseline="middle">{label}</text>"#,
            x = view.plot.y_label_x,
            y = tick.pos,
            label = escape(&tick.label),
        );
    }
    for tick in &view.x_ticks {
        let _ = write!(
            svg,
            r#"<text x="{x:.2}" y="{y:.2}" text-anchor="middle">{label}</text>"#,
            x = tick.pos,
            y = view.plot.x_label_y,
            label = escape(&tick.label),
        );
    }
    let _ = write!(
        svg,
        r#"<text class="chart-unit" x="{x:.2}" y="{y:.2}" text-anchor="middle" transform="rotate(-90 {x:.2} {y:.2})">{unit}</text>"#,
        x = view.plot.unit_x,
        y = view.plot.unit_y,
        unit = escape(&chart.unit),
    );
    svg.push_str("</g>");
    let all_hidden = !view.series.is_empty() && view.series.iter().all(|series| series.hidden);
    if view.series.is_empty() || all_hidden {
        let cx = f64::midpoint(view.plot.left, view.plot.right);
        let cy = f64::midpoint(view.plot.top, view.plot.bottom);
        let note = if all_hidden {
            "All series hidden"
        } else {
            "No data"
        };
        let _ = write!(
            svg,
            r#"<text class="chart-no-data" x="{cx:.2}" y="{cy:.2}" text-anchor="middle" dominant-baseline="middle">{note}</text>"#,
        );
    }
    svg.push_str(r#"<g class="chart-series">"#);
    for series in &view.series {
        if series.hidden {
            continue;
        }
        let _ = write!(
            svg,
            r#"<g class="chart-line" data-series-id="{}">"#,
            escape(&series.id.to_string()),
        );
        for run in &series.runs {
            let _ = write!(
                svg,
                r#"<polyline points="{}" fill="none" stroke="{}" stroke-linejoin="round" stroke-linecap="round"/>"#,
                escape(run),
                series.color,
            );
        }
        for (x, y) in &series.dots {
            let _ = write!(
                svg,
                r#"<circle cx="{x:.2}" cy="{y:.2}" r="2.5" fill="{}"/>"#,
                series.color,
            );
        }
        svg.push_str("</g>");
    }
    svg.push_str("</g>");
    let _ = write!(
        svg,
        r#"<line class="chart-crosshair" x1="{:.2}" y1="{:.2}" x2="{:.2}" y2="{:.2}"/>"#,
        view.plot.left, view.plot.top, view.plot.left, view.plot.bottom,
    );
    if chart.legend_mode == Legend::Inline {
        svg.push_str(r#"<g class="chart-legend">"#);
        for item in &view.legend {
            let _ = write!(
                svg,
                r#"<g class="chart-legend-item" data-series-id="{}"><rect x="{:.2}" y="{:.2}" width="9" height="9" rx="3" fill="{}"/><text x="{:.2}" y="{:.2}">{}</text></g>"#,
                escape(&item.id.to_string()),
                item.swatch_x,
                item.swatch_y,
                item.color,
                item.text_x,
                item.text_y,
                escape(&item.name),
            );
        }
        svg.push_str("</g>");
    }
    svg.push_str("</svg>");
    let tooltips = render_tooltips(&view, &chart.unit);
    let meta = ChartMeta {
        series: view
            .series
            .iter()
            .map(|series| SeriesMeta {
                id: series.id.clone(),
                name: series.name.clone(),
                color: series.color,
                hidden: series.hidden,
            })
            .collect(),
        x_ticks: view.x_ticks.clone(),
        y_ticks: view.y_ticks.clone(),
        unit: chart.unit.clone(),
    };
    RenderedChart {
        svg,
        tooltips,
        meta,
        view,
    }
}

#[allow(
    clippy::float_arithmetic,
    clippy::cast_possible_truncation,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    reason = "chart geometry is real-valued; anchor keys are tiny"
)]
fn render_tooltips(view: &ChartView, unit: &str) -> String {
    use std::collections::BTreeMap;

    // Tooltip anchors are the union of rendered x positions. At each anchor,
    // every visible series contributes its nearest point, so a shared
    // crosshair always lists every line even when scrape timestamps differ.
    let mut anchors: BTreeMap<i64, f64> = BTreeMap::new();
    for series in &view.series {
        if series.hidden {
            continue;
        }
        for point in &series.points {
            let key = (point.x * 1000.0).round() as i64;
            anchors.entry(key).or_insert(point.x);
        }
    }
    let anchors: Vec<(i64, f64)> = anchors.into_iter().collect();
    let stride = anchors.len().div_ceil(512).max(1);
    let mut out = String::with_capacity(anchors.len() * 256);
    for (index, (_, x)) in anchors.into_iter().enumerate() {
        if index % stride != 0 {
            continue;
        }
        let mut rows: Vec<(String, &'static str, String, f64, String)> = Vec::new();
        for series in &view.series {
            if series.hidden {
                continue;
            }
            let nearest = series
                .points
                .iter()
                .min_by(|a, b| (a.x - x).abs().total_cmp(&(b.x - x).abs()));
            if let Some(point) = nearest {
                rows.push((
                    series.name.clone(),
                    series.color,
                    point.value.clone(),
                    point.y,
                    point.at.clone(),
                ));
            }
        }
        let Some((_, _, _, y, at)) = rows.first() else {
            continue;
        };
        let (y, at) = (*y, at.clone());
        let total = rows.len();
        rows.truncate(10);
        let _ = write!(
            out,
            r#"<template class="chart-tooltip-template" data-x="{x:.2}" data-y="{y:.2}"><div class="chart-tooltip"><div class="chart-tooltip-time">{at}</div>"#,
            at = escape(&at),
        );
        for (name, color, value, _, _) in rows {
            let _ = write!(
                out,
                r#"<div class="chart-tooltip-row"><svg class="chart-tooltip-swatch" viewBox="0 0 9 9" aria-hidden="true"><rect width="9" height="9" rx="3" fill="{color}"/></svg><span class="chart-tooltip-name">{name}</span><b>{value}</b><span class="chart-tooltip-unit">{unit}</span></div>"#,
                name = escape(&name),
                value = escape(&value),
                unit = escape(unit),
            );
        }
        if total > 10 {
            let _ = write!(
                out,
                r#"<div class="chart-tooltip-more">+{} more series</div>"#,
                total.saturating_sub(10)
            );
        }
        out.push_str("</div></template>");
    }
    out
}

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}
