//! Scales and ticks: projection, nice steps, labels, and the legend flow.

use super::{
    Chart, LEGEND_CHAR, LEGEND_ITEM_GAP, LEGEND_ROW, LegendItem, MARGIN_LEFT, MARGIN_RIGHT,
    MARGIN_TOP, MAX_TICKS, PALETTE, SWATCH, SWATCH_GAP, SeriesData, TARGET_TICKS, X_PAD_SECONDS,
    X_STEPS, XAlignment,
};
use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, Unit};

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
pub(crate) fn legend_layout(
    series: &[SeriesData],
    width: f64,
    legend_top: f64,
) -> (Vec<LegendItem>, f64) {
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
pub(crate) fn f64_of(value: usize) -> f64 {
    value as f64
}

pub(crate) fn pad_span(x_min: Timestamp, x_max: Timestamp) -> (Timestamp, Timestamp) {
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
pub(crate) fn auto_bounds(data_min: f64, data_max: f64) -> (f64, f64) {
    if data_min == data_max {
        let pad = (data_max.abs() * 0.1).max(1.0);
        (data_min - pad, data_max + pad)
    } else {
        (data_min, data_max + (data_max - data_min) * 0.1)
    }
}

pub(crate) fn color(index: usize) -> &'static str {
    let palette_index = index.checked_rem(PALETTE.len()).unwrap_or_default();
    PALETTE.get(palette_index).copied().unwrap_or(PALETTE[0])
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
pub(crate) fn project_x(at: Timestamp, chart: &Chart) -> f64 {
    let span = chart.x_max.duration_since(chart.x_min).as_secs_f64();
    let offset = at.duration_since(chart.x_min).as_secs_f64();
    chart.plot_left() + (chart.plot_right() - chart.plot_left()) * (offset / span)
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
pub(crate) fn project_y(value: f64, chart: &Chart, min: f64, max: f64) -> f64 {
    chart.plot_bottom() - (chart.plot_bottom() - MARGIN_TOP) * ((value - min) / (max - min))
}

#[allow(clippy::float_arithmetic, reason = "chart geometry is real-valued")]
pub(crate) fn nice_step(rough: f64) -> f64 {
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
pub(crate) fn y_ticks(min: f64, max: f64) -> Vec<f64> {
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
pub(crate) fn x_step(span_secs: i64, ticks: i32) -> i64 {
    X_STEPS
        .iter()
        .copied()
        .find(|step| span_secs <= step.saturating_mul(i64::from(ticks)))
        .unwrap_or(86_400)
}

/// Snaps the domain outward to the alignment period, so charts start and end
/// on period boundaries (e.g. UTC midnight) and no tick falls outside the plot.
pub(crate) fn align_span(
    x_min: Timestamp,
    x_max: Timestamp,
    alignment: XAlignment,
) -> (Timestamp, Timestamp) {
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

pub(crate) fn x_ticks(chart: &Chart) -> Vec<Timestamp> {
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

pub(crate) fn x_label(tick: Timestamp, span_secs: i64) -> String {
    let format = if span_secs <= 21_600 {
        "%H:%M"
    } else if span_secs <= 259_200 {
        "%m-%d %H:%M"
    } else {
        "%Y-%m-%d"
    };
    tick.strftime(format).to_string()
}

pub(crate) fn fmt_value(value: f64) -> String {
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

/// How many x labels fit side by side: the plot's width over a label's (its
/// characters at about 7 px, and a gap), at least 2, at most the target.
#[allow(
    clippy::float_arithmetic,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "a count of labels from a width in pixels"
)]
pub(crate) fn x_labels_that_fit(plot_width: f64, span_secs: i64) -> i32 {
    let chars = x_label(Timestamp::UNIX_EPOCH, span_secs).chars().count();
    let label = 7.0 * chars as f64 + 18.0;
    ((plot_width / label).floor() as i32).clamp(2, TARGET_TICKS)
}
