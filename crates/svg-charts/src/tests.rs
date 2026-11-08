//! Tests for the crate root.

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
