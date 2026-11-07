//! The mechanism of a metric registry: what one metric *is* (`MetricDef`), the
//! row macros that declare a group of metrics and generate their emission
//! functions, label-vocabulary enums, an RAII timing `Probe`, and the lossy
//! float conversions gauges need. It knows no domain: the vocabulary (names,
//! labels, help text) lives in the crate that emits it.
//!
//! ```text
//!   metrics!("ns_", group; { gauge fn CONST "name" Unit "help" (arg: u64); ... })
//!       -> pub(crate) const CONST = "ns_name"
//!       -> pub(crate) fn fn(arg)          emits through the `metrics` facade
//!       -> group!()                       a [MetricDef; N] for the crate's registry
//! ```
//!
//! Conventions every row relies on:
//! - the namespace is applied by the macro, so a row never writes it; `""`
//!   opts out for names that are a shared convention (Prometheus `process_*`);
//! - label keys are constants and label values come from `label_enum!` enums,
//!   so every `{label="value"}` combination is enumerable at compile time;
//! - HLC-like 64-bit values reach a gauge through a conversion row
//!   (`gauge_via`) that reduces them to a range `f64` holds exactly.

#[doc(hidden)]
pub use metrics as metrics_crate;
pub use metrics::Unit;

/// Label key of the `state` label an outcome-labelled probe records.
pub const LABEL_STATE: &str = "state";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// One canonical metric: everything a recorder or a dashboard needs to know.
#[derive(Debug, Clone, Copy)]
pub struct MetricDef {
    pub name: &'static str,
    pub kind: MetricKind,
    pub unit: Option<Unit>,
    pub help: &'static str,
}

/// `Seconds` → `Some(Unit::Seconds)`, `None` → `None`. Unit slot for a row.
#[doc(hidden)]
#[macro_export]
macro_rules! unit {
    (None) => {
        None
    };
    ($u:ident) => {
        Some($crate::Unit::$u)
    };
}

/// Declare a subsystem group of metrics as rows (see the file's group blocks).
/// Declare a group of metrics as rows. Each row declares one metric: emission fn name, const name, metric string,
/// (kind+unit per shape), help prose, and signature. The block expands each
/// row to its `pub(crate) const` and (except `custom`) its `#[doc = help]`
/// emission fn, and defines a zero-arg macro named `<group>` that expands to a
/// `[MetricDef; N]` array for `ALL_METRICS` (assembled as `&[ &group!(), .. ]`,
/// a `&[&[MetricDef]]` — see the assembly step).
#[macro_export]
macro_rules! metrics {
    (pub $prefix:literal, $group:ident; { $($row:tt)* }) => {
        $crate::metrics_impl!(pub, $group, $prefix, [], $($row)*);
    };
    ($prefix:literal, $group:ident; { $($row:tt)* }) => {
        $crate::metrics_impl!(pub(crate), $group, $prefix, [], $($row)*);
    };
}

// Accumulated `MetricDef` fragments are captured as `tt` (not `expr`) and each
// is wrapped in parentheses so it is a single token tree; the base case emits
// the group's defs as a bracketed array literal.

#[doc(hidden)]
#[macro_export]
macro_rules! metrics_impl {
    // base case: emit the group's `[MetricDef; N]` array as a zero-arg macro
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*] $(,)?) => {
        macro_rules! $group { () => { [ $($defs),* ] } }
    };

    // gauge:   set_gauge(CONST, v)                    arg u64,  unit required
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], gauge $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: u64); $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn($arg: u64) { $crate::set_gauge($const, $arg); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Gauge, unit: $crate::unit!($unit), help: $help })], $($rest)*);
    };

    // flag:    set_flag(CONST, on)                    arg bool, no unit
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], flag $fn:ident $const:ident $name:literal $help:literal ($arg:ident: bool); $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn($arg: bool) { $crate::set_flag($const, $arg); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Gauge, unit: None, help: $help })], $($rest)*);
    };

    // counter: counter!(CONST).increment(1)           no args, no unit
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], counter $fn:ident $const:ident $name:literal $help:literal; $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn() { $crate::metrics_crate::counter!($const).increment(1); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Counter, unit: None, help: $help })], $($rest)*);
    };

    // labeled_counter: counter!(CONST, <labels>).increment(1)
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], labeled_counter $fn:ident $const:ident $name:literal $help:literal ($arg:ident: $ty:ty) [$($label:tt)*]; $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn($arg: $ty) { $crate::metrics_crate::counter!($const, $($label)*).increment(1); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Counter, unit: None, help: $help })], $($rest)*);
    };

    // gauge_via: set_gauge(CONST, conv(arg))        arg $ty, conversion path fn($ty) -> u64, unit required
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], gauge_via $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: $ty:ty => $conv:path); $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn($arg: $ty) { $crate::set_gauge($const, $conv($arg)); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Gauge, unit: $crate::unit!($unit), help: $help })], $($rest)*);
    };

    // gauge_usize: set_gauge_usize(CONST, v)          arg usize, unit required
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], gauge_usize $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: usize); $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn($arg: usize) { $crate::set_gauge_usize($const, $arg); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Gauge, unit: $crate::unit!($unit), help: $help })], $($rest)*);
    };

    // hist:    record_usize_hist(CONST, n)            arg usize, unit required
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], hist $fn:ident $const:ident $name:literal $unit:tt $help:literal ($arg:ident: usize); $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn($arg: usize) { $crate::record_usize_hist($const, $arg); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Histogram, unit: $crate::unit!($unit), help: $help })], $($rest)*);
    };

    // hist_duration: histogram!(CONST).record(d)      arg Duration, unit Seconds implicit
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], hist_duration $fn:ident $const:ident $name:literal $help:literal ($arg:ident: Duration); $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn($arg: ::std::time::Duration) { $crate::metrics_crate::histogram!($const).record($arg); }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Histogram, unit: Some($crate::Unit::Seconds), help: $help })], $($rest)*);
    };

    // probe:   Probe::start(CONST, OpState::Failure)  no args, unit Seconds implicit
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], probe $fn:ident $const:ident $name:literal $help:literal; $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn() -> $crate::Probe<$crate::OpState> { $crate::Probe::start($const, $crate::OpState::Failure) }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Histogram, unit: Some($crate::Unit::Seconds), help: $help })], $($rest)*);
    };

    // probe_unlabeled: Probe::unlabeled(CONST)        no args, unit Seconds implicit
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], probe_unlabeled $fn:ident $const:ident $name:literal $help:literal; $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        #[doc = $help]
        $vis fn $fn() -> $crate::Probe<$crate::NoOutcome> { $crate::Probe::unlabeled($const) }
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::Histogram, unit: Some($crate::Unit::Seconds), help: $help })], $($rest)*);
    };

    // custom: const + MetricDef only; the emission fn is hand-written below
    // the block and references $const. $fn is documentation-only.
    ($vis:vis, $group:ident, $prefix:literal, [$($defs:tt),*], custom $fn:ident $const:ident $name:literal $kind:ident $unit:tt $help:literal; $($rest:tt)*) => {
        $vis const $const: &str = concat!($prefix, $name);
        $crate::metrics_impl!($vis, $group, $prefix, [$($defs,)* ($crate::MetricDef { name: $const, kind: $crate::MetricKind::$kind, unit: $crate::unit!($unit), help: $help })], $($rest)*);
    };
}

/// Fieldless label-vocabulary enum with a `const fn as_str`. `$name` takes the
/// standard `Debug, Clone, Copy, PartialEq, Eq` derive; extra attributes pass
/// through before the name. Variant `///` doc comments pass through to the
/// enum variants. Enum-level docs must be given as `#[doc = "..."]` INSIDE the
/// invocation (a `///` before the invocation would warn `unused_doc_comments`
/// under `-D warnings`).
#[macro_export]
macro_rules! label_enum {
    // `pub` for a label vocabulary other crates read; the default is crate-private.
    ($(#[$meta:meta])* pub $name:ident { $($body:tt)+ }) => {
        $crate::label_enum!(@emit [pub] $(#[$meta])* $name { $($body)+ });
    };
    ($(#[$meta:meta])* $name:ident { $($body:tt)+ }) => {
        $crate::label_enum!(@emit [pub(crate)] $(#[$meta])* $name { $($body)+ });
    };
    (@emit [$vis:vis] $(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $value:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $vis enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            #[must_use]
            $vis const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $value ),+
                }
            }
        }
    };
}

/// Like [`label_enum!`] plus `impl $crate::ProbeOutcome for $name`.
#[macro_export]
macro_rules! probe_enum {
    ($(#[$meta:meta])* pub $name:ident { $($body:tt)+ }) => {
        $crate::label_enum!($(#[$meta])* pub $name { $($body)+ });
        $crate::probe_enum!(@outcome $name);
    };
    ($(#[$meta:meta])* $name:ident { $($body:tt)+ }) => {
        $crate::label_enum!($(#[$meta])* $name { $($body)+ });
        $crate::probe_enum!(@outcome $name);
    };
    (@outcome $name:ident) => {
        impl $crate::ProbeOutcome for $name {
            fn as_str(self) -> &'static str {
                $name::as_str(self)
            }
        }
    };
}

/// Outcome label vocabulary usable as a [`Probe`] `state` label.
pub trait ProbeOutcome: Copy {
    #[must_use]
    fn as_str(self) -> &'static str;
}

/// Marker type for probes that record an unlabeled histogram (no outcome label).
///
/// See [`Probe::unlabeled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoOutcome;

impl ProbeOutcome for NoOutcome {
    fn as_str(self) -> &'static str {
        unreachable!("NoOutcome is never serialised into a label value")
    }
}

/// Sentinel `label_key` value for unlabeled probes.
pub const LABEL_NONE: &str = "";

/// Times a code region and records a `*_duration_seconds` histogram labeled by
/// outcome when it drops — replacing the manual
/// `let t0 = Instant::now(); …; metrics::foo(t0.elapsed())` pattern.
///
/// The histogram's `_count{state}` doubles as the outcome-breakdown counter and
/// `_bucket` carries the latency, so one probe usually stands in for a histogram
/// *and* a counter. The `outcome` defaults to a failure-ish variant so an early
/// `return`/`?`/panic still records (as that default) rather than vanishing.
#[must_use = "a Probe records its metric on drop; bind it to a name, do not discard it"]
pub struct Probe<O: ProbeOutcome> {
    t0: tokio::time::Instant,
    hist: &'static str,
    label_key: &'static str,
    outcome: O,
    /// One extra static label pair (e.g. `(LABEL_OP, "put")` for dual-label
    /// metrics). `None` → single outcome label or unlabeled histogram.
    extra: Option<(&'static str, &'static str)>,
    armed: bool,
}

impl<O: ProbeOutcome> Probe<O> {
    pub fn new(
        hist: &'static str,
        label_key: &'static str,
        extra: Option<(&'static str, &'static str)>,
        default_outcome: O,
        armed: bool,
    ) -> Self {
        Self {
            t0: tokio::time::Instant::now(),
            hist,
            label_key,
            outcome: default_outcome,
            extra,
            armed,
        }
    }

    pub fn start(hist: &'static str, default_outcome: O) -> Self {
        Self::new(hist, LABEL_STATE, None, default_outcome, true)
    }

    /// Like `start` but with one extra static label pair baked in at
    /// construction (e.g. `(LABEL_OP, "put")`).
    pub fn start_with_extra(
        hist: &'static str,
        label_key: &'static str,
        extra: (&'static str, &'static str),
        default_outcome: O,
    ) -> Self {
        Self::new(hist, label_key, Some(extra), default_outcome, true)
    }

    /// Update the outcome that will be recorded on drop; keeps timing.
    pub fn set(&mut self, outcome: O) {
        self.outcome = outcome;
    }

    /// Set the final outcome and record immediately (consumes the probe).
    pub fn finish(mut self, outcome: O) {
        self.outcome = outcome;
        // `drop` does the emission.
    }

    /// Suppress emission entirely (e.g. clean shutdown, not a real operation).
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<O: ProbeOutcome> Probe<O> {
    /// Record the histogram now, regardless of `armed`. Shared by `Drop` and
    /// the probe impl.
    #[allow(clippy::expect_used, reason = "elapsed is infallible")]
    fn emit(&self) {
        let elapsed = self.t0.elapsed();
        if self.label_key == LABEL_NONE {
            if let Some((ek, ev)) = self.extra {
                // One static label, no outcome (Pattern 4: per-store timings).
                metrics_crate::histogram!(self.hist, ek => ev).record(elapsed);
            } else {
                // Unlabeled histogram (Pattern 3).
                metrics_crate::histogram!(self.hist).record(elapsed);
            }
        } else if let Some((ek, ev)) = self.extra {
            // Dual-label (Pattern 2): outcome state + one extra static label.
            metrics_crate::histogram!(self.hist, self.label_key => self.outcome.as_str(), ek => ev)
                .record(elapsed);
        } else {
            // Single outcome label (Pattern 1, status quo).
            metrics_crate::histogram!(self.hist, self.label_key => self.outcome.as_str())
                .record(elapsed);
        }
    }
}

/// zslg's RAII probe: a caller binds one and lets it drop, and `Drop` records
/// the histogram.
impl<O: ProbeOutcome> Drop for Probe<O> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.emit();
    }
}

// ── NoOutcome-specific constructors ─────────────────────────────────────

impl Probe<NoOutcome> {
    /// Unlabeled histogram: records just `histogram!(hist).record(elapsed)`.
    pub fn unlabeled(hist: &'static str) -> Self {
        Self::new(hist, LABEL_NONE, None, NoOutcome, true)
    }

    /// Record the histogram immediately and disarm (consumes the probe).
    /// Useful when the timing boundary doesn't align with scope exit.
    pub fn record(mut self) {
        self.armed = false;
        metrics_crate::histogram!(self.hist).record(self.t0.elapsed());
    }
}

probe_enum!(#[doc = "Generic success/failure outcome (`state` label)."] pub OpState {
    Success => "success",
    Failure => "failure",
});

impl OpState {
    /// Convenience: derive the label value from any `Result`.
    #[must_use]
    pub const fn from_ok(ok: bool) -> Self {
        if ok { Self::Success } else { Self::Failure }
    }
}

/// Every registered histogram, as `(name, declared unit)`.
///
/// The unit is what bucket ladders are derived from, so a `_bytes` histogram
/// never lands on a latency scale.
pub fn histograms<'a>(
    groups: &'a [&'a [MetricDef]],
) -> impl Iterator<Item = (&'static str, Option<Unit>)> + 'a {
    groups
        .iter()
        .copied()
        .flatten()
        .filter(|m| m.kind == MetricKind::Histogram)
        .map(|m| (m.name, m.unit))
}

/// Register help text and units for every metric with the installed recorder.
pub fn describe_all(groups: &[&[MetricDef]]) {
    for def in groups.iter().copied().flatten() {
        match (def.kind, def.unit) {
            (MetricKind::Counter, Some(unit)) => {
                metrics_crate::describe_counter!(def.name, unit, def.help);
            }
            (MetricKind::Counter, None) => metrics_crate::describe_counter!(def.name, def.help),
            (MetricKind::Gauge, Some(unit)) => {
                metrics_crate::describe_gauge!(def.name, unit, def.help);
            }
            (MetricKind::Gauge, None) => metrics_crate::describe_gauge!(def.name, def.help),
            (MetricKind::Histogram, Some(unit)) => {
                metrics_crate::describe_histogram!(def.name, unit, def.help);
            }
            (MetricKind::Histogram, None) => metrics_crate::describe_histogram!(def.name, def.help),
        }
    }
}

/// The crate's single sanctioned lossy integer→float conversion, for gauge
/// readings only. Values above 2^53 lose precision; every gauge emitted
/// through this is bounded well below that (counts, byte sizes, indices,
/// HLC *physical-ms parts* — never `Hlc::raw()`).
#[must_use]
#[allow(
    clippy::as_conversions,
    reason = "audited lossy f64 conversion for bounded gauge values; see doc comment"
)]
pub fn lossy_f64(value: u64) -> f64 {
    value as f64
}

/// Signed variant of [`lossy_f64`] (e.g. clock drift).
#[must_use]
#[allow(
    clippy::as_conversions,
    reason = "audited lossy f64 conversion; see lossy_f64"
)]
pub fn lossy_f64_signed(value: i64) -> f64 {
    value as f64
}

/// Set an unlabeled gauge from a `u64` reading.
pub fn set_gauge(name: &'static str, value: u64) {
    metrics_crate::gauge!(name).set(lossy_f64(value));
}

/// Set an unlabeled gauge from a `usize` reading (counts, queue depths).
pub fn set_gauge_usize(name: &'static str, value: usize) {
    set_gauge(
        name,
        u64::try_from(value).expect("usize gauge value fits u64"),
    );
}

/// Record an unlabeled histogram from a `usize` reading (byte sizes, batch counts).
pub fn record_usize_hist(name: &'static str, value: usize) {
    metrics_crate::histogram!(name).record(lossy_f64(
        u64::try_from(value).expect("usize histogram value fits u64"),
    ));
}

/// Set an unlabeled 0/1 gauge.
pub fn set_flag(name: &'static str, on: bool) {
    metrics_crate::gauge!(name).set(if on { 1.0 } else { 0.0 });
}

/// Wall-clock seconds since the Unix epoch, for "last run at" timestamp gauges
/// (Grafana renders "time since" as `time() - metric`). Clock-skew safe.
#[must_use]
pub fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests {
    use std::time::Duration;

    use super::*;

    fn hlc_ms(h: u64) -> u64 {
        h >> 16
    }

    crate::metrics!("ns_", synthetic; {
        gauge   synth_gauge   SYNTH_GAUGE   "synth_gauge"  Seconds  "A synthetic gauge." (v: u64);
        flag    synth_flag    SYNTH_FLAG    "synth_flag"   "A synthetic flag." (on: bool);
        counter synth_counter SYNTH_COUNTER "synth_counter" "A synthetic counter.";
        labeled_counter synth_led SYNTH_LED "synth_led" "A synthetic labeled counter."
            (s: OpState) [LABEL_STATE => s.as_str()];
        gauge_via synth_via   SYNTH_VIA     "synth_via"    Milliseconds  "A synthetic converted gauge." (h: u64 => hlc_ms);
        gauge_usize synth_gu  SYNTH_GU      "synth_gu"     Count  "A synthetic usize gauge." (n: usize);
        hist    synth_hist    SYNTH_HIST    "synth_hist"   Bytes  "A synthetic histogram." (n: usize);
        hist_duration synth_hd SYNTH_HD     "synth_hd"     "A synthetic duration histogram." (d: Duration);
        probe   synth_probe   SYNTH_PROBE   "synth_probe"  "A synthetic probe.";
        probe_unlabeled synth_up SYNTH_UP   "synth_up"     "A synthetic unlabeled probe.";
        custom  synth_custom  SYNTH_CUSTOM  "synth_custom" Gauge None "A synthetic custom.";
    });

    crate::metrics!("", synthetic_raw; {
        gauge   synth_raw_gauge SYNTH_RAW_GAUGE "synth_raw_gauge" Count
            "A synthetic gauge in an unprefixed group." (v: u64);
    });

    fn emitted(f: impl FnOnce()) -> Vec<String> {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, f);
        snapshotter
            .snapshot()
            .into_hashmap()
            .keys()
            .map(|k| k.key().name().to_string())
            .collect()
    }

    /// The empty namespace is what keeps `process_*` on the Prometheus
    /// standard names; without a test the exemption is one typo from silently
    /// namespacing them.
    #[test]
    fn an_empty_namespace_keeps_the_bare_name() {
        assert_eq!(SYNTH_RAW_GAUGE, "synth_raw_gauge");
        assert!(emitted(|| synth_raw_gauge(1)).contains(&"synth_raw_gauge".to_string()));
        assert_eq!(synthetic_raw!()[0].name, "synth_raw_gauge");
    }

    #[test]
    fn a_namespaced_group_defines_consts_and_emits() {
        // The namespace is applied by the macro, so the const and the emitted
        // name both carry it and a row never writes it.
        assert_eq!(SYNTH_GAUGE, "ns_synth_gauge");
        assert_eq!(SYNTH_CUSTOM, "ns_synth_custom");

        let names = emitted(|| {
            synth_gauge(7);
            synth_flag(true);
            synth_counter();
            synth_led(OpState::Success);
            synth_via(1 << 20);
            synth_gu(3);
            synth_hist(4);
            synth_hd(Duration::from_millis(1));
            synth_probe().finish(OpState::Success);
            let _probe = synth_up();
        });
        for name in [
            "ns_synth_gauge",
            "ns_synth_flag",
            "ns_synth_counter",
            "ns_synth_led",
            "ns_synth_via",
            "ns_synth_gu",
            "ns_synth_hist",
            "ns_synth_hd",
            "ns_synth_probe",
            "ns_synth_up",
        ] {
            assert!(
                names.iter().any(|n| n == name),
                "missing emitted metric {name}"
            );
        }
    }

    #[test]
    fn a_group_assembles_its_metric_defs() {
        let defs = synthetic!();
        assert_eq!(defs.len(), 11);
        assert_eq!(defs[0].name, SYNTH_GAUGE);
        assert_eq!(defs[0].kind, MetricKind::Gauge);
        assert_eq!(defs[0].unit, Some(Unit::Seconds));
        assert_eq!(defs[2].unit, None); // counter
        assert_eq!(defs[4].unit, Some(Unit::Milliseconds)); // gauge_via takes a unit token
        assert_eq!(defs[5].unit, Some(Unit::Count)); // gauge_usize
        assert_eq!(defs[7].unit, Some(Unit::Seconds)); // hist_duration implicit
        assert_eq!(defs[8].unit, Some(Unit::Seconds)); // probe implicit
        assert_eq!(defs[10].kind, MetricKind::Gauge);
        assert_eq!(defs[10].unit, None); // custom None
    }

    #[test]
    fn a_converted_gauge_reports_the_converted_value() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || synth_via(5 << 16));
        let value = snapshotter
            .snapshot()
            .into_hashmap()
            .into_iter()
            .find(|(k, _)| k.key().name() == SYNTH_VIA)
            .map(|(_, (_, _, v))| v);
        assert!(matches!(
            value,
            Some(metrics_util::debugging::DebugValue::Gauge(g)) if g.into_inner() == 5.0
        ));
    }

    #[test]
    fn a_probe_records_the_chosen_outcome_and_a_default_on_drop() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            synth_probe().finish(OpState::Success);
            let _dropped = synth_probe();
        });
        let states: Vec<String> = snapshotter
            .snapshot()
            .into_hashmap()
            .keys()
            .filter(|k| k.key().name() == SYNTH_PROBE)
            .flat_map(|k| {
                k.key()
                    .labels()
                    .filter(|l| l.key() == LABEL_STATE)
                    .map(|l| l.value().to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(states.contains(&"success".to_string()), "{states:?}");
        assert!(states.contains(&"failure".to_string()), "{states:?}");
    }
}
