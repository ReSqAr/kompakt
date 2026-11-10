//! What a capability says about the histograms it records, as data.

/// The bounds of one histogram: seconds, bytes per second, whatever its unit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Histogram {
    pub metric: &'static str,
    pub bounds: &'static [f64],
}
