//! The histograms the cache records, each with the bounds that suit it.
//!
//! The process that exports them applies these bounds to its recorder; the
//! cache only declares them. Buckets, not summaries: a percentile then adds up
//! across stages and processes, and covers exactly a chart's bucket.

/// Seconds from milliseconds (a cached thumbnail) to minutes (a video at the
/// worker).
const SECONDS: &[f64] = &[
    0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 180.0,
];

/// Throughput from a slow link to a local disk.
const BYTES_PER_SECOND: &[f64] = &[1e5, 3e5, 1e6, 3e6, 1e7, 3e7, 1e8, 3e8, 1e9, 3e9];

/// `(metric, bounds)` of every histogram the cache records.
pub const HISTOGRAMS: &[(&str, &[f64])] = &[
    ("objcache_store_queue_seconds", SECONDS),
    ("objcache_store_first_byte_seconds", SECONDS),
    ("objcache_store_fetch_seconds", SECONDS),
    ("objcache_store_bytes_per_second", BYTES_PER_SECOND),
];
