//! Metrics of the RocksDB store.

use metrics_kit::{MetricDef, lossy_f64, metrics};

const LABEL_CF: &str = "cf";

metrics!(pub "nvzn_", store_rocksdb; {
    custom store_live_sst_files_size_bytes STORE_LIVE_SST_FILES_SIZE_BYTES "store_live_sst_files_size_bytes" Gauge Bytes
        "rocksdb.live-sst-files-size: on-disk size of the live SST files, per \
               column family. Sampled on every store fence tick; matches zslg's \
               zslg_storage_live_sst_files_size_bytes.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &store_rocksdb!();

/// `rocksdb.live-sst-files-size` for one column family.
pub fn store_live_sst_files_size_bytes(cf: &'static str, bytes: u64) {
    metrics_kit::metrics_crate::gauge!(STORE_LIVE_SST_FILES_SIZE_BYTES, LABEL_CF => cf)
        .set(lossy_f64(bytes));
}
