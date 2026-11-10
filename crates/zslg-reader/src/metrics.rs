//! What the reader emits: requests by source, entries by serving tier, resyncs and quorum-read waits.

use metrics_kit::{LABEL_STATE, MetricDef, OpState, label_enum, metrics};

const LABEL_SOURCE: &str = "source";

metrics!(pub "zslg_", reader; {
    custom  read_request READS_TOTAL "reads_total" Counter None
        "Read requests served, by source (stable vs unstable).";
    counter stable_read_resync STABLE_READ_RESYNCS_TOTAL
        "stable_read_resyncs_total"
        "Stable reads (`stable_tail` / `subscribe_stable_log`) that fell \
         behind the append broadcast and re-entered historical replay from \
         their own cursor. Benign by construction — the consumer sees no gap \
         and no error, only latency — but a sustained rate means consumers \
         are slower than the write rate or their subscriber queue \
         (`stable_log_subscriber_queue_capacity`) is too shallow for the \
         burst size, and each resync costs a re-scan of the durable log.";
    counter unstable_read_resync UNSTABLE_READ_RESYNCS_TOTAL
        "unstable_read_resyncs_total"
        "Unstable-tail subscriptions (`subscribe_unstable_tail`) the local \
         store's fan-out hung up on, which re-entered their cycle: re-read \
         the watermark and rescan the whole unstable window. The consumer \
         sees duplicates, never a gap. The under-pressure signal: a steadily \
         rising rate means consumers cannot keep up with their queue depth \
         and are paying an O(window) rescan each time.";
    counter read_token_wait_timeout READ_TOKEN_WAIT_TIMEOUTS_TOTAL
        "read_token_wait_timeouts_total"
        "Read-token waits that hit their timeout.";
    labeled_counter quorum_read QUORUM_READS_TOTAL "quorum_reads_total"
        "Debounced quorum end_index reads by outcome."
        (state: OpState) [LABEL_STATE => state.as_str()];
    custom  read_entry  READ_ENTRIES_TOTAL  "read_entries_total"  Counter None
        "Entries streamed to readers, by the storage tier that served each \
         (source=local_store|stable_log|object_store). Per-entry, unlike \
         reads_total (per-request). object_store is non-zero only when a \
         read reaches below the stable-log purge floor (design doc §794).";
    probe   read_token_wait_probe READ_TOKEN_WAIT_DURATION_SECONDS
        "read_token_wait_duration_seconds"
        "Time read-your-writes tokens waited for the store to catch up.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &reader!();

pub fn read_request(source: ReadSource) {
    metrics_kit::metrics_crate::counter!(
        READS_TOTAL,
        LABEL_SOURCE => source.as_str(),
    )
    .increment(1);
}

/// One entry streamed to a reader, labeled by the storage tier that served it:
/// `local_store` (unstable tail), `stable_log` (local stable range), or
/// `object_store` (purged prefix re-streamed from a snapshot — design doc §794).
/// Per-entry, unlike the per-request `reads_total`.
pub fn read_entry(source: ReadEntrySource) {
    metrics_kit::metrics_crate::counter!(
        READ_ENTRIES_TOTAL,
        LABEL_SOURCE => source.as_str(),
    )
    .increment(1);
}

label_enum!(#[doc = "`source` label of [`READS_TOTAL`]."] pub ReadSource {
    Stable => "stable",
    /// The one-shot `unstable_tail` scan.
    Unstable => "unstable",
    /// A `subscribe_unstable_tail` subscription, counted once at the call.
    UnstableSubscription => "unstable_subscription",
});

label_enum!(#[doc = "`source` label of [`READ_ENTRIES_TOTAL`]: which storage tier actually served a streamed entry. Distinct from [`ReadSource`], which is the per-request entry-point label; this one is per-entry and names the tier the data came from, so a single stable read that dips below the purge floor is attributed partly to `object_store` and partly to `stable_log`."] pub ReadEntrySource {
    LocalStore => "local_store",
    StableLog => "stable_log",
    ObjectStore => "object_store",
});
