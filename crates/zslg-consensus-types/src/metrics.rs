//! Metrics every leader job crate emits the same way: the per-job heartbeat.

use std::time::Duration;

use metrics_kit::{MetricDef, label_enum, lossy_f64, metrics, unix_now_secs};

/// Which leader background job a `leader_job_*` metric describes (see
/// [`LeaderJob`]). Deliberately **not** `job`: Prometheus attaches its own
/// `job=<job_name>` label at scrape time and, without `honor_labels: true`,
/// renames any exposed `job` to `exported_job` — which would silently break
/// every per-job breakdown.
const LABEL_JOB: &str = "leader_job";

metrics!(pub "zslg_", leader_shared; {
    custom  leader_job_ran  LEADER_JOB_LAST_RUN_TIMESTAMP_SECONDS
        "leader_job_last_run_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds when each leader job last completed an iteration. \
         Grafana shows staleness as time() - this; a job that is wedged stops \
         advancing it. Only meaningful on the leader.";
    custom  leader_job_ran  LEADER_JOB_RUNS_TOTAL "leader_job_runs_total" Counter None
        "Completed iterations per leader job.";
    custom  leader_job_ran  LEADER_JOB_DURATION_SECONDS "leader_job_duration_seconds"
        Histogram Seconds "How long each leader-job iteration took. The snapshot \
        and compaction jobs are the long ones; they run concurrently, so a long \
        tail here does not imply other jobs are blocked.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &leader_shared!();

/// One leader job's iteration finished: stamp the wall clock, count the run, and
/// record how long it took.
///
/// The stamp is the liveness signal for the leader's background work. Each job is
/// a single serial task, so "it has not run recently" is the only way a wedged
/// job shows up — there is no queue to inspect. Grafana derives staleness as
/// `time() - leader_job_last_run_timestamp_seconds`, which keeps climbing even if
/// the job (and therefore the emitter) is stuck, unlike a duration histogram that
/// simply stops receiving samples.
pub fn leader_job_ran(job: LeaderJob, elapsed: Duration) {
    metrics_kit::metrics_crate::gauge!(LEADER_JOB_LAST_RUN_TIMESTAMP_SECONDS, LABEL_JOB => job.as_str())
        .set(lossy_f64(unix_now_secs()));
    metrics_kit::metrics_crate::counter!(LEADER_JOB_RUNS_TOTAL, LABEL_JOB => job.as_str())
        .increment(1);
    metrics_kit::metrics_crate::histogram!(LEADER_JOB_DURATION_SECONDS, LABEL_JOB => job.as_str())
        .record(elapsed.as_secs_f64());
}

label_enum!(#[doc = "`job` label of the `leader_job_*` metrics — one value per leader task (the leader job crates). Label values come from this enum, never ad-hoc strings (CLAUDE.md §7)."] pub LeaderJob {
    /// Publishes the HLC cutoff (snapshot protocol step 1).
    Cutoff => "cutoff",
    /// Builds and publishes snapshots (steps 2-5).
    Snapshot => "snapshot",
    /// Merges snapshot objects and splices the manifest.
    Compaction => "compaction",
    /// Deletes unreferenced snapshot objects.
    Cleanup => "cleanup",
    /// Drives replica-set membership changes.
    Membership => "membership",
    /// Evicts Raft voters absent from `config.peers`.
    Voters => "voters",
    /// Integrates every leader intent into Raft — the single writer.
    Integrator => "integrator",
});
