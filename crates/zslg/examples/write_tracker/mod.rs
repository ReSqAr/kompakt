//! Shared write-stability tracking helper for example binaries (loadgen, writer).
//!
//! Tracks successful writes and emits these Prometheus metrics:
//! 1. `follower_oldest_write_not_stable_age_seconds` — age of oldest write not
//!    yet observed in the stable log.
//! 2. `follower_oldest_write_not_in_unstable_age_seconds` — age of oldest write
//!    not yet observed in either the stable log or the local (unstable) store.
//!    "Seen in the stable log counts as seen everywhere": tracked writes at or
//!    below the stable watermark are excluded (the unstable tail rightly hides
//!    them per §12, and the local store never permanently misses an entry on a
//!    lagged channel — the learner resumes from its durable cursor), so this
//!    gauge measures pre-stability integration lag only.
//! 3. `follower_write_stability_violations_total` — counter incremented ONCE
//!    per tracked write whose HLC falls below the stable watermark without the
//!    write having been seen in the stable log (a genuine lost-write scenario).
//!    Flagged writes move to a `violated` set so they stop re-counting on every
//!    later watermark and stop pinning the age gauges.
//! 4. `follower_write_stability_violations_resolved_total` — a previously
//!    flagged write whose `Appended` arrived late after all (transient ordering
//!    violation, not a lost write).
//! 5. `follower_write_acked_after_stable_total` — a write whose quorum ACK
//!    arrived at this node *after* its entry had already been observed in the
//!    stable log (the ack-vs-append race, docs/STABLE_LOG_GAP_FIX.md §"The
//!    ack-vs-append race"). Resolved by a bounded stable-log scan at watermark
//!    time — see below. Not a violation: it is already stable.
//!
//! ## Gap-free consumption (no tripwire, no blind windows)
//!
//! [`zslg::Reader::subscribe_stable_log`] is gapless by contract: a subscriber
//! the broadcast ring laps re-enters historical replay from its own cursor
//! inside the stream, so no `Appended` event is ever lost and this consumer
//! needs no resync logic of its own. It still tracks the **highest stable-log
//! index it has processed**, but for the ack-race scan floor below, not for
//! recovery. This replaces two earlier mechanisms:
//! - the *lag tripwire / resync policy*: lag used to invalidate tracking
//!   (panic, or drop the tracked set and suspend violation detection for a
//!   grace window). With in-stream replay, lag is just latency — verification
//!   keeps full fidelity through it.
//! - the *recent-appends ring*: a 65,536-id FIFO (~6–8 MB per node warm) that
//!   absorbed the ack-vs-append race by remembering every recently appended id.
//!   Gone entirely; see below.
//!
//! ## The ack-vs-append race, ring-free
//!
//! A write becomes quorum-durable on the replicas (and thus snapshot-eligible)
//! *before* its ACK travels back to this node, so its `Appended` event can be
//! consumed BEFORE `record_write` runs. Such a write sits in `tracked` with its
//! eviction already in the past, and a later `Watermark` would flag it as a
//! false "lost write". Instead of remembering all recent appends in RAM, the
//! consumer keeps a tiny (~1/s) history of `(time, last_seen_index)` samples;
//! `record_write` stamps each write with a **scan floor** — the index the
//! consumer had processed one [`ACK_RACE_WINDOW`] ago. When a watermark would
//! flag a write, the tracker first re-reads the stable log from that floor
//! (bounded: floor → the index current at check time, via
//! [`zslg::Reader::stable_tail`]) — if the write is found there, it was
//! the ack race (counted as `acked_after_stable`), otherwise it is a genuine
//! violation. The scan runs only for would-be violations (rare) and its range
//! is bounded by the race window, trading a few MB of permanent RAM for an
//! occasional bounded read.
//!
//! Uses only the public [`Writer`] and [`Reader`] traits — no crate-internal
//! APIs.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use zslg::{DataId, LogIndex, Reader, StableLogEvent};

/// How far back an already-consumed `Appended` event can belong to a write
/// whose ACK only just returned: bounded by `WriterConfig::write_timeout`
/// (default 10 s — a write that returns `Ok` did so within the timeout) plus
/// slack for consumer lag. (Keep in sync with the write_timeout default if
/// that ever changes.)
const ACK_RACE_WINDOW: Duration = Duration::from_secs(15);

/// Cadence of the `(time, index)` cursor history samples. One sample per
/// second for a 15 s window keeps the history at ~16 entries — the entire
/// permanent state this module holds besides the in-flight `tracked` map.
const CURSOR_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// The consumer's position in the stable-log event stream, shared with
/// `record_write`. `history` is a sparse trail of recent positions used to
/// compute ack-race scan floors; it never grows past the race window.
#[derive(Default)]
struct Cursor {
    /// Highest stable-log index whose `Appended` event has been processed.
    last_seen: Option<LogIndex>,
    /// Sparse `(sampled_at, last_seen)` trail, oldest first, ~1 sample/s,
    /// pruned to just cover [`ACK_RACE_WINDOW`].
    history: VecDeque<(Instant, LogIndex)>,
}

impl Cursor {
    fn observe(&mut self, index: LogIndex) {
        let newer = self.last_seen.is_none_or(|seen| index > seen);
        if newer {
            self.last_seen = Some(index);
        }
        let now = Instant::now();
        let due = self
            .history
            .back()
            .is_none_or(|(t, _)| now.duration_since(*t) >= CURSOR_SAMPLE_INTERVAL);
        if due && let Some(seen) = self.last_seen {
            self.history.push_back((now, seen));
            // Keep exactly one sample older than the window (the floor
            // candidate); prune everything older than that.
            while self.history.len() > 1 {
                let second_oldest_expired = self
                    .history
                    .get(1)
                    .is_some_and(|(t, _)| now.duration_since(*t) >= ACK_RACE_WINDOW);
                if second_oldest_expired {
                    self.history.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    /// The index the consumer had processed one race window ago — a lower
    /// bound for where an already-consumed `Appended` of a *just-acked* write
    /// can sit. `None` = no events consumed yet, so the race is impossible.
    fn scan_floor(&self) -> Option<LogIndex> {
        let now = Instant::now();
        self.history
            .iter()
            .rev()
            .find(|(t, _)| now.duration_since(*t) >= ACK_RACE_WINDOW)
            .or_else(|| self.history.front())
            .map(|(_, idx)| *idx)
    }
}

/// A write awaiting its `Appended` eviction.
struct TrackedWrite {
    recorded_at: Instant,
    /// Ack-race scan floor captured at `record_write` time (see module docs).
    /// `None` = no events had been consumed yet → the race is impossible and a
    /// covering watermark without an eviction is immediately a violation.
    scan_floor: Option<LogIndex>,
}

/// Tracks successful writes and monitors their stability via a [`Reader`].
///
/// Two background tasks run after [`spawn`](WriteTracker::spawn):
/// 1. **Stable log consumer** — processes the unified
///    [`Reader::subscribe_stable_log`] stream: evicts tracked DataIds on each
///    `Appended` event, counts genuine stability violations at each `Watermark`
///    event; the stream itself is gapless across a broadcast lag
///    (gap-free replay from the durable log; see the module docs).
/// 2. **Metrics** — on a 1 s tick, calls [`Reader::unstable_tail`] to collect
///    unstable DataIds, then emits the two age gauges.
pub(crate) struct WriteTracker {
    tracked: DashMap<DataId, TrackedWrite>,
    /// Writes already counted as violations (flagged once, then moved here so a
    /// single lost write does not re-increment the counter on every subsequent
    /// `Watermark` event). If a flagged write's `Appended` arrives later after
    /// all, it is logged as a late resolution and
    /// `follower_write_stability_violations_resolved_total` increments, so
    /// permanent loss and delayed delivery can be told apart.
    violated: DashMap<DataId, Instant>,
    /// Consumer position + recent history. Locked only for short synchronous
    /// sections (never across an `.await`).
    cursor: Mutex<Cursor>,
}

impl WriteTracker {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            tracked: DashMap::new(),
            violated: DashMap::new(),
            cursor: Mutex::new(Cursor::default()),
        }
    }

    /// Record a successful write's DataId. Call immediately after `writer.write()`
    /// returns `Ok(data_id)`.
    ///
    /// Contract: only record writes issued *after* [`spawn`](WriteTracker::spawn)
    /// — the tracker subscribes to the **live** stable-log tail, so the
    /// `Appended` eviction for a write made before the subscription would never
    /// arrive and the write would be falsely flagged as a violation.
    ///
    /// A write whose `Appended` event was already consumed (the ack-vs-append
    /// race) is still tracked here; it is recognized and dropped by the bounded
    /// stable-log scan at watermark time (module docs) instead of via a ring.
    pub(crate) fn record_write(&self, data_id: DataId) {
        let scan_floor = self
            .cursor
            .lock()
            .expect("cursor lock poisoned")
            .scan_floor();
        self.tracked.insert(
            data_id,
            TrackedWrite {
                recorded_at: Instant::now(),
                scan_floor,
            },
        );
    }

    /// Spawn the background monitoring tasks. Each runs until `cancel` is
    /// fired (typically the node's shutdown token).
    pub(crate) fn spawn(self: Arc<Self>, reader: Arc<dyn Reader>, cancel: CancellationToken) {
        // ── Task 1: unified stable-log consumer ────────────────────────────
        // Evicts entries on Appended events; checks for violations on Watermark.
        {
            let t = self.clone();
            let r = reader.clone();
            let c = cancel.clone();
            tokio::spawn(async move {
                // The stream is gapless by contract, lag included: it
                // re-enters historical replay from its own cursor rather than
                // surfacing a `Lagged`. Nothing to resync here.
                let mut stream = r.subscribe_stable_log(LogIndex::ZERO).into_stream();
                loop {
                    tokio::select! {
                        item = stream.next() => {
                            let event = match item {
                                Some(Ok(ev)) => ev,
                                Some(Err(e)) => {
                                    tracing::warn!(
                                        error = %e,
                                        "stable-log event stream failed; resubscribing"
                                    );
                                    stream = r.subscribe_stable_log(LogIndex::ZERO).into_stream();
                                    continue;
                                }
                                None => continue,
                            };
                            match event {
                                StableLogEvent::Appended { index, data } => {
                                    t.cursor
                                        .lock()
                                        .expect("cursor lock poisoned")
                                        .observe(index);
                                    let evicted = t.tracked.remove(&data.data_id);
                                    if let Some((_, evicted)) = &evicted {
                                        metrics::histogram!("follower_write_stable_lag_seconds")
                                            .record(evicted.recorded_at.elapsed().as_secs_f64());
                                    }
                                    if evicted.is_none()
                                        && let Some((id, flagged_at)) =
                                            t.violated.remove(&data.data_id)
                                    {
                                        // The write WAS delivered, just after its
                                        // covering watermark — a violation of the
                                        // entries-before-watermark ordering, not a
                                        // lost write. Keep both signals visible.
                                        metrics::counter!(
                                            "follower_write_stability_violations_resolved_total"
                                        )
                                        .increment(1);
                                        tracing::warn!(
                                            data_id = ?id,
                                            flagged_secs_ago =
                                                flagged_at.elapsed().as_secs(),
                                            "previously-flagged write appeared in the \
                                             stable log late (transient violation, \
                                             not a lost write)"
                                        );
                                    }
                                }
                                StableLogEvent::Watermark(watermark_hlc) => {
                                    t.check_watermark(&r, watermark_hlc).await;
                                }
                            }
                        }
                        _ = c.cancelled() => break,
                    }
                }
            });
        }

        // ── Task 2: periodic metric emission ──────────────────────────────
        {
            let t = self.clone();
            let r = reader.clone();
            let c = cancel.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            t.emit_metrics(&r).await;
                        }
                        _ = c.cancelled() => break,
                    }
                }
            });
        }
    }

    /// Watermark check: every tracked write with `hlc < watermark` should have
    /// been evicted by its `Appended` event already (the stream is gap-free).
    /// The ones that were not are either the ack-vs-append race — their event
    /// was consumed *before* `record_write` — or genuinely lost. A bounded
    /// stable-log scan from the writes' recorded scan floors tells the two
    /// apart. On a scan error nothing is flagged; the candidates stay tracked
    /// and the next watermark retries.
    async fn check_watermark(&self, reader: &Arc<dyn Reader>, watermark_hlc: zslg::Hlc) {
        // Collect first, mutate after: removing from a DashMap while holding
        // its iterator can deadlock on the shard lock.
        let candidates: Vec<(DataId, Option<LogIndex>)> = self
            .tracked
            .iter()
            .filter(|entry| entry.key().hlc() < watermark_hlc)
            .map(|entry| (*entry.key(), entry.value().scan_floor))
            .collect();
        if candidates.is_empty() {
            return;
        }

        // Race-impossible candidates (no events consumed before record_write)
        // are violations outright; the rest need the scan.
        let mut pending: HashSet<DataId> = HashSet::new();
        let mut floor: Option<LogIndex> = None;
        for (id, scan_floor) in &candidates {
            match scan_floor {
                None => self.flag_violation(*id, watermark_hlc),
                Some(f) => {
                    pending.insert(*id);
                    floor = Some(match floor {
                        Some(cur) if cur <= *f => cur,
                        _ => *f,
                    });
                }
            }
        }
        let Some(floor) = floor else {
            return;
        };
        let Some(until) = self.cursor.lock().expect("cursor lock poisoned").last_seen else {
            return; // no events processed: nothing scannable, retry next watermark
        };

        // One bounded scan covers every pending candidate: [floor, until].
        // `stable_tail(floor)` replays the durable log from the floor;
        // `until` was already consumed from the broadcast, so the historical
        // part is guaranteed to cover it — the loop cannot hang on the live
        // tail.
        let mut found: HashSet<DataId> = HashSet::new();
        let mut scan = reader
            .subscribe_stable_log(floor)
            .into_channels()
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();
        let mut scan_failed = false;
        while let Some(item) = scan.next().await {
            match item {
                Ok((idx, data)) => {
                    if pending.contains(&data.data_id) {
                        found.insert(data.data_id);
                        if found.len() == pending.len() {
                            break;
                        }
                    }
                    if idx >= until {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "ack-race scan failed; leaving candidates tracked for \
                         the next watermark"
                    );
                    scan_failed = true;
                    break;
                }
            }
        }
        drop(scan);

        for id in &pending {
            if found.contains(id) {
                // Ack-vs-append race: the write was already stable when its
                // ACK returned. Not tracked any further — it cannot be lost.
                if let Some((_, tw)) = self.tracked.remove(id) {
                    metrics::histogram!("follower_write_stable_lag_seconds")
                        .record(tw.recorded_at.elapsed().as_secs_f64());
                }
                metrics::counter!("follower_write_acked_after_stable_total").increment(1);
                tracing::debug!(
                    data_id = ?id,
                    "write ACK arrived after its stable-log append (slow quorum \
                     ack); resolved by scan, not a violation"
                );
            } else if !scan_failed {
                self.flag_violation(*id, watermark_hlc);
            }
        }
    }

    /// Genuine violation: the stream is gap-free, so every `Appended` was
    /// observed — yet this tracked write is below the stable watermark, never
    /// appeared, and the ack-race scan (if applicable) came up empty. Count +
    /// warn ONCE per write (then move it to `violated`).
    fn flag_violation(&self, data_id: DataId, watermark_hlc: zslg::Hlc) {
        if self.tracked.remove(&data_id).is_none() {
            return;
        }
        self.violated.insert(data_id, Instant::now());
        metrics::counter!("follower_write_stability_violations_total").increment(1);
        tracing::warn!(
            data_id = ?data_id,
            hlc = ?data_id.hlc(),
            watermark = ?watermark_hlc,
            "follower write stability violation: HLC below stable watermark \
             but write not in stable log"
        );
    }

    async fn emit_metrics(&self, reader: &Arc<dyn Reader>) {
        // Collect unstable DataIds into a set for the "not in unstable" check.
        let mut unstable: HashSet<DataId> = HashSet::new();
        {
            let mut ut = reader.unstable_tail().into_stream();
            while let Some(Ok(data)) = ut.next().await {
                unstable.insert(data.data_id);
            }
        }
        // Seen in the stable log counts as seen everywhere: `unstable_tail`
        // rightly excludes entries at or below the stable watermark (§12), so
        // a watermark-covered tracked write (an ack-raced one awaiting its
        // scan, or a violation the next watermark will flag) must not read as
        // "missing from the local store" here. The gauge measures pre-stability
        // integration lag only — and the local store itself never permanently
        // misses anything on a lagged channel (the learner resumes from its
        // durable subscription cursor).
        let stable_wm = reader.stable_watermark();

        let now = Instant::now();
        let mut not_stable_count: usize = 0;
        let mut not_in_unstable_count: usize = 0;
        let mut oldest_stable_age = 0.0_f64;
        let mut oldest_unstable_age = 0.0_f64;

        for entry in self.tracked.iter() {
            let age = now
                .saturating_duration_since(entry.value().recorded_at)
                .as_secs_f64();
            // saturating: counts are bounded by the tracked-set size; keeps us off a
            // bare `+` per CLAUDE.md §2.
            not_stable_count = not_stable_count.saturating_add(1);
            oldest_stable_age = oldest_stable_age.max(age);
            let covered_by_stable = stable_wm.is_some_and(|wm| entry.key().hlc() <= wm);
            if !unstable.contains(entry.key()) && !covered_by_stable {
                not_in_unstable_count = not_in_unstable_count.saturating_add(1);
                oldest_unstable_age = oldest_unstable_age.max(age);
            }
        }

        // usize -> f64 for the gauges: tracked counts are tiny, so the u32 clamp never
        // bites; try_from + From keep us off a bare `as` cast (CLAUDE.md §2).
        let not_stable_gauge = f64::from(u32::try_from(not_stable_count).unwrap_or(u32::MAX));
        let not_in_unstable_gauge =
            f64::from(u32::try_from(not_in_unstable_count).unwrap_or(u32::MAX));

        metrics::gauge!("follower_oldest_write_not_stable_age_seconds").set(oldest_stable_age);
        metrics::gauge!("follower_writes_not_stable").set(not_stable_gauge);

        metrics::gauge!("follower_oldest_write_not_in_unstable_age_seconds")
            .set(oldest_unstable_age);
        metrics::gauge!("follower_writes_not_in_unstable").set(not_in_unstable_gauge);
    }
}
