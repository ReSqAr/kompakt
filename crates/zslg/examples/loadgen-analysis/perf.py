#!/usr/bin/env python3
"""Consolidated performance harness for the `loadgen` engine (see docs/PERFORMANCE.md).

ONE driver that replaces the old run_baseline.sh / bench.py / cmp_fsync.py /
perf_probe.py / loadgen_report.py. Pure stdlib (no uv/duckdb) so it runs offline
in the container. It drives the unchanged `crates/zslg/examples/loadgen` binary, runs N reps
per scenario on (optionally) pinned CPUs, gates on run-to-run variance (auto-rerun
extra reps, then flag), prints the steady-state latency decomposition, and appends
one row per (commit, scenario) to the git-tracked timeline docs/perf-timeline.csv.

Subcommands:
  perf.py run     --inflight N [--target-rps R] [--reps 5] [--dur 30] ...   one scenario
  perf.py sweep   [--dur 30] [--comment "..."] ...                          the scenario matrix
  perf.py compare --a "<flags>" --b "<flags>" [--inflight 128] ...          interleaved A/B
  perf.py report  <metrics.jsonl> [<metrics.jsonl> ...]                     ad-hoc analysis

Any loadgen flag not recognised by perf.py (e.g. --fsync-min-interval-ms 3,
--write-streams-per-peer 8) is forwarded verbatim to the binary, so every tuning
knob is sweepable / A/B-able without editing this file.

Env: LOADGEN_BIN, CARGO_TARGET_DIR, REPLICAS, WRITERS, RUST_LOG, RUN_TIMEOUT.
"""
import argparse
import csv
import glob
import json
import os
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".."))
TARGET = os.environ.get("CARGO_TARGET_DIR") or os.path.join(REPO_ROOT, "target")
DEFAULT_BIN = os.environ.get("LOADGEN_BIN") or os.path.join(TARGET, "release/examples/loadgen")
CSV_PATH = os.path.join(REPO_ROOT, "docs", "perf-timeline.csv")
CLK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100

# ---------------------------------------------------------------------------
# Analysis (ported from perf_probe.py — trusted bucket math; entries 16/18).
# Each probe carries a stable `key` for CSV extraction plus a display label.
# ---------------------------------------------------------------------------
# (key, display, metric_base, {label_key: label_val} filter) — histograms.
HISTS = [
    ("client_write", "client_write       (end-to-end)", "loadgen_write_latency_seconds", {}),
    ("writer_write", "writer_write       ", "write_duration_seconds", {"state": "success"}),
    ("rpc_roundtrip", "  └ rpc_roundtrip   ", "rpc_roundtrip_seconds", {}),
    ("open_stream", "    └ open_stream   ", "rpc_open_stream_seconds", {}),
    ("conn_lock", "      └ conn_lock   ", "rpc_conn_lock_wait_seconds", {}),
    ("pool_write", "  └ pool_write      ", "rpc_pool_write_seconds", {}),
    ("pool_queue_wait", "    └ pool_queuewait ", "rpc_pool_queue_wait_seconds", {}),
    ("srv_stream_queue", "  └ srv_stream_queue ", "rpc_server_stream_queue_seconds", {}),
    ("rpc_handler", "rpc_handler        ", "rpc_server_request_duration_seconds", {"method": "ReplicaWrite"}),
    ("srv_finish", "  └ srv_finish      ", "rpc_server_finish_seconds", {}),
    ("replica_log_append", "replica_log_append ", "replica_log_append_duration_seconds", {"state": "success"}),
    ("replica_queue_wait", "  └ queue_wait      ", "replica_append_queue_wait_seconds", {}),
    ("replica_commit", "  └ commit         ", "replica_log_commit_seconds", {}),
    ("persist_fsync", "  └ persist(fsync) ", "storage_persist_duration_seconds", {"state": "success"}),
    ("postcommit", "  └ postcommit      ", "replica_append_postcommit_seconds", {}),
    ("learner_integrate", "learner_integrate  ", "local_store_integrate_duration_seconds", {"state": "success"}),
]
# (key, display, metric, filter) — cumulative counters reported as a rate / s.
RATES = [
    ("achieved_writes", "achieved writes/s  ", "loadgen_writes_total", {}),
    ("offered", "offered/s          ", "loadgen_offered_total", {}),
    ("errors", "errors/s           ", "loadgen_errors_total", {}),
    ("backpressure", "backpressure/s     ", "loadgen_backpressure_total", {}),
    ("persist_rate", "persist/s (fsync)  ", "storage_persist_duration_seconds_count", {"state": "success"}),
]
# (key, display, metric_base, filter) — plain mean of a histogram (sum/count).
MEANS = [
    ("repl_batch", "repl_batch mean    ", "replica_log_flush_entries", {}),
]
# (key, display, metric, filter) — gauge last value in the window (printed in ms).
GAUGES = [
]
QKEYS = (0.5, 0.9, 0.99, 0.999, 0.9999, 1.0)
QLABELS = ("p50", "p90", "p99", "p99.9", "max")


def match(labels, flt):
    return all(labels.get(k) == v for k, v in flt.items())


def load(path):
    rows, bad = [], 0
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                # JSONL can tear on the final flush if the writer is killed
                # mid-line (loadgen watchdog process::exit). Skip the torn record.
                bad += 1
    if bad:
        print(f"# warning: skipped {bad} malformed line(s) in {path}", file=sys.stderr)
    return rows


def steady_window(rows):
    ts = [r["ts_ms"] for r in rows if r["name"] == "loadgen_steady_state" and r["value"] == 1]
    if not ts:
        allts = [r["ts_ms"] for r in rows]
        return (min(allts), max(allts)) if allts else (0, 0)
    return min(ts), max(ts)


def series_delta(rows, name, flt, t0, t1):
    """Count within [t0,t1] for a cumulative counter (baseline = last sample <=t0)."""
    pts = sorted((r["ts_ms"], r["value"]) for r in rows if r["name"] == name and match(r["labels"], flt))
    if not pts:
        return None
    base = end = None
    for ts, v in pts:
        if ts <= t0:
            base = v
        if ts <= t1:
            end = v
    if base is None:
        base = pts[0][1]
    if end is None:
        end = pts[-1][1]
    return end - base


def labeled_rate_breakdown(rows, name, label_key, t0, t1, secs):
    """Per-label-value rate (/s) for a labeled counter. Each distinct full label
    set is its own cumulative series; we delta each and aggregate by `label_key`.
    (A plain `series_delta` with an empty filter would interleave label series and
    produce garbage — counters like `loadgen_errors_total{kind}` need this.)"""
    series = defaultdict(list)
    for r in rows:
        if r["name"] == name:
            series[tuple(sorted(r["labels"].items()))].append((r["ts_ms"], r["value"]))
    out = defaultdict(float)
    for key, pts in series.items():
        pts.sort()
        base = end = None
        for ts, v in pts:
            if ts <= t0:
                base = v
            if ts <= t1:
                end = v
        if base is None:
            base = pts[0][1]
        if end is None:
            end = pts[-1][1]
        out[dict(key).get(label_key, "")] += end - base
    return {k: v / secs for k, v in out.items() if v}


def agg_breakdown(runs, key):
    """Median across reps of a per-label breakdown dict (e.g. `_errors_by_kind`)."""
    labels = set()
    for r in runs:
        labels |= set(r.get(key, {}))
    out = {}
    for lbl in labels:
        m = med([r.get(key, {}).get(lbl, 0.0) for r in runs])
        if m:
            out[lbl] = m
    return out


def gauge_last(rows, name, flt, t0, t1):
    pts = sorted(
        (r["ts_ms"], r["value"])
        for r in rows
        if r["name"] == name and match(r["labels"], flt) and t0 <= r["ts_ms"] <= t1
    )
    return pts[-1][1] if pts else None


def hist_percentiles(rows, base, flt, t0, t1, qs=QKEYS):
    by_le = defaultdict(list)
    for r in rows:
        if r["name"] == base + "_bucket" and match(r["labels"], flt):
            by_le[r["labels"].get("le")].append((r["ts_ms"], r["value"]))
    if not by_le:
        return None
    deltas = {}
    for le, pts in by_le.items():
        pts.sort()
        base_v = end_v = None
        for ts, v in pts:
            if ts <= t0:
                base_v = v
            if ts <= t1:
                end_v = v
        if base_v is None:
            base_v = pts[0][1]
        if end_v is None:
            end_v = pts[-1][1]
        deltas[le] = end_v - base_v

    def le_f(s):
        return float("inf") if s.lstrip("+").lower() == "inf" else float(s)

    les = sorted(deltas, key=le_f)
    total = max(deltas[le] for le in les)
    if total <= 0:
        return None
    out = {}
    for q in qs:
        tgt = q * total
        chosen = les[-1]
        for le in les:
            if deltas[le] >= tgt:
                chosen = le
                break
        out[q] = le_f(chosen) * 1000.0  # ms
    out["n"] = total
    s = series_delta(rows, base + "_sum", flt, t0, t1)
    c = series_delta(rows, base + "_count", flt, t0, t1)
    out["mean"] = (s / c * 1000.0) if (s and c) else None
    return out


def analyze_one(path):
    """Steady-state decomposition for one metrics.jsonl. Keyed by probe `key`."""
    rows = load(path)
    t0, t1 = steady_window(rows)
    secs = (t1 - t0) / 1000.0 or 1.0
    res = {"_secs": secs, "_t0": t0, "_t1": t1}
    for key, _disp, base, flt in HISTS:
        res[key] = hist_percentiles(rows, base, flt, t0, t1)
    for key, _disp, name, flt in RATES:
        d = series_delta(rows, name, flt, t0, t1)
        res[key] = (d / secs) if d is not None else None
    for key, _disp, base, flt in MEANS:
        s = series_delta(rows, base + "_sum", flt, t0, t1)
        c = series_delta(rows, base + "_count", flt, t0, t1)
        res[key] = (s / c) if (s and c) else None
    for key, _disp, name, flt in GAUGES:
        v = gauge_last(rows, name, flt, t0, t1)
        res[key] = (v * 1000.0) if v is not None else None
    # ── error / failure classification (the Phase-0 instrumentation) ──
    res["_errors_by_kind"] = labeled_rate_breakdown(rows, "loadgen_errors_total", "kind", t0, t1, secs)
    res["_fanout_by_state"] = labeled_rate_breakdown(rows, "write_fanout_responses_total", "state", t0, t1, secs)
    res["_replica_by_state"] = labeled_rate_breakdown(rows, "replica_write_requests_total", "state", t0, t1, secs)
    # Override the RATES `errors` (which mixes the now-labeled series) with the
    # label-correct sum across kinds.
    res["errors"] = sum(res["_errors_by_kind"].values()) or None
    lc = series_delta(rows, "raft_leader_changes_total", {}, t0, t1)
    res["raft_leader_changes"] = (lc / secs) if lc is not None else None
    res["raft_config_generation"] = gauge_last(rows, "raft_config_generation", {}, t0, t1)
    return res


def med(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def hmed(runs, key, q):
    """Median across reps of histogram `key` at quantile `q`."""
    return med([r[key][q] for r in runs if r.get(key)])


def rmed(runs, key):
    """Median across reps of a scalar (rate/mean/gauge) `key`."""
    return med([r[key] for r in runs if r.get(key) is not None])


def report(paths, cpus=None, label=None):
    """Print the median+spread decomposition for 1+ metrics.jsonl runs."""
    runs = [analyze_one(p) for p in paths]
    nrep = len(runs)
    if label:
        print(f"=== {label} ===")
    print(f"# {nrep} rep(s), steady window ~{med([r['_secs'] for r in runs]) or 0:.0f}s each")
    hdr = f"{'metric':32s} " + "".join(f"{ql:>8s} " for ql in QLABELS)
    hdr += "  (median across reps; [min..max] p50)"
    print(hdr)
    for key, disp, _base, _flt in HISTS:
        vals = [r[key] for r in runs if r.get(key)]
        if not vals:
            continue
        p50s = sorted(v[0.5] for v in vals)
        spread = f"[{p50s[0]:.2f}..{p50s[-1]:.2f}]" if nrep > 1 else ""
        line = f"{disp:32s} " + "".join(f"{med([v[q] for v in vals]):>7.1f} " for q in QKEYS)
        print(line + f" {spread}")
    print("-" * 60)
    for key, disp, _name, _flt in RATES:
        vals = [r[key] for r in runs if r.get(key) is not None]
        if vals:
            vs = sorted(vals)
            spread = f"[{vs[0]:.0f}..{vs[-1]:.0f}]" if nrep > 1 else ""
            print(f"{disp:32s} {med(vals):>15.1f}/s {spread}")
    for key, disp, _base, _flt in MEANS:
        vals = [r[key] for r in runs if r.get(key) is not None]
        if vals:
            print(f"{disp:32s} {med(vals):>15.2f}")
    for key, disp, _name, _flt in GAUGES:
        vals = [r[key] for r in runs if r.get(key) is not None]
        if vals:
            print(f"{disp:32s} {med(vals):>15.3f}")
    if cpus:
        cm = med(cpus)
        if cm is not None:
            print(f"{'cpu (cores, whole process)':32s} {cm:>15.2f}")
            thr = rmed(runs, "achieved_writes")
            if thr and cm > 0:
                print(f"{'writes/core/s':32s} {thr / cm:>15.0f}")
    # ── failure classification (where the errors actually come from) ──
    def show_breakdown(title, key):
        bd = agg_breakdown(runs, key)
        if bd:
            parts = ", ".join(f"{k}={v:.1f}" for k, v in sorted(bd.items(), key=lambda x: -x[1]))
            print(f"  {title:22s} {parts}")
    show_breakdown("errors by kind (/s):", "_errors_by_kind")
    show_breakdown("fanout by state (/s):", "_fanout_by_state")
    show_breakdown("replica by state(/s):", "_replica_by_state")
    lc = rmed(runs, "raft_leader_changes")
    if lc is not None:
        print(f"  {'raft leader changes/s:':22s} {lc:.3f}")
    return runs


# ---------------------------------------------------------------------------
# CPU sampling (ported from run_baseline.sh proc_cpu) — loadgen is our direct
# child, so proc.pid IS the binary (no comm scan needed; we don't go via cargo).
# ---------------------------------------------------------------------------
def proc_cpu(pid):
    """Cumulative (utime+stime) jiffies for `pid`. Linux /proc, else macOS `ps`."""
    statf = f"/proc/{pid}/stat"
    if os.path.exists(statf):
        try:
            with open(statf) as f:
                data = f.read()
            # comm may contain ') ' — split after the LAST ') ', then utime/stime
            # are fields 14/15 overall = index 11/12 of the remainder.
            after = data.rsplit(") ", 1)[1].split()
            return float(after[11]) + float(after[12])
        except (OSError, IndexError, ValueError):
            return None
    try:  # macOS / BSD: "[DD-]HH:MM:SS[.ss]" → seconds × CLK
        out = subprocess.run(["ps", "-o", "time=", "-p", str(pid)],
                             capture_output=True, text=True).stdout.strip()
        if not out:
            return None
        days = 0
        if "-" in out:
            d, out = out.split("-", 1)
            days = int(d)
        parts = [float(x) for x in out.split(":")]
        secs = parts[-1] + (parts[-2] * 60 if len(parts) >= 2 else 0) + (parts[-3] * 3600 if len(parts) >= 3 else 0)
        return (days * 86400 + secs) * CLK
    except (OSError, ValueError, IndexError):
        return None


def cpu_cores_in_window(samples, t0, t1):
    """Steady-state whole-process cores from (ts_ms, jiffies) samples."""
    w = [(ts, j) for ts, j in samples if t0 <= ts <= t1]
    if len(w) < 2:
        w = samples
    if len(w) < 2:
        return None
    dt = (w[-1][0] - w[0][0]) / 1000.0
    return (w[-1][1] - w[0][1]) / CLK / dt if dt > 0 else None


# ---------------------------------------------------------------------------
# Rep runner (ported from bench.py::run_config): fresh data-dir per rep, hard
# subprocess timebox, delete data-dir after each rep (tmpfs hygiene, entry 8).
# Samples CPU concurrently while the rep runs.
# ---------------------------------------------------------------------------
def _read_proc(path):
    """Best-effort read of a /proc file; '' if unreadable."""
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def dump_hang_diagnostic(pid, od):
    """Snapshot every thread of a hung loadgen before we SIGKILL it, so a teardown
    deadlock (the "TIMED OUT … HANG") can be diagnosed after the fact. Records each
    thread's name (`comm`) + wait-channel (`wchan` — e.g. `futex_*` pinpoints a parked
    blocking worker), plus the kernel `stack` and a `gdb` backtrace when permitted.
    Pure /proc + optional gdb, so it needs nothing extra in the container; degrades to
    just comm+wchan (always world-readable) when ptrace is denied. Returns the path or
    None."""
    out = os.path.join(od, "hang-diagnostic.txt")
    taskdir = f"/proc/{pid}/task"
    lines = [f"# hang diagnostic for pid {pid}\n"]
    try:
        tids = sorted(os.listdir(taskdir))
    except OSError as e:
        tids = []
        lines.append(f"# cannot list {taskdir}: {e}\n")
    for tid in tids:
        comm = _read_proc(f"{taskdir}/{tid}/comm").strip()
        wchan = _read_proc(f"{taskdir}/{tid}/wchan").strip()
        lines.append(f"\n## tid {tid}  comm={comm!r}  wchan={wchan!r}\n")
        stack = _read_proc(f"{taskdir}/{tid}/stack")
        if stack.strip():
            lines.append(stack if stack.endswith("\n") else stack + "\n")
    if shutil.which("gdb"):
        try:
            bt = subprocess.run(
                ["gdb", "-p", str(pid), "-batch", "-nx",
                 "-ex", "set pagination off", "-ex", "thread apply all bt"],
                capture_output=True, text=True, timeout=20)
            lines.append("\n## gdb: thread apply all bt\n")
            lines.append(bt.stdout)
            if bt.stderr.strip():
                lines.append(f"# gdb stderr: {bt.stderr.strip()}\n")
        except (subprocess.SubprocessError, OSError) as e:
            lines.append(f"# gdb failed: {e}\n")
    try:
        with open(out, "w") as f:
            f.writelines(lines)
    except OSError as e:
        print(f"# warning: could not write hang diagnostic: {e}", file=sys.stderr)
        return None
    return out


def run_rep(binp, scenario_flags, common, extra, dd, od, dur, timeout, pin):
    os.makedirs(dd, exist_ok=True)
    os.makedirs(od, exist_ok=True)
    cmd = [binp, *common, *scenario_flags,
           "--duration-secs", str(dur), "--warmup-secs", "3",
           "--sample-interval-ms", "500", "--data-dir", dd, "--out-dir", od, *extra]
    if pin:
        if shutil.which("taskset"):
            cmd = ["taskset", "-c", pin, *cmd]
        else:
            print(f"# warning: --pin-cpus {pin} ignored (taskset not found)", file=sys.stderr)
    samples = []
    log_path = os.path.join(od, "stderr.log")
    with open(log_path, "w") as logf:
        proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=logf, stderr=subprocess.STDOUT)
        # proc.pid is the loadgen binary even when pinned: `taskset` execs into
        # the command in place, so the PID is preserved.
        sample_pid = proc.pid
        start = time.time()
        while proc.poll() is None:
            if time.time() - start > timeout:
                # Capture WHERE it wedged before we SIGKILL it — a hung run is almost
                # always a teardown deadlock, and the thread dump pinpoints the parked
                # worker (docs/PERFORMANCE.md — the 70s-HANG follow-up).
                diag = dump_hang_diagnostic(sample_pid, od)
                proc.kill()
                proc.wait()
                shutil.rmtree(dd, ignore_errors=True)
                where = f" — diagnostic: {diag}" if diag else ""
                print(f"  TIMED OUT after {timeout}s (HANG) — aborting{where}", file=sys.stderr)
                sys.exit(124)
            j = proc_cpu(sample_pid)
            if j is not None:
                samples.append((int(time.time() * 1000), j))
            time.sleep(0.5)
    found = glob.glob(os.path.join(od, "**", "metrics.jsonl"), recursive=True)
    if not found:
        shutil.rmtree(dd, ignore_errors=True)
        print("  rep produced no metrics.jsonl — aborting", file=sys.stderr)
        sys.exit(1)
    shutil.rmtree(dd, ignore_errors=True)  # free the (possibly tmpfs) data dir now
    return found[0], samples


def cv(values):
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0
    m = statistics.mean(vals)
    return (statistics.pstdev(vals) / m) if m else 0.0


def signal_values(runs, signal):
    if signal == "client_p50":
        return [r["client_write"][0.5] for r in runs if r.get("client_write")]
    return [r["achieved_writes"] for r in runs if r.get("achieved_writes") is not None]


def enforce_window(res, ctx, label, i, eff_dur):
    """Hard-fail if the steady-state window is far shorter than the (effective)
    requested duration — the run died early (e.g. SIGKILL/OOM on the RAM-backed
    data dir) or the sampler stalled, so the numbers are NOT trustworthy. A
    truncated window silently reporting a healthy-looking rate is the classic
    'measurement was lying' trap (PERFORMANCE.md entry 3); crash loudly instead."""
    win = res.get("_secs", 0.0)
    floor = ctx["min_window_frac"] * eff_dur
    if win < floor:
        sys.exit(
            f"\nFATAL: {label} rep{i} steady-state window {win:.1f}s << effective "
            f"duration {eff_dur}s (floor {floor:.1f}s = {ctx['min_window_frac']:.0%}). "
            f"The loadgen run was truncated (died early / sampler stalled), so its "
            f"throughput/latency are NOT trustworthy. Fix the run (e.g. the data dir "
            f"filled the RAM tmpfs at this rate) before measuring. Aborting.")


def run_scenario(scn, ctx):
    """Run one scenario: reps_baseline reps, then auto-rerun extra reps until the
    variance gate passes or the cap is hit. Returns (runs, jsonls, cpus, extra)."""
    runs, jsonls, cpus = [], [], []
    label = scn["name"]
    log = ctx.get("log", print)
    eff_dur = scenario_dur(scn, ctx)
    if eff_dur != ctx["dur"]:
        log(f"  {label}: duration capped {ctx['dur']}s → {eff_dur}s (RAM-tmpfs footprint)")
    dataroot, outbase = ctx["dataroot"], os.path.join("results/perf", label)
    shutil.rmtree(os.path.join(REPO_ROOT, outbase), ignore_errors=True)

    def one(i):
        dd = os.path.join(dataroot, f"{label}-rep{i}")
        od = os.path.join(REPO_ROOT, outbase, f"rep{i}")
        shutil.rmtree(dd, ignore_errors=True)
        j, samples = run_rep(ctx["bin"], scn["flags"], ctx["common"], ctx["extra"],
                             dd, od, eff_dur, ctx["timeout"], ctx["pin"])
        res = analyze_one(j)
        enforce_window(res, ctx, label, i, eff_dur)
        runs.append(res)
        jsonls.append(j)
        cpus.append(cpu_cores_in_window(samples, res["_t0"], res["_t1"]))
        log(f"  {label} rep{i} done (thrpt={res.get('achieved_writes') or 0:.0f}/s)")

    for i in range(1, ctx["reps"] + 1):
        one(i)
    extra = 0
    sig = scn["signal"]
    if sig:
        while cv(signal_values(runs, sig)) > ctx["cv_threshold"] and extra < ctx["max_extra"]:
            extra += 1
            log(f"  {label}: CV={cv(signal_values(runs, sig)):.3f} > {ctx['cv_threshold']} "
                f"— extra rep {extra}/{ctx['max_extra']}")
            one(ctx["reps"] + extra)
    return runs, jsonls, cpus, extra


# ---------------------------------------------------------------------------
# Environment / git provenance for the CSV.
# ---------------------------------------------------------------------------
def git_out(*args):
    try:
        return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True).stdout.strip()
    except OSError:
        return ""


def git_dirty(pathspec=None):
    args = ["status", "--porcelain"]
    if pathspec:
        args += ["--", pathspec]
    return "dirty" if git_out(*args) else "clean"


def cpu_model():
    try:
        with open("/proc/cpuinfo") as f:
            for ln in f:
                if ln.startswith("model name"):
                    return ln.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def detect_fs(path):
    """Filesystem type backing `path` (tmpfs/overlay/etc) via /proc/mounts."""
    try:
        real = os.path.realpath(path)
        best, best_type = "", "unknown"
        with open("/proc/mounts") as f:
            for ln in f:
                parts = ln.split()
                if len(parts) >= 3:
                    mnt, fstype = parts[1], parts[2]
                    if (real == mnt or real.startswith(mnt.rstrip("/") + "/")) and len(mnt) > len(best):
                        best, best_type = mnt, fstype
        return best_type
    except OSError:
        return "unknown"


def flag_val(args_list, *names):
    """Return the value following any of `names` in a token list, else None."""
    for i, tok in enumerate(args_list):
        if tok in names and i + 1 < len(args_list):
            return args_list[i + 1]
    return None


def has_flag(args_list, *names):
    return any(tok in names for tok in args_list)


# ---------------------------------------------------------------------------
# CSV timeline.
# ---------------------------------------------------------------------------
SCHEMA_VERSION = "4"
COLUMNS = [
    # provenance
    "schema_version", "timestamp_utc", "git_commit_short", "git_commit_full",
    "git_branch", "src_tree_dirty", "full_tree_dirty", "comment",
    # environment
    "hostname", "operating_system", "cpu_model", "cpu_cores_available",
    "pinned_cpus", "data_dir_filesystem", "rust_log",
    # scenario configuration
    "scenario_name", "load_mode", "max_inflight", "target_rps", "replica_count",
    "writer_count", "payload_min_bytes", "payload_max_bytes", "duration_secs",
    "warmup_secs", "node_runtime_threads", "shared_runtime", "fsync_min_interval_ms",
    "pool_channel_capacity",
    # reps & variance
    "reps_total", "reps_baseline", "reps_extra", "steady_window_secs_median",
    "variance_signal", "variance_coefficient", "variance_threshold", "variance_flag",
    # throughput & errors
    "throughput_writes_per_sec_median", "throughput_writes_per_sec_min",
    "throughput_writes_per_sec_max", "offered_writes_per_sec_median",
    "errors_per_sec_median", "error_rate_percent", "backpressure_per_sec_median",
    # client end-to-end latency
    "client_write_latency_p50_ms", "client_write_latency_p90_ms",
    "client_write_latency_p99_ms", "client_write_latency_p999_ms",
    "client_write_latency_max_ms",
    # latency decomposition
    "pool_write_p50_ms", "pool_write_p99_ms", "pool_queue_wait_p50_ms",
    "server_handler_p50_ms", "server_handler_p99_ms", "server_stream_queue_p50_ms",
    "replica_log_append_p50_ms", "replica_log_append_p99_ms", "replica_log_append_max_ms",
    "replica_log_queue_wait_p50_ms", "replica_log_queue_wait_p99_ms",
    "replica_log_commit_p50_ms", "replica_log_commit_p99_ms", "replica_log_commit_max_ms",
    "storage_persist_fsync_p50_ms", "storage_persist_fsync_p99_ms",
    "replica_postcommit_p50_ms",
    "learner_integrate_p50_ms", "learner_integrate_p99_ms",
    # storage / batching / resource
    "replica_log_batch_size_mean",
    "persist_per_sec", "cpu_cores_used_mean", "writes_per_core_per_sec",
    # failure classification (schema v2)
    "error_kind_top", "raft_leader_changes_per_sec", "raft_config_generation",
    "fanout_ack_per_sec", "fanout_rejected_per_sec", "fanout_unreachable_per_sec",
    "replica_fail_hlc_cutoff_per_sec", "replica_fail_clock_skew_per_sec",
    "replica_fail_not_replica_per_sec", "replica_fail_storage_per_sec",
    # network knobs (schema v3): the writer admission gate + per-peer connection
    # sharding. Default 0/1 (off / single connection) — captured so ablation rows
    # that toggle them are self-describing.
    "writer_max_inflight", "connections_per_peer",
    # stale-column cleanup (schema v4): dropped the per-store engine
    # worker-threads knob (no such CLI flag) and the global-fsync-worker
    # series (that mechanism was removed; the engine fsyncs in-line now).
]


def _n(x, nd=2):
    """Format a number for the CSV, blank for None."""
    return "" if x is None else f"{x:.{nd}f}"


def build_row(scn, ctx, runs, cpus, extra):
    cfg = ctx["common"] + scn["flags"] + ctx["extra"]
    thr = [r["achieved_writes"] for r in runs if r.get("achieved_writes") is not None]
    offered = rmed(runs, "offered")
    errors = rmed(runs, "errors")
    cpu_med = med(cpus)
    thr_med = med(thr)
    sig = scn["signal"] or "none"
    sig_vals = signal_values(runs, scn["signal"]) if scn["signal"] else []
    errors_by_kind = agg_breakdown(runs, "_errors_by_kind")
    fanout = agg_breakdown(runs, "_fanout_by_state")
    replica = agg_breakdown(runs, "_replica_by_state")
    err_top = max(errors_by_kind, key=errors_by_kind.get) if errors_by_kind else ""
    row = {
        "schema_version": SCHEMA_VERSION,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_commit_short": git_out("rev-parse", "--short", "HEAD"),
        "git_commit_full": git_out("rev-parse", "HEAD"),
        "git_branch": git_out("rev-parse", "--abbrev-ref", "HEAD"),
        "src_tree_dirty": git_dirty("src"),
        "full_tree_dirty": git_dirty(),
        "comment": ctx["comment"],
        "hostname": socket.gethostname(),
        "operating_system": f"{platform.system()} {platform.release()}",
        "cpu_model": cpu_model(),
        "cpu_cores_available": os.cpu_count() or "",
        "pinned_cpus": ctx["pin"] or "none",
        "data_dir_filesystem": detect_fs(ctx["dataroot"]),
        "rust_log": ctx["rust_log"],
        "scenario_name": scn["name"],
        "load_mode": scn["mode"],
        "max_inflight": flag_val(cfg, "--max-inflight") or "",
        "target_rps": flag_val(cfg, "--target-rps") or "",
        "replica_count": flag_val(cfg, "--replicas") or "",
        "writer_count": flag_val(cfg, "--writers") or "",
        "payload_min_bytes": flag_val(cfg, "--payload-min") or "64",
        "payload_max_bytes": flag_val(cfg, "--payload-max") or "4096",
        "duration_secs": scenario_dur(scn, ctx),
        "warmup_secs": 3,
        "node_runtime_threads": flag_val(cfg, "--node-runtime-threads") or "1",
        "shared_runtime": "true" if has_flag(cfg, "--shared-runtime") else "false",
        "fsync_min_interval_ms": flag_val(cfg, "--fsync-min-interval-ms") or "",
        "pool_channel_capacity": flag_val(cfg, "--pool-channel-capacity") or "",
        "writer_max_inflight": flag_val(cfg, "--writer-max-inflight") or "0",
        "connections_per_peer": flag_val(cfg, "--connections-per-peer") or "1",
        "reps_total": len(runs),
        "reps_baseline": ctx["reps"],
        "reps_extra": extra,
        "steady_window_secs_median": _n(med([r["_secs"] for r in runs]), 1),
        "variance_signal": sig,
        "variance_coefficient": _n(cv(sig_vals), 4) if sig_vals else "",
        "variance_threshold": _n(ctx["cv_threshold"], 4),
        "variance_flag": ("n/a" if not scn["signal"]
                          else ("HIGH" if cv(sig_vals) > ctx["cv_threshold"] else "ok")),
        "throughput_writes_per_sec_median": _n(thr_med, 1),
        "throughput_writes_per_sec_min": _n(min(thr) if thr else None, 1),
        "throughput_writes_per_sec_max": _n(max(thr) if thr else None, 1),
        "offered_writes_per_sec_median": _n(offered, 1),
        "errors_per_sec_median": _n(errors, 2),
        "error_rate_percent": _n((errors / offered * 100) if (errors is not None and offered) else None, 3),
        "backpressure_per_sec_median": _n(rmed(runs, "backpressure"), 2),
        "client_write_latency_p50_ms": _n(hmed(runs, "client_write", 0.5)),
        "client_write_latency_p90_ms": _n(hmed(runs, "client_write", 0.9)),
        "client_write_latency_p99_ms": _n(hmed(runs, "client_write", 0.99)),
        "client_write_latency_p999_ms": _n(hmed(runs, "client_write", 0.999)),
        "client_write_latency_max_ms": _n(hmed(runs, "client_write", 1.0)),
        "pool_write_p50_ms": _n(hmed(runs, "pool_write", 0.5)),
        "pool_write_p99_ms": _n(hmed(runs, "pool_write", 0.99)),
        "pool_queue_wait_p50_ms": _n(hmed(runs, "pool_queue_wait", 0.5)),
        "server_handler_p50_ms": _n(hmed(runs, "rpc_handler", 0.5)),
        "server_handler_p99_ms": _n(hmed(runs, "rpc_handler", 0.99)),
        "server_stream_queue_p50_ms": _n(hmed(runs, "srv_stream_queue", 0.5)),
        "replica_log_append_p50_ms": _n(hmed(runs, "replica_log_append", 0.5)),
        "replica_log_append_p99_ms": _n(hmed(runs, "replica_log_append", 0.99)),
        "replica_log_append_max_ms": _n(hmed(runs, "replica_log_append", 1.0)),
        "replica_log_queue_wait_p50_ms": _n(hmed(runs, "replica_queue_wait", 0.5)),
        "replica_log_queue_wait_p99_ms": _n(hmed(runs, "replica_queue_wait", 0.99)),
        "replica_log_commit_p50_ms": _n(hmed(runs, "replica_commit", 0.5)),
        "replica_log_commit_p99_ms": _n(hmed(runs, "replica_commit", 0.99)),
        "replica_log_commit_max_ms": _n(hmed(runs, "replica_commit", 1.0)),
        "storage_persist_fsync_p50_ms": _n(hmed(runs, "persist_fsync", 0.5)),
        "storage_persist_fsync_p99_ms": _n(hmed(runs, "persist_fsync", 0.99)),
        "replica_postcommit_p50_ms": _n(hmed(runs, "postcommit", 0.5)),
        "learner_integrate_p50_ms": _n(hmed(runs, "learner_integrate", 0.5)),
        "learner_integrate_p99_ms": _n(hmed(runs, "learner_integrate", 0.99)),
        "replica_log_batch_size_mean": _n(rmed(runs, "repl_batch"), 2),
        "persist_per_sec": _n(rmed(runs, "persist_rate"), 1),
        "cpu_cores_used_mean": _n(cpu_med),
        "writes_per_core_per_sec": _n((thr_med / cpu_med) if (thr_med and cpu_med) else None, 0),
        # failure classification (schema v2)
        "error_kind_top": err_top,
        "raft_leader_changes_per_sec": _n(rmed(runs, "raft_leader_changes"), 3),
        "raft_config_generation": _n(rmed(runs, "raft_config_generation"), 0),
        "fanout_ack_per_sec": _n(fanout.get("ack"), 1),
        "fanout_rejected_per_sec": _n(fanout.get("rejected"), 2),
        "fanout_unreachable_per_sec": _n(fanout.get("unreachable"), 2),
        "replica_fail_hlc_cutoff_per_sec": _n(replica.get("fail_hlc_cutoff"), 2),
        "replica_fail_clock_skew_per_sec": _n(replica.get("fail_clock_skew"), 2),
        "replica_fail_not_replica_per_sec": _n(replica.get("fail_not_replica"), 2),
        "replica_fail_storage_per_sec": _n(replica.get("fail_storage"), 2),
    }
    return row


def append_csv(row):
    expected = ",".join(COLUMNS)
    exists = os.path.exists(CSV_PATH)
    if exists:
        with open(CSV_PATH) as f:
            header = f.readline().rstrip("\n")
        if header != expected:
            print(f"WARNING: {CSV_PATH} header does not match schema v{SCHEMA_VERSION}; "
                  f"refusing to append (resolve the schema mismatch first).", file=sys.stderr)
            return False
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in COLUMNS})
    return True


# ---------------------------------------------------------------------------
# Context + scenario specs.
# ---------------------------------------------------------------------------
def build_ctx(binp, replicas, writers, dataroot, dur, reps, max_extra,
              cv_threshold, pin, comment, extra, log=print, min_window_frac=0.5):
    """Construct the run context (shared by the CLI and the TUI)."""
    rust_log = os.environ.get("RUST_LOG", "error,loadgen=warn")
    if not os.path.exists(binp):
        raise FileNotFoundError(
            f"loadgen binary not found at {binp}\n"
            f"  build it: cargo build --release --example loadgen\n"
            f"  or set LOADGEN_BIN / CARGO_TARGET_DIR.")
    os.environ["RUST_LOG"] = rust_log
    return {
        "bin": binp,
        "common": ["--replicas", str(replicas), "--writers", str(writers)],
        "extra": list(extra),
        "dataroot": dataroot,
        "dur": dur,
        "reps": reps,
        "max_extra": max_extra,
        "cv_threshold": cv_threshold,
        "pin": pin,
        "comment": comment,
        "rust_log": rust_log,
        "timeout": int(os.environ.get("RUN_TIMEOUT", str(dur + 40))),
        "log": log,
        "min_window_frac": min_window_frac,
    }


def make_ctx(args, extra):
    try:
        return build_ctx(args.bin, args.replicas, args.writers, args.dataroot, args.dur,
                         args.reps, args.max_extra_reps, args.cv_threshold, args.pin_cpus,
                         args.comment, extra, min_window_frac=args.min_window_frac)
    except FileNotFoundError as e:
        sys.exit(str(e))


# High-throughput saturation produces data faster than purge reclaims it during a
# short run (measured ~0.8-1 GB/s at i512/i1024: 6-node replication x LSM write-amp,
# purge debounced to 30s, so nothing is reclaimed mid-run). On a space-constrained
# data dir (e.g. a 16 GB RAM-backed tmpfs) a full 30s run OOMs the box. So the cap
# is driven by ACTUAL FREE SPACE on the data dir, not hardcoded per inflight: on a
# roomy box the full --dur always fits and nothing is capped; only a tight box
# shortens high-inflight runs to a footprint that fits. The crash guard's floor is
# computed against this effective duration, so a deliberately-capped run still
# counts as a full window (a truncated/OOM run does not).
# Data-rate estimate at high-inflight saturation, measured at the loadgen's
# DEFAULT payload (avg ~2 KB). The footprint scales linearly with payload size,
# so the cap below scales this estimate by the actual avg payload — tiny payloads
# (e.g. 64 B) produce ~30x less data and run the full --dur.
HIGH_INFLIGHT_PEAK_BYTES_PER_SEC = 0.8e9  # at ~2 KB avg payload
DEFAULT_AVG_PAYLOAD = (64 + 4096) / 2     # loadgen default --payload-min/max midpoint
DATA_BUDGET_FRAC = 0.6                     # use at most this share of free space


def scenario_dur(scn, ctx):
    """Effective duration: the requested --dur, shortened ONLY when a saturating
    scenario's estimated data footprint would exceed the free space on the data
    dir. The footprint estimate scales with the actual payload size, so low/medium
    inflight, tiny payloads, and roomy data dirs all run the full --dur."""
    cfg = scn["flags"] + ctx.get("extra", [])
    v = flag_val(cfg, "--max-inflight")
    inflight = int(v) if v and v.isdigit() else 0
    if inflight < 256:  # modest data rate at low/medium inflight — never cap
        return ctx["dur"]
    pmin = flag_val(cfg, "--payload-min")
    pmax = flag_val(cfg, "--payload-max")
    pmin = int(pmin) if pmin and pmin.isdigit() else 64
    pmax = int(pmax) if pmax and pmax.isdigit() else 4096
    avg_payload = max((pmin + pmax) / 2, 1)
    rate = HIGH_INFLIGHT_PEAK_BYTES_PER_SEC * avg_payload / DEFAULT_AVG_PAYLOAD
    try:
        free = shutil.disk_usage(ctx["dataroot"]).free
    except OSError:
        return ctx["dur"]
    max_dur = int(free * DATA_BUDGET_FRAC / rate)
    return min(ctx["dur"], max(max_dur, 4))  # floor 4s so a capped run is still usable


def sat_scenario(inflight):
    return {
        "name": f"sat_inflight{inflight}",
        "mode": "saturate",
        "flags": ["--saturate", "--max-inflight", str(inflight)],
        "signal": "client_p50" if inflight == 1 else "throughput",
    }


SWEEP_SCENARIOS = [
    {"name": "idle", "mode": "idle", "flags": ["--target-rps", "0"], "signal": None},
    {"name": "paced_50rps", "mode": "paced", "flags": ["--target-rps", "50"], "signal": "throughput"},
    sat_scenario(1),
    sat_scenario(16),
    sat_scenario(128),
    sat_scenario(512),
    sat_scenario(1024),
]

# One-line explanation per standard scenario, for the TUI and `--help`-style use.
EXPLANATIONS = {
    "idle": "No writes — background CPU floor (control plane + replication).",
    "paced_50rps": "Fixed 50 writes/s offered — low-rate latency, no saturation.",
    "sat_inflight1": "1 write in flight — the latency floor (noise-immune signal).",
    "sat_inflight16": "16 in flight — light concurrency.",
    "sat_inflight128": "128 in flight — moderate saturation; throughput regime.",
    "sat_inflight512": "512 in flight — heavy saturation.",
    "sat_inflight1024": "1024 in flight — peak/overload; tail latency surfaces here.",
}


def run_and_emit(scn, ctx, write_csv):
    print(f"\n########## {scn['name']} ##########")
    runs, jsonls, cpus, extra = run_scenario(scn, ctx)
    report(jsonls, cpus)
    if write_csv:
        row = build_row(scn, ctx, runs, cpus, extra)
        if append_csv(row):
            print(f"  → appended row to {os.path.relpath(CSV_PATH, REPO_ROOT)} "
                  f"(variance={row['variance_flag']})")


# ---------------------------------------------------------------------------
# Subcommands.
# ---------------------------------------------------------------------------
def cmd_run(args, extra):
    ctx = make_ctx(args, extra)
    if args.target_rps is not None:
        scn = {"name": f"paced_{args.target_rps}rps", "mode": "paced",
               "flags": ["--target-rps", str(args.target_rps)], "signal": "throughput"}
    else:
        scn = sat_scenario(args.inflight)
    run_and_emit(scn, ctx, not args.no_csv)


def cmd_sweep(args, extra):
    ctx = make_ctx(args, extra)
    only = set(args.only.split(",")) if args.only else None
    for scn in SWEEP_SCENARIOS:
        if only and scn["name"] not in only:
            continue
        run_and_emit(scn, ctx, not args.no_csv)
    print(f"\nALL DONE — timeline: {os.path.relpath(CSV_PATH, REPO_ROOT)}")


def cmd_compare(args, extra):
    ctx = make_ctx(args, extra)
    flags_a = args.a.split()
    flags_b = args.b.split()
    base = ["--saturate", "--max-inflight", str(args.inflight)]
    scn_a = {"name": "cmp_A", "mode": "saturate", "flags": base + flags_a,
             "signal": "client_p50" if args.inflight == 1 else "throughput"}
    scn_b = {"name": "cmp_B", "mode": "saturate", "flags": base + flags_b,
             "signal": "client_p50" if args.inflight == 1 else "throughput"}
    eff_dur = scenario_dur(scn_a, ctx)
    # Interleave reps for noise immunity (entry 7): A, B, A, B, ...
    runs_a, jsonls_a, cpus_a = [], [], []
    runs_b, jsonls_b, cpus_b = [], [], []
    for i in range(1, args.reps + 1):
        for scn, jl, cl in ((scn_a, jsonls_a, cpus_a), (scn_b, jsonls_b, cpus_b)):
            dd = os.path.join(ctx["dataroot"], f"{scn['name']}-rep{i}")
            od = os.path.join(REPO_ROOT, "results/perf", scn["name"], f"rep{i}")
            shutil.rmtree(dd, ignore_errors=True)
            j, samples = run_rep(ctx["bin"], scn["flags"], ctx["common"], ctx["extra"],
                                 dd, od, eff_dur, ctx["timeout"], ctx["pin"])
            res = analyze_one(j)
            enforce_window(res, ctx, scn["name"], i, eff_dur)
            jl.append(j)
            cl.append(cpu_cores_in_window(samples, res["_t0"], res["_t1"]))
            print(f"  rep{i} {scn['name']} done")
    print(f"\n# inflight={args.inflight}, {args.reps} reps each, interleaved")
    report(jsonls_a, cpus_a, label=f"A: '{args.a}'")
    print()
    report(jsonls_b, cpus_b, label=f"B: '{args.b}'")


def cmd_report(args, extra):
    report(list(args.paths) + list(extra))


def add_common(p, need_data=True):
    p.add_argument("--reps", type=int, default=5)
    p.add_argument("--dur", type=int, default=30, help="steady-state seconds per rep")
    p.add_argument("--dataroot", default=os.environ.get("LOADGEN_DATAROOT", "/workspace/.scratch"),
                   help="base dir for per-rep data (fast FS: tmpfs/NVMe)")
    p.add_argument("--pin-cpus", default=None, help="cpuset for taskset, e.g. 0-3")
    p.add_argument("--cv-threshold", type=float, default=0.15,
                   help="coefficient-of-variation gate on the robust signal")
    p.add_argument("--max-extra-reps", type=int, default=5)
    p.add_argument("--min-window-frac", type=float, default=0.5,
                   help="hard-fail a rep whose steady window < this fraction of --dur "
                        "(guards against truncated/early-death runs reporting mirages)")
    p.add_argument("--comment", default="", help="experiment note → CSV comment column")
    p.add_argument("--no-csv", action="store_true", help="do not append to perf-timeline.csv")
    p.add_argument("--bin", default=DEFAULT_BIN)
    p.add_argument("--replicas", type=int, default=int(os.environ.get("REPLICAS", "3")))
    p.add_argument("--writers", type=int, default=int(os.environ.get("WRITERS", "3")))


def main():
    ap = argparse.ArgumentParser(
        description="Consolidated loadgen performance harness (see docs/PERFORMANCE.md).",
        epilog="Unrecognised flags are forwarded verbatim to the loadgen binary.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="one scenario, N reps")
    add_common(p_run)
    p_run.add_argument("--inflight", type=int, default=16, help="saturate max-inflight (ignored with --target-rps)")
    p_run.add_argument("--target-rps", type=float, default=None, help="paced offered rate instead of saturate")
    p_run.set_defaults(func=cmd_run)

    p_sweep = sub.add_parser("sweep", help="the scenario matrix")
    add_common(p_sweep)
    p_sweep.add_argument("--only", default=None, help="comma-separated scenario names to run")
    p_sweep.set_defaults(func=cmd_sweep)

    p_cmp = sub.add_parser("compare", help="interleaved A/B of two flag-sets")
    add_common(p_cmp)
    p_cmp.add_argument("--a", required=True, help="extra loadgen flags for variant A (quoted)")
    p_cmp.add_argument("--b", required=True, help="extra loadgen flags for variant B (quoted)")
    p_cmp.add_argument("--inflight", type=int, default=128)
    p_cmp.set_defaults(func=cmd_compare)

    p_rep = sub.add_parser("report", help="analyse existing metrics.jsonl file(s)")
    p_rep.add_argument("paths", nargs="*")
    p_rep.set_defaults(func=cmd_report)

    args, extra = ap.parse_known_args()
    args.func(args, extra)


if __name__ == "__main__":
    main()
