#!/usr/bin/env python3
"""Static HTML+SVG performance report for a single loadgen run directory.

Usage:
    report.py <run_dir>

Reads (all but metrics.jsonl are optional and degrade gracefully):
    <run_dir>/metrics.jsonl      raw metric samples (required)
    <run_dir>/phases.json        [{index,name,target_rps,start_ms,end_ms}]
    <run_dir>/run-meta.json      provenance + resolved config
    <run_dir>/heap-index.jsonl   [{ts_ms,seq,file}] + <run_dir>/heap/*.heap
    <run_dir>/*.svg              a --pprof CPU flamegraph

Writes:
    <run_dir>/report.html        one self-contained HTML file with inline SVG
                                  charts (native hover tooltips) and the
                                  flamegraph inlined as live, interactive SVG
                                  (its own embedded zoom/search JS, not an
                                  inert image). No LLM, no network, pure
                                  stdlib (imports the trusted analysis helpers
                                  from the sibling perf.py rather than
                                  duplicating the bucket/rate math; renders
                                  its own small markdown-subset -> HTML
                                  converter rather than adding a markdown
                                  library dependency).

Env:
    LOADGEN_BIN   path to the loadgen binary that produced the heap dumps
                  (needed to symbolize them with jeprof). Defaults to the
                  first of target/{profiling,release,debug}/examples/loadgen
                  that exists.
    JEPROF_BIN    jeprof executable name/path (default "jeprof").
"""

import glob
import html
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import perf  # noqa: E402  (reuse load/series_delta/hist_percentiles/etc — see CLAUDE.md's "reuse" rule)

# ---------------------------------------------------------------------------
# Palette — dataviz skill reference palette (light chart surface). A static
# markdown report has no reliable dark-mode signal to key off, so this ships
# one validated light-mode palette rather than guessing a theme.
# ---------------------------------------------------------------------------
CATEGORICAL = [
    "#2a78d6",  # blue
    "#1baf7a",  # aqua
    "#eda100",  # yellow
    "#008300",  # green
    "#4a3aa7",  # violet
    "#e34948",  # red
    "#e87ba4",  # magenta
    "#eb6834",  # orange
]
SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
FONT = "system-ui,-apple-system,'Segoe UI',sans-serif"

MY_QLABELS = ("p50", "p90", "p99", "p99.9", "p99.99", "max")  # zipped with perf.QKEYS


# ---------------------------------------------------------------------------
# Small formatting / IO helpers
# ---------------------------------------------------------------------------
def esc(s):
    return html.escape(str(s), quote=True)


def esc_code(s):
    """For content going inside a backtick code span: Markdown code spans are
    verbatim text that the renderer HTML-escapes on its own when converting
    to HTML, so pre-escaping with `esc()` double-escapes (`<` shows up as the
    literal text `&lt;`). The only thing that actually needs guarding inside
    a markdown *table cell* is a literal `|`, which would otherwise be read
    as a column delimiter even inside a code span."""
    return str(s).replace("|", "\\|")


def load_json_maybe(path):
    if not os.path.isfile(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def fmt_num(v):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)


def fmt_compact(v):
    if v is None:
        return "—"
    av = abs(v)
    if av >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if av >= 1_000:
        return f"{v / 1_000:.2f}k"
    if float(v).is_integer():
        return f"{int(v)}"
    return f"{v:.2f}"


def fmt_bytes(v):
    if v is None:
        return "—"
    av = abs(v)
    if av >= 1024**3:
        return f"{v / 1024**3:.2f}GiB"
    if av >= 1024**2:
        return f"{v / 1024**2:.2f}MiB"
    if av >= 1024:
        return f"{v / 1024:.2f}KiB"
    return f"{v:.0f}B"


def fmt_ms(v):
    if v is None:
        return "—"
    if math.isinf(v):
        return "∞"  # quantile fell in the histogram's +Inf overflow bucket
    return f"{v:.2f}" if v < 100 else f"{v:.0f}"


def fmt_dur_ms(ms):
    s = ms / 1000.0
    if s < 90:
        return f"{s:.0f}s"
    return f"{s / 60.0:.1f}m"


# ---------------------------------------------------------------------------
# Time-series extraction (builds on perf.py's `match` helper; perf.py's own
# helpers are windowed-scalar reducers, these return full point series).
# ---------------------------------------------------------------------------
def counter_series(rows, name, flt, t0, t1):
    """Sorted (ts_ms, cumulative value) points covering [t0,t1], anchored with
    the last point at/before t0 so the first in-window delta is meaningful."""
    pts = sorted((r["ts_ms"], r["value"]) for r in rows if r["name"] == name and perf.match(r["labels"], flt))
    if not pts:
        return []
    anchor = None
    for ts, v in pts:
        if ts <= t0:
            anchor = (ts, v)
    windowed = [p for p in pts if t0 <= p[0] <= t1]
    if anchor is not None and (not windowed or windowed[0][0] != anchor[0]):
        windowed = [anchor] + windowed
    return windowed


def rate_from_counter(points):
    """[(ts_ms, rate_per_sec)] from adjacent deltas of a monotonic series."""
    out = []
    for (t0, v0), (t1, v1) in zip(points, points[1:]):
        dt = (t1 - t0) / 1000.0
        if dt > 0:
            out.append((t1, max(0.0, (v1 - v0) / dt)))
    return out


def gauge_series(rows, name, flt, t0, t1):
    return sorted(
        (r["ts_ms"], r["value"]) for r in rows if r["name"] == name and perf.match(r["labels"], flt) and t0 <= r["ts_ms"] <= t1
    )


def rolling_mean_from_hist(rows, base, flt, t0, t1):
    """[(ts_ms, mean_ms)] of a histogram's mean latency between adjacent
    samples — the finest-grained latency-over-time signal available (one point
    per sample interval). See `rolling_percentiles` for a coarser but
    distribution-aware companion (p50/p90/p99 over sliding windows)."""
    sum_pts = counter_series(rows, base + "_sum", flt, t0, t1)
    cnt_by_ts = dict(counter_series(rows, base + "_count", flt, t0, t1))
    out = []
    for (ts0, s0), (ts1, s1) in zip(sum_pts, sum_pts[1:]):
        c0, c1 = cnt_by_ts.get(ts0), cnt_by_ts.get(ts1)
        if c0 is None or c1 is None:
            continue
        dc = c1 - c0
        if dc > 0:
            out.append((ts1, (s1 - s0) / dc * 1000.0))
    return out


def rolling_percentiles(rows, base, flt, t0, t1, qs=(0.5, 0.9, 0.99)):
    """{q: [(ts_mid, ms), ...]} — true percentiles (via perf.hist_percentiles'
    bucket-delta math) over sliding windows, not just the mean. Window/step
    auto-scale to the run length (~30 windows) so this reads sensibly on both
    a 20s smoke test and an 18-minute scenario."""
    span = max(1000, t1 - t0)
    window_ms = max(5000, span // 30)
    step_ms = max(1, window_ms // 2)
    series = {q: [] for q in qs}
    t = t0
    while t < t1:
        w1 = min(t1, t + window_ms)
        h = perf.hist_percentiles(rows, base, flt, t, w1, qs=qs)
        if h:
            mid = (t + w1) // 2
            for q in qs:
                if q in h:
                    series[q].append((mid, h[q]))
        t += step_ms
    return series


def coefficient_of_variation(values):
    values = [v for v in values if v is not None]
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    if mean == 0:
        return None
    return statistics.pstdev(values) / mean


# ---------------------------------------------------------------------------
# SVG chart primitives
# ---------------------------------------------------------------------------
def nice_ticks(vmin, vmax, n=4):
    if vmax <= vmin:
        vmax = vmin + 1.0
    raw_step = (vmax - vmin) / n
    if raw_step <= 0:
        raw_step = 1.0
    mag = 10 ** math.floor(math.log10(raw_step))
    step = mag
    for mult in (1, 2, 5, 10):
        step = mag * mult
        if step >= raw_step:
            break
    start = math.floor(vmin / step) * step
    ticks = []
    v = start
    while v <= vmax + step * 1e-6 and len(ticks) < 20:
        ticks.append(v)
        v += step
    return ticks or [vmin, vmax]


def _phase_bands(phases, t0, t1, X, y0, h):
    out = []
    if not phases:
        return out
    for i, ph in enumerate(phases):
        if i % 2 != 1:
            continue
        x0, x1 = X(max(t0, ph["start_ms"])), X(min(t1, ph["end_ms"]))
        out.append(f'<rect x="{x0:.1f}" y="{y0}" width="{max(0.0, x1 - x0):.1f}" height="{h}" fill="{INK_PRIMARY}" opacity="0.035"/>')
    return out


def _phase_boundaries(phases, X, y0, h):
    out = []
    for ph in phases or []:
        xb = X(ph["start_ms"])
        out.append(f'<line x1="{xb:.1f}" y1="{y0}" x2="{xb:.1f}" y2="{y0 + h}" stroke="{BASELINE}" stroke-width="1" stroke-dasharray="2,2"/>')
        out.append(
            f'<text x="{xb + 3:.1f}" y="{y0 + h + 14}" font-size="10" fill="{INK_MUTED}" font-family="{FONT}">{esc(ph["name"])}</text>'
        )
    return out


def line_chart(series, *, title, y_label="", width=760, height=220, phases=None, t0=None, t1=None, y_fmt=None):
    """series: [{"name","color","points":[(ts_ms,value)]}]. A phase-shaded
    multi-line chart with gridlines, y ticks, end markers, and a legend."""
    y_fmt = y_fmt or fmt_compact
    pts_all = [p for s in series for p in s["points"]]
    if not pts_all:
        return f'<p><em>{esc(title)} — no data.</em></p>'
    t0 = t0 if t0 is not None else min(p[0] for p in pts_all)
    t1 = t1 if t1 is not None else max(p[0] for p in pts_all)
    if t1 <= t0:
        t1 = t0 + 1
    vmin = min(0.0, min(v for _, v in pts_all))
    vmax = max(v for _, v in pts_all)
    ticks = nice_ticks(vmin, vmax, 4)
    vmin_t, vmax_t = min(ticks[0], vmin), max(ticks[-1], vmax)
    if vmax_t <= vmin_t:
        vmax_t = vmin_t + 1.0

    pad_l, pad_r, pad_t, pad_b = 88, 130 if len(series) > 1 else 20, 30, 26
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    def X(ts):
        return pad_l + (ts - t0) / (t1 - t0) * plot_w

    def Y(v):
        return pad_t + plot_h - (v - vmin_t) / (vmax_t - vmin_t) * plot_h

    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="{esc(title)}">']
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="{SURFACE}"/>')
    parts += _phase_bands(phases, t0, t1, X, pad_t, plot_h)
    for tck in ticks:
        y = Y(tck)
        parts.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{pad_l - 6}" y="{y + 4:.1f}" text-anchor="end" font-size="11" fill="{INK_MUTED}" font-family="{FONT}">{esc(y_fmt(tck))}</text>')
    parts.append(f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{width - pad_r}" y2="{pad_t + plot_h}" stroke="{BASELINE}" stroke-width="1"/>')
    parts.append(f'<text x="{pad_l}" y="16" font-size="12" font-weight="600" fill="{INK_PRIMARY}" font-family="{FONT}">{esc(title)}</text>')
    if y_label:
        parts.append(f'<text x="{width - pad_r}" y="16" text-anchor="end" font-size="10" fill="{INK_SECONDARY}" font-family="{FONT}">{esc(y_label)}</text>')
    parts += _phase_boundaries(phases, X, pad_t, plot_h)

    for s in series:
        pts = sorted(s["points"])
        if not pts:
            continue
        d = " ".join(f'{"M" if i == 0 else "L"}{X(t):.1f},{Y(v):.1f}' for i, (t, v) in enumerate(pts))
        parts.append(f'<path d="{d}" fill="none" stroke="{s["color"]}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
        lt, lv = pts[-1]
        parts.append(f'<circle cx="{X(lt):.1f}" cy="{Y(lv):.1f}" r="4" fill="{s["color"]}" stroke="{SURFACE}" stroke-width="2"/>')

    if len(series) > 1:
        ly = pad_t
        for s in series:
            parts.append(f'<circle cx="{width - pad_r + 10}" cy="{ly}" r="4" fill="{s["color"]}"/>')
            parts.append(f'<text x="{width - pad_r + 18}" y="{ly + 4}" font-size="10" fill="{INK_SECONDARY}" font-family="{FONT}">{esc(s["name"])}</text>')
            ly += 14
    parts.append("</svg>")
    return "".join(parts)


def stacked_area_chart(dump_rows, order, colors, *, title, width=760, height=280, phases=None):
    """dump_rows: [{"ts_ms":..., "totals": {name: bytes}}]. Renders a stacked
    area of `order` layers using each layer's fixed `colors[name]`."""
    dump_rows = sorted(dump_rows, key=lambda r: r["ts_ms"])
    if not dump_rows:
        return f'<p><em>{esc(title)} — no data.</em></p>'
    t0, t1 = dump_rows[0]["ts_ms"], dump_rows[-1]["ts_ms"]
    if t1 <= t0:
        t1 = t0 + 1
    stacks = []
    for r in dump_rows:
        cum = 0.0
        layer = {}
        for name in order:
            v = r["totals"].get(name, 0)
            layer[name] = (cum, cum + v)
            cum += v
        stacks.append((r["ts_ms"], layer, cum))
    vmax = max(s[2] for s in stacks) or 1.0
    ticks = nice_ticks(0, vmax, 4)
    vmax_t = max(ticks[-1], vmax)

    pad_l, pad_r, pad_t, pad_b = 88, 150, 30, 26
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    def X(ts):
        return pad_l + (ts - t0) / (t1 - t0) * plot_w

    def Y(v):
        return pad_t + plot_h - (v / vmax_t) * plot_h

    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="{esc(title)}">']
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="{SURFACE}"/>')
    parts += _phase_bands(phases, t0, t1, X, pad_t, plot_h)
    for tck in ticks:
        y = Y(tck)
        parts.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{pad_l - 6}" y="{y + 4:.1f}" text-anchor="end" font-size="11" fill="{INK_MUTED}" font-family="{FONT}">{esc(fmt_bytes(tck))}</text>')
    parts.append(f'<text x="{pad_l}" y="16" font-size="12" font-weight="600" fill="{INK_PRIMARY}" font-family="{FONT}">{esc(title)}</text>')
    parts += _phase_boundaries(phases, X, pad_t, plot_h)

    for name in order:
        color = colors.get(name, INK_MUTED)
        top = [(t, layer[name][1]) for t, layer, _ in stacks]
        bot = [(t, layer[name][0]) for t, layer, _ in stacks]
        d = " ".join(f'{"M" if i == 0 else "L"}{X(t):.1f},{Y(v):.1f}' for i, (t, v) in enumerate(top))
        d += " " + " ".join(f'L{X(t):.1f},{Y(v):.1f}' for t, v in reversed(bot))
        d += " Z"
        parts.append(f'<path d="{d}" fill="{color}" opacity="0.88" stroke="{SURFACE}" stroke-width="1"/>')

    ly = pad_t
    for name in order:
        color = colors.get(name, INK_MUTED)
        parts.append(f'<rect x="{width - pad_r + 10}" y="{ly - 8}" width="10" height="10" fill="{color}"/>')
        parts.append(f'<text x="{width - pad_r + 24}" y="{ly}" font-size="10" fill="{INK_SECONDARY}" font-family="{FONT}">{esc(name)}</text>')
        ly += 14
    parts.append(f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{width - pad_r}" y2="{pad_t + plot_h}" stroke="{BASELINE}" stroke-width="1"/>')
    parts.append("</svg>")
    return "".join(parts)


def gantt(phases, width=760, height=64):
    if not phases:
        return ""
    t0, t1 = phases[0]["start_ms"], phases[-1]["end_ms"]
    if t1 <= t0:
        t1 = t0 + 1
    pad_l, pad_r = 8, 8
    plot_w = width - pad_l - pad_r

    def X(ts):
        return pad_l + (ts - t0) / (t1 - t0) * plot_w

    bar_y, bar_h = 18, 20
    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="phase timeline">']
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="{SURFACE}"/>')
    for i, ph in enumerate(phases):
        x0, x1 = X(ph["start_ms"]), X(ph["end_ms"])
        color = CATEGORICAL[i % len(CATEGORICAL)]
        parts.append(f'<rect x="{x0:.1f}" y="{bar_y}" width="{max(1.0, x1 - x0 - 2):.1f}" height="{bar_h}" rx="3" fill="{color}"/>')
        label = f'{ph["name"]} ({fmt_num(ph.get("target_rps"))}rps, {fmt_dur_ms(ph["end_ms"] - ph["start_ms"])})'
        parts.append(
            f'<text x="{(x0 + x1) / 2:.1f}" y="{bar_y + bar_h + 14}" text-anchor="middle" font-size="10" fill="{INK_SECONDARY}" font-family="{FONT}">{esc(label)}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------
FAST_FILESYSTEMS = {"tmpfs", "ramfs"}


def fs_type_for_path(path):
    """Best-effort filesystem type for `path`'s mount, by longest-prefix match
    against /proc/mounts. Returns None off Linux or if unreadable — this is a
    diagnostic nicety, not something worth failing the report over."""
    try:
        with open("/proc/mounts") as f:
            mounts = [line.split() for line in f if line.strip()]
    except OSError:
        return None
    path = os.path.abspath(path)
    best = None
    for parts in mounts:
        if len(parts) < 3:
            continue
        mnt, fstype = parts[1], parts[2]
        if path == mnt or path.startswith(mnt.rstrip("/") + "/") or mnt == "/":
            if best is None or len(mnt) > len(best[0]):
                best = (mnt, fstype)
    return best[1] if best else None


def header_section(run_dir, run_meta):
    lines = ["# Loadgen performance report", "", f"`{esc(run_dir)}`", ""]
    if not run_meta:
        lines.append("*(no `run-meta.json` found — provenance unavailable; report is metrics-only)*")
        return "\n".join(lines)
    lines += ["| field | value |", "|---|---|"]
    for k in (
        "hostname", "git_commit", "git_branch", "git_dirty", "replicas", "writers",
        "target_rps", "saturate", "max_inflight", "duration_secs", "warmup_secs",
        "payload_min", "payload_max", "seed", "shared_runtime",
    ):
        if k in run_meta:
            lines.append(f"| {k} | {esc(run_meta[k])} |")
    if run_meta.get("load_profile"):
        prof = ", ".join(f'{p["name"]}:{p["rps"]}@{p["dur_secs"]}s' for p in run_meta["load_profile"])
        lines.append(f"| load_profile | {esc(prof)} |")
    data_dir = run_meta.get("data_dir")
    if data_dir:
        fstype = fs_type_for_path(data_dir)
        label = f"{data_dir} ({fstype})" if fstype else data_dir
        lines.append(f"| data_dir | {esc(label)} |")
        if fstype and fstype not in FAST_FILESYSTEMS:
            lines.append(
                "\n> ⚠ **`data_dir` is not on tmpfs/ramfs** (`"
                + esc(fstype)
                + "`). loadgen.rs's own docs warn that overlay/FUSE/network "
                "mounts can spike fsync latency and trigger write-stall cascades — "
                "treat the latency numbers below with that caveat. Memory/heap "
                "findings are unaffected (they don't depend on storage speed)."
            )
    return "\n".join(lines)


def phase_timeline_section(phases):
    lines = ["## Phase timeline", "", gantt(phases), "", "| # | name | target rps | duration | start (ms) | end (ms) |", "|---|---|---|---|---|---|"]
    for ph in phases:
        lines.append(
            f'| {ph.get("index", "")} | {esc(ph["name"])} | {fmt_num(ph.get("target_rps"))} | '
            f'{fmt_dur_ms(ph["end_ms"] - ph["start_ms"])} | {ph["start_ms"]} | {ph["end_ms"]} |'
        )
    return "\n".join(lines)


def per_phase_section(rows, phases):
    lines = ["## Per-phase summary", ""]
    for ph in phases:
        t0, t1 = ph["start_ms"], ph["end_ms"]
        secs = max(1.0, (t1 - t0) / 1000.0)
        lines.append(f'### Phase {ph.get("index", "")}: {ph["name"]} (target {fmt_num(ph.get("target_rps"))} rps, {fmt_dur_ms(t1 - t0)})')
        lines.append("")

        lines += ["| metric | value |", "|---|---|"]
        for _key, disp, name, flt in perf.RATES:
            d = perf.series_delta(rows, name, flt, t0, t1)
            v = (d / secs) if d is not None else None
            lines.append(f"| {esc(disp.strip())} | {fmt_compact(v)} |")
        pts = counter_series(rows, "loadgen_writes_total", {}, t0, t1)
        cv = coefficient_of_variation([r for _, r in rate_from_counter(pts)])
        lines.append(f"| throughput CV (variance) | {f'{cv:.3f}' if cv is not None else '—'} |")
        lines.append("")

        errs = perf.labeled_rate_breakdown(rows, "loadgen_errors_total", "kind", t0, t1, secs)
        if errs:
            lines += ["**errors by kind (per s):**", "", "| kind | rate |", "|---|---|"]
            for k, v in sorted(errs.items(), key=lambda kv: -kv[1]):
                lines.append(f"| {esc(k)} | {v:.2f} |")
            lines.append("")

        lines += [
            "**latency distribution (ms):**",
            "",
            "| metric | " + " | ".join(MY_QLABELS) + " | mean |",
            "|---|" + "---|" * (len(MY_QLABELS) + 1),
        ]
        for _key, disp, base, flt in perf.HISTS:
            h = perf.hist_percentiles(rows, base, flt, t0, t1)
            if not h:
                continue
            row = [esc(disp.strip())] + [fmt_ms(h.get(q)) for q in perf.QKEYS] + [fmt_ms(h.get("mean"))]
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")
    return "\n".join(lines)


KEY_COUNTERS = [
    ("achieved writes/s", "loadgen_writes_total", {}),
    ("offered writes/s", "loadgen_offered_total", {}),
    ("backpressure/s", "loadgen_backpressure_total", {}),
]
KEY_PERSIST = [
    ("persist(fsync)/s", "storage_persist_duration_seconds_count", {"state": "success"}),
]
KEY_LATENCY = [
    ("client write (mean)", "loadgen_write_latency_seconds", {}),
    ("persist/fsync (mean)", "storage_persist_duration_seconds", {"state": "success"}),
    ("replica log append (mean)", "replica_log_append_duration_seconds", {"state": "success"}),
]
KEY_MEMORY = [
    ("RSS", "process_memory_rss_bytes", {}),
    ("anon", "process_memory_anon_bytes", {}),
    ("jemalloc allocated", "jemalloc_allocated_bytes", {}),
    ("jemalloc resident", "jemalloc_resident_bytes", {}),
]
KEY_SATURATION = [
    ("writes in flight", "writes_in_flight", {}),
    ("replica log queue", "replica_log_queue_depth", {}),
    ("write pool inflight", "write_pool_inflight_requests", {}),
    ("data fetch queue", "data_fetch_queue_depth", {}),
]


def _run_window(rows):
    all_t = [r["ts_ms"] for r in rows]
    return (min(all_t), max(all_t)) if all_t else (0, 0)


def throughput_section(rows, phases, t0, t1):
    lines = ["## Throughput", ""]
    series = []
    for i, (name, metric, flt) in enumerate(KEY_COUNTERS + KEY_PERSIST):
        pts = rate_from_counter(counter_series(rows, metric, flt, t0, t1))
        if pts:
            series.append({"name": name, "color": CATEGORICAL[i % len(CATEGORICAL)], "points": pts})
    lines += [
        line_chart(series, title="Writes/s: achieved, offered, backpressure, fsync", y_label="/s", height=260, phases=phases, t0=t0, t1=t1),
        "",
    ]

    # Achieved-vs-offered on its own: the gap between the two lines *is* the
    # backpressure story, easier to read isolated than buried in the 4-line chart above.
    achieved = rate_from_counter(counter_series(rows, "loadgen_writes_total", {}, t0, t1))
    offered = rate_from_counter(counter_series(rows, "loadgen_offered_total", {}, t0, t1))
    gap_series = []
    if achieved:
        gap_series.append({"name": "achieved writes/s", "color": CATEGORICAL[0], "points": achieved})
    if offered:
        gap_series.append({"name": "offered writes/s", "color": CATEGORICAL[2], "points": offered})
    lines.append(line_chart(gap_series, title="Achieved vs. offered rate (gap = backpressure)", y_label="/s", height=260, phases=phases, t0=t0, t1=t1))
    return "\n".join(lines)


def latency_section(rows, phases, t0, t1):
    lines = ["## Write latency", ""]
    mean_series = []
    for i, (name, base, flt) in enumerate(KEY_LATENCY):
        pts = rolling_mean_from_hist(rows, base, flt, t0, t1)
        if pts:
            mean_series.append({"name": name, "color": CATEGORICAL[i % len(CATEGORICAL)], "points": pts})
    lines += [
        line_chart(mean_series, title="Mean write latency (rolling, per-sample)", y_label="ms", height=260, phases=phases, t0=t0, t1=t1),
        "",
    ]

    q_labels = {0.5: "p50", 0.9: "p90", 0.99: "p99"}
    pct = rolling_percentiles(rows, "loadgen_write_latency_seconds", {}, t0, t1, qs=tuple(q_labels))
    pct_series = []
    for i, q in enumerate(sorted(q_labels)):
        pts = pct[q]
        if pts:
            pct_series.append({"name": f"client write {q_labels[q]}", "color": CATEGORICAL[(i + 3) % len(CATEGORICAL)], "points": pts})
    lines.append(
        line_chart(pct_series, title="Client write latency — p50/p90/p99 over time (sliding window)", y_label="ms", height=260, phases=phases, t0=t0, t1=t1)
    )
    return "\n".join(lines)


def consensus_section(rows, phases, t0, t1):
    lines = ["## HLC & consensus health", ""]

    drift_pts = gauge_series(rows, "hlc_wall_clock_drift_ms", {}, t0, t1)
    drift_series = (
        [{"name": "HLC wall-clock drift (this node's HLC vs its own wall clock)", "color": CATEGORICAL[0], "points": drift_pts}] if drift_pts else []
    )
    lines += [line_chart(drift_series, title="HLC wall-clock drift", y_label="ms", height=220, phases=phases, t0=t0, t1=t1), ""]

    behind = rate_from_counter(counter_series(rows, "hlc_drift_rejections_total", {"direction": "behind"}, t0, t1))
    ahead = rate_from_counter(counter_series(rows, "hlc_drift_rejections_total", {"direction": "ahead"}, t0, t1))
    rej_series = []
    if behind:
        rej_series.append({"name": "rejections/s (behind — usually recv-buffer latency)", "color": CATEGORICAL[5], "points": behind})
    if ahead:
        rej_series.append({"name": "rejections/s (ahead — genuine fast peer clock)", "color": CATEGORICAL[7], "points": ahead})
    lines += [
        line_chart(rej_series, title="HLC drift rejections (peer integrate() failures)", y_label="/s", height=220, phases=phases, t0=t0, t1=t1),
        "",
    ]

    raft_pts = rate_from_counter(counter_series(rows, "raft_applied_log_index", {}, t0, t1))
    raft_series = [{"name": "raft apply rate", "color": CATEGORICAL[0], "points": raft_pts}] if raft_pts else []
    lines += [line_chart(raft_series, title="Raft apply rate", y_label="/s", height=220, phases=phases, t0=t0, t1=t1), ""]

    lc_delta = perf.series_delta(rows, "raft_leader_changes_total", {}, t0, t1)
    is_leader = gauge_series(rows, "raft_is_leader", {}, t0, t1)
    has_leader = gauge_series(rows, "raft_has_leader", {}, t0, t1)
    term_pts = gauge_series(rows, "raft_term", {}, t0, t1)
    lines += ["**Leadership stability**", "", "| metric | value |", "|---|---|"]
    lc_display = f"{int(lc_delta)}" if lc_delta is not None else "0 (none observed — healthy for a stable single-leader run)"
    lines.append(f"| leader changes (whole run) | {lc_display} |")
    lines.append(f"| `raft_is_leader` at run end | {fmt_num(is_leader[-1][1]) if is_leader else '—'} |")
    lines.append(f"| `raft_has_leader` at run end | {fmt_num(has_leader[-1][1]) if has_leader else '—'} |")
    lines.append(f"| `raft_term` at run end | {fmt_num(term_pts[-1][1]) if term_pts else '—'} |")
    lines.append(
        "\n*(`raft_is_leader`/`raft_has_leader`/`raft_term` are unlabeled per-node gauges; "
        "in a single-process multi-replica loadgen run they reflect whichever node last "
        "wrote the shared metrics recorder, not a specific node — `raft_leader_changes_total` "
        "is the reliable whole-cluster signal.)*"
    )
    return "\n".join(lines)


def resource_section(rows, phases, t0, t1):
    lines = ["## Resource utilization", ""]

    cpu_pts = rate_from_counter(counter_series(rows, "process_cpu_seconds", {}, t0, t1))
    cpu_series = [{"name": "cpu cores used", "color": CATEGORICAL[0], "points": cpu_pts}] if cpu_pts else []
    lines += [line_chart(cpu_series, title="CPU (cores used)", y_label="cores", height=220, phases=phases, t0=t0, t1=t1), ""]

    mem = []
    for i, (name, metric, flt) in enumerate(KEY_MEMORY):
        pts = gauge_series(rows, metric, flt, t0, t1)
        if pts:
            mem.append({"name": name, "color": CATEGORICAL[i % len(CATEGORICAL)], "points": pts})
    lines += [line_chart(mem, title="Memory envelope", y_label="bytes", height=260, phases=phases, t0=t0, t1=t1, y_fmt=fmt_bytes), ""]

    sat = []
    for i, (name, metric, flt) in enumerate(KEY_SATURATION):
        pts = gauge_series(rows, metric, flt, t0, t1)
        if pts:
            sat.append({"name": name, "color": CATEGORICAL[i % len(CATEGORICAL)], "points": pts})
    lines.append(line_chart(sat, title="Saturation / queue depths", y_label="count", height=220, phases=phases, t0=t0, t1=t1))
    return "\n".join(lines)


def disk_section(rows, phases, t0, t1):
    lines = ["## Disk usage by component", ""]

    dense = []
    for log_type, name, color_i in [
        ("replica", "replica-log (dense)", 0),
        ("stable", "stable-log (dense)", 3),
        ("local", "local-log (dense)", 4),
    ]:
        pts = gauge_series(rows, "dense_log_disk_bytes", {"log_type": log_type}, t0, t1)
        if pts:
            dense.append({"name": name, "color": CATEGORICAL[color_i % len(CATEGORICAL)], "points": pts})
    if dense:
        lines += [
            line_chart(dense, title="Dense-log disk bytes (segment-file engine, by log type)", y_label="bytes", height=240, phases=phases, t0=t0, t1=t1, y_fmt=fmt_bytes),
            "",
        ]

    # One RocksDB per node, one column family per store: on-disk size is the
    # live-SST size per cf. Replaces the deleted `strata_disk_bytes{store}`.
    cf_lines = []
    for cf, color_i in [
        ("replica_log", 0),
        ("stable_log", 3),
        ("local_store_data", 4),
        ("local_store_cursors", 5),
        ("raft_log", 2),
        ("raft_meta", 1),
    ]:
        pts = gauge_series(rows, "storage_live_sst_files_size_bytes", {"cf": cf}, t0, t1)
        if pts:
            cf_lines.append({"name": cf, "color": CATEGORICAL[color_i % len(CATEGORICAL)], "points": pts})
    if cf_lines:
        lines += [
            line_chart(cf_lines, title="Live SST bytes (by column family)", y_label="bytes", height=240, phases=phases, t0=t0, t1=t1, y_fmt=fmt_bytes),
            "",
        ]

    # Peek values — absolute on-disk size at key moments, not a time-series.
    if dense or cf_lines:
        lines += [
            "**Disk size at phase boundaries**",
            "",
            "| component | " + " | ".join(f'end of "{esc(p["name"])}"' for p in phases) + " |",
            "|---|---|" + "---|" * len(phases),
        ]
        all_series = dense + cf_lines
        for s in all_series:
            row = [esc(s["name"])]
            pts = sorted(s["points"])
            for p in phases:
                row.append(fmt_bytes(value_at_or_before(pts, p["end_ms"])))
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")
    else:
        lines.append("*(no disk-usage metrics emitted for this run — the dense-log / live-SST disk gauges are only present in newer builds)*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Heap-by-subsystem attribution (jeprof-folded jemalloc dumps)
# ---------------------------------------------------------------------------
SUBSYSTEM_RULES = [
    ("replica-log", ["zslg::replica"]),
    ("local-store", ["zslg::learner::local_store", "zslg::learner"]),
    ("stable-log", ["stable_log"]),
    ("hlc", ["zslg::hlc"]),
    ("storage", ["zslg::database", "zslg::storage"]),
    ("raft", ["zslg::raft", "openraft"]),
    ("network", ["zslg::network", "zslg::rpc"]),
    ("rocksdb", ["rocksdb", "librocksdb", "ROCKSDB_NAMESPACE"]),
    ("runtime", ["tokio", "mio"]),
    ("std-alloc", ["alloc::", "core::", "std::", "hashbrown", "__rust"]),
]
GENERIC_SUBSYSTEMS = {"runtime", "std-alloc"}
SUBSYSTEM_COLOR = {}


def _assign_subsystem_colors():
    hue_i = 0
    for name, _ in SUBSYSTEM_RULES:
        if name in GENERIC_SUBSYSTEMS:
            continue
        SUBSYSTEM_COLOR[name] = CATEGORICAL[hue_i % len(CATEGORICAL)]
        hue_i += 1
    # Background/noise buckets fold into muted grays, not a 9th+ hue (dataviz
    # skill: an Nth categorical series is never a generated hue).
    SUBSYSTEM_COLOR["runtime"] = "#c3c2b7"
    SUBSYSTEM_COLOR["std-alloc"] = "#e1e0d9"
    SUBSYSTEM_COLOR["other"] = "#898781"


_assign_subsystem_colors()


def classify_stack(frames):
    """First app-subsystem match anywhere in the stack; falls back to a
    generic bucket. Order-independent (jeprof's collapsed-stack frame order
    isn't guaranteed root-to-leaf across versions), so this scans the whole
    stack for the most specific match rather than assuming a position."""
    for name, prefixes in SUBSYSTEM_RULES:
        if name in GENERIC_SUBSYSTEMS:
            continue
        if any(any(p in f for p in prefixes) for f in frames):
            return name
    for name, prefixes in SUBSYSTEM_RULES:
        if name not in GENERIC_SUBSYSTEMS:
            continue
        if any(any(p in f for p in prefixes) for f in frames):
            return name
    return "other"


def jeprof_collapsed(jeprof_bin, exe, heap_file):
    try:
        out = subprocess.run(
            [jeprof_bin, "--collapsed", exe, heap_file],
            capture_output=True, text=True, timeout=120, check=False,
        )
    except OSError as e:
        return None, str(e)
    if out.returncode != 0:
        return None, out.stderr.strip()[:300]
    return out.stdout, None


def parse_collapsed(collapsed_text):
    """[(bytes, frames)] — one entry per unique call stack. jeprof/flamegraph
    collapsed format is root-first, leaf-last (`frames[-1]` is the actual
    allocation site); `bytes` is that exact stack's own attribution, not an
    inclusive sum over descendants, so grouping by any single frame is a
    valid "self" aggregation."""
    out = []
    for line in collapsed_text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            stack, count_s = line.rsplit(" ", 1)
            count = int(count_s)
        except ValueError:
            continue
        out.append((count, stack.split(";")))
    return out


def subsystem_bytes_from_parsed(parsed):
    totals = defaultdict(int)
    for count, frames in parsed:
        totals[classify_stack(frames)] += count
    return dict(totals)


def guess_loadgen_bin():
    candidates = [os.environ.get("LOADGEN_BIN")]
    candidates += [
        os.path.join(perf.TARGET, p)
        for p in ("profiling/examples/loadgen", "release/examples/loadgen", "debug/examples/loadgen")
    ]
    for c in candidates:
        if c and os.path.isfile(c):
            return c
    return None


def value_at_or_before(series, ts):
    best = None
    for t, v in series:
        if t <= ts:
            best = v
        else:
            break
    return best


def compute_heap_data(run_dir, phases=None):
    """Returns (data, reason). `data` is None if unavailable (with `reason`
    explaining why — always a markdown-safe string); otherwise a dict with
    `dump_rows`, `all_names`, `peak`, `stack_order`, `series_by_name`. Shared
    by heap_section (chart+table) and callouts_section (drain-check)."""
    idx_path = os.path.join(run_dir, "heap-index.jsonl")
    if not os.path.isfile(idx_path):
        return None, (
            "no heap profile captured — build with `--features heap-profiling`, run with "
            "`MALLOC_CONF=prof:true,prof_active:true` and pass `--heap-profile-interval-secs`; "
            "`just perf-scenario` does all three"
        )
    entries = []
    with open(idx_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    if not entries:
        return None, "`heap-index.jsonl` present but empty"

    jeprof_bin = os.environ.get("JEPROF_BIN", "jeprof")
    if shutil.which(jeprof_bin) is None:
        return None, f"`{esc(jeprof_bin)}` not found on PATH — install gperftools' jeprof to symbolize heap dumps; skipping"
    exe = guess_loadgen_bin()
    if not exe:
        return None, "cannot locate the loadgen binary that produced these dumps — set `LOADGEN_BIN`"

    dump_rows = []
    for e in sorted(entries, key=lambda e: e["ts_ms"]):
        heap_path = os.path.join(run_dir, "heap", e["file"])
        if not os.path.isfile(heap_path):
            continue
        collapsed, _err = jeprof_collapsed(jeprof_bin, exe, heap_path)
        if collapsed is None:
            continue
        parsed = parse_collapsed(collapsed)
        dump_rows.append({"ts_ms": e["ts_ms"], "file": e["file"], "totals": subsystem_bytes_from_parsed(parsed)})
    if not dump_rows:
        return None, "heap dumps present but `jeprof` could not symbolize any of them — check `LOADGEN_BIN` matches the binary that produced them"

    all_names = set()
    for r in dump_rows:
        all_names |= set(r["totals"])
    peak = {n: max(r["totals"].get(n, 0) for r in dump_rows) for n in all_names}
    stack_order = sorted(all_names, key=lambda n: (n in GENERIC_SUBSYSTEMS or n == "other", -peak[n]))
    series_by_name = {n: sorted((r["ts_ms"], r["totals"].get(n, 0)) for r in dump_rows) for n in all_names}

    # Re-parse the steady-state "end of run" dump and the single-largest dump
    # (peak) to get full per-stack detail — cheap (1-2 extra jeprof calls) vs.
    # holding every dump's raw stacks in memory the whole time.
    #
    # "Steady-state end" is the last dump at-or-before the last phase's
    # end_ms, NOT simply dump_rows[-1]. The literal last dump can be taken
    # during process teardown: engine teardown releases its buffered
    # frames without issuing a final flush, which
    # frees a large chunk of heap that was never actually reclaimed by any
    # live-process eviction path. Using that dump as "end of run" understates
    # retained memory and silently disagrees with heap_section's phase-
    # boundary table, which already excludes it via `value_at_or_before`.
    last_row = dump_rows[-1]
    if phases:
        candidates = [r for r in dump_rows if r["ts_ms"] <= phases[-1]["end_ms"]]
        if candidates:
            last_row = candidates[-1]
    peak_row = max(dump_rows, key=lambda r: sum(r["totals"].values()))

    def _reload_parsed(row):
        collapsed, _err = jeprof_collapsed(jeprof_bin, exe, os.path.join(run_dir, "heap", row["file"]))
        return parse_collapsed(collapsed) if collapsed is not None else []

    raw_last = _reload_parsed(last_row)
    raw_peak = raw_last if peak_row["ts_ms"] == last_row["ts_ms"] else _reload_parsed(peak_row)

    return {
        "dump_rows": dump_rows,
        "all_names": all_names,
        "peak": peak,
        "stack_order": stack_order,
        "series_by_name": series_by_name,
        "raw_last": raw_last,
        "raw_peak": raw_peak,
        "last_ts": last_row["ts_ms"],
        "peak_ts": peak_row["ts_ms"],
    }, None


def dominant_subsystems(peak, max_span_ratio=20.0):
    """Names to drop from a 'zoomed' chart: repeatedly exclude the current
    largest subsystem until the remaining set's peak span (largest/smallest)
    is within `max_span_ratio`, or only 3 subsystems remain. An *adjacent*-pair
    ratio check isn't enough here — e.g. local-store/replica-log/runtime/
    rocksdb can each be within 4x of their neighbor while the chain as a
    whole still spans 500x from top to bottom, squashing the smallest flat."""
    ordered = sorted(peak, key=lambda n: -peak[n])
    excluded = set()
    while len(ordered) - len(excluded) > 3:
        remaining = [n for n in ordered if n not in excluded]
        top, smallest = peak[remaining[0]], peak[remaining[-1]]
        if smallest > 0 and top > max_span_ratio * smallest:
            excluded.add(remaining[0])
        else:
            break
    return excluded


def heap_section(run_dir, phases, heap_data, heap_reason):
    lines = ["## Memory by subsystem (jemalloc heap attribution)", ""]
    if heap_data is None:
        lines.append(f"*({heap_reason})*")
        return "\n".join(lines)

    dump_rows = heap_data["dump_rows"]
    peak, stack_order = heap_data["peak"], heap_data["stack_order"]
    series_by_name = heap_data["series_by_name"]

    lines.append(stacked_area_chart(dump_rows, stack_order, SUBSYSTEM_COLOR, title="Resident heap bytes by subsystem", phases=phases))
    lines.append("")

    excluded = dominant_subsystems(peak)
    if excluded:
        zoom_order = [n for n in stack_order if n not in excluded]
        zoom_rows = [{"ts_ms": r["ts_ms"], "totals": {k: v for k, v in r["totals"].items() if k in zoom_order}} for r in dump_rows]
        lines.append(
            f"*(`{'`, `'.join(sorted(excluded))}` dominate(s) the y-axis above and are excluded below "
            "so the smaller subsystems are actually visible.)*"
        )
        lines.append("")
        lines.append(stacked_area_chart(zoom_rows, zoom_order, SUBSYSTEM_COLOR, title="Resident heap bytes by subsystem (zoomed, excludes the above)", phases=phases))
        lines.append("")

    lines.append("| subsystem | peak | " + " | ".join(f'end of "{esc(p["name"])}"' for p in phases) + " |")
    lines.append("|---|---|" + "---|" * len(phases))
    for n in stack_order:
        row = [esc(n), fmt_bytes(peak[n])]
        for p in phases:
            row.append(fmt_bytes(value_at_or_before(series_by_name[n], p["end_ms"])))
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


_FRAME_HEX_SUFFIX = re.compile(r"<[0-9a-f]{6,}>$")


def _clean_frame(frame, max_len=110):
    """A jeprof frame stripped of its trailing hex disambiguator (jeprof's
    own per-symbol tiebreaker, e.g. `<00000000005dedc0>` — meaningless to a
    reader) and capped in length: Rust's monomorphized generics can turn one
    frame into 1000+ characters (nested closures/iterator adapters), which is
    what actually made these stacks unreadable, more than the frame count."""
    frame = _FRAME_HEX_SUFFIX.sub("", frame).strip()
    if len(frame) > max_len:
        frame = frame[:max_len].rstrip() + "…"
    return frame


# jemalloc's own profiling instrumentation: `prof_backtrace_impl` and every
# `_rjem_je_prof_*`/`_rjem_je_malloc_*` frame are always the last handful of
# entries in *every* captured stack (the profiler capturing itself), never
# the actual allocation call site. Left in, they make every single stack's
# "leaf" look identical (always `prof_backtrace_impl`) — dropping them before
# picking a leaf/preview restores the frame that's actually informative.
_NOISE_FRAME_PATTERNS = ("_rjem_je_", "prof_backtrace_impl")


def _is_noise_frame(frame):
    return any(p in frame for p in _NOISE_FRAME_PATTERNS)


def _leaf_frame(frames):
    """The last frame that isn't jemalloc's own profiling instrumentation —
    see `_NOISE_FRAME_PATTERNS`. Falls back to the literal last frame only if
    a stack is somehow made of noise frames alone (shouldn't happen)."""
    for f in reversed(frames):
        if not _is_noise_frame(f):
            return f
    return frames[-1] if frames else "?"


def _render_stack_details(frames, max_frames=10):
    """A collapsible <details> block: the leaf frame (the actual allocation
    call site — the single most useful line) as the always-visible summary,
    the full leaf-to-root chain (one cleaned frame per line, monospace, noise
    frames dropped) revealed on click. Rendered as raw HTML (stashed) rather
    than a markdown code span — a 10-frame call chain each 100+ chars needs
    real line breaks and a scrollable block, which a backtick span can't give."""
    useful = [f for f in frames if not _is_noise_frame(f)] or frames
    tail = useful[-max_frames:]
    cleaned = [_clean_frame(f) for f in reversed(tail)]  # leaf-first
    leaf_preview = cleaned[0] if cleaned else "?"
    extra = len(useful) - 1
    more = f" <span class=\"muted\">(+{extra} more frame{'s' if extra != 1 else ''})</span>" if extra > 0 else ""
    frame_rows = "".join(f'<div class="frame">{esc(c)}</div>' for c in cleaned)
    if len(useful) > max_frames:
        frame_rows += '<div class="frame frame-elided">… root not shown</div>'
    return (
        f'<details><summary><code>{esc(leaf_preview)}</code>{more}</summary>'
        f'<div class="stack">{frame_rows}</div></details>'
    )


def top_stacks_table(parsed, subsystem_filter, top_n):
    """Raw-HTML table of the `top_n` heaviest individual call stacks,
    restricted to `subsystem_filter` (or all, if None). Each row is one exact
    stack — the finest-grained attribution jeprof gives us, short of a full
    flamegraph — with its full chain behind a click, not inline."""
    stacks = [(b, frames) for b, frames in parsed if subsystem_filter is None or classify_stack(frames) == subsystem_filter]
    stacks.sort(key=lambda x: -x[0])
    total = sum(b for b, _ in stacks)
    shown = sum(b for b, _ in stacks[:top_n])
    rows = []
    for b, frames in stacks[:top_n]:
        pct = (b / total * 100) if total else 0.0
        rows.append(f"<tr><td>{esc(fmt_bytes(b))}</td><td>{pct:.1f}%</td><td>{_render_stack_details(frames)}</td></tr>")
    table_html = (
        f'<p class="muted">sum of shown rows: {esc(fmt_bytes(shown))} of {esc(fmt_bytes(total))} total '
        f"({len(stacks)} unique stacks)</p>"
        '<table class="stacktable"><thead><tr><th>bytes</th><th>% of scope</th>'
        "<th>call stack (click to expand — leaf first)</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )
    return _stash_raw(table_html)


def top_leaf_frames_table(parsed, subsystem_filter, top_n):
    """Markdown table aggregating bytes by leaf frame (the exact allocating
    function/line) across all stacks — collapses the many different call
    paths that bottom out at the same allocation site into one row, which
    `top_stacks_table` alone can't show (each unique path is its own row there)."""
    totals = defaultdict(int)
    for b, frames in parsed:
        if subsystem_filter is not None and classify_stack(frames) != subsystem_filter:
            continue
        if frames:
            totals[_leaf_frame(frames)] += b
    top = sorted(totals.items(), key=lambda kv: -kv[1])[:top_n]
    lines = ["| bytes | allocation call site (leaf frame) |", "|---|---|"]
    for frame, b in top:
        lines.append(f"| {fmt_bytes(b)} | `{esc_code(_clean_frame(frame, max_len=140))}` |")
    return "\n".join(lines)


def allocation_hotspots_section(heap_data, heap_reason, run_t0, top_n=20):
    """Answers "why is subsystem X big" at the level jeprof actually supports:
    per-stack and per-leaf-frame byte attribution from the end-of-run heap
    dump (plus the peak dump, if it's a different snapshot) — not just the
    coarse subsystem bucket from heap_section."""
    lines = ["## Allocation hotspots", ""]
    if heap_data is None:
        lines.append(f"*({heap_reason})*")
        return "\n".join(lines)

    dominant = sorted(dominant_subsystems(heap_data["peak"]) or (max(heap_data["peak"], key=heap_data["peak"].get),))
    snapshots = [("end-of-run", heap_data["last_ts"], heap_data["raw_last"])]
    if heap_data["peak_ts"] != heap_data["last_ts"]:
        snapshots.append(("peak", heap_data["peak_ts"], heap_data["raw_peak"]))

    for snap_label, snap_ts, parsed in snapshots:
        elapsed = fmt_dur_ms(max(0, snap_ts - run_t0))
        note = " — first dump to reach it" if snap_label == "peak" else ""
        lines.append(f"### {snap_label} snapshot (t+{elapsed} from run start{note})")
        lines.append("")
        for name in dominant:
            lines.append(f"**Top {top_n} stacks — `{esc_code(name)}` only:**")
            lines.append("")
            lines.append(top_stacks_table(parsed, name, top_n))
            lines.append("")
            lines.append(f"**Top {top_n} allocation call sites — `{esc_code(name)}` only (same bytes, grouped by leaf frame instead of full stack):**")
            lines.append("")
            lines.append(top_leaf_frames_table(parsed, name, top_n))
            lines.append("")
        lines.append(f"**Top {top_n} stacks — whole process (context for the above):**")
        lines.append("")
        lines.append(top_stacks_table(parsed, None, top_n))
        lines.append("")
    return "\n".join(lines)


def callouts_section(rows, phases, t0, t1, heap_data):
    """A 'read this first' narrative: computed drain-checks and health signals,
    not just raw numbers — the report's answer to "did anything unexpected
    happen?" rather than making the reader spot it in a chart."""
    lines = ["## Summary & callouts", ""]
    notes = []
    is_rest_like = len(phases) >= 2 and phases[-1].get("target_rps") in (0, 0.0, None)

    if is_rest_like:
        for label, metric in (("RSS", "process_memory_rss_bytes"), ("jemalloc allocated", "jemalloc_allocated_bytes")):
            pts = gauge_series(rows, metric, {}, t0, t1)
            first_end = value_at_or_before(pts, phases[0]["end_ms"])
            last_end = value_at_or_before(pts, phases[-1]["end_ms"])
            if first_end and last_end:
                delta_pct = (last_end - first_end) / first_end * 100
                verdict = "grew — did **not** drain" if delta_pct > 5 else ("shrank" if delta_pct < -5 else "flat")
                notes.append(f"- **{label}** across the rest phase: {fmt_bytes(first_end)} → {fmt_bytes(last_end)} ({delta_pct:+.0f}%) — {verdict}")

        if heap_data is not None:
            grown = []
            for name, series in heap_data["series_by_name"].items():
                a = value_at_or_before(series, phases[0]["end_ms"])
                b = value_at_or_before(series, phases[-1]["end_ms"])
                if a is not None and b is not None and a > 0 and (b - a) / a * 100 > 10:
                    grown.append((name, a, b, (b - a) / a * 100))
            if grown:
                notes.append("- **Heap subsystems that grew during the idle/rest phase (did not drain):**")
                for name, a, b, pct in sorted(grown, key=lambda x: -x[3]):
                    notes.append(f"  - `{name}`: {fmt_bytes(a)} → {fmt_bytes(b)} ({pct:+.0f}%)")
            else:
                notes.append("- No heap subsystem grew more than 10% during the rest phase.")

    rej_total = 0.0
    for direction in ("behind", "ahead"):
        d = perf.series_delta(rows, "hlc_drift_rejections_total", {"direction": direction}, t0, t1)
        if d:
            rej_total += d
    if rej_total > 0:
        notes.append(f"- **HLC drift rejections:** {rej_total:.0f} peer `integrate()` call(s) rejected over the whole run — see HLC & consensus health.")

    lc = perf.series_delta(rows, "raft_leader_changes_total", {}, t0, t1)
    if lc:
        notes.append(f"- **Leadership changed {int(lc)} time(s)** during the run.")

    # labeled_rate_breakdown divides by `secs`; secs=1.0 recovers the raw total count.
    err_total = sum(perf.labeled_rate_breakdown(rows, "loadgen_errors_total", "kind", t0, t1, 1.0).values())
    if err_total > 0:
        notes.append(f"- **{err_total:.0f} client write error(s)** observed — see the per-phase error breakdown.")

    if not notes:
        notes.append("- Nothing flagged: memory drained as expected (or no rest-like phase to check), no HLC rejections, no leader changes, no errors.")
    return "\n".join(lines + notes)


def flamegraph_section(run_dir):
    lines = ["## CPU flamegraph", ""]
    svgs = sorted(p for p in glob.glob(os.path.join(run_dir, "*.svg")))
    if not svgs:
        lines.append("*(no `--pprof` flamegraph captured for this run)*")
        return "\n".join(lines)
    try:
        with open(svgs[0]) as f:
            svg_content = f.read()
    except OSError as e:
        lines.append(f"*(failed to read {esc(os.path.basename(svgs[0]))}: {esc(e)})*")
        return "\n".join(lines)
    # Inline the raw SVG rather than a markdown image link: pprof/inferno's
    # flamegraph is already a fully interactive document (embedded JS for
    # click-to-zoom/search/unzoom, native <title> tooltips per frame) — a
    # markdown `![]()` forces it to render as an inert <img>, throwing all of
    # that away. `_stash_raw` keeps its many embedded newlines (real <script>
    # code, `//` comments and all) out of the line-based markdown-subset
    # parser below, which only understands single-line chart/paragraph blocks.
    lines.append(_stash_raw(f'<div class="flamegraph">{svg_content}</div>'))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Minimal markdown-subset -> HTML converter
#
# Every section builder above already emits only a small, fixed vocabulary:
# #/##/### headers, **bold**, `code`, *(italic notes)*, "> " blockquotes,
# "- "/"  - " bullet lists (one level of nesting), pipe tables, blank-line
# paragraphs, and raw single-line <svg>/<p>/<details> blocks our own chart
# and stack-trace functions already build. This walks that vocabulary
# directly rather than pulling in a markdown library — the whole point is to
# stay pure-stdlib.
#
# Escaping discipline (matches how section builders already construct their
# strings): plain text and table cells are pre-escaped via `esc()` at the
# point of construction, so the converter must NOT re-escape them (that would
# double-escape, e.g. "<" -> "&amp;lt;"). Backtick code-span content, by
# contrast, is only pipe-escaped upstream via `esc_code()` (safe for a
# markdown code span, since a *markdown* renderer would HTML-escape it) — but
# since this converter builds the final HTML directly, it is the one place
# that must HTML-escape code-span content itself.
# ---------------------------------------------------------------------------
_RAW_HTML_BLOCKS = {}


def _stash_raw(raw_html):
    """Register a raw (possibly multi-line) HTML/SVG blob and return a
    single-line placeholder token safe to pass through the line-based
    converter untouched; restored verbatim by `_unstash_raw` at the end."""
    token = f"\x00RAWHTML{len(_RAW_HTML_BLOCKS)}\x00"
    _RAW_HTML_BLOCKS[token] = raw_html
    return token


def _unstash_raw(html_out):
    for token, raw in _RAW_HTML_BLOCKS.items():
        html_out = html_out.replace(token, raw)
    return html_out


def _inline(text):
    """Markdown inline spans -> HTML, on already-`esc()`-escaped text."""
    codes = []

    def _code_sub(m):
        codes.append(html.escape(m.group(1)))
        return f"\x00CODE{len(codes) - 1}\x00"

    text = re.sub(r"`([^`]*)`", _code_sub, text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)
    for i, c in enumerate(codes):
        text = text.replace(f"\x00CODE{i}\x00", f"<code>{c}</code>")
    return text


def _split_table_row(line):
    """Split a `| a | b |` row on unescaped `|` (esc_code() escapes literal
    pipes in cell content as `\\|`, so those must not split the row)."""
    cells = [c.strip() for c in re.split(r"(?<!\\)\|", line.strip())]
    if cells and cells[0] == "":
        cells = cells[1:]
    if cells and cells[-1] == "":
        cells = cells[:-1]
    return [c.replace("\\|", "|") for c in cells]


def _is_table_separator(cells):
    return bool(cells) and all(c and set(c) <= set("-: ") for c in cells)


def _render_table(table_lines):
    rows = [_split_table_row(tl) for tl in table_lines]
    header = rows[0]
    body = rows[2:] if len(rows) > 1 and _is_table_separator(rows[1]) else rows[1:]
    out = ["<table><thead><tr>"]
    out += [f"<th>{_inline(c)}</th>" for c in header]
    out.append("</tr></thead><tbody>")
    for r in body:
        out.append("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in r) + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def _render_list(list_lines):
    out = ["<ul>"]
    i, n = 0, len(list_lines)
    while i < n:
        content = list_lines[i].strip()[2:]
        i += 1
        sub = []
        while i < n and list_lines[i].startswith("  - "):
            sub.append(list_lines[i].strip()[2:])
            i += 1
        if sub:
            out.append(f"<li>{_inline(content)}<ul>")
            out += [f"<li>{_inline(s)}</li>" for s in sub]
            out.append("</ul></li>")
        else:
            out.append(f"<li>{_inline(content)}</li>")
    out.append("</ul>")
    return "".join(out)


def md_subset_to_html(md):
    lines = md.split("\n")
    out = []
    i, n = 0, len(lines)
    while i < n:
        stripped = lines[i].strip()

        if not stripped:
            i += 1
            continue

        # Raw HTML our own chart functions / _stash_raw already built —
        # never line-parsed, passed through as-is.
        if stripped.startswith("<") or stripped.startswith("\x00RAWHTML"):
            out.append(lines[i])
            i += 1
            continue

        if stripped.startswith("### "):
            out.append(f"<h3>{_inline(stripped[4:])}</h3>")
            i += 1
        elif stripped.startswith("## "):
            out.append(f"<h2>{_inline(stripped[3:])}</h2>")
            i += 1
        elif stripped.startswith("# "):
            out.append(f"<h1>{_inline(stripped[2:])}</h1>")
            i += 1
        elif stripped.startswith("> "):
            out.append(f"<blockquote>{_inline(stripped[2:])}</blockquote>")
            i += 1
        elif stripped.startswith("|"):
            table_lines = []
            while i < n and lines[i].strip().startswith("|"):
                table_lines.append(lines[i].strip())
                i += 1
            out.append(_render_table(table_lines))
        elif stripped.startswith("- ") or lines[i].startswith("  - "):
            list_lines = []
            while i < n and (lines[i].strip().startswith("- ") or lines[i].startswith("  - ")):
                list_lines.append(lines[i])
                i += 1
            out.append(_render_list(list_lines))
        else:
            out.append(f"<p>{_inline(stripped)}</p>")
            i += 1
    return "\n".join(out)


def html_shell(title, body_html):
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>
  :root {{
    --surface: {SURFACE};
    --ink-primary: {INK_PRIMARY};
    --ink-secondary: {INK_SECONDARY};
    --ink-muted: {INK_MUTED};
    --grid: {GRID};
    --baseline: {BASELINE};
  }}
  * {{ box-sizing: border-box; }}
  body {{
    background: var(--surface);
    color: var(--ink-primary);
    font-family: {FONT};
    max-width: 920px;
    margin: 0 auto;
    padding: 24px 16px 80px;
    line-height: 1.5;
  }}
  h1 {{ font-size: 22px; margin: 24px 0 8px; }}
  h2 {{ font-size: 17px; margin: 32px 0 8px; border-bottom: 1px solid var(--grid); padding-bottom: 6px; }}
  h3 {{ font-size: 14px; margin: 20px 0 6px; color: var(--ink-secondary); }}
  p {{ margin: 6px 0; overflow-wrap: anywhere; }}
  code {{
    background: var(--grid);
    border-radius: 3px;
    padding: 1px 4px;
    font-size: 12px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    overflow-wrap: anywhere;
  }}
  table {{ border-collapse: collapse; width: 100%; margin: 8px 0 16px; font-size: 13px; }}
  th, td {{ text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--grid); vertical-align: top; }}
  th {{ color: var(--ink-secondary); font-weight: 600; }}
  ul {{ margin: 8px 0; padding-left: 20px; }}
  li {{ margin: 3px 0; }}
  blockquote {{
    border-left: 3px solid #eda100;
    background: rgba(237,161,0,0.08);
    margin: 12px 0;
    padding: 8px 12px;
    border-radius: 0 4px 4px 0;
  }}
  em {{ color: var(--ink-secondary); }}
  svg {{ max-width: 100%; height: auto; display: block; margin: 8px 0; }}
  .muted {{ color: var(--ink-secondary); font-size: 12px; margin: 4px 0; }}
  .stacktable td:last-child {{ min-width: 280px; }}
  details {{ margin: 2px 0; }}
  details summary {{
    cursor: pointer;
    font-size: 12px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    overflow-wrap: anywhere;
  }}
  details summary::marker {{ color: var(--ink-muted); }}
  .stack {{
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
    background: var(--grid);
    border-radius: 4px;
    padding: 6px 8px;
    margin-top: 4px;
  }}
  .stack .frame {{ padding: 1px 0; overflow-wrap: anywhere; }}
  .stack .frame-elided {{ color: var(--ink-muted); font-style: italic; }}
  .flamegraph {{
    width: 100%;
    overflow-x: auto;
    margin: 8px 0;
  }}
  .flamegraph > svg {{
    min-width: 100%;
    display: block;
  }}
</style>
</head>
<body>
{body_html}
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def build_report(run_dir):
    jsonl_path = os.path.join(run_dir, "metrics.jsonl")
    if not os.path.isfile(jsonl_path):
        raise SystemExit(f"error: {jsonl_path} not found")
    rows = perf.load(jsonl_path)

    run_meta = load_json_maybe(os.path.join(run_dir, "run-meta.json"))
    phases = load_json_maybe(os.path.join(run_dir, "phases.json"))
    if not phases:
        t0, t1 = perf.steady_window(rows)
        phases = [{"index": 0, "name": "steady", "target_rps": None, "start_ms": t0, "end_ms": t1}]
    run_t0, run_t1 = _run_window(rows)
    heap_data, heap_reason = compute_heap_data(run_dir, phases)

    _RAW_HTML_BLOCKS.clear()
    sections = [
        header_section(run_dir, run_meta),
        phase_timeline_section(phases),
        callouts_section(rows, phases, run_t0, run_t1, heap_data),
        per_phase_section(rows, phases),
        throughput_section(rows, phases, run_t0, run_t1),
        latency_section(rows, phases, run_t0, run_t1),
        consensus_section(rows, phases, run_t0, run_t1),
        resource_section(rows, phases, run_t0, run_t1),
        disk_section(rows, phases, run_t0, run_t1),
        heap_section(run_dir, phases, heap_data, heap_reason),
        allocation_hotspots_section(heap_data, heap_reason, run_t0),
        flamegraph_section(run_dir),
    ]
    body_md = "\n\n".join(sections) + "\n"
    body_html = _unstash_raw(md_subset_to_html(body_md))
    return html_shell(f"Loadgen report — {os.path.basename(run_dir)}", body_html)


def main():
    if len(sys.argv) != 2:
        print("usage: report.py <run_dir>", file=sys.stderr)
        sys.exit(2)
    run_dir = os.path.abspath(sys.argv[1])
    report_html = build_report(run_dir)
    out_path = os.path.join(run_dir, "report.html")
    with open(out_path, "w") as f:
        f.write(report_html)
    print(out_path)


if __name__ == "__main__":
    main()
