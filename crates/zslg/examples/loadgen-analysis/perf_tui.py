# /// script
# requires-python = ">=3.11"
# dependencies = ["textual>=0.60"]
# ///
"""Textual TUI front-end for the loadgen performance harness.

A visual driver over examples/loadgen-analysis/perf.py: pick a scenario (each with
a one-line explanation of what it measures), or run the whole standard set, tune
reps/duration/CPU-pin/comment, watch progress stream live, and see results land in
a table. Appends to docs/perf-timeline.csv just like the CLI.

Run it:  just perf-tui        (builds loadgen, then `uv run perf_tui.py`)
or:      uv run examples/loadgen-analysis/perf_tui.py

Keys:  r = run selected · a = run all standard · x = clear results · q = quit
"""
import os
import shlex
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import perf  # noqa: E402  (the harness engine — stdlib only)

from textual import work  # noqa: E402
from textual.app import App, ComposeResult  # noqa: E402
from textual.containers import Horizontal, Vertical  # noqa: E402
from textual.widgets import (  # noqa: E402
    Checkbox, DataTable, Footer, Header, Input, Label, RichLog, Static,
)


def _inflight_of(scn):
    v = perf.flag_val(scn["flags"], "--max-inflight")
    if v:
        return v
    rps = perf.flag_val(scn["flags"], "--target-rps")
    return f"{rps} rps" if rps and rps != "0" else "-"


class PerfTUI(App):
    CSS = """
    #left  { width: 48%; }
    #right { width: 52%; }
    DataTable { height: auto; max-height: 50%; }
    #log { height: 1fr; border: round $accent; }
    #settings { height: auto; padding: 1 0; }
    .field { width: 1fr; }
    Label { padding: 0 1; }
    #status { padding: 0 1; color: $text-muted; }
    """
    BINDINGS = [
        ("r", "run_selected", "Run selected"),
        ("a", "run_all", "Run all standard"),
        ("x", "clear", "Clear results"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self):
        super().__init__()
        self.scenarios = perf.SWEEP_SCENARIOS
        self._busy = False
        self._result_keys = set()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(id="left"):
                yield Static("[b]Scenarios[/b]  (↑/↓ to select)")
                yield DataTable(id="scenarios", cursor_type="row")
                yield Static("[b]Settings[/b]")
                with Vertical(id="settings"):
                    with Horizontal():
                        yield Label("reps")
                        yield Input("5", id="reps", classes="field")
                        yield Label("dur s")
                        yield Input("20", id="dur", classes="field")
                        yield Label("pin")
                        yield Input(placeholder="e.g. 0-3", id="pin", classes="field")
                    with Horizontal():
                        yield Label("comment")
                        yield Input(placeholder="experiment note → CSV", id="comment", classes="field")
                    with Horizontal():
                        yield Label("extra loadgen flags")
                        yield Input(placeholder="--fsync-min-interval-ms 3 …", id="extra", classes="field")
                    yield Checkbox("Append to perf-timeline.csv", value=True, id="csv")
                yield Static("", id="status")
            with Vertical(id="right"):
                yield Static("[b]Results[/b]  (median across reps)")
                yield DataTable(id="results", cursor_type="row")
                yield RichLog(id="log", highlight=True, markup=True, wrap=True)
        yield Footer()

    def on_mount(self):
        self.title = "loadgen perf harness"
        st = self.query_one("#scenarios", DataTable)
        st.add_columns("Scenario", "Inflight", "What it measures")
        for scn in self.scenarios:
            st.add_row(scn["name"], _inflight_of(scn), perf.EXPLANATIONS.get(scn["name"], ""))
        rt = self.query_one("#results", DataTable)
        rt.add_columns("Scenario", "thrpt/s", "p50 ms", "p99 ms", "cpu", "w/core", "var")
        self._set_status(f"binary: {perf.DEFAULT_BIN}")

    # ---- helpers (main thread) ----
    def _set_status(self, msg):
        self.query_one("#status", Static).update(msg)

    def _log(self, msg):
        self.query_one("#log", RichLog).write(msg)

    def _gather(self):
        def field(wid, default=""):
            return (self.query_one(wid, Input).value or "").strip() or default
        return {
            "reps": int(field("#reps", "5")),
            "dur": int(field("#dur", "20")),
            "pin": field("#pin", "") or None,
            "comment": self.query_one("#comment", Input).value,
            "extra": shlex.split(self.query_one("#extra", Input).value or ""),
            "csv": self.query_one("#csv", Checkbox).value,
        }

    def _add_result(self, name, row):
        rt = self.query_one("#results", DataTable)
        cells = (
            name,
            row["throughput_writes_per_sec_median"] or "-",
            row["client_write_latency_p50_ms"] or "-",
            row["client_write_latency_p99_ms"] or "-",
            row["cpu_cores_used_mean"] or "-",
            row["writes_per_core_per_sec"] or "-",
            row["variance_flag"],
        )
        if name in self._result_keys:
            rt.remove_row(name)
        rt.add_row(*cells, key=name)
        self._result_keys.add(name)

    # ---- actions ----
    def action_run_selected(self):
        st = self.query_one("#scenarios", DataTable)
        idx = st.cursor_row
        if idx is None or idx < 0:
            self._log("[yellow]no scenario selected[/yellow]")
            return
        self._launch([self.scenarios[idx]])

    def action_run_all(self):
        self._launch(list(self.scenarios))

    def action_clear(self):
        self.query_one("#results", DataTable).clear()
        self._result_keys.clear()
        self.query_one("#log", RichLog).clear()

    def _launch(self, scenarios):
        if self._busy:
            self._log("[yellow]a run is already in progress[/yellow]")
            return
        try:
            settings = self._gather()
        except ValueError:
            self._log("[red]reps/dur must be integers[/red]")
            return
        self._busy = True
        names = ", ".join(s["name"] for s in scenarios)
        self._set_status(f"running: {names} …")
        self._run(scenarios, settings)

    # ---- worker (thread) ----
    @work(thread=True, exclusive=True, group="bench")
    def _run(self, scenarios, settings):
        def log(msg):
            self.call_from_thread(self._log, msg)
        try:
            ctx = perf.build_ctx(
                perf.DEFAULT_BIN, replicas=int(os.environ.get("REPLICAS", "3")),
                writers=int(os.environ.get("WRITERS", "3")), dataroot=os.environ.get(
                    "LOADGEN_DATAROOT", "/workspace/.scratch"),
                dur=settings["dur"], reps=settings["reps"], max_extra=5, cv_threshold=0.15,
                pin=settings["pin"], comment=settings["comment"], extra=settings["extra"],
                log=log)
        except FileNotFoundError as e:
            log(f"[red]{e}[/red]")
            self.call_from_thread(self._finish)
            return
        for scn in scenarios:
            log(f"\n[b cyan]### {scn['name']} ###[/b cyan]  {perf.EXPLANATIONS.get(scn['name'], '')}")
            try:
                runs, jsonls, cpus, extra = perf.run_scenario(scn, ctx)
            except SystemExit as e:
                log(f"[red]scenario aborted (exit {e.code}) — likely a hang/timeout[/red]")
                continue
            row = perf.build_row(scn, ctx, runs, cpus, extra)
            if settings["csv"]:
                ok = perf.append_csv(row)
                log("  [green]→ appended to perf-timeline.csv[/green]" if ok
                    else "  [red]→ CSV skipped (schema mismatch)[/red]")
            self.call_from_thread(self._add_result, scn["name"], row)
        log("\n[b green]=== done ===[/b green]")
        self.call_from_thread(self._finish)

    def _finish(self):
        self._busy = False
        self._set_status("idle")


if __name__ == "__main__":
    PerfTUI().run()
