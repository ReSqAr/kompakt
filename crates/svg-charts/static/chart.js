/* The reusable chart behavior. The server owns the SVG and the tooltip
 * templates; this file only positions the nearest template and handles input.
 *
 * Explicit initialization (no auto-start):
 *
 *   chart.init();             // standard
 *   chart.init({ htmx });     // standard + htmx swap lifecycle
 *
 * A frame is any element with `data-chart` containing `svg.chart` and
 * `template.chart-tooltip-template` elements with data-x/data-y in viewBox units. */
(function () {
    "use strict";

    const NS = "chart";
    const controllers = new WeakMap();
    const registrations = new WeakSet();

    function init(arg) {
        const options = arg && !arg.nodeType && typeof arg !== "string" ? arg : {};
        const root = arg && (arg.nodeType || typeof arg === "string") ? arg : options.root || document;
        if (options.htmx) registerHtmx(options.htmx);
        scan(root);
    }

    function registerHtmx(htmx) {
        if (!htmx || registrations.has(htmx)) return;
        registrations.add(htmx);
        if (typeof htmx.onLoad === "function") {
            htmx.onLoad((target) => init(target));
        } else if (typeof htmx.on === "function") {
            htmx.on("htmx:afterSwap", (event) => init(event.target));
        }
    }

    function scan(root) {
        if (!root || !root.querySelectorAll) return;
        if (root.matches && root.matches("[data-chart]")) initFrame(root);
        root.querySelectorAll("[data-chart]").forEach(initFrame);
    }

    function initFrame(frame) {
        const previous = controllers.get(frame);
        if (previous) previous.abort();
        const controller = new AbortController();
        controllers.set(frame, controller);

        const svg = frame.querySelector("svg.chart");
        const templates = Array.from(frame.querySelectorAll("template.chart-tooltip-template"));
        if (!svg) return;
        frame.querySelectorAll(".chart-tooltip.is-visible").forEach((node) => node.remove());
        frame.querySelectorAll(".chart-crosshair.is-visible").forEach((node) => node.classList.remove("is-visible"));

        const samples = templates.map((template) => ({
            x: Number(template.dataset.x || "0"),
            y: Number(template.dataset.y || "0"),
            template,
        })).filter((sample) => Number.isFinite(sample.x) && Number.isFinite(sample.y));
        if (samples.length === 0) return;

        let active = -1;
        let live = null;
        let touch = false;
        const crosshair = frame.querySelector(".chart-crosshair");
        const on = (target, type, handler, options) => {
            target.addEventListener(type, handler, { ...options, signal: controller.signal });
        };

        const hide = () => {
            active = -1;
            if (live) {
                live.remove();
                live = null;
            }
            if (crosshair) crosshair.classList.remove("is-visible");
        };

        const show = (index) => {
            if (index < 0 || index >= samples.length) return;
            if (index === active && live) return;
            hide();
            const sample = samples[index];
            const content = sample.template.content.firstElementChild;
            if (!content) return;
            live = content.cloneNode(true);
            live.classList.add("is-visible");
            frame.appendChild(live);
            active = index;
            if (crosshair) {
                crosshair.setAttribute("x1", String(sample.x));
                crosshair.setAttribute("x2", String(sample.x));
                crosshair.classList.add("is-visible");
            }
            place(sample);
        };

        const place = (sample) => {
            if (!live) return;
            const view = svg.viewBox && svg.viewBox.baseVal;
            const width = view && view.width ? view.width : svg.clientWidth || 1;
            const height = view && view.height ? view.height : svg.clientHeight || 1;
            const box = frame.getBoundingClientRect();
            const svgBox = svg.getBoundingClientRect();
            const left = svgBox.left - box.left + (sample.x / width) * svgBox.width;
            const top = svgBox.top - box.top + (sample.y / height) * svgBox.height;
            const widthNow = frame.clientWidth;
            const tooltipWidth = live.offsetWidth;
            const tooltipHeight = live.offsetHeight;
            const clampedLeft = Math.max(4, Math.min(left + 8, widthNow - tooltipWidth - 4));
            const clampedTop = Math.max(4, top - tooltipHeight - 8);
            live.style.left = clampedLeft + "px";
            live.style.top = clampedTop + "px";
        };

        const nearest = (clientX) => {
            const svgBox = svg.getBoundingClientRect();
            const view = svg.viewBox && svg.viewBox.baseVal;
            const width = view && view.width ? view.width : svg.clientWidth || 1;
            const x = ((clientX - svgBox.left) / (svgBox.width || 1)) * width;
            let best = 0;
            let distance = Infinity;
            samples.forEach((sample, index) => {
                const d = Math.abs(sample.x - x);
                if (d < distance) {
                    distance = d;
                    best = index;
                }
            });
            return best;
        };

        svg.setAttribute("tabindex", "0");
        on(svg, "pointerenter", (event) => {
            if (event.pointerType !== "touch") show(nearest(event.clientX));
        });
        on(svg, "pointerdown", (event) => {
            if (event.pointerType === "touch") {
                touch = true;
                show(nearest(event.clientX));
            }
        });
        on(svg, "pointermove", (event) => {
            if (event.pointerType === "touch") touch = true;
            if (touch && event.pointerType === "touch" && event.buttons === 0) return;
            show(nearest(event.clientX));
        });
        on(svg, "pointerleave", () => {
            if (!touch) hide();
        });
        on(svg, "pointercancel", () => hide());
        on(svg, "keydown", (event) => {
            if (event.key === "Escape") {
                hide();
                return;
            }
            if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
            event.preventDefault();
            const step = event.key === "ArrowLeft" ? -1 : 1;
            const next = active < 0 ? (step > 0 ? 0 : samples.length - 1) : active + step;
            show(Math.max(0, Math.min(samples.length - 1, next)));
        });
        on(svg, "blur", hide);
        on(frame, "pointerleave", () => {
            if (!touch) hide();
        });
        on(frame, "focusout", (event) => {
            if (!frame.contains(event.relatedTarget)) hide();
        });
    }

    function hideAll() {
        document.querySelectorAll(".chart-tooltip.is-visible").forEach((node) => node.remove());
        document.querySelectorAll(".chart-crosshair.is-visible").forEach((node) => node.classList.remove("is-visible"));
    }

    // One touch crosshair at a time: a new tap (inside or outside a chart)
    // dismisses the previous one; the chart's own pointerdown then shows the
    // new sample. Mouse hover keeps its normal pointerleave path.
    document.addEventListener("pointerdown", (event) => {
        if (event.pointerType === "touch") hideAll();
    }, true);

    window.chart = { init: init, hide: hideAll, namespace: NS };
})();
