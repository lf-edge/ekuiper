#!/usr/bin/env python3

"""
Render perf_daily OpenMetrics dump into:
1) GitHub Actions Job Summary (Mermaid xychart + fallback ASCII)
2) A standalone HTML report (SVG charts with axes)

No third-party deps; stdlib only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import re
from typing import Dict, List, Optional, Tuple

Sample = Tuple[int, float]  # (ts_ms, value)

TRACKED_METRICS = (
    "cpu_usage",
    "memory_usage_bytes",
    "heap_in_use_bytes",
    "heap_in_allocator_bytes",
    "processor_records_in_total",
)

# We render in GitHub Actions Job Summary; prefer dark theme and high-contrast lines.
MERMAID_THEME = "dark"
MERMAID_PALETTE = (
    "#4ea1ff",  # series 1 (blue)
    "#ff6b6b",  # series 2 (red)
    "#37d67a",  # series 3 (green)
)

_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+]?(?:\d+\.?\d*|\d*\.?\d+)(?:[eE][-+]?\d+)?)(?:\s+(\d+))?$"
)


def _read_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _to_mib(v: float) -> float:
    return v / (1024.0 * 1024.0)


def _format_ts_ms(ts_ms: int) -> str:
    return dt.datetime.fromtimestamp(ts_ms / 1000.0).isoformat(timespec="seconds")


def parse_openmetrics(path: str, instance: Optional[str]) -> Dict[str, List[Sample]]:
    series: Dict[str, List[Sample]] = {k: [] for k in TRACKED_METRICS}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            m = _SAMPLE_RE.match(line)
            if not m:
                continue
            name = m.group(1)
            if name not in series:
                continue
            labels = m.group(2) or ""
            if instance is not None and f'instance="{instance}"' not in labels:
                continue
            try:
                val = float(m.group(3))
            except ValueError:
                continue
            ts_ms = m.group(4)
            if ts_ms is None:
                continue
            try:
                ts = int(ts_ms)
            except ValueError:
                continue
            series[name].append((ts, val))

    for k, items in series.items():
        items.sort(key=lambda x: x[0])
        dedup: List[Sample] = []
        for ts, v in items:
            if dedup and dedup[-1][0] == ts:
                dedup[-1] = (ts, v)
            else:
                dedup.append((ts, v))
        series[k] = dedup
    return series


def filter_window(series: Dict[str, List[Sample]], start_ms: int, end_ms: int) -> Dict[str, List[Sample]]:
    out: Dict[str, List[Sample]] = {}
    for k, items in series.items():
        out[k] = [(ts, v) for ts, v in items if start_ms <= ts <= end_ms]
    return out


def mermaid_xychart(
    title: str,
    x_label: str,
    y_label: str,
    xs: List[int],
    series: List[Tuple[str, List[float]]],
    y_min: Optional[float] = None,
    y_max: Optional[float] = None,
) -> str:
    # Mermaid xychart-beta expects x-axis labels and y-series values (same length).
    #
    # GitHub Actions job summary doesn't show a legend for xychart-beta; to make it
    # less ambiguous, force a deterministic palette so we can document mapping.
    init_cfg = {
        "theme": MERMAID_THEME,
        "themeVariables": {
            "cScale0": MERMAID_PALETTE[0],
            "cScale1": MERMAID_PALETTE[1],
            "cScale2": MERMAID_PALETTE[2],
        },
    }
    init = f"%%{{init: {json.dumps(init_cfg, separators=(',', ':'))} }}%%\n"
    # Mermaid's xychart-beta uses double quotes in syntax; avoid raw `"` in user strings.
    safe_title = title.replace('"', "'")
    x_axis = ", ".join(str(x) for x in xs)
    y_axis = f'  y-axis "{y_label}"'
    if y_min is not None and y_max is not None:
        y_axis = f'  y-axis "{y_label}" {y_min:.3f} --> {y_max:.3f}'
    lines = [f"xychart-beta", f'  title "{safe_title}"', f'  x-axis "{x_label}" [{x_axis}]', y_axis]
    for name, ys in series:
        y_vals = ", ".join(f"{y:.3f}" if isinstance(y, float) else str(y) for y in ys)
        lines.append(f'  line "{name}" [{y_vals}]')
    return "```mermaid\n" + init + "\n".join(lines) + "\n```\n"


def _bin_by_second(items: List[Sample], start_ms: int) -> Tuple[List[int], List[float]]:
    """
    Bin samples by integer second offset from start, keeping the last value in each second.
    This makes charts more readable and avoids duplicated x labels.
    """
    latest: Dict[int, float] = {}
    for ts, v in items:
        # Use floor so bins are stable even when scrape jitter exists.
        sec = int((ts - start_ms) // 1000)
        latest[sec] = v
    xs = sorted(latest.keys())
    ys = [latest[x] for x in xs]
    return xs, ys


def _pad_range(vmin: float, vmax: float) -> Tuple[float, float]:
    if vmax <= vmin:
        return vmin, vmin + 1.0
    pad = (vmax - vmin) * 0.05
    return max(0.0, vmin - pad), vmax + pad


def _compute_rate(samples: List[Sample], start_ms: int) -> Tuple[List[int], List[float]]:
    """
    Convert a counter series into per-second rate using adjacent deltas.
    Returns (xs_secs, rates) aligned to the *later* sample timestamp.
    """
    if len(samples) < 2:
        return [], []
    xs: List[int] = []
    rs: List[float] = []
    prev_ts, prev_v = samples[0]
    for ts, v in samples[1:]:
        dt = (ts - prev_ts) / 1000.0
        dv = v - prev_v
        prev_ts, prev_v = ts, v
        if dt <= 0:
            continue
        if dv < 0:
            continue
        xs.append(int((ts - start_ms) // 1000))
        rs.append(dv / dt)
    return xs, rs


def build_summary_markdown(result: Dict, series: Dict[str, List[Sample]], openmetrics_path: str) -> str:
    cfg = result.get("config", {})
    sent = result.get("sent_messages")
    eff = result.get("effective_rate_mps")
    payload_bytes = result.get("payload_bytes")

    start_ms = int(result.get("publish_start_ts_ms") or 0)
    end_ms = int(result.get("publish_end_ts_ms") or 0)
    all_ts = [ts for items in series.values() for ts, _ in items]
    start_ms_eff = start_ms or (min(all_ts) if all_ts else 0)
    end_ms_eff = end_ms or (max(all_ts) if all_ts else 0)

    lines: List[str] = []
    lines.append(f"## {result.get('_title', 'perf_daily')}")
    lines.append(f"- result.json: `{result.get('_result_path', '')}`".rstrip())
    lines.append(f"- openmetrics: `{openmetrics_path}`")
    eff_s = f"{eff:.2f}" if isinstance(eff, (int, float)) else "n/a"
    sent_s = str(sent) if sent is not None else "n/a"
    payload_s = str(payload_bytes) if payload_bytes is not None else "n/a"
    lines.append(
        f"- workload: sql_mode={cfg.get('sql_mode','n/a')} pipeline_id={cfg.get('pipeline_id','n/a')} "
        f"columns={cfg.get('columns','n/a')} str_len={cfg.get('str_len','n/a')} cases={cfg.get('cases','n/a')} "
        f"rate={cfg.get('rate','n/a')} msg/s duration={cfg.get('duration_secs','n/a')}s "
        f"payload_bytes={payload_s} sent={sent_s} effective_rate={eff_s} msg/s"
    )
    if start_ms_eff and end_ms_eff:
        lines.append(f"- window: {_format_ts_ms(start_ms_eff)} .. {_format_ts_ms(end_ms_eff)}")
    else:
        lines.append("- window: n/a")
    lines.append("")

    # Build x-axis as seconds from start.
    cpu_items = series.get("cpu_usage", [])
    if cpu_items:
        xs, cpu_vals = _bin_by_second(cpu_items, start_ms=start_ms_eff)
        y0, y1 = _pad_range(min(cpu_vals), max(cpu_vals))
        lines.append("### CPU (Grafana-like)")
        lines.append(mermaid_xychart("cpu_usage", "t (s)", "%", xs, [("cpu_usage", cpu_vals)], y_min=y0, y_max=y1))

    # Ingress throughput: datasource records_in rate.
    ds_in = series.get("processor_records_in_total", [])
    if ds_in:
        xs_r, rs = _compute_rate(ds_in, start_ms=start_ms_eff)
        if xs_r and rs:
            y0, y1 = _pad_range(min(rs), max(rs))
            lines.append("### Datasource Records In Rate (Grafana-like)")
            lines.append(
                mermaid_xychart(
                    "processor_records_in_total kind=datasource rate",
                    "t (s)",
                    "records/s",
                    xs_r,
                    [("records_in_per_s", rs)],
                    y_min=y0,
                    y_max=y1,
                )
            )

    mem_items = series.get("memory_usage_bytes", [])
    heap_items = series.get("heap_in_use_bytes", [])
    heap_sys_items = series.get("heap_in_allocator_bytes", [])
    if mem_items:
        xs_mem, mem_raw = _bin_by_second(mem_items, start_ms=start_ms_eff)
        mem_vals = [_to_mib(v) for v in mem_raw]

        chart_series: List[Tuple[str, List[float]]] = [("memory_rss_mib", mem_vals)]
        all_vals = mem_vals[:]

        if heap_items:
            xs_h, heap_raw = _bin_by_second(heap_items, start_ms=start_ms_eff)
            if xs_h == xs_mem:
                heap_vals = [_to_mib(v) for v in heap_raw]
                chart_series.append(("heap_in_use_mib", heap_vals))
                all_vals.extend(heap_vals)
        if heap_sys_items:
            xs_hs, heap_sys_raw = _bin_by_second(heap_sys_items, start_ms=start_ms_eff)
            if xs_hs == xs_mem:
                heap_sys_vals = [_to_mib(v) for v in heap_sys_raw]
                chart_series.append(("heap_in_allocator_mib", heap_sys_vals))
                all_vals.extend(heap_sys_vals)

        y0, y1 = _pad_range(min(all_vals), max(all_vals))
        lines.append("### Memory (Grafana-like)")
        lines.append(mermaid_xychart("memory/heap", "t (s)", "MiB", xs_mem, chart_series, y_min=y0, y_max=y1))
        lines.append(
            f"Legend: [{MERMAID_PALETTE[0]}]=memory_usage_bytes (rss), "
            f"[{MERMAID_PALETTE[1]}]=heap_in_use_bytes, "
            f"[{MERMAID_PALETTE[2]}]=heap_in_allocator_bytes"
        )
        lines.append("")
    return "\n".join(lines) + "\n"


def svg_line_chart(
    title: str,
    unit: str,
    items: List[Sample],
    start_ms: int,
    width: int = 900,
    height: int = 260,
    padding: int = 40,
) -> str:
    if not items:
        return f"<div class='chart'><h3>{html.escape(title)}</h3><pre>(no data)</pre></div>"

    xs_raw = [(ts - start_ms) / 1000.0 for ts, _ in items]
    ys_raw = [v for _, v in items]
    if unit == "MiB":
        ys_raw = [_to_mib(v) for v in ys_raw]

    xmin, xmax = min(xs_raw), max(xs_raw)
    ymin, ymax = min(ys_raw), max(ys_raw)
    if xmax <= xmin:
        xmax = xmin + 1.0
    if ymax <= ymin:
        ymax = ymin + 1.0

    w = width - padding * 2
    h = height - padding * 2
    xs = [padding + (x - xmin) / (xmax - xmin) * w for x in xs_raw]
    ys = [padding + (1.0 - (y - ymin) / (ymax - ymin)) * h for y in ys_raw]
    pts = " ".join(f"{x:.2f},{y:.2f}" for x, y in zip(xs, ys))

    # Simple axes + ticks at min/max.
    x0 = padding
    y0 = height - padding
    x1 = width - padding
    y1 = padding

    return f"""
<div class="chart">
  <h3>{html.escape(title)} <span class="meta">({html.escape(unit)})</span></h3>
  <svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
    <rect x="0" y="0" width="{width}" height="{height}" fill="white" stroke="#ddd"/>
    <line x1="{x0}" y1="{y0}" x2="{x1}" y2="{y0}" stroke="#333" />
    <line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y1}" stroke="#333" />
    <text x="{x0}" y="{y0 + 22}" font-size="12" fill="#333">t=0s</text>
    <text x="{x1 - 40}" y="{y0 + 22}" font-size="12" fill="#333">t={xmax:.0f}s</text>
    <text x="6" y="{y0}" font-size="12" fill="#333">{ymin:.2f}</text>
    <text x="6" y="{y1 + 12}" font-size="12" fill="#333">{ymax:.2f}</text>
    <polyline fill="none" stroke="#2f6fdb" stroke-width="2" points="{pts}"/>
  </svg>
</div>
""".strip()


def build_html_report(result: Dict, series: Dict[str, List[Sample]], openmetrics_path: str, title: str) -> str:
    start_ms = int(result.get("publish_start_ts_ms") or 0)
    charts = [
        svg_line_chart("cpu_usage", "pct", series.get("cpu_usage", []), start_ms=start_ms),
        svg_line_chart("memory_usage_bytes", "MiB", series.get("memory_usage_bytes", []), start_ms=start_ms),
        svg_line_chart("heap_in_use_bytes", "MiB", series.get("heap_in_use_bytes", []), start_ms=start_ms),
        svg_line_chart("heap_in_allocator_bytes", "MiB", series.get("heap_in_allocator_bytes", []), start_ms=start_ms),
    ]

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; margin: 20px; }}
    code, pre {{ background: #f6f8fa; padding: 2px 4px; border-radius: 4px; }}
    .meta {{ color: #555; font-size: 12px; }}
    .chart {{ margin: 18px 0; }}
    h1 {{ margin: 0 0 6px 0; }}
    h3 {{ margin: 0 0 6px 0; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <div class="meta">openmetrics: <code>{html.escape(openmetrics_path)}</code></div>
  {''.join(charts)}
</body>
</html>
"""


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(description="Render perf_daily OpenMetrics into summary + html.")
    p.add_argument("--result-json", required=True, help="Path to result.json produced by perf_daily")
    p.add_argument("--openmetrics", help="Path to metrics.openmetrics (defaults to result.json.metrics.openmetrics_path)")
    p.add_argument("--instance", default="local")
    p.add_argument("--out-html", help="Write HTML report to this path")
    p.add_argument("--summary-path", help="Append markdown to this path (e.g. $GITHUB_STEP_SUMMARY)")
    p.add_argument("--title", default="perf_daily report")
    args = p.parse_args(argv)

    result = _read_json(args.result_json)
    result["_result_path"] = args.result_json
    result["_title"] = args.title
    openmetrics_path = args.openmetrics or result.get("metrics", {}).get("openmetrics_path")
    if not openmetrics_path:
        raise SystemExit("error: missing openmetrics path")

    raw_series = parse_openmetrics(openmetrics_path, instance=args.instance)
    start_ms = int(result.get("publish_start_ts_ms") or 0)
    end_ms = int(result.get("publish_end_ts_ms") or 0)
    series = filter_window(raw_series, start_ms=start_ms, end_ms=end_ms) if start_ms and end_ms else raw_series

    if args.out_html:
        os.makedirs(os.path.dirname(args.out_html) or ".", exist_ok=True)
        with open(args.out_html, "w", encoding="utf-8") as f:
            f.write(build_html_report(result, series, openmetrics_path=openmetrics_path, title=args.title))

    if args.summary_path:
        md = build_summary_markdown(result, series, openmetrics_path=openmetrics_path)
        with open(args.summary_path, "a", encoding="utf-8") as f:
            f.write(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))
