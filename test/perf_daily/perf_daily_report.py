#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Any, Dict, List, Tuple


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        v = json.load(f)
    if not isinstance(v, dict):
        raise ValueError("result json must be an object")
    return v


def _rate_series(scrapes: List[Dict[str, Any]], key: str) -> Tuple[List[int], List[float]]:
    # Convert counter to a per-second rate between scrapes.
    xs: List[int] = []
    ys: List[float] = []
    prev_t = None
    prev_v = None
    for s in scrapes:
        t = float(s.get("t_rel_secs", 0.0))
        v = float(s.get(key, 0.0))
        if prev_t is not None and prev_v is not None:
            dt = t - prev_t
            dv = v - prev_v
            if dt > 0:
                xs.append(int(round(t)))
                ys.append(dv / dt)
        prev_t = t
        prev_v = v
    return xs, ys


def _mermaid_xychart(title: str, xs: List[int], ys: List[float], series_name: str, y_label: str) -> str:
    if not xs or not ys:
        return f"_No data for chart: {title}_\n"
    ymax = max(ys)
    # Round up to a "nice" max.
    yceil = int(math.ceil(ymax / 10.0) * 10) if ymax > 0 else 1
    x_axis = ", ".join(str(x) for x in xs)
    y_axis = ", ".join(f\"{y:.2f}\" for y in ys)
    return (
        "```mermaid\n"
        "xychart-beta\n"
        f'  title \"{title}\"\n'
        f'  x-axis \"t (s)\" [{x_axis}]\n'
        f'  y-axis \"{y_label}\" 0 --> {yceil}\n'
        f'  line \"{series_name}\" [{y_axis}]\n'
        "```\n"
    )


def render(result: Dict[str, Any]) -> str:
    case = result.get("case", {})
    metrics = result.get("metrics", {})
    publisher = result.get("publisher", {})
    scrapes = result.get("scrapes", [])
    if not isinstance(scrapes, list):
        scrapes = []

    title = str(result.get("title", "Perf Result"))
    rule_id = str(case.get("rule_id", ""))
    sql_mode = str(case.get("sql_mode", ""))
    columns = case.get("columns")
    duration = case.get("duration_secs")
    rate = case.get("rate")

    src_delta = metrics.get("src_ok_delta")
    src_rate = metrics.get("src_ok_per_sec")
    cpu_delta = metrics.get("rule_cpu_secs_delta")
    pub_ok = publisher.get("published")
    pub_err = publisher.get("errors")

    out = []
    out.append(f"## {title}\n")
    out.append("| item | value |\n|---|---:|\n")
    out.append(f"| sql_mode | `{sql_mode}` |\n")
    out.append(f"| rule_id | `{rule_id}` |\n")
    out.append(f"| columns | {columns} |\n")
    out.append(f"| duration_secs | {duration} |\n")
    out.append(f"| publish_rate_target (msg/s) | {rate} |\n")
    out.append(f"| publisher_published | {pub_ok} |\n")
    out.append(f"| publisher_errors | {pub_err} |\n")
    out.append(f"| kuiper_source_ok_delta | {src_delta} |\n")
    out.append(f"| kuiper_source_ok_avg (msg/s) | {src_rate:.2f} |\n" if isinstance(src_rate, (int, float)) else "| kuiper_source_ok_avg (msg/s) | n/a |\n")
    out.append(f"| kuiper_rule_cpu_seconds_delta | {cpu_delta} |\n")
    out.append("\n")

    xs, ys = _rate_series([s for s in scrapes if isinstance(s, dict)], key="src_ok")
    out.append(_mermaid_xychart(f"{sql_mode} source throughput (kuiper_io_counter delta)", xs, ys, series_name="msg/s", y_label="msg/s"))

    return "".join(out)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--result-json", required=True)
    ap.add_argument("--summary-path", required=True)
    ap.add_argument("--append", action="store_true", help="append to summary instead of overwrite")
    args = ap.parse_args(argv)

    res = _load_json(args.result_json)
    md = render(res)

    os.makedirs(os.path.dirname(args.summary_path) or ".", exist_ok=True)
    mode = "a" if args.append else "w"
    with open(args.summary_path, mode, encoding="utf-8") as f:
        f.write(md)
        if not md.endswith("\n"):
            f.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

