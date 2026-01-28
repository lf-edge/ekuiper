#!/usr/bin/env python3
"""
Perf scenario runner for "wide MQTT" in CI:
- Uses eKuiper REST API to create stream/rule.
- Publishes wide JSON payload to an MQTT broker.
- Scrapes Prometheus `/metrics` periodically and writes `result.json` + raw metrics.

Keep dependencies minimal; optional deps are imported lazily to make `--help` work
even if the workflow hasn't installed them yet.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _http_json(
    method: str,
    url: str,
    body: Optional[dict] = None,
    timeout_secs: float = 10.0,
    ok_statuses: Iterable[int] = (200, 201, 204),
) -> Tuple[int, Any, str]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
            status = resp.getcode()
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        raw = e.read().decode("utf-8", errors="replace")
    except Exception as e:
        raise RuntimeError(f"HTTP {method} {url} failed: {e}") from e

    parsed: Any = None
    if raw.strip().startswith("{") or raw.strip().startswith("["):
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None

    if status not in set(ok_statuses):
        # include raw response to help debugging in CI artifacts
        raise RuntimeError(f"HTTP {method} {url} => {status}: {raw[:2000]}")
    return status, parsed, raw


def _http_text(
    method: str,
    url: str,
    timeout_secs: float = 10.0,
    ok_statuses: Iterable[int] = (200,),
) -> Tuple[int, str]:
    req = urllib.request.Request(url, method=method, headers={"Accept": "text/plain"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
            status = resp.getcode()
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        raw = e.read().decode("utf-8", errors="replace")
    except Exception as e:
        raise RuntimeError(f"HTTP {method} {url} failed: {e}") from e

    if status not in set(ok_statuses):
        raise RuntimeError(f"HTTP {method} {url} => {status}: {raw[:2000]}")
    return status, raw


def wait_for_ping(base_url: str, timeout_secs: float = 30.0) -> None:
    deadline = time.time() + timeout_secs
    ping_url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "ping")
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            _http_text("GET", ping_url, timeout_secs=2.0, ok_statuses=(200,))
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(0.2)
    raise RuntimeError(f"eKuiper not ready at {ping_url} after {timeout_secs}s; last error: {last_err}")


def list_rules(base_url: str) -> List[dict]:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "rules")
    _, parsed, raw = _http_json("GET", url, ok_statuses=(200,))
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]
    raise RuntimeError(f"Unexpected /rules response: {raw[:500]}")


def delete_rule(base_url: str, rule_id: str) -> None:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", f"rules/{urllib.parse.quote(rule_id)}")
    _http_text("DELETE", url, ok_statuses=(200,))


def delete_all_rules(base_url: str, keep_ids: Iterable[str]) -> List[str]:
    keep = set(keep_ids)
    deleted: List[str] = []
    for r in list_rules(base_url):
        rid = r.get("id")
        if not isinstance(rid, str):
            continue
        if rid in keep:
            continue
        delete_rule(base_url, rid)
        deleted.append(rid)
    return deleted


def create_stream(base_url: str, stream_name: str, topic: str) -> None:
    # Use schema-less stream; wide payloads are inferred from runtime JSON.
    sql = f'CREATE STREAM {stream_name}() WITH (DATASOURCE="{topic}", FORMAT="json", TYPE="mqtt");'
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "streams")
    _http_json("POST", url, body={"sql": sql}, ok_statuses=(200, 201))


def create_rule(base_url: str, rule_id: str, sql: str, actions: List[dict]) -> None:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "rules")
    body = {"id": rule_id, "sql": sql, "actions": actions}
    _http_json("POST", url, body=body, ok_statuses=(201,))


def get_metrics(metrics_url: str) -> str:
    _, raw = _http_text("GET", metrics_url, ok_statuses=(200,))
    return raw


def _parse_openmetrics_samples(text: str, metric_name: str) -> List[Tuple[Dict[str, str], float]]:
    # Minimal parser for lines like:
    #   name{a="b",c="d"} 123.4
    #   name 123
    out: List[Tuple[Dict[str, str], float]] = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if not line.startswith(metric_name):
            continue

        labels: Dict[str, str] = {}
        rest = line[len(metric_name) :]
        if rest.startswith("{"):
            idx = rest.find("}")
            if idx < 0:
                continue
            label_str = rest[1:idx]
            rest = rest[idx + 1 :]
            if label_str.strip():
                # split by commas, but assume no escaped commas (OK for our use).
                for kv in label_str.split(","):
                    k, v = kv.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if v.startswith("\"") and v.endswith("\""):
                        v = v[1:-1]
                    labels[k] = v

        parts = rest.strip().split()
        if not parts:
            continue
        try:
            val = float(parts[0])
        except Exception:
            continue
        out.append((labels, val))
    return out


def _sum_counter(
    metrics_text: str,
    metric_name: str,
    match: Dict[str, str],
) -> float:
    total = 0.0
    for labels, val in _parse_openmetrics_samples(metrics_text, metric_name):
        ok = True
        for k, v in match.items():
            if labels.get(k) != v:
                ok = False
                break
        if ok:
            total += val
    return total


def build_wide_payload(columns: int, str_len: int) -> str:
    # Deterministic payload to keep publisher overhead small and stable.
    value = "a" * str_len
    obj = {f"c{i}": value for i in range(columns)}
    return json.dumps(obj, separators=(",", ":"))


def build_rule_sql(stream_name: str, columns: int, sql_mode: str) -> str:
    if sql_mode == "star":
        return f"SELECT * FROM {stream_name};"
    if sql_mode != "explicit":
        raise ValueError(f"unknown sql_mode: {sql_mode}")
    cols = ", ".join([f"c{i}" for i in range(columns)])
    return f"SELECT {cols} FROM {stream_name};"


@dataclasses.dataclass
class ScrapePoint:
    t_rel_secs: float
    src_ok: float
    sink_ok: float
    rule_cpu_secs: float
    proc_cpu_secs: float
    rss_bytes: float
    go_goroutines: float


def run_publisher(
    broker_url: str,
    topic: str,
    qos: int,
    payload: str,
    rate: float,
    duration_secs: float,
) -> Dict[str, Any]:
    # Lazy import to keep script usable without deps.
    try:
        import paho.mqtt.client as mqtt  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Missing dependency paho-mqtt. In GitHub Actions, install it via pip before running."
        ) from e

    # paho expects host/port, but accepts a full tcp:// URL in Connect? no.
    # Parse tcp://host:port
    u = urllib.parse.urlparse(broker_url)
    host = u.hostname or "127.0.0.1"
    port = u.port or 1883

    client = mqtt.Client(client_id=f"ekuiper_perf_daily_{int(time.time())}", clean_session=True)
    published = 0
    errors = 0

    client.connect(host, port, keepalive=30)
    client.loop_start()
    try:
        start = time.time()
        period = 1.0 / rate if rate > 0 else 0.0
        next_t = start
        while True:
            now = time.time()
            if now - start >= duration_secs:
                break
            if period > 0 and now < next_t:
                time.sleep(min(0.01, next_t - now))
                continue

            info = client.publish(topic, payload=payload, qos=qos, retain=False)
            # For QoS>0 we wait to keep error visibility; rate is low enough for CI.
            if qos > 0:
                info.wait_for_publish(timeout=5)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                errors += 1
            else:
                published += 1
            if period > 0:
                next_t += period
    finally:
        client.loop_stop()
        client.disconnect()

    return {"published": published, "errors": errors}


def run_case(args: argparse.Namespace) -> None:
    os.makedirs(args.out_dir, exist_ok=True)

    wait_for_ping(args.base_url, timeout_secs=60.0)

    # Create stream if requested (case1).
    if args.create_stream:
        create_stream(args.base_url, args.stream_name, args.topic)

    if args.force:
        # Remove any existing rule with the same id to make reruns idempotent.
        try:
            delete_rule(args.base_url, args.rule_id)
        except Exception:
            pass

    rule_sql = build_rule_sql(args.stream_name, args.columns, args.sql_mode)
    # Nop sink: ignore outputs to isolate source + SQL cost.
    create_rule(args.base_url, args.rule_id, rule_sql, actions=[{"nop": {}}])

    # Initial metrics snapshot for deltas.
    m0 = get_metrics(args.metrics_url)
    t0 = time.time()

    # Scrape loop runs while publisher is active.
    points: List[ScrapePoint] = []

    # Run publisher synchronously; interleave scrapes on our side.
    # To keep it simple, we do publisher loop here and scrape periodically.
    payload = build_wide_payload(args.columns, args.str_len)

    # Publisher uses paho and sleeps for rate control; we piggyback scrapes.
    try:
        import paho.mqtt.client as mqtt  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Missing dependency paho-mqtt. In GitHub Actions, install it via pip before running."
        ) from e

    u = urllib.parse.urlparse(args.broker_url)
    host = u.hostname or "127.0.0.1"
    port = u.port or 1883
    client = mqtt.Client(client_id=f"ekuiper_perf_daily_{int(time.time())}", clean_session=True)
    client.connect(host, port, keepalive=30)
    client.loop_start()
    published = 0
    pub_errors = 0

    scrape_period = max(0.2, args.scrape_interval_ms / 1000.0)
    next_scrape = time.time()
    pub_period = 1.0 / args.rate if args.rate > 0 else 0.0
    next_pub = time.time()

    try:
        while True:
            now = time.time()
            if now - t0 >= args.duration_secs:
                break

            if now >= next_scrape:
                mt = get_metrics(args.metrics_url)
                t_rel = now - t0

                src_ok = _sum_counter(
                    mt, "kuiper_io_counter", {"io": "source", "status": "success", "rule": args.rule_id}
                )
                sink_ok = _sum_counter(
                    mt, "kuiper_io_counter", {"io": "sink", "status": "success", "rule": args.rule_id}
                )
                rule_cpu = _sum_counter(mt, "kuiper_rule_cpu_time_seconds_total", {"rule": args.rule_id})
                proc_cpu = _sum_counter(mt, "process_cpu_seconds_total", {})
                rss = _sum_counter(mt, "process_resident_memory_bytes", {})
                gor = _sum_counter(mt, "go_goroutines", {})

                points.append(
                    ScrapePoint(
                        t_rel_secs=t_rel,
                        src_ok=src_ok,
                        sink_ok=sink_ok,
                        rule_cpu_secs=rule_cpu,
                        proc_cpu_secs=proc_cpu,
                        rss_bytes=rss,
                        go_goroutines=gor,
                    )
                )
                next_scrape = now + scrape_period
                # Persist last scrape for artifacts (useful when job fails mid-run).
                with open(os.path.join(args.out_dir, "metrics.openmetrics"), "w", encoding="utf-8") as f:
                    f.write(mt)

            if pub_period > 0 and now < next_pub:
                time.sleep(min(0.01, next_pub - now))
                continue

            info = client.publish(args.topic, payload=payload, qos=args.qos, retain=False)
            if args.qos > 0:
                info.wait_for_publish(timeout=5)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                pub_errors += 1
            else:
                published += 1
            if pub_period > 0:
                next_pub += pub_period
    finally:
        client.loop_stop()
        client.disconnect()

    m1 = get_metrics(args.metrics_url)
    with open(os.path.join(args.out_dir, "metrics.openmetrics"), "w", encoding="utf-8") as f:
        f.write(m1)

    # Compute deltas for a few headline metrics.
    src0 = _sum_counter(m0, "kuiper_io_counter", {"io": "source", "status": "success", "rule": args.rule_id})
    src1 = _sum_counter(m1, "kuiper_io_counter", {"io": "source", "status": "success", "rule": args.rule_id})
    sink0 = _sum_counter(m0, "kuiper_io_counter", {"io": "sink", "status": "success", "rule": args.rule_id})
    sink1 = _sum_counter(m1, "kuiper_io_counter", {"io": "sink", "status": "success", "rule": args.rule_id})
    cpu0 = _sum_counter(m0, "kuiper_rule_cpu_time_seconds_total", {"rule": args.rule_id})
    cpu1 = _sum_counter(m1, "kuiper_rule_cpu_time_seconds_total", {"rule": args.rule_id})

    duration = float(args.duration_secs)
    res: Dict[str, Any] = {
        "title": args.title,
        "case": {
            "sql_mode": args.sql_mode,
            "columns": args.columns,
            "str_len": args.str_len,
            "rate": args.rate,
            "duration_secs": args.duration_secs,
            "topic": args.topic,
            "qos": args.qos,
            "stream_name": args.stream_name,
            "rule_id": args.rule_id,
        },
        "publisher": {"published": published, "errors": pub_errors},
        "metrics": {
            "src_ok_delta": src1 - src0,
            "sink_ok_delta": sink1 - sink0,
            "rule_cpu_secs_delta": cpu1 - cpu0,
            "src_ok_per_sec": (src1 - src0) / duration if duration > 0 else None,
        },
        "scrapes": [dataclasses.asdict(p) for p in points],
    }

    with open(os.path.join(args.out_dir, "result.json"), "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2, sort_keys=True)

    if args.delete_rule_after:
        delete_rule(args.base_url, args.rule_id)


def cmd_cleanup(args: argparse.Namespace) -> None:
    wait_for_ping(args.base_url, timeout_secs=60.0)
    deleted = delete_all_rules(args.base_url, keep_ids=args.keep_rule_id)
    out = {"deleted": deleted, "kept": list(args.keep_rule_id)}
    if args.out_json:
        os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, sort_keys=True)
    else:
        print(json.dumps(out, indent=2, sort_keys=True))


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run one perf case")
    run.add_argument("--title", required=True)
    run.add_argument("--base-url", required=True)
    run.add_argument("--metrics-url", required=True)
    run.add_argument("--broker-url", required=True)
    run.add_argument("--topic", required=True)
    run.add_argument("--qos", type=int, default=1)
    run.add_argument("--columns", type=int, default=2000)
    run.add_argument("--str-len", type=int, default=10)
    run.add_argument("--sql-mode", choices=["explicit", "star"], required=True)
    run.add_argument("--stream-name", default="perfDailyStream")
    run.add_argument("--rule-id", required=True)
    run.add_argument("--duration-secs", dest="duration_secs", type=int, default=120)
    run.add_argument("--rate", type=float, default=50.0)
    run.add_argument("--scrape-interval-ms", type=int, default=15000)
    run.add_argument("--out-dir", required=True)
    run.add_argument("--create-stream", action="store_true")
    run.add_argument("--force", action="store_true")
    run.add_argument("--delete-rule-after", action="store_true")
    run.set_defaults(func=run_case)

    cleanup = sub.add_parser("cleanup", help="delete rules (used between restart and case2)")
    cleanup.add_argument("--base-url", required=True)
    cleanup.add_argument("--keep-rule-id", action="append", default=[])
    cleanup.add_argument("--out-json")
    cleanup.set_defaults(func=cmd_cleanup)

    args = p.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

