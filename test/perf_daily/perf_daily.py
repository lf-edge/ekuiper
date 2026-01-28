#!/usr/bin/env python3

"""
eKuiper variant of veloFlux "Perf Daily Wide MQTT".

What it does:
- Create a wide MQTT stream (schema required) and a rule (nop sink).
- Publish wide JSON payloads to MQTT (QoS1).
- Scrape eKuiper Prometheus `/metrics` during the publish window.
- Emit a veloFlux-compatible `metrics.openmetrics` (with timestamps) + `result.json`.

We intentionally keep deps stdlib-only (MQTT publisher is in mqtt_qos0_pub.py).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import socket
import string
import sys
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

from mqtt_qos0_pub import MqttError, parse_tcp_broker_url, publish_qos1_with_timing


class PerfDailyError(RuntimeError):
    pass


DEFAULT_STREAM_NAME = "perf_daily_stream"
DEFAULT_PIPELINE_ID = "perf_daily_pipeline"  # mapped to eKuiper rule id for report compatibility

DEFAULT_METRICS_URL = "http://127.0.0.1:20499/metrics"
DEFAULT_OUT_DIR = "tmp/perf_daily"

TRACKED_METRICS = (
    "cpu_usage",
    "memory_usage_bytes",
    "heap_in_use_bytes",
    "heap_in_allocator_bytes",
    "processor_records_in_total",
)

_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+]?(?:\d+\.?\d*|\d*\.?\d+)(?:[eE][-+]?\d+)?)(?:\s+(\d+))?$"
)


def _encode_json(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _join_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/") + "/"
    return urllib.parse.urljoin(base, path.lstrip("/"))


class ApiError(RuntimeError):
    def __init__(self, method: str, path: str, status_code: int, message: str) -> None:
        super().__init__(f"{method} {path} failed: HTTP {status_code}: {message}")
        self.method = method
        self.path = path
        self.status_code = status_code
        self.message = message


class Client:
    def __init__(self, base_url: str, timeout_secs: float) -> None:
        self.base_url = base_url
        self.timeout_secs = timeout_secs

    def request_json(self, method: str, path: str, body: Optional[Any]) -> Any:
        url = _join_url(self.base_url, path)
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = _encode_json(body)
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(method, path, e.code, message) from None
        except urllib.error.URLError as e:  # type: ignore[attr-defined]
            raise ApiError(method, path, 0, str(e)) from None

    def request_text(self, method: str, path: str, body: Optional[Any] = None) -> str:
        url = _join_url(self.base_url, path)
        data = None
        headers = {"Accept": "text/plain"}
        if body is not None:
            data = _encode_json(body)
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace") if raw else ""
        except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(method, path, e.code, message) from None
        except urllib.error.URLError as e:  # type: ignore[attr-defined]
            raise ApiError(method, path, 0, str(e)) from None


def _ignore_not_found(fn) -> None:
    try:
        fn()
    except ApiError as e:
        if e.status_code == 404:
            return
        raise


def wait_for_ekuiper(base_url: str, timeout_secs: float, wait_secs: float = 60.0) -> None:
    client = Client(base_url, timeout_secs)
    deadline = time.time() + wait_secs
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            client.request_text("GET", "/ping")
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(0.5)
    raise PerfDailyError(f"ekuiper not ready after {wait_secs}s: {last_err}")


def wait_for_tcp(host: str, port: int, wait_secs: float = 60.0) -> None:
    deadline = time.time() + wait_secs
    last_err: Optional[str] = None
    while time.time() < deadline:
        s = socket.socket()
        try:
            s.settimeout(2.0)
            s.connect((host, port))
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(1.0)
        finally:
            try:
                s.close()
            except Exception:
                pass
    raise PerfDailyError(f"broker {host}:{port} not ready after {wait_secs}s: {last_err}")


def delete_all_rules(client: Client) -> None:
    items = client.request_json("GET", "/rules", None) or []
    ids: List[str] = []
    for it in items:
        if isinstance(it, dict) and isinstance(it.get("id"), str):
            ids.append(it["id"])
    for rid in ids:
        _ignore_not_found(lambda rid=rid: client.request_text("DELETE", f"/rules/{urllib.parse.quote(rid)}"))


def build_columns(count: int) -> List[str]:
    # Match veloFlux naming (a1..aN) for easier cross-project comparison.
    return [f"a{i}" for i in range(1, count + 1)]


def build_stream_sql(stream_name: str, columns: int, topic: str) -> str:
    fields = ", ".join([f"{c} string" for c in build_columns(columns)])
    # Keep it explicit (schema required by user). Source connection uses global mqtt config (etc/mqtt_source.yaml).
    return f'CREATE STREAM {stream_name} ({fields}) WITH (TYPE="mqtt", FORMAT="json", DATASOURCE="{topic}");'


def build_select_sql(stream_name: str, column_count: int) -> str:
    cols = build_columns(column_count)
    return f"SELECT {','.join(cols)} FROM {stream_name}"


def build_select_sql_by_mode(stream_name: str, column_count: int, sql_mode: str) -> str:
    if sql_mode == "star":
        return f"SELECT * FROM {stream_name}"
    if sql_mode == "explicit":
        return build_select_sql(stream_name, column_count)
    raise PerfDailyError(f"unknown --sql-mode: {sql_mode}")


def provision(
    base_url: str,
    timeout_secs: float,
    stream_name: str,
    pipeline_id: str,
    columns: int,
    topic: str,
    sql_mode: str,
    batch_size: int,
    linger_interval_ms: int,
    force: bool,
    create_stream: bool,
    dry_run: bool,
) -> None:
    client = Client(base_url, timeout_secs)
    sql = build_select_sql_by_mode(stream_name, columns, sql_mode=sql_mode)
    stream_sql = build_stream_sql(stream_name, columns, topic)

    if dry_run:
        print(f"rule sql bytes: {len(sql.encode('utf-8'))}", file=sys.stderr)
        print(f"stream sql bytes: {len(stream_sql.encode('utf-8'))}", file=sys.stderr)
        return

    if force:
        delete_all_rules(client)

    if create_stream:
        _ignore_not_found(lambda: client.request_text("DELETE", f"/streams/{urllib.parse.quote(stream_name)}"))
        # POST /streams expects {"sql": "..."} and returns text.
        client.request_text("POST", "/streams", {"sql": stream_sql})

    # Recreate rule (use pipeline_id as rule id).
    _ignore_not_found(lambda: client.request_text("DELETE", f"/rules/{urllib.parse.quote(pipeline_id)}"))
    # Mirror veloFlux perf harness sink batching knobs:
    # - batchCount -> batchSize
    # - batchDuration(ms) -> lingerInterval(ms)
    rule_req = {
        "id": pipeline_id,
        "sql": sql,
        "actions": [
            {
                "nop": {
                    "log": False,
                    "batchSize": batch_size,
                    "lingerInterval": linger_interval_ms,
                }
            }
        ],
    }
    client.request_text("POST", "/rules", rule_req)


def _alnum_rand_str(rng: random.Random, length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(rng.choices(alphabet, k=length))


def generate_payload_cases(column_count: int, cases: int, str_len: int) -> List[bytes]:
    if cases <= 0:
        return []
    keys = build_columns(column_count)
    rng = random.Random()

    payloads: List[bytes] = []
    for _ in range(cases):
        obj: Dict[str, Any] = {}
        for k in keys:
            obj[k] = _alnum_rand_str(rng, str_len)
        payloads.append(_encode_json(obj))
    return payloads


def scrape_prometheus_text(url: str, timeout_secs: float) -> str:
    req = urllib.request.Request(url=url, method="GET", headers={"Accept": "text/plain"})
    with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
        raw = resp.read()
        return raw.decode("utf-8", errors="replace") if raw else ""


def _labels_have_kv(labels: str, key: str, value: str) -> bool:
    return f'{key}="{value}"' in labels


def _metric_sum(text: str, name: str, require_any: Optional[List[Tuple[str, str]]] = None, require_all: Optional[List[Tuple[str, str]]] = None) -> float:
    """
    Sum all samples matching metric name and label predicates.
    Predicates are simple substring checks; good enough for Prometheus exposition format.
    """
    total = 0.0
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        if m.group(1) != name:
            continue
        labels = m.group(2) or ""
        if require_all:
            ok = True
            for k, v in require_all:
                if not _labels_have_kv(labels, k, v):
                    ok = False
                    break
            if not ok:
                continue
        if require_any:
            ok2 = False
            for k, v in require_any:
                if _labels_have_kv(labels, k, v):
                    ok2 = True
                    break
            if not ok2:
                continue
        try:
            total += float(m.group(3))
        except ValueError:
            continue
    return total


def _write_meta(f) -> None:
    # Minimal HELP/TYPE lines for nicer downstream parsing/debugging.
    f.write("# HELP cpu_usage Derived from irate(process_cpu_seconds_total)\n")
    f.write("# TYPE cpu_usage gauge\n")
    f.write("# HELP memory_usage_bytes go_memstats_heap_sys_bytes (rss-like)\n")
    f.write("# TYPE memory_usage_bytes gauge\n")
    f.write("# HELP heap_in_use_bytes go_memstats_heap_inuse_bytes\n")
    f.write("# TYPE heap_in_use_bytes gauge\n")
    f.write("# HELP heap_in_allocator_bytes go_memstats_alloc_bytes\n")
    f.write("# TYPE heap_in_allocator_bytes gauge\n")
    f.write("# HELP processor_records_in_total Derived from kuiper_source_records_in_total\n")
    f.write("# TYPE processor_records_in_total counter\n")


class OpenMetricsDumper:
    def __init__(
        self,
        metrics_url: str,
        timeout_secs: float,
        interval_ms: int,
        out_path: str,
        instance: str,
        rule_id: str,
    ) -> None:
        self.metrics_url = metrics_url
        self.timeout_secs = timeout_secs
        self.interval_ms = interval_ms
        self.out_path = out_path
        self.instance = instance
        self.rule_id = rule_id
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.samples = 0
        self.errors = 0
        self._prev_cpu: Optional[float] = None
        self._prev_ts_ms: Optional[int] = None

    def start(self) -> None:
        os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
        t = threading.Thread(target=self._run, name="metrics-dumper", daemon=True)
        self.thread = t
        t.start()

    def finish(self) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=10.0)
        try:
            with open(self.out_path, "a", encoding="utf-8") as f:
                f.write("# EOF\n")
        except Exception:
            pass

    def _run(self) -> None:
        interval = max(0.05, self.interval_ms / 1000.0)
        with open(self.out_path, "w", encoding="utf-8") as f:
            _write_meta(f)
            while not self.stop.is_set():
                ts_ms = int(time.time() * 1000)
                try:
                    raw = scrape_prometheus_text(self.metrics_url, timeout_secs=self.timeout_secs)
                    cpu_total = _metric_sum(raw, "process_cpu_seconds_total")
                    mem_sys = _metric_sum(raw, "go_memstats_heap_sys_bytes")
                    mem_alloc = _metric_sum(raw, "go_memstats_alloc_bytes")
                    mem_inuse = _metric_sum(raw, "go_memstats_heap_inuse_bytes")
                    # Aggregate all source counters for the target rule id.
                    src_total = _metric_sum(
                        raw,
                        "kuiper_source_records_in_total",
                        require_any=[("rule", self.rule_id), ("ruleId", self.rule_id)],
                    )

                    cpu_usage = 0.0
                    if self._prev_cpu is not None and self._prev_ts_ms is not None:
                        dt_s = max(0.001, (ts_ms - self._prev_ts_ms) / 1000.0)
                        dv = cpu_total - self._prev_cpu
                        if dv >= 0:
                            cpu_usage = dv / dt_s
                    self._prev_cpu = cpu_total
                    self._prev_ts_ms = ts_ms

                    # Emit synthetic metrics with timestamps (veloFlux report requires ts_ms).
                    f.write(f'cpu_usage{{instance="{self.instance}"}} {cpu_usage} {ts_ms}\n')
                    f.write(f'memory_usage_bytes{{instance="{self.instance}"}} {mem_sys} {ts_ms}\n')
                    f.write(f'heap_in_use_bytes{{instance="{self.instance}"}} {mem_inuse} {ts_ms}\n')
                    f.write(f'heap_in_allocator_bytes{{instance="{self.instance}"}} {mem_alloc} {ts_ms}\n')
                    f.write(
                        f'processor_records_in_total{{instance="{self.instance}",kind="datasource",rule="{self.rule_id}"}} {src_total} {ts_ms}\n'
                    )
                    f.flush()
                    self.samples += 1
                except Exception:
                    self.errors += 1
                time.sleep(interval)

    def result(self) -> Dict[str, Any]:
        return {
            "metrics_url": self.metrics_url,
            "scrape_interval_ms": self.interval_ms,
            "samples": self.samples,
            "errors": self.errors,
            "openmetrics_path": self.out_path,
            "instance": self.instance,
        }


def trim_openmetrics_to_window(in_path: str, start_ms: int, end_ms: int) -> None:
    if start_ms <= 0 or end_ms <= 0 or end_ms < start_ms:
        return
    try:
        with open(in_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        return

    meta_seen: set[str] = set()
    out: List[str] = []
    for line in lines:
        if not line or line == "# EOF":
            continue
        if line.startswith("# HELP ") or line.startswith("# TYPE "):
            parts = line.split(" ", 3)
            if len(parts) >= 3 and parts[2] in TRACKED_METRICS and line not in meta_seen:
                out.append(line)
                meta_seen.add(line)
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        if name not in TRACKED_METRICS:
            continue
        ts = m.group(4)
        if ts is None:
            continue
        try:
            ts_ms = int(ts)
        except ValueError:
            continue
        if start_ms <= ts_ms <= end_ms:
            out.append(line)

    with open(in_path, "w", encoding="utf-8") as f:
        if out:
            f.write("\n".join(out))
            f.write("\n")
        f.write("# EOF\n")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Daily perf scenario: 15k-column MQTT stream + nop rule (eKuiper).")
    p.add_argument("--base-url", default="http://127.0.0.1:9081")
    p.add_argument("--timeout-secs", type=float, default=60.0)

    p.add_argument("--stream-name", default=DEFAULT_STREAM_NAME)
    p.add_argument("--pipeline-id", default=DEFAULT_PIPELINE_ID, help="mapped to eKuiper rule id")
    p.add_argument("--columns", type=int, default=15000)
    p.add_argument("--sql-mode", choices=("explicit", "star"), default="explicit")

    p.add_argument("--broker-url", default="tcp://127.0.0.1:1883")
    p.add_argument("--topic", default="/perf/daily")
    p.add_argument("--qos", type=int, default=1)

    p.add_argument("--cases", type=int, default=20)
    p.add_argument("--str-len", type=int, default=10)
    p.add_argument("--duration-secs", type=int, default=120)
    p.add_argument("--rate", type=int, default=50, help="Messages per second (default: 50)")
    p.add_argument("--batch-size", type=int, default=50, help="sink batchSize (default: 50)")
    p.add_argument("--linger-interval-ms", type=int, default=100, help="sink lingerInterval in ms (default: 100)")

    p.add_argument("--metrics-url", default=DEFAULT_METRICS_URL)
    p.add_argument("--scrape-interval-ms", type=int, default=15000)
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    p.add_argument("--no-metrics", action="store_true")

    p.add_argument("--force", action="store_true", help="delete all rules before creating the case rule")
    p.add_argument("--create-stream", action="store_true", help="(re)create the stream before creating the rule")
    p.add_argument("--dry-run", action="store_true")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("run")
    sub.add_parser("cleanup-rules")
    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)

    if args.columns <= 0:
        raise PerfDailyError("--columns must be > 0")
    if args.cases < 0:
        raise PerfDailyError("--cases must be >= 0")
    if args.str_len <= 0:
        raise PerfDailyError("--str-len must be > 0")
    if args.duration_secs < 0:
        raise PerfDailyError("--duration-secs must be >= 0")
    if args.rate < 0:
        raise PerfDailyError("--rate must be >= 0")
    if args.qos != 1:
        raise PerfDailyError("--qos must be 1 (QoS1 only) for publish")

    wait_for_ekuiper(args.base_url, timeout_secs=args.timeout_secs, wait_secs=60.0)
    broker = parse_tcp_broker_url(args.broker_url)
    wait_for_tcp(broker.host, broker.port, wait_secs=60.0)

    if args.command == "cleanup-rules":
        if args.dry_run:
            return 0
        delete_all_rules(Client(args.base_url, args.timeout_secs))
        return 0

    # run
    provision(
        base_url=args.base_url,
        timeout_secs=args.timeout_secs,
        stream_name=args.stream_name,
        pipeline_id=args.pipeline_id,
        columns=args.columns,
        topic=args.topic,
        sql_mode=args.sql_mode,
        batch_size=args.batch_size,
        linger_interval_ms=args.linger_interval_ms,
        force=args.force,
        create_stream=args.create_stream,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        payloads = generate_payload_cases(5, min(args.cases, 1), args.str_len)
        print(f"sample payload bytes: {len(payloads[0]) if payloads else 0}", file=sys.stderr)
        return 0

    payloads = generate_payload_cases(args.columns, args.cases, args.str_len)
    payload_bytes = len(payloads[0]) if payloads else 0

    dumper = None
    os.makedirs(args.out_dir, exist_ok=True)
    openmetrics_path = os.path.join(args.out_dir, "metrics.openmetrics")

    publish_start_ts_ms = 0
    publish_end_ts_ms = 0
    if not args.no_metrics:
        dumper = OpenMetricsDumper(
            metrics_url=args.metrics_url,
            timeout_secs=args.timeout_secs,
            interval_ms=args.scrape_interval_ms,
            out_path=openmetrics_path,
            instance="local",
            rule_id=args.pipeline_id,
        )
        dumper.start()

    pub = None
    try:
        pub = publish_qos1_with_timing(
            broker_url=args.broker_url,
            topic=args.topic,
            payloads=payloads,
            duration_secs=args.duration_secs,
            rate_per_sec=args.rate,
            client_id=f"perf-daily-{os.getpid()}",
            keepalive_secs=60,
        )
    except MqttError as e:
        raise PerfDailyError(str(e)) from None
    finally:
        if pub is None:
            publish_start_ts_ms = int(time.time() * 1000)
            publish_end_ts_ms = publish_start_ts_ms
        else:
            publish_start_ts_ms = pub.start_ts_ms
            publish_end_ts_ms = pub.end_ts_ms
        if dumper is not None:
            dumper.finish()

    if dumper is not None:
        trim_openmetrics_to_window(openmetrics_path, start_ms=publish_start_ts_ms, end_ms=publish_end_ts_ms)
        duration_s = max(0.001, (publish_end_ts_ms - publish_start_ts_ms) / 1000.0)
        result_path = os.path.join(args.out_dir, "result.json")
        result = {
            "publish_start_ts_ms": publish_start_ts_ms,
            "publish_end_ts_ms": publish_end_ts_ms,
            "publish_duration_secs": duration_s,
            "sent_messages": pub.sent if pub is not None else 0,
            "effective_rate_mps": (pub.sent / duration_s) if pub is not None else 0.0,
            "payload_bytes": payload_bytes,
            "config": {
                "base_url": args.base_url,
                "stream_name": args.stream_name,
                "pipeline_id": args.pipeline_id,
                "columns": args.columns,
                "sql_mode": args.sql_mode,
                "broker_url": args.broker_url,
                "topic": args.topic,
                "qos": args.qos,
                "cases": args.cases,
                "str_len": args.str_len,
                "duration_secs": args.duration_secs,
                "rate": args.rate,
            },
            "metrics": dumper.result(),
        }
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"result_json: {result_path}")
        print(f"openmetrics: {openmetrics_path}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except (PerfDailyError, ApiError) as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2)
