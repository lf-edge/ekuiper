# Perf Daily Wide MQTT (GitHub Actions harness)

This folder contains a small perf harness used by the GitHub Actions workflow
`.github/workflows/perf-daily-wide-mqtt.yaml`.

The scenario mirrors the `veloFlux` workflow "Perf Daily Wide MQTT":
- Create a stream (MQTT source).
- Create a rule (case 1) with sink batching (`batchSize`/`lingerInterval`) and run traffic while scraping Prometheus metrics.
- Restart eKuiper.
- Delete other rules, then create a rule (case 2) and run again while scraping metrics.
- Render charts into the GitHub step summary and upload artifacts.

Files:
- `test/perf_daily/perf_daily.py`: scenario runner (REST + MQTT publish + metrics scrape).
- `test/perf_daily/perf_daily_report.py`: render `result.json` into Markdown (step summary).

Metrics mapping (for veloFlux-compatible charts):
- `memory_usage_bytes` is mapped from Prometheus `process_resident_memory_bytes` (true RSS).
