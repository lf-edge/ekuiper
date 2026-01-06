# Format Encode/Decode Benchmark Results

Benchmark comparing data serialization formats with 1500 numeric columns in `lfek`.

## Results (1500 columns)

| Format | Encode Time (ns/op) | Decode Time (ns/op) | Encoded Size (Bytes) |
|--------|---------------------|---------------------|----------------------|
| **Delimited** | ~89,000 | ~120,000 | 9,573 |
| **JSON** | ~304,000 | ~208,000 | 23,465 |
| **URL-encoded**| ~418,000 | ~259,000 | 20,463 |

## Insights

- **Delimited (CSV)** is the clear winner for performance and size, but requires strict schema alignment.
- **JSON** is standard but 3x slower to encode than delimited.
- **URL-encoded** is the slowest due to overhead.

## Run

```bash
go test -bench=BenchmarkAllFormats -benchmem ./internal/converter/
```
