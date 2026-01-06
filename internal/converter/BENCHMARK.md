# Delimited Converter Benchmark Report

## 1. Optimization Strategy Results

We compared three implementation strategies for the `delimited` format converter:

1. **Baseline**: Original manual parsing implementation.
2. **encoding/csv**: Using Go standard library `encoding/csv` (RFC 4180 compliant).
3. **Optimized Manual**: Enhanced manual parsing with pre-allocation and validation.

### Results (1500 columns, single row)

| Implementation | Time (ns/op) | Memory (B/op) | Allocs | vs Baseline |
|----------------|--------------|---------------|--------|-------------|
| **Original Baseline** | 117,700 | 218,653 | 1,525 | - |
| #1: encoding/csv | 126,500 | 331,586 | 1,557 | +7.5% slower, +51% memory |
| | | | | |
| **#3: Optimized Manual** | **70,000** | **140,371** | **1,509** | **40% faster, 36% less memory** |

**Decision:** Adopted **Optimized Manual** approach.
- **Trade-off:** Does not handle quoted fields/escapes (not RFC 4180 compliant).
- **Benefit:** Significant performance check for high-throughput scenarios.

## 2. Header Support (hasHeader) Analysis

Added support for `hasHeader` configuration, allowing dynamic column deduction.

### Impact of Dynamic Header Parsing

| Operation | Variation | Time (ns/op) | Memory (B/op) | Allocs |
|-----------|-----------|--------------|---------------|--------|
| **Encode** | Static Fields | ~102,000 | 60,961 | 2,259 |
| | **Dynamic Header** | ~96,000 | 61,125 | 2,259 |
| **Decode** | Static Fields | ~78,000 | 140,371 | 1,509 |
| | **Dynamic Header** | ~108,000 | 200,294 | 1,511 |

**Analysis:**
- **Encode:** Negligible difference. Field deduction happens once and is cached.
- **Decode:** ~38% overhead (~30µs) per batch.
  - This cost is due to parsing the header line (splitting string) and allocating new strings for column names.
  - In a single-row batch (benchmark worst case), this is noticeable.
  - In larger batches, this cost is amortized.

## 3. Comparison vs JSON

Delimited format remains significantly faster than JSON.

| Operation | Format | Time (approx) | Speedup |
|-----------|--------|---------------|---------|
| **Encode** | JSON | ~315,000 ns | 1x |
| | **Delimited** | ~100,000 ns | **3.1x faster** |
| | | | |
| **Decode** | JSON | ~210,000 ns | 1x |
| | **Delimited** (No Header) | ~78,000 ns | **2.7x faster** |
| | **Delimited** (With Header)| ~108,000 ns | **1.9x faster** |
