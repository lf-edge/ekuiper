// Copyright 2026 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package vector provides the shared vector primitives used by the vector
// database lookup sources (Milvus, pgvector).  It covers parsing the query
// vector out of arbitrary rule payloads, metric selection and similarity
// scoring so that every backend behaves the same way.
package vector

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Metric is the similarity metric used to rank the retrieved vectors.
type Metric string

const (
	// MetricCosine ranks candidates by cosine similarity, the most common
	// metric for text and sensor embeddings.
	MetricCosine Metric = "COSINE"
	// MetricL2 ranks candidates by euclidean distance.
	MetricL2 Metric = "L2"
	// MetricIP ranks candidates by inner product.
	MetricIP Metric = "IP"
)

// DefaultMetric is used when the source configuration leaves the metric unset.
const DefaultMetric = MetricCosine

// ParseMetric validates a configured metric string.  An empty value resolves
// to the default (COSINE).  The lookup is case insensitive to be friendly to
// JSON rule definitions.
func ParseMetric(v string) (Metric, error) {
	switch Metric(strings.ToUpper(strings.TrimSpace(v))) {
	case "":
		return DefaultMetric, nil
	case MetricCosine:
		return MetricCosine, nil
	case MetricL2:
		return MetricL2, nil
	case MetricIP:
		return MetricIP, nil
	default:
		return "", fmt.Errorf("invalid metric %q, supported values are COSINE, L2 and IP", v)
	}
}

// ParseVector converts a query vector coming from a rule payload into a
// normalized []float64.  Payloads arrive from SQL literals, JSON messages or
// upstream operators, so the accepted shapes are:
//   - []float64 (a previously computed embedding)
//   - []any of numbers or numeric strings
//   - JSON array string, e.g. "[0.1, 0.2, 0.3]"
//   - comma separated string, e.g. "0.1,0.2,0.3"
func ParseVector(v any) ([]float64, error) {
	switch value := v.(type) {
	case []float64:
		out := make([]float64, len(value))
		copy(out, value)
		return out, nil
	case []float32:
		out := make([]float64, len(value))
		for i, e := range value {
			out[i] = float64(e)
		}
		return out, nil
	case []any:
		return parseVectorSlice(value)
	case string:
		trimmed := strings.TrimSpace(value)
		if strings.HasPrefix(trimmed, "[") {
			var arr []float64
			if err := json.Unmarshal([]byte(trimmed), &arr); err != nil {
				return nil, fmt.Errorf("invalid vector literal: %v", err)
			}
			return arr, nil
		}
		return parseVectorString(trimmed)
	case []int:
		out := make([]float64, len(value))
		for i, e := range value {
			out[i] = float64(e)
		}
		return out, nil
	case nil:
		return nil, fmt.Errorf("query vector is null")
	default:
		return nil, fmt.Errorf("unsupported vector type %T, expected []float64, []any or a string", v)
	}
}

func parseVectorSlice(slice []any) ([]float64, error) {
	out := make([]float64, 0, len(slice))
	for i, e := range slice {
		switch ev := e.(type) {
		case float64:
			out = append(out, ev)
		case float32:
			out = append(out, float64(ev))
		case int:
			out = append(out, float64(ev))
		case int64:
			out = append(out, float64(ev))
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(ev), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid vector element at index %d: %v", i, err)
			}
			out = append(out, f)
		default:
			return nil, fmt.Errorf("invalid vector element type %T at index %d", e, i)
		}
	}
	return out, nil
}

func parseVectorString(s string) ([]float64, error) {
	if s == "" {
		return nil, fmt.Errorf("query vector is empty")
	}
	parts := strings.Split(s, ",")
	out := make([]float64, 0, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid vector element at index %d: %v", i, err)
		}
		out = append(out, f)
	}
	return out, nil
}

// FormatVector renders a vector as the bracketed text form understood by both
// the pgvector literal syntax and the Milvus REST payloads.
func FormatVector(v []float64) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, e := range v {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(e, 'f', -1, 64))
	}
	sb.WriteByte(']')
	return sb.String()
}

// ValidateDimension checks that the query vector matches the configured
// collection dimension.  A dim <= 0 means "not enforced".
func ValidateDimension(v []float64, dim int) error {
	if dim > 0 && len(v) != dim {
		return fmt.Errorf("query vector dimension %d does not match configured dimension %d", len(v), dim)
	}
	for i, e := range v {
		if math.IsNaN(e) || math.IsInf(e, 0) {
			return fmt.Errorf("invalid vector element at index %d: must be a finite number", i)
		}
	}
	return nil
}

// CosineSimilarity returns the cosine similarity of two equally sized
// vectors.  The result lies in [-1, 1]; a zero-magnitude vector yields 0.
func CosineSimilarity(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vector dimension mismatch: %d vs %d", len(a), len(b))
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0, nil
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb)), nil
}

// L2Distance returns the euclidean distance between two vectors.
func L2Distance(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vector dimension mismatch: %d vs %d", len(a), len(b))
	}
	var sum float64
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum), nil
}

// InnerProduct returns the inner product of two vectors.
func InnerProduct(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vector dimension mismatch: %d vs %d", len(a), len(b))
	}
	var dot float64
	for i := range a {
		dot += a[i] * b[i]
	}
	return dot, nil
}

// Score converts a raw backend distance into a unified similarity score in
// [0, 1] so downstream rules can threshold consistently across metrics:
//   - COSINE: score = (similarity + 1) / 2
//   - L2:     score = 1 / (1 + distance)
//   - IP:     score = (innerProduct + 1) / 2
//
// The similarity value carries the raw backend output (distance for L2,
// similarity for the others) through the `distance` result field as well.
func Score(m Metric, distance float64) float64 {
	switch m {
	case MetricL2:
		return 1 / (1 + math.Abs(distance))
	case MetricIP:
		return (distance + 1) / 2
	default:
		return (distance + 1) / 2
	}
}

// Normalize returns a unit-length copy of the vector.  A zero-magnitude
// vector is returned unchanged.
func Normalize(v []float64) []float64 {
	var norm float64
	for _, e := range v {
		norm += e * e
	}
	if norm == 0 {
		return v
	}
	norm = math.Sqrt(norm)
	out := make([]float64, len(v))
	for i, e := range v {
		out[i] = e / norm
	}
	return out
}
