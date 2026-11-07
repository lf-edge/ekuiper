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

package milvus

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func testProps(endpoint string) map[string]any {
	return map[string]any{
		"endpoint":    endpoint,
		"collection":  "fault_history",
		"vectorField": "embedding",
		"topK":        3,
		"metric":      "COSINE",
	}
}

func TestValidate(t *testing.T) {
	s := &lookupSource{}
	assert.Error(t, s.Validate(map[string]any{"collection": "c"}))
	assert.Error(t, s.Validate(map[string]any{"endpoint": "http://x", "collection": "c"}))
	assert.Error(t, s.Validate(map[string]any{"endpoint": "http://x", "vectorField": "v"}))
	assert.Error(t, s.Validate(map[string]any{
		"endpoint": "http://x", "collection": "c", "vectorField": "v", "metric": "bad",
	}))
	assert.NoError(t, s.Validate(testProps("http://x")))
	assert.Equal(t, 3, s.c.TopK)

	// topK fallback to default
	s2 := &lookupSource{}
	assert.NoError(t, s2.Validate(map[string]any{
		"endpoint": "http://x", "collection": "c", "vectorField": "v",
	}))
	assert.Equal(t, defaultTopK, s2.c.TopK)
	assert.Equal(t, "COSINE", string(s2.metric))
}

func TestLookupSearchRequestAndResults(t *testing.T) {
	var capturedBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/vectordb/entities/search", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedBody))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": []map[string]any{
				{"id": 1, "sop": "check bearing", "distance": 0.123},
				{"id": 2, "sop": "check motor", "distance": 0.9},
			},
		})
	}))
	defer ts.Close()

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps(ts.URL)))
	results, err := s.Lookup(mockContext.NewMockContext("test", "tt"), []string{"id", "sop"}, []string{"embedding"}, []any{[]float64{0.1, 0.2, 0.3}})
	require.NoError(t, err)
	require.Len(t, results, 2)

	// request payload checks
	assert.Equal(t, "fault_history", capturedBody["collectionName"])
	data := capturedBody["data"].([]any)[0].(map[string]any)
	assert.Equal(t, []any{0.1, 0.2, 0.3}, data["vector"])
	sp := capturedBody["searchParams"].(map[string]any)
	assert.Equal(t, "embedding", sp["annsField"])
	assert.Equal(t, float64(3), sp["topK"])
	assert.Equal(t, "COSINE", sp["metricType"])
	assert.Equal(t, []any{"id", "sop"}, capturedBody["outputFields"])

	// result mapping: fields kept, distance/score unified
	assert.Equal(t, "check bearing", results[0]["sop"])
	assert.Equal(t, float64(1), results[0]["id"])
	assert.InDelta(t, 0.123, results[0]["distance"], 1e-9)
	assert.InDelta(t, (0.123+1)/2, results[0]["score"], 1e-9)
	assert.InDelta(t, 0.95, results[1]["score"], 1e-9)

	// distance/score are not duplicated as plain fields
	_, ok := results[0]["score"].(float64)
	require.True(t, ok)
}

func TestLookupWithStringVector(t *testing.T) {
	var capturedBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedBody))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": []map[string]any{}})
	}))
	defer ts.Close()

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps(ts.URL)))
	results, err := s.Lookup(mockContext.NewMockContext("test", "tt"), nil, []string{"embedding"}, []any{"0.1,0.2"})
	require.NoError(t, err)
	assert.Empty(t, results)
	data := capturedBody["data"].([]any)[0].(map[string]any)
	assert.Equal(t, []any{0.1, 0.2}, data["vector"])
}

func TestLookupFilters(t *testing.T) {
	var capturedBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedBody))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": []map[string]any{}})
	}))
	defer ts.Close()

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps(ts.URL)))
	_, err := s.Lookup(mockContext.NewMockContext("test", "tt"), nil,
		[]string{"embedding", "model", "year"},
		[]any{[]float64{1, 2}, "A100", 2026})
	require.NoError(t, err)
	assert.Equal(t, `model == "A100" and year == 2026`, capturedBody["filter"])
}

func TestLookupNoVector(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps("http://localhost:1")))
	_, err := s.Lookup(mockContext.NewMockContext("test", "tt"), nil, []string{"model"}, []any{"A100"})
	assert.Error(t, err)
}

func TestLookupDimensionMismatch(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(map[string]any{
		"endpoint": "http://localhost:1", "collection": "c",
		"vectorField": "v", "dimension": 3,
	}))
	_, err := s.Lookup(mockContext.NewMockContext("test", "tt"), nil, []string{"v"}, []any{[]float64{1, 2}})
	assert.Error(t, err)
}

func TestLookupBackendError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 100, "message": "collection not loaded"})
	}))
	defer ts.Close()

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps(ts.URL)))
	_, err := s.Lookup(mockContext.NewMockContext("test", "tt"), nil, []string{"embedding"}, []any{[]float64{1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "collection not loaded")
}

func TestLookupHTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps(ts.URL)))
	_, err := s.Lookup(mockContext.NewMockContext("test", "tt"), nil, []string{"embedding"}, []any{[]float64{1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestPing(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/vectordb/collections/has", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "has": true})
	}))
	defer ts.Close()

	s := &lookupSource{}
	assert.NoError(t, s.Ping(mockContext.NewMockContext("test", "tt"), testProps(ts.URL)))

	// collection missing
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "has": false})
	}))
	defer ts2.Close()
	assert.Error(t, s.Ping(mockContext.NewMockContext("test", "tt"), testProps(ts2.URL)))
}

