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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestLifecycleMethods(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "has": true})
	}))
	defer ts.Close()

	s := &lookupSource{}
	ctx := mockContext.NewMockContext("test", "tt")
	require.NoError(t, s.Provision(ctx, testProps(ts.URL)))
	assert.NoError(t, s.Open(ctx))
	assert.NoError(t, s.Connect(ctx, func(_, _ string) {}))
	assert.NoError(t, s.Close(ctx))
	assert.NotNil(t, GetLookupSource())
}

func TestConnectFailure(t *testing.T) {
	// server that never returns a valid collection
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "has": false})
	}))
	defer ts.Close()

	s := &lookupSource{}
	require.NoError(t, s.Provision(mockContext.NewMockContext("test", "tt"), testProps(ts.URL)))
	var state string
	var msg string
	err := s.Connect(mockContext.NewMockContext("test", "tt"), func(st string, m string) {
		state = st
		msg = m
	})
	assert.Error(t, err)
	assert.Equal(t, api.ConnectionDisconnected, state)
	assert.Contains(t, msg, "does not exist")
}

func TestPingCodeError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 99, "message": "boom"})
	}))
	defer ts.Close()

	s := &lookupSource{}
	err := s.Ping(mockContext.NewMockContext("test", "tt"), testProps(ts.URL))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestPingInvalidEndpoint(t *testing.T) {
	s := &lookupSource{}
	err := s.Ping(mockContext.NewMockContext("test", "tt"), map[string]any{
		"endpoint": "http://127.0.0.1:1", "collection": "c", "vectorField": "v", "timeout": 1,
	})
	assert.Error(t, err)
}

func TestPostDecodeError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer ts.Close()

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps(ts.URL)))
	var out map[string]any
	assert.Error(t, s.post(mockContext.NewMockContext("test", "tt"), searchPath, map[string]any{}, &out))
}

func TestPostRequestBuildError(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps("http://x")))
	// malformed method/host is exercised through a canceled context
	ctx, cancel := mockContext.NewMockContext("test", "tt").WithCancel()
	cancel()
	var out map[string]any
	assert.Error(t, s.post(ctx, searchPath, map[string]any{}, &out))
}

func TestToFloatVariants(t *testing.T) {
	for _, v := range []any{float32(1.5), int(2), int64(3), "4.5"} {
		f, ok := toFloat(v)
		require.True(t, ok)
		assert.InDelta(t, float64(f), f, 1e-9)
	}
	_, ok := toFloat(struct{}{})
	assert.False(t, ok)
	_, ok = toFloat("abc")
	assert.False(t, ok)
}

func TestValidateTimeout(t *testing.T) {
	s := &lookupSource{}
	props := testProps("http://x")
	props["timeout"] = 0
	require.NoError(t, s.Validate(props))
	assert.Equal(t, defaultTimout, s.c.Timeout)
}

func TestExtractQueryVectorParseError(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps("http://x")))
	_, _, err := s.extractQuery([]string{"embedding"}, []any{12345})
	assert.Error(t, err)
}

func TestLookupKeyMismatch(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps("http://x")))
	_, err := s.Lookup(mockContext.NewMockContext("test", "tt"), nil, []string{"a"}, []any{})
	assert.Error(t, err)
}
