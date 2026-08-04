// Copyright 2024 EMQ Technologies Co., Ltd.
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

package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestLookup(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	hls := &HttpLookupSource{}
	require.NoError(t, hls.Provision(ctx, map[string]any{
		"url":        server.URL,
		"datasource": "/get",
		"method":     "get",
	}))
	require.NoError(t, hls.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	got, err := hls.Lookup(ctx, []string{"code"}, []string{"code"}, []any{float64(200)})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{
		{
			"code": float64(200),
		},
	}, got)
	require.NoError(t, hls.Close(ctx))
}

func TestLookupOAuthConcurrentRefresh(t *testing.T) {
	var tokenRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			token := fmt.Sprintf("token-%d", tokenRequests.Add(1))
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": token,
				"expires_in":   2,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		case "/data":
			if r.Header.Get("Authorization") == "" {
				http.Error(w, "missing authorization", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"code": 200}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	timex.Set(1000)
	ctx := mockContext.NewMockContext("lookupOAuth", "op")
	hls := &HttpLookupSource{}
	require.NoError(t, hls.Provision(ctx, map[string]any{
		"url":        server.URL,
		"datasource": "/data",
		"method":     "get",
		"headers": map[string]any{
			"Authorization": "Bearer {{.access_token}}",
		},
		"oauth": map[string]any{
			"access": map[string]any{
				"url":    server.URL + "/token",
				"expire": "2",
			},
		},
	}))
	require.NoError(t, hls.Connect(ctx, func(status string, message string) {}))
	require.Equal(t, int32(1), tokenRequests.Load())

	timex.Add(2 * time.Second)
	const workers = 16
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			_, err := hls.Lookup(ctx, []string{"code"}, []string{"code"}, []any{float64(200)})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, int32(2), tokenRequests.Load(), "concurrent lookups must share one token refresh")
	state := hls.oauthRuntimeState()
	require.NotNil(t, state)
	require.Equal(t, "Bearer token-2", state.headers["Authorization"])
}

func TestLookupOAuthConcurrentRefreshFailure(t *testing.T) {
	const workers = 16
	var tokenRequests atomic.Int32
	var dataRequests atomic.Int32
	allDataRequests := make(chan struct{})
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var allDataOnce sync.Once
	var refreshOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if tokenRequests.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]any{
					"access_token": "initial-token",
					"expires_in":   2,
				}); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
			refreshOnce.Do(func() { close(refreshStarted) })
			<-releaseRefresh
			http.Error(w, "token service unavailable", http.StatusServiceUnavailable)
		case "/data":
			if dataRequests.Add(1) == workers {
				allDataOnce.Do(func() { close(allDataRequests) })
			}
			<-allDataRequests
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"code": 200}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	timex.Set(1000)
	ctx := mockContext.NewMockContext("lookupOAuthFailure", "op")
	hls := &HttpLookupSource{}
	require.NoError(t, hls.Provision(ctx, map[string]any{
		"url":        server.URL,
		"datasource": "/data",
		"method":     "get",
		"headers": map[string]any{
			"Authorization": "Bearer {{.access_token}}",
		},
		"oauth": map[string]any{
			"access": map[string]any{
				"url":    server.URL + "/token",
				"expire": "2",
			},
		},
	}))
	require.NoError(t, hls.Connect(ctx, func(status string, message string) {}))
	timex.Add(2 * time.Second)

	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			_, err := hls.Lookup(ctx, []string{"code"}, []string{"code"}, []any{float64(200)})
			errs <- err
		}()
	}
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the shared refresh attempt")
	}
	// Give all requests released by the data endpoint time to join the same
	// in-flight refresh before completing it with an error.
	time.Sleep(100 * time.Millisecond)
	close(releaseRefresh)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, int32(2), tokenRequests.Load(), "a failed concurrent refresh must make only one token request")
	state := hls.oauthRuntimeState()
	require.NotNil(t, state)
	require.Equal(t, "Bearer initial-token", state.headers["Authorization"], "a failed refresh must retain the previous token state")
}
