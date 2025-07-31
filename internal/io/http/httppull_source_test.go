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
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

type Response struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

func handleCodeErr(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "Err",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(resp)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "Hello, GET!",
		Code:    200,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "Hello, POST!",
		Code:    200,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func handleErr(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "Err",
		Code:    400,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func handleAuth(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "auth",
		Code:    200,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func handleRefresh(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "refresh",
		Code:    200,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func handleParam(w http.ResponseWriter, r *http.Request) {
	v, _ := strconv.ParseInt(r.URL.Query().Get("a"), 10, 64)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	v = v + 1
	json.NewEncoder(w).Encode(map[string]interface{}{"a": v})
}

func createServer() *httptest.Server {
	router := http.NewServeMux()
	router.HandleFunc("/get", handleGet)
	router.HandleFunc("/post", handlePost)
	router.HandleFunc("/err", handleErr)
	router.HandleFunc("/codeErr", handleCodeErr)
	router.HandleFunc("/auth", handleAuth)
	router.HandleFunc("/refresh", handleRefresh)
	router.HandleFunc("/param", handleParam)
	server := httptest.NewServer(router)
	return server
}

func TestHttpPullStateSource(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	source := &HttpPullSource{}
	require.NoError(t, source.Provision(ctx, map[string]any{
		"url":        server.URL,
		"datasource": "/param?a={{.a}}",
		"method":     "get",
		"parameters": map[string]interface{}{"a": 1},
	}))
	require.NoError(t, source.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	dataCh := make(chan any, 1)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataCh <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, []map[string]interface{}{
		{
			"a": float64(2),
		},
	}, <-dataCh)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataCh <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, []map[string]interface{}{
		{
			"a": float64(3),
		},
	}, <-dataCh)
	source.Close(ctx)
	close(dataCh)
}

func TestHttpPullSource(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	source := &HttpPullSource{}
	require.NoError(t, source.Provision(ctx, map[string]any{
		"url":        server.URL,
		"datasource": "/get",
		"method":     "get",
	}))
	require.NoError(t, source.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	dataCh := make(chan any, 1)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataCh <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, []map[string]interface{}{
		{
			"message": "Hello, GET!",
			"code":    float64(200),
		},
	}, <-dataCh)
	source.Close(ctx)
	close(dataCh)

	source = &HttpPullSource{}
	require.NoError(t, source.Provision(ctx, map[string]any{
		"url":          server.URL,
		"datasource":   "/post",
		"method":       "post",
		"responseType": "body",
	}))
	require.NoError(t, source.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	dataCh = make(chan any, 1)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataCh <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, []map[string]interface{}{
		{
			"message": "Hello, POST!",
			"code":    float64(200),
		},
	}, <-dataCh)
	close(dataCh)
	source.Close(ctx)

	source = &HttpPullSource{}
	require.NoError(t, source.Provision(ctx, map[string]any{
		"url":          server.URL,
		"datasource":   "/err",
		"method":       "post",
		"responseType": "body",
	}))
	require.NoError(t, source.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	errCh := make(chan error, 1)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) { errCh <- err })
	require.Error(t, <-errCh)
	close(errCh)
	source.Close(ctx)

	source = &HttpPullSource{}
	require.NoError(t, source.Provision(ctx, map[string]any{
		"url":          server.URL,
		"datasource":   "/codeErr",
		"method":       "post",
		"responseType": "code",
	}))
	require.NoError(t, source.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	errCh = make(chan error, 1)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) { errCh <- err })
	require.Error(t, <-errCh)
	close(errCh)
	source.Close(ctx)
}

func TestSourceIncremental(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	source := &HttpPullSource{}
	require.NoError(t, source.Provision(ctx, map[string]any{
		"url":         server.URL,
		"datasource":  "/get",
		"method":      "get",
		"incremental": true,
	}))
	require.NoError(t, source.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	dataCh := make(chan any, 1)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataCh <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, []map[string]interface{}{
		{
			"message": "Hello, GET!",
			"code":    float64(200),
		},
	}, <-dataCh)

	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataCh <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Nil(t, <-dataCh)
}
