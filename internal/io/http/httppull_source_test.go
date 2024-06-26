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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

type Response struct {
	Message string `json:"message"`
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "Hello, GET!",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Message: "Hello, POST!",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func createServer() *httptest.Server {
	router := http.NewServeMux()
	router.HandleFunc("/get", handleGet)
	router.HandleFunc("/post", handlePost)
	server := httptest.NewServer(router)
	return server
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
	require.NoError(t, source.Connect(ctx))
	dataCh := make(chan any, 1)
	source.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataCh <- data
	}, func(ctx api.StreamContext, err error) {

	})
	require.Equal(t, []map[string]interface{}{
		{
			"message": "Hello, GET!",
		},
	}, <-dataCh)
	source.Close(ctx)
}
