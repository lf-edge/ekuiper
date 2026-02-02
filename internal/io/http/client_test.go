// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func init() {
	testx.InitEnv("http")
	conf.Config.Basic.EnablePrivateNet = true
}

func TestInitConf(t *testing.T) {
	m := map[string]interface{}{}
	ctx := mockContext.NewMockContext("1", "2")
	c := &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))
	m = map[string]interface{}{
		"url": "",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"method": "123",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"timeout": -1,
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"timeout": -1,
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"responseType": "mock",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"method": "post",
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"bodyType": "123",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"url": "scae::",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"compression": "zlib",
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"compression": "mock",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"oauth": map[string]any{
			"access": map[string]interface{}{
				"url":    "http://example.com/auth",
				"expire": "hello",
			},
		},
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"oauth": map[string]any{
			"access": map[string]interface{}{
				"expire": "hello",
			},
		},
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"oauth": map[string]any{
			"refresh": map[string]interface{}{
				"expire": "hello",
			},
		},
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))
}

func TestDecode(t *testing.T) {
	testcases := []struct {
		v   interface{}
		got []map[string]interface{}
	}{
		{
			v: map[string]interface{}{
				"method": "post",
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
		{
			v: []map[string]interface{}{
				{
					"method": "post",
				},
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
		{
			v: []interface{}{
				map[string]interface{}{
					"method": "post",
				},
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
	}
	for _, tc := range testcases {
		data, err := json.Marshal(tc.v)
		require.NoError(t, err)
		g, err := decode(data)
		require.NoError(t, err)
		require.Equal(t, tc.got, g)
	}
}

func TestClientAuth(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()

	c := &ClientConf{}
	ctx := mockContext.NewMockContext("1", "2")
	require.NoError(t, c.InitConf(ctx, "", map[string]interface{}{
		"oauth": map[string]interface{}{
			"access": map[string]interface{}{
				"url":    fmt.Sprintf("%s/auth", server.URL),
				"expire": "3600",
				"body":   `{"a":1}`,
			},
			"refresh": map[string]interface{}{
				"url": fmt.Sprintf("%s/refresh", server.URL),
				"headers": map[string]interface{}{
					"a": "{{.message}}",
				},
				"body": `{"a":1}`,
			},
		},
	}))
	require.NoError(t, c.auth(ctx))
	require.NoError(t, c.refresh(ctx))
}

func TestOAuthClientCredentials(t *testing.T) {
	// 1. Create a mock OAuth server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			// Verify Header
			contentType := r.Header.Get("Content-Type")
			if contentType != "application/x-www-form-urlencoded" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(fmt.Sprintf("Invalid Content-Type: %s", contentType)))
				return
			}

			// Verify Body
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			bodyStr := string(body)
			expectedBody := "grant_type=client_credentials&client_id=test&client_secret=test&scope=https://eventhubs.azure.net/.default"
			if bodyStr != expectedBody {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(fmt.Sprintf("Invalid Body: %s", bodyStr)))
				return
			}

			// Return Token
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "mock_access_token",
				"expires_in":   3600,
			})
			return
		}

		// Verify Protected Resource Access
		if r.URL.Path == "/data" {
			authHeader := r.Header.Get("Authorization")
			if authHeader != "Bearer mock_access_token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	// 2. Configure Client with OAuth
	ctx := mockContext.NewMockContext("rule1", "op1")
	c := &ClientConf{}

	// Simulation of user configuration
	props := map[string]interface{}{
		"url":    ts.URL + "/data",
		"method": "POST",
		"headers": map[string]interface{}{
			"Authorization": "Bearer {{.access_token}}",
		},
		"oauth": map[string]interface{}{
			"access": map[string]interface{}{
				"url": ts.URL + "/token",
				// Manually constructed body for client credentials
				"body": "grant_type=client_credentials&client_id=test&client_secret=test&scope=https://eventhubs.azure.net/.default",
				// WORKAROUND: Explicitly set Content-Type header
				"headers": map[string]interface{}{
					"Content-Type": "application/x-www-form-urlencoded",
				},
				"expire": "3600",
			},
		},
	}

	err := c.InitConf(ctx, "", props)
	require.NoError(t, err)

	// 3. Connect (Triggers Auth)
	// This is where the auth flow happens. If it fails (e.g. wrong content type), this should error.
	err = c.Conn(ctx)
	require.NoError(t, err, "Connection failed, likely due to auth failure")

	// 4. Send Data (Verifies Token Usage)
	data, _ := json.Marshal(map[string]interface{}{"data": 123})
	resp, err := c.Send(ctx, "json", "POST", c.config.Url, c.parsedHeaders, nil, "", data)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
