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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

type request struct {
	Method      string
	Body        string
	ContentType string
}

func TestRestSink_Apply(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]interface{}
		data   []map[string]interface{}
		result []request
	}{
		{
			name: "1",
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"sendSingle": true,
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "POST",
				Body:        `{"ab":"hello1"}`,
				ContentType: "application/json",
			}, {
				Method:      "POST",
				Body:        `{"ab":"hello2"}`,
				ContentType: "application/json",
			}},
		}, {
			name: "2",
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"headers": map[string]any{
					"Content-Type": "application/vnd.microsoft.servicebus.json",
				},
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "POST",
				Body:        `[{"ab":"hello1"},{"ab":"hello2"}]`,
				ContentType: "application/vnd.microsoft.servicebus.json",
			}},
		}, {
			name: "3",
			config: map[string]interface{}{
				"method": "get",
				//"url": "http://localhost/test",  //set dynamically to the test server
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "GET",
				ContentType: "",
			}},
		}, {
			name: "4",
			config: map[string]interface{}{
				"method": "put",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType": "text",
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "PUT",
				ContentType: "text/plain",
				Body:        `[{"ab":"hello1"},{"ab":"hello2"}]`,
			}},
		}, {
			name: "5",
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType": "form",
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "POST",
				ContentType: "application/x-www-form-urlencoded;param=value",
				Body:        `result=%5B%7B%22ab%22%3A%22hello1%22%7D%2C%7B%22ab%22%3A%22hello2%22%7D%5D`,
			}},
		}, {
			name: "6",
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType":   "form",
				"sendSingle": true,
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "POST",
				ContentType: "application/x-www-form-urlencoded;param=value",
				Body:        `ab=hello1`,
			}, {
				Method:      "POST",
				ContentType: "application/x-www-form-urlencoded;param=value",
				Body:        `ab=hello2`,
			}},
		}, {
			name: "7",
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType":   "json",
				"sendSingle": true,
				//"timeout":    float64(1000),
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "POST",
				Body:        `{"ab":"hello1"}`,
				ContentType: "application/json",
			}, {
				Method:      "POST",
				Body:        `{"ab":"hello2"}`,
				ContentType: "application/json",
			}},
		},
	}
	ctx := mockContext.NewMockContext("testApply", "op")

	var requests []request
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("Error reading body: %v", err)
			http.Error(w, "can't read body", http.StatusBadRequest)
			return
		}

		requests = append(requests, request{
			Method:      r.Method,
			Body:        string(body),
			ContentType: r.Header.Get("Content-Type"),
		})
		ctx.GetLogger().Debugf(string(body))
		fmt.Fprint(w, string(body))
	}))
	defer ts.Close()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests = nil
			ss, ok := tt.config["sendSingle"]
			if !ok {
				ss = false
			}
			s := &RestSink{}
			tt.config["url"] = ts.URL
			e := s.Provision(ctx, tt.config)
			assert.NoError(t, e)
			e = s.Connect(ctx, func(status string, message string) {
				// do nothing
			})
			assert.NoError(t, e)
			if ss.(bool) {
				for _, d := range tt.data {
					e = s.Collect(ctx, &xsql.Tuple{
						Message: d,
					})
					assert.NoError(t, e)
				}
			} else {
				b := &xsql.WindowTuples{
					Content: make([]xsql.Row, 0, len(tt.data)),
				}
				for _, d := range tt.data {
					b.Content = append(b.Content, &xsql.Tuple{
						Message: d,
					})
				}
				e = s.CollectList(ctx, b)
				assert.NoError(t, e)
			}

			err := s.Close(ctx)
			assert.NoError(t, err)
			assert.Equal(t, tt.result, requests)
		})
	}
}

func TestRestSinkCollect(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	s := &RestSink{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"url":       fmt.Sprintf("%s/get", server.URL),
		"method":    "get",
		"debugResp": true,
	}))
	data := &xsql.Tuple{
		Message: map[string]interface{}{
			"a": 1,
		},
	}
	require.NoError(t, s.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	require.NoError(t, s.collect(ctx, data, data.ToMap()))
	require.NoError(t, s.Close(ctx))
}

func TestRestSinkRecoverErr(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	data := &xsql.Tuple{
		Message: map[string]interface{}{
			"a": 1,
		},
	}
	sErr := &RestSink{}
	require.NoError(t, sErr.Provision(ctx, map[string]any{
		"url":    fmt.Sprintf("%s/get123", server.URL),
		"method": "get",
	}))
	require.NoError(t, sErr.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	err := sErr.collect(ctx, data, data.ToMap())
	require.Error(t, err)
	require.False(t, errorx.IsIOError(err))
	s := &RestSink{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"url":    fmt.Sprintf("%s/get", server.URL),
		"method": "get",
	}))
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/http/recoverAbleErr", "return(true)")
	defer failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/http/recoverAbleErr")
	require.NoError(t, s.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	err = s.collect(ctx, data, data.ToMap())
	require.Error(t, err)
	require.True(t, errorx.IsIOError(err))
}
