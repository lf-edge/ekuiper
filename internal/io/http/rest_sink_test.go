// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type request struct {
	Method      string
	Body        string
	ContentType string
}

func TestRestSink_Apply(t *testing.T) {
	tests := []struct {
		config map[string]interface{}
		data   []map[string]interface{}
		result []request
	}{
		{
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
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
			},
			data: []map[string]interface{}{{
				"ab": "hello1",
			}, {
				"ab": "hello2",
			}},
			result: []request{{
				Method:      "POST",
				Body:        `[{"ab":"hello1"},{"ab":"hello2"}]`,
				ContentType: "application/json",
			}},
		}, {
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
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType":   "json",
				"sendSingle": true,
				"timeout":    float64(1000),
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
	t.Logf("The test bucket size is %d.", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestRestSink_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

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
		contextLogger.Debugf(string(body))
		fmt.Fprint(w, string(body))
	}))
	tf, _ := transform.GenTransform("", "json", "", "", "", []string{})
	defer ts.Close()
	for i, tt := range tests {
		requests = nil
		ss, ok := tt.config["sendSingle"]
		if !ok {
			ss = false
		}
		s := &RestSink{}
		tt.config["url"] = ts.URL
		s.Configure(tt.config)
		s.Open(ctx)
		vCtx := context.WithValue(ctx, context.TransKey, tf)
		if ss.(bool) {
			for _, d := range tt.data {
				s.Collect(vCtx, d)
			}
		} else {
			s.Collect(vCtx, tt.data)
		}

		s.Close(ctx)
		if !reflect.DeepEqual(tt.result, requests) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, requests)
		}
	}
}

func TestRestSinkTemplate_Apply(t *testing.T) {
	tests := []struct {
		config map[string]interface{}
		data   [][]byte
		result []request
	}{
		{
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"sendSingle":   true,
				"dataTemplate": `{"wrapper":"w1","content":{{json .}},"ab":"{{.ab}}"}`,
			},
			data: [][]byte{[]byte(`{"wrapper":"w1","content":{"ab":"hello1"},"ab":"hello1"}`), []byte(`{"wrapper":"w1","content":{"ab":"hello2"},"ab":"hello2"}`)},
			result: []request{{
				Method:      "POST",
				Body:        `{"wrapper":"w1","content":{"ab":"hello1"},"ab":"hello1"}`,
				ContentType: "application/json",
			}, {
				Method:      "POST",
				Body:        `{"wrapper":"w1","content":{"ab":"hello2"},"ab":"hello2"}`,
				ContentType: "application/json",
			}},
		}, {
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"dataTemplate": `{"wrapper":"arr","content":{{json .}},"content0":{{json (index . 0)}},ab0":"{{index . 0 "ab"}}"}`,
			},
			data: [][]byte{[]byte(`{"wrapper":"arr","content":[{"ab":"hello1"},{"ab":"hello2"}],"content0":{"ab":"hello1"},ab0":"hello1"}`)},
			result: []request{{
				Method:      "POST",
				Body:        `{"wrapper":"arr","content":[{"ab":"hello1"},{"ab":"hello2"}],"content0":{"ab":"hello1"},ab0":"hello1"}`,
				ContentType: "application/json",
			}},
		}, {
			config: map[string]interface{}{
				"method": "get",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"dataTemplate": `{"wrapper":"w1","content":{{json .}},"ab":"{{.ab}}"}`,
			},
			data: [][]byte{[]byte(`{"wrapper":"w1","content":{"ab":"hello1"},"ab":"hello1"}`)},
			result: []request{{
				Method:      "GET",
				ContentType: "",
			}},
		}, {
			config: map[string]interface{}{
				"method": "put",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType":     "html",
				"dataTemplate": `<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>`,
			},
			data: [][]byte{[]byte(`<div>results</div><ul><li>hello1</li><li>hello2</li></ul>`)},
			result: []request{{
				Method:      "PUT",
				ContentType: "text/html",
				Body:        `<div>results</div><ul><li>hello1</li><li>hello2</li></ul>`,
			}},
		}, {
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType":     "form",
				"dataTemplate": `{"content":{{json .}}}`,
			},
			data: [][]byte{[]byte(`{"content":[{"ab":"hello1"},{"ab":"hello2"}]}`)},
			result: []request{{
				Method:      "POST",
				ContentType: "application/x-www-form-urlencoded;param=value",
				Body:        `content=%5B%7B%22ab%22%3A%22hello1%22%7D%2C%7B%22ab%22%3A%22hello2%22%7D%5D`,
			}},
		}, {
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType":     "form",
				"sendSingle":   true,
				"dataTemplate": `{"newab":"{{.ab}}"}`,
			},
			data: [][]byte{[]byte(`{"newab":"hello1"}`), []byte(`{"newab":"hello2"}`)},
			result: []request{{
				Method:      "POST",
				ContentType: "application/x-www-form-urlencoded;param=value",
				Body:        `newab=hello1`,
			}, {
				Method:      "POST",
				ContentType: "application/x-www-form-urlencoded;param=value",
				Body:        `newab=hello2`,
			}},
		}, {
			config: map[string]interface{}{
				"method": "post",
				//"url": "http://localhost/test",  //set dynamically to the test server
				"bodyType":     "json",
				"sendSingle":   true,
				"timeout":      float64(1000),
				"dataTemplate": `{"newab":"{{.ab}}"}`,
			},
			data: [][]byte{[]byte(`{"newab":"hello1"}`), []byte(`{"newab":"hello2"}`)},
			result: []request{{
				Method:      "POST",
				Body:        `{"newab":"hello1"}`,
				ContentType: "application/json",
			}, {
				Method:      "POST",
				Body:        `{"newab":"hello2"}`,
				ContentType: "application/json",
			}},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestRestSink_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

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
		contextLogger.Debugf(string(body))
		fmt.Fprint(w, string(body))
	}))
	defer ts.Close()
	for i, tt := range tests {
		requests = nil
		s := &RestSink{}
		tt.config["url"] = ts.URL
		s.Configure(tt.config)
		s.Open(ctx)
		vCtx := context.WithValue(ctx, context.TransKey, transform.TransFunc(func(d interface{}) ([]byte, bool, error) {
			return d.([]byte), true, nil
		}))
		for _, d := range tt.data {
			s.Collect(vCtx, d)
		}
		s.Close(ctx)
		if !reflect.DeepEqual(tt.result, requests) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, requests)
		}
	}
}

func TestRestSinkErrorLog(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("Error reading body: %v", err)
			http.Error(w, "can't read body", http.StatusBadRequest)
			return
		}
		if strings.Contains(string(body), "success") {
			fmt.Fprint(w, "result")
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	t.Run("Test rest sink timeout and return correct error info", func(t *testing.T) {
		s := &RestSink{}
		config := map[string]interface{}{
			"url":     ts.URL,
			"timeout": float64(10),
		}
		s.Configure(config)
		s.Open(context.Background())

		tf, _ := transform.GenTransform("", "json", "", "", "", []string{})
		vCtx := context.WithValue(context.Background(), context.TransKey, tf)
		reqBody := []map[string]interface{}{
			{"ab": "hello1"},
			{"ab": "hello2"},
		}
		err := s.Collect(vCtx, reqBody)

		if errorx.IsIOError(err) && !strings.Contains(err.Error(), "hello1") {
			t.Errorf("should include request body, but got %s", err.Error())
		}
		fmt.Println(err.Error())
		s.Close(context.Background())
	})

	t.Run("Test  error info", func(t *testing.T) {
		s := &RestSink{}
		config := map[string]interface{}{
			"url":          ts.URL,
			"method":       "put",
			"bodyType":     "text",
			"responseType": "body",
			"timeout":      float64(1000),
		}
		s.Configure(config)
		s.Open(context.Background())
		tf, _ := transform.GenTransform("", "json", "", "", "", []string{})
		vCtx := context.WithValue(context.Background(), context.TransKey, tf)
		err := s.Collect(vCtx, []map[string]interface{}{
			{"ab": "hello1"},
			{"ab": "hello2"},
		})
		assert.Error(t, err)
		fmt.Println(err.Error())
		if errorx.IsIOError(err) && !strings.Contains(err.Error(), "404") {
			t.Errorf("should start with io error, but got %s", err.Error())
		}

		s.Close(context.Background())
	})

	t.Run("Test decode error", func(t *testing.T) {
		s := &RestSink{}
		config := map[string]interface{}{
			"url":       ts.URL,
			"timeout":   float64(10),
			"method":    "post",
			"debugResp": true,
		}
		s.Configure(config)
		s.Open(context.Background())

		tf, _ := transform.GenTransform("", "delimited", "", "", "", []string{})
		vCtx := context.WithValue(context.Background(), context.TransKey, tf)
		reqBody := map[string]interface{}{
			"ab": "success",
		}
		err := s.Collect(vCtx, reqBody)
		// for parse error, omit it.
		assert.NoError(t, err)
		s.Close(context.Background())
	})
}
