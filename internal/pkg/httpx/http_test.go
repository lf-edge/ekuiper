// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestIsUrl(t *testing.T) {
	urls := []string{
		"http://localhost:8080/abc",
		"https://localhost:8080/abc",
		"http://122.122.122:8080/abc",
	}
	for _, u := range urls {
		if err := IsHttpUrl(u); err != nil {
			t.Errorf("expect %s is url but got %v", u, err)
		}
	}
	badUrls := []string{
		"ws://localhost:8080/abc",
		"http:/baidu.com:8080/abc",
		"localhost:8080/abc",
	}
	for _, u := range badUrls {
		if err := IsHttpUrl(u); err == nil {
			t.Errorf("expect %s is not url but passed", u)
		}
	}
}

func TestErr(t *testing.T) {
	tests := []struct {
		name string
		u    string
		data any
		err  string
	}{
		{
			name: "wrong data",
			u:    "http://noexist.org",
			data: 45,
			err:  "http send only supports bytes but receive invalid content: 45",
		},
		{
			name: "wrong url",
			u:    "\\\abc",
			data: "test",
			err:  "fail to create request: parse \"\\\\\\abc\": net/url: invalid control character in URL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := SendWithFormData(conf.Log, nil, "formdata", "POST", test.u, nil, nil, "", test.data)
			require.EqualError(t, err, test.err)
		})
	}
}

func TestIsValidUrl(t *testing.T) {
	tests := []struct {
		url   string
		valid bool
	}{
		{"http://google.com", true},
		{"https://google.com", true},
		{"file:///tmp/file", true},
		{"ftp://server", false},
		{"not url", false},
		{"http://", false},          // empty host
		{"file://host/path", false}, // file URI should not have host
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			assert.Equal(t, tt.valid, IsValidUrl(tt.url))
		})
	}
}

func TestGetSSRFDialContext(t *testing.T) {
	// Mock config
	origConfig := conf.Config
	defer func() { conf.Config = origConfig }()
	conf.Config = &model.KuiperConf{}

	t.Run("Block private IP by default", func(t *testing.T) {
		conf.Config.Basic.EnablePrivateNet = false
		dialer := GetSSRFDialContext(time.Second)

		conn, err := dialer(context.Background(), "tcp", "127.0.0.1:45321")
		assert.Error(t, err)
		if err != nil {
			assert.Contains(t, err.Error(), "in internal network")
		}
		if conn != nil {
			conn.Close()
		}
	})

	t.Run("Allow private IP when enabled", func(t *testing.T) {
		conf.Config.Basic.EnablePrivateNet = true
		dialer := GetSSRFDialContext(time.Second)

		conn, err := dialer(context.Background(), "tcp", "127.0.0.1:45321")
		if err != nil {
			assert.NotContains(t, err.Error(), "in internal network")
		}
		if conn != nil {
			conn.Close()
		}
	})
}

func TestSend(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	// Test Send
	resp, err := Send(conf.Log, http.DefaultClient, "text", "POST", ts.URL, nil, []byte("data"))
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Test SendWithFormData
	// Case 1: normal form data
	resp, err = SendWithFormData(conf.Log, http.DefaultClient, "form", "POST", ts.URL, nil, map[string]string{"key": "value"}, "", []byte("data"))
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Case 2: mixed form data
	resp, err = SendWithFormData(conf.Log, http.DefaultClient, "formdata", "POST", ts.URL, nil, map[string]string{"key": "value"}, "file", []byte("filecontent"))
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestSendWithFormData_BodyTypes(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// "none" body type
	resp, err := Send(conf.Log, http.DefaultClient, "none", "POST", ts.URL, nil, nil)
	assert.NoError(t, err)
	resp.Body.Close()

	// "binary" body type (byte slice)
	resp, err = Send(conf.Log, http.DefaultClient, "binary", "POST", ts.URL, nil, []byte("binary"))
	assert.NoError(t, err)
	resp.Body.Close()

	// "binary" body type (string)
	resp, err = Send(conf.Log, http.DefaultClient, "binary", "POST", ts.URL, nil, "stringbinary")
	assert.NoError(t, err)
	resp.Body.Close()

	// "json" body type (string)
	resp, err = Send(conf.Log, http.DefaultClient, "json", "POST", ts.URL, nil, `{"a":1}`)
	assert.NoError(t, err)
	resp.Body.Close()

	// Unsupported body type
	_, err = Send(conf.Log, http.DefaultClient, "unsupported", "POST", ts.URL, nil, nil)
	assert.Error(t, err)
}
