// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"net/http"
	"reflect"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx/httptestx"
	"github.com/lf-edge/ekuiper/internal/topo/context"
)

func TestConfigureLookup(t *testing.T) {
	tests := []struct {
		name        string
		props       map[string]interface{}
		err         error
		config      *RawConf
		accessConf  *AccessTokenConf
		refreshConf *RefreshTokenConf
		tokens      map[string]interface{}
	}{
		// Test oAuth
		{
			name: "oAuth with access token and constant expire",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "3600",
					},
				},
			},
			config: &RawConf{
				Url:          "http://localhost:52345/",
				ResendUrl:    "http://localhost:52345/",
				Method:       http.MethodGet,
				Interval:     DefaultInterval,
				Timeout:      DefaultTimeout,
				BodyType:     "none",
				ResponseType: "code",
				Headers: map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				HeadersMap: map[string]string{
					"Authorization": "Bearer {{.token}}",
				},
				OAuth: map[string]map[string]interface{}{
					"access": {
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "3600",
					},
				},
			},
			accessConf: &AccessTokenConf{
				Url:            "http://localhost:52345/token",
				Body:           "{\"username\": \"admin\",\"password\": \"0000\"}",
				Expire:         "3600",
				ExpireInSecond: 3600,
			},
			tokens: map[string]interface{}{
				"token":         httptestx.DefaultToken,
				"refresh_token": httptestx.RefreshToken,
				"client_id":     "test",
				"expires":       float64(36000),
			},
		},
		{
			name: "oAuth with access token and dynamic expire",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "{{.expires}}",
					},
				},
			},
			config: &RawConf{
				Url:          "http://localhost:52345/",
				ResendUrl:    "http://localhost:52345/",
				Method:       http.MethodGet,
				Interval:     DefaultInterval,
				Timeout:      DefaultTimeout,
				BodyType:     "none",
				ResponseType: "code",
				Headers: map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				HeadersMap: map[string]string{
					"Authorization": "Bearer {{.token}}",
				},
				OAuth: map[string]map[string]interface{}{
					"access": {
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "{{.expires}}",
					},
				},
			},
			accessConf: &AccessTokenConf{
				Url:            "http://localhost:52345/token",
				Body:           "{\"username\": \"admin\",\"password\": \"0000\"}",
				Expire:         "{{.expires}}",
				ExpireInSecond: 36000,
			},
			tokens: map[string]interface{}{
				"token":         httptestx.DefaultToken,
				"refresh_token": httptestx.RefreshToken,
				"client_id":     "test",
				"expires":       float64(36000),
			},
		},
		{
			name: "oAuth with access token and refresh token",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "3600",
					},
					"refresh": map[string]interface{}{
						"url": "http://localhost:52345/refresh",
						"headers": map[string]interface{}{
							"Authorization": "Bearer {{.token}}",
							"RefreshToken":  "{{.refresh_token}}",
						},
					},
				},
			},
			config: &RawConf{
				Url:          "http://localhost:52345/",
				ResendUrl:    "http://localhost:52345/",
				Method:       http.MethodGet,
				Interval:     DefaultInterval,
				Timeout:      DefaultTimeout,
				BodyType:     "none",
				ResponseType: "code",
				Headers: map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				HeadersMap: map[string]string{
					"Authorization": "Bearer {{.token}}",
				},
				OAuth: map[string]map[string]interface{}{
					"access": {
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "3600",
					},
					"refresh": {
						"url": "http://localhost:52345/refresh",
						"headers": map[string]interface{}{
							"Authorization": "Bearer {{.token}}",
							"RefreshToken":  "{{.refresh_token}}",
						},
					},
				},
			},
			accessConf: &AccessTokenConf{
				Url:            "http://localhost:52345/token",
				Body:           "{\"username\": \"admin\",\"password\": \"0000\"}",
				Expire:         "3600",
				ExpireInSecond: 3600,
			},
			refreshConf: &RefreshTokenConf{
				Url: "http://localhost:52345/refresh",
				Headers: map[string]string{
					"Authorization": "Bearer {{.token}}",
					"RefreshToken":  "{{.refresh_token}}",
				},
			},
			tokens: map[string]interface{}{
				"token":         httptestx.DefaultToken,
				"refresh_token": httptestx.RefreshToken,
				"client_id":     "test",
				"expires":       float64(36000),
			},
		},
		// test default
		{
			name: "default",
			props: map[string]interface{}{
				"url": "http://localhost:9090/",
			},
			config: &RawConf{
				Url:          "http://localhost:9090/",
				ResendUrl:    "http://localhost:9090/",
				Method:       http.MethodGet,
				Interval:     DefaultInterval,
				Timeout:      DefaultTimeout,
				BodyType:     "none",
				ResponseType: "code",
			},
		},
	}

	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test %d: %s", i, tt.name), func(t *testing.T) {
			r := &lookupSource{}
			err := r.Configure("", tt.props)
			if err != nil {
				if tt.err == nil {
					t.Errorf("Expected error: %v", err)
				} else {
					if err.Error() != tt.err.Error() {
						t.Errorf("Error mismatch\nexp\t%v\ngot\t%v", tt.err, err)
					}
				}
				return
			}
			if !reflect.DeepEqual(r.config, tt.config) {
				t.Errorf("Config mismatch\nexp\t%+v\ngot\t%+v", tt.config, r.config)
			}
			if !reflect.DeepEqual(r.accessConf, tt.accessConf) {
				t.Errorf("AccessConf mismatch\nexp\t%+v\ngot\t%+v", tt.accessConf, r.accessConf)
			}
			if !reflect.DeepEqual(r.refreshConf, tt.refreshConf) {
				t.Errorf("RefreshConf mismatch\nexp\t%+v\ngot\t%+v", tt.refreshConf, r.refreshConf)
			}
			if !reflect.DeepEqual(r.tokens, tt.tokens) {
				t.Errorf("Tokens mismatch\nexp\t%s\ngot\t%s", tt.tokens, r.tokens)
			}
		})
	}
}

func TestLookupPull(t *testing.T) {
	conf.IsTesting = false
	conf.InitClock()
	r := &lookupSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("data3", map[string]interface{}{
		"url":          "http://localhost:52345/",
		"responseType": "body",
	})
	require.NoError(t, err)
	resp, err := r.pull(context.Background())
	require.NoError(t, err)
	require.Equal(t, []map[string]interface{}{
		{
			"code": float64(200),
			"data": map[string]interface{}{
				"device_id":   "d1",
				"temperature": float64(25.5),
				"humidity":    float64(60),
			},
		},
		{
			"code": float64(200),
			"data": map[string]interface{}{
				"device_id":   "d2",
				"temperature": float64(25.5),
				"humidity":    float64(60),
			},
		},
	}, resp)
}

func withCompressedPayloadEndpoint() httptestx.MockServerRouterOption {
	return func(r *mux.Router, ctx *sync.Map) error {
		r.HandleFunc("/lookup1", func(w http.ResponseWriter, r *http.Request) {
			out := []*struct {
				Code int `json:"code"`
				Data struct {
					DeviceId    string  `json:"device_id"`
					Temperature float64 `json:"temperature"`
					Humidity    float64 `json:"humidity"`
				} `json:"data"`
			}{
				{
					Code: 200,
					Data: struct {
						DeviceId    string  `json:"device_id"`
						Temperature float64 `json:"temperature"`
						Humidity    float64 `json:"humidity"`
					}{
						DeviceId:    "d1",
						Temperature: 30.5,
						Humidity:    67.0,
					},
				},
				{
					Code: 200,
					Data: struct {
						DeviceId    string  `json:"device_id"`
						Temperature float64 `json:"temperature"`
						Humidity    float64 `json:"humidity"`
					}{
						DeviceId:    "d2",
						Temperature: 35.5,
						Humidity:    80.0,
					},
				},
			}
			httptestx.JSONOut(w, out)
		}).Methods(http.MethodGet)

		r.Use(httptestx.CompressHandler)

		return nil
	}
}

func testLookupPullWithCompressionAlgorithm(algo string, t *testing.T) {
	conf.IsTesting = false
	conf.InitClock()
	r := &lookupSource{}
	server, closer := httptestx.MockAuthServer(
		withCompressedPayloadEndpoint(),
	)
	server.Start()
	defer closer()
	err := r.Configure("lookup1", map[string]interface{}{
		"url":          "http://localhost:52345/",
		"responseType": "body",
		"compression":  algo,
	})
	require.NoError(t, err)
	resp, err := r.pull(context.Background())
	require.NoError(t, err)
	require.Equal(t, []map[string]interface{}{
		{
			"code": float64(200),
			"data": map[string]interface{}{
				"device_id":   "d1",
				"temperature": float64(30.5),
				"humidity":    float64(67.0),
			},
		},
		{
			"code": float64(200),
			"data": map[string]interface{}{
				"device_id":   "d2",
				"temperature": float64(35.5),
				"humidity":    float64(80.0),
			},
		},
	}, resp)
}

func TestLookupPullWithGZipAlgorithm(t *testing.T) {
	testLookupPullWithCompressionAlgorithm(string(compressor.GZIP), t)
}

func TestLookupPullWithFlateAlgorithm(t *testing.T) {
	testLookupPullWithCompressionAlgorithm(string(compressor.FLATE), t)
}

func TestLookupPullWithZLibAlgorithm(t *testing.T) {
	testLookupPullWithCompressionAlgorithm(string(compressor.ZLIB), t)
}

func TestLookupPullWithZSTDAlgorithm(t *testing.T) {
	testLookupPullWithCompressionAlgorithm(string(compressor.ZSTD), t)
}

func TestLookupJoin(t *testing.T) {
	datas := []map[string]interface{}{
		{
			"a": 1,
			"b": 3,
			"c": 5,
		},
		{
			"a": 2,
			"b": 4,
			"c": 6,
		},
	}
	keys := []string{"a"}
	values := []interface{}{1}
	l := &lookupSource{}
	got := l.lookupJoin(datas, keys, values)
	require.Equal(t, []map[string]interface{}{
		{
			"a": 1,
			"b": 3,
			"c": 5,
		},
	}, got)
}

func TestLookup(t *testing.T) {
	conf.IsTesting = false
	conf.InitClock()
	r := &lookupSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("data3", map[string]interface{}{
		"url":          "http://localhost:52345/",
		"responseType": "body",
	})
	require.NoError(t, err)
	tuples, err := r.Lookup(context.Background(), nil, []string{"code"}, []interface{}{float64(200)})
	require.NoError(t, err)
	require.Len(t, tuples, 2)
}

func TestLookupActions(t *testing.T) {
	r := &lookupSource{}
	require.NoError(t, r.Open(context.Background()))
	require.NoError(t, r.Close(context.Background()))
	require.NotNil(t, GetLookUpSource())
}
