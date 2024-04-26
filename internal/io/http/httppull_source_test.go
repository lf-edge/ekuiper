// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx/httptestx"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/mock"
)

var wrongPath, _ = filepath.Abs("/tmp/wrong")

// Test configure to properties
func TestConfigure(t *testing.T) {
	tests := []struct {
		name        string
		props       map[string]interface{}
		err         error
		config      *RawConf
		accessConf  *AccessTokenConf
		refreshConf *RefreshTokenConf
		tokens      map[string]interface{}
	}{
		{
			name: "default",
			props: map[string]interface{}{
				"incremental": true,
				"url":         "http://localhost:9090/",
			},
			config: &RawConf{
				Incremental:  true,
				Url:          "http://localhost:9090/",
				ResendUrl:    "http://localhost:9090/",
				Method:       http.MethodGet,
				Interval:     DefaultInterval,
				Timeout:      DefaultTimeout,
				BodyType:     "none",
				ResponseType: "code",
			},
		},
		// Test wrong properties
		{
			name: "wrong props",
			props: map[string]interface{}{
				"incremental": true,
				"url":         123,
			},
			err: fmt.Errorf("fail to parse the properties: 1 error(s) decoding:\n\n* 'url' expected type 'string', got unconvertible type 'int', value: '123'"),
		},
		{
			name: "empty url",
			props: map[string]interface{}{
				"incremental": true,
				"url":         "",
			},
			err: fmt.Errorf("url is required"),
		},
		{
			name: "wrong method",
			props: map[string]interface{}{
				"url":    "http://localhost:9090/",
				"method": "wrong",
			},
			err: fmt.Errorf("Not supported HTTP method wrong."),
		},
		{
			name: "wrong bodytype",
			props: map[string]interface{}{
				"url":      "http://localhost:9090/",
				"bodyType": "wrong",
			},
			err: fmt.Errorf("Not valid body type value wrong."),
		},
		{
			name: "wrong response type",
			props: map[string]interface{}{
				"url":          "http://localhost:9090/",
				"responseType": "wrong",
			},
			err: fmt.Errorf("Not valid response type value wrong."),
		},
		{
			name: "wrong url",
			props: map[string]interface{}{
				"url": "http:/localhost:9090/",
			},
			err: fmt.Errorf("Invalid url, host not found"),
		},
		{
			name: "wrong interval",
			props: map[string]interface{}{
				"url":      "http:/localhost:9090/",
				"interval": -2,
			},
			err: fmt.Errorf("interval must be greater than 0"),
		},
		{
			name: "wrong timeout",
			props: map[string]interface{}{
				"url":     "http:/localhost:9090/",
				"timeout": -2,
			},
			err: fmt.Errorf("timeout must be greater than or equal to 0"),
		},
		{
			name: "wrong tls",
			props: map[string]interface{}{
				"url":               "http://localhost:9090/",
				"certificationPath": wrongPath,
			},
			err: fmt.Errorf(fmt.Sprintf("stat %s: no such file or directory", wrongPath)),
		},
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
		// Wrong auth configs
		{
			name: "oAuth wrong access token config",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": 3600,
					},
				},
			},
			err: errors.New("fail to parse the access properties of oAuth: 1 error(s) decoding:\n\n* 'expire' expected type 'string', got unconvertible type 'int', value: '3600'"),
		},
		{
			name: "oAuth wrong access url",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url": "",
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
			},
		},
		{
			name: "oAuth miss access",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"refresh": map[string]interface{}{
						"url": "http://localhost:52345/",
					},
				},
			},
			err: errors.New("if setting oAuth, `access` property is required"),
		},
		{
			name: "oAuth wrong refresh token config",
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
						"url": 1202,
					},
				},
			},
			accessConf: &AccessTokenConf{
				Url:            "http://localhost:52345/token",
				Body:           "{\"username\": \"admin\",\"password\": \"0000\"}",
				Expire:         "3600",
				ExpireInSecond: 3600,
			},
			err: errors.New("fail to parse the refresh token properties: 1 error(s) decoding:\n\n* 'url' expected type 'string', got unconvertible type 'int', value: '1202'"),
		},
		{
			name: "oAuth refresh token missing url",
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
		// oAuth authentication flow errors
		{
			name: "oAuth auth error",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"1234\"}",
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
				BodyType:     "json",
				ResponseType: "code",
				Headers: map[string]string{
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
			err: errors.New("fail to authorize by oAuth: Cannot parse access token response to json: response code error: 400"),
		},
		{
			name: "oAuth refresh error",
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
							"RefreshToken":  "{{.token}}",
						},
					},
				},
			},
			err: errors.New("fail to authorize by oAuth: Cannot parse refresh token response to json: response code error: 400"),
		},
		{
			name: "oAuth wrong access expire template",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "{{..expp}}",
					},
				},
			},
			err: errors.New("fail to authorize by oAuth: fail to parse the expire time for access token: Template Invalid: template: sink:1: unexpected . after term \".\""),
		},
		{
			name: "oAuth wrong access expire type",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http://localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "{{.token}}",
					},
				},
			},
			err: errors.New("fail to authorize by oAuth: fail to covert the expire time privatisation for access token: cannot convert string(privatisation) to int"),
		},
		{
			name: "oAuth wrong access url",
			props: map[string]interface{}{
				"url": "http://localhost:52345/",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
				},
				"oAuth": map[string]interface{}{
					"access": map[string]interface{}{
						"url":    "http:localhost:52345/token",
						"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
						"expire": "{{.token}}",
					},
				},
			},
			err: errors.New("fail to authorize by oAuth: fail to get access token: Post \"http:localhost:52345/token\": http: no Host in request URL"),
		},
		{
			name: "oAuth wrong refresh header template",
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
							"RefreshToken":  "{{..token}}",
						},
					},
				},
			},
			err: errors.New("fail to authorize by oAuth: fail to parse the header for refresh token request RefreshToken: Template Invalid: template: sink:1: unexpected . after term \".\""),
		},
		{
			name: "oAuth wrong refresh url",
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
						"url": "http:localhost:52345/refresh2",
						"headers": map[string]interface{}{
							"Authorization": "Bearer {{.token}}",
							"RefreshToken":  "{{.token}}",
						},
					},
				},
			},
			err: errors.New("fail to authorize by oAuth: fail to get refresh token: Post \"http:localhost:52345/refresh2\": http: no Host in request URL"),
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
			r := &PullSource{}
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

func TestPullWithAuth(t *testing.T) {
	conf.IsTesting = false
	conf.InitClock()
	r := &PullSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("data", map[string]interface{}{
		"url":      "http://localhost:52345/",
		"interval": 100,
		"headers": map[string]interface{}{
			"Authorization": "Bearer {{.token}}",
		},
		"oAuth": map[string]interface{}{
			"access": map[string]interface{}{
				"url":    "http://localhost:52345/token",
				"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
				"expire": "10",
			},
			"refresh": map[string]interface{}{
				"url": "http://localhost:52345/refresh",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
					"RefreshToken":  "{{.refresh_token}}",
				},
			},
		},
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mc := conf.Clock
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"device_id": "device1", "humidity": 60.0, "temperature": 25.5}, map[string]interface{}{}, mc.Now()),
	}
	mock.TestSourceOpen(r, exp, t)
}

func testPullWithAuthAndCompression(t *testing.T, compressionAlgorithm string) {
	conf.IsTesting = false
	conf.InitClock()

	// create mock handler
	withMockCompressionHandler := func() httptestx.MockServerRouterOption {
		return func(r *mux.Router, ctx *sync.Map) error {
			// we need create sub router for compression endpoint,
			// cause in actual auth endpoints don't need to compression
			subr := r.NewRoute().Subrouter()
			subr.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
				token := r.Header.Get("Authorization")
				if token != "Bearer "+httptestx.DefaultToken {
					http.Error(w, "invalid token", http.StatusBadRequest)
				}

				out := &struct {
					DeviceId    string  `json:"device_id"`
					Temperature float64 `json:"temperature"`
					Humidity    float64 `json:"humidity"`
				}{
					DeviceId:    "device1",
					Temperature: 30.1,
					Humidity:    65.5,
				}
				httptestx.JSONOut(w, out)
			})
			subr.Use(httptestx.CompressHandler)
			return nil
		}
	}

	r := &PullSource{}
	server, closer := httptestx.MockAuthServer(
		withMockCompressionHandler(),
	)
	server.Start()
	defer closer()

	err := r.Configure("data", map[string]any{
		"url":      "http://localhost:52345/",
		"interval": 100,
		"headers": map[string]any{
			"Authorization": "Bearer {{.token}}",
		},
		"oAuth": map[string]interface{}{
			"access": map[string]interface{}{
				"url":    "http://localhost:52345/token",
				"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
				"expire": "10",
			},
			"refresh": map[string]interface{}{
				"url": "http://localhost:52345/refresh",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
					"RefreshToken":  "{{.refresh_token}}",
				},
			},
		},
		"compression": compressionAlgorithm,
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	mc := conf.Clock
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"device_id": "device1", "humidity": 65.5, "temperature": 30.1}, map[string]interface{}{}, mc.Now()),
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestPullWithAuthAndGZipCompression(t *testing.T) {
	testPullWithAuthAndCompression(t, compressor.GZIP)
}

func TestPullWithAuthAndFlateCompression(t *testing.T) {
	testPullWithAuthAndCompression(t, compressor.FLATE)
}

func TestPullWithAuthAndZLibCompression(t *testing.T) {
	testPullWithAuthAndCompression(t, compressor.ZLIB)
}

func TestPullWithAuthAndZStdCompression(t *testing.T) {
	testPullWithAuthAndCompression(t, compressor.ZSTD)
}

func TestPullBodyAuth(t *testing.T) {
	r := &PullSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("data6", map[string]interface{}{
		"method":   "POST",
		"body":     `{"device": "d1", "token": "{{.token}}"}`,
		"url":      "http://localhost:52345/",
		"interval": 100,
		"oAuth": map[string]interface{}{
			"access": map[string]interface{}{
				"url":    "http://localhost:52345/token",
				"body":   "{\"username\": \"admin\",\"password\": \"0000\"}",
				"expire": "10",
			},
			"refresh": map[string]interface{}{
				"url": "http://localhost:52345/refresh",
				"headers": map[string]interface{}{
					"Authorization": "Bearer {{.token}}",
					"RefreshToken":  "{{.refresh_token}}",
				},
			},
		},
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mc := conf.Clock
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"device_id": "device1", "humidity": 60.0, "temperature": 25.5}, map[string]interface{}{}, mc.Now()),
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestPullIncremental(t *testing.T) {
	conf.IsTesting = false
	conf.InitClock()
	r := &PullSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("data2", map[string]interface{}{
		"url":          "http://localhost:52345/",
		"interval":     100,
		"incremental":  true,
		"responseType": "body",
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mc := conf.Clock
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "device0", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}, mc.Now()),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "device1", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}, mc.Now()),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "device2", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}, mc.Now()),
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestPullJsonList(t *testing.T) {
	conf.IsTesting = false
	conf.InitClock()
	r := &PullSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("data3", map[string]interface{}{
		"url":          "http://localhost:52345/",
		"interval":     100,
		"responseType": "body",
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mc := conf.Clock
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "d1", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}, mc.Now()),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "d2", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}, mc.Now()),
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestPullUrlTimeRange(t *testing.T) {
	r := &PullSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("", map[string]interface{}{
		"url":          "http://localhost:52345/data4?device=d1&start={{.LastPullTime}}&end={{.PullTime}}",
		"interval":     110,
		"responseType": "body",
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	// Mock time
	mockclock.ResetClock(143)
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "d1", "humidity": float64(43), "temperature": float64(33)}}, map[string]interface{}{}, time.UnixMilli(143)),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "d1", "humidity": float64(53), "temperature": float64(43)}}, map[string]interface{}{}, time.UnixMilli(253)),
	}
	c := mockclock.GetMockClock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Add(350 * time.Millisecond)
	}()
	mock.TestSourceOpen(r, exp, t)
}

func TestPullBodyTimeRange(t *testing.T) {
	r := &PullSource{}
	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()
	err := r.Configure("data5", map[string]interface{}{
		"url":          "http://localhost:52345/",
		"interval":     110,
		"responseType": "body",
		"method":       "POST",
		"body":         `{"device": "d1", "start": {{.LastPullTime}}, "end": {{.PullTime}}}`,
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	// Mock time
	mockclock.ResetClock(143)
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "d1", "humidity": float64(43), "temperature": float64(33)}}, map[string]interface{}{}, time.UnixMilli(143)),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "d1", "humidity": float64(53), "temperature": float64(43)}}, map[string]interface{}{}, time.UnixMilli(253)),
	}
	c := mockclock.GetMockClock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Add(350 * time.Millisecond)
	}()
	mock.TestSourceOpen(r, exp, t)
}

func TestPullErrorTest(t *testing.T) {
	conf.IsTesting = false
	conf.InitClock()

	tests := []struct {
		name string
		conf map[string]interface{}
		exp  []api.SourceTuple
	}{
		{
			name: "wrong url template",
			conf: map[string]interface{}{"url": "http://localhost:52345/data4?device=d1&start={{.lastPullTime}}&end={{.PullTime}", "interval": 10},
			exp: []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: errors.New("parse url http://localhost:52345/data4?device=d1&start={{.lastPullTime}}&end={{.PullTime} error Template Invalid: template: sink:1: bad character U+007D '}'"),
				},
			},
		}, {
			name: "wrong header template",
			conf: map[string]interface{}{"url": "http://localhost:52345/data4", "interval": 10, "HeadersTemplate": "\"Authorization\": \"Bearer {{.aatoken}}"},
			exp: []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: errors.New("parse headers error parsed header template is not json: \"Authorization\": \"Bearer <no value>"),
				},
			},
		}, {
			name: "wrong body template",
			conf: map[string]interface{}{"url": "http://localhost:52345/data4", "interval": 10, "body": `{"device": "d1", "start": {{.LastPullTime}}, "end": {{.pullTime}}}`},
			exp: []api.SourceTuple{
				api.NewDefaultSourceTupleWithTime(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "", "humidity": float64(0), "temperature": float64(0)}}, map[string]interface{}{}, time.UnixMilli(143)),
			},
		}, {
			name: "wrong response",
			conf: map[string]interface{}{"url": "http://localhost:52345/aa/data4", "interval": 10},
			exp: []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: errors.New("parse response error response code error: 404"),
				},
			},
		}, {
			name: "wrong request",
			conf: map[string]interface{}{"url": "http://localhost:52345/aa/data4", "interval": 10, "bodyType": "form", "body": "ddd"},
			exp: []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: errors.New("send request error invalid content: ddd"),
				},
			},
		},
	}

	server, closer := httptestx.MockAuthServer(
		httptestx.WithBuiltinTestDataEndpoints(),
	)
	server.Start()
	defer closer()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := &PullSource{}
			err := r.Configure("", test.conf)
			assert.NoError(t, err)
			mock.TestSourceOpen(r, test.exp, t)
		})
	}
}
