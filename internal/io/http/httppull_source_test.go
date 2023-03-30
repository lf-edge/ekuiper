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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/pkg/api"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func jsonOut(w http.ResponseWriter, err error, out interface{}) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	err = enc.Encode(out)
	// Problems encoding
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

const (
	DefaultToken = "privatisation"
	RefreshToken = "privaterefresh"
)

// mock http auth server
func mockAuthServer() *httptest.Server {
	l, _ := net.Listen("tcp", "127.0.0.1:52345")
	router := mux.NewRouter()
	i := 0
	router.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		body := &struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}{}
		err := json.NewDecoder(r.Body).Decode(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		if body.Username != "admin" || body.Password != "0000" {
			http.Error(w, "invalid username or password", http.StatusBadRequest)
		}
		out := &struct {
			Token        string `json:"token"`
			RefreshToken string `json:"refresh_token"`
			ClientId     string `json:"client_id"`
			Expires      int64  `json:"expires"`
		}{
			Token:        DefaultToken,
			RefreshToken: RefreshToken,
			ClientId:     "test",
			Expires:      36000,
		}
		jsonOut(w, err, out)
	}).Methods(http.MethodPost)
	router.HandleFunc("/refresh", func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token != "Bearer "+DefaultToken {
			http.Error(w, "invalid token", http.StatusBadRequest)
		}
		rt := r.Header.Get("RefreshToken")
		if rt != RefreshToken {
			http.Error(w, "invalid refresh token", http.StatusBadRequest)
		}
		out := &struct {
			Token        string `json:"token"`
			RefreshToken string `json:"refresh_token"`
			ClientId     string `json:"client_id"`
			Expires      int64  `json:"expires"`
		}{
			Token:        DefaultToken,
			RefreshToken: RefreshToken,
			ClientId:     "test",
			Expires:      36000,
		}
		jsonOut(w, nil, out)
	}).Methods(http.MethodPost)
	router.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token != "Bearer "+DefaultToken {
			http.Error(w, "invalid token", http.StatusBadRequest)
		}
		out := &struct {
			DeviceId    string  `json:"device_id"`
			Temperature float64 `json:"temperature"`
			Humidity    float64 `json:"humidity"`
		}{
			DeviceId:    "device1",
			Temperature: 25.5,
			Humidity:    60.0,
		}
		jsonOut(w, nil, out)
	}).Methods(http.MethodGet)
	// Return same data for 3 times
	router.HandleFunc("/data2", func(w http.ResponseWriter, r *http.Request) {
		out := &struct {
			Code int `json:"code"`
			Data struct {
				DeviceId    string  `json:"device_id"`
				Temperature float64 `json:"temperature"`
				Humidity    float64 `json:"humidity"`
			} `json:"data"`
		}{
			Code: 200,
			Data: struct {
				DeviceId    string  `json:"device_id"`
				Temperature float64 `json:"temperature"`
				Humidity    float64 `json:"humidity"`
			}{
				DeviceId:    "device" + strconv.Itoa(i/3),
				Temperature: 25.5,
				Humidity:    60.0,
			},
		}
		i++
		jsonOut(w, nil, out)
	}).Methods(http.MethodGet)
	server := httptest.NewUnstartedServer(router)
	server.Listener.Close()
	server.Listener = l
	return server
}

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
				Incremental:        true,
				Url:                "http://localhost:9090/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "none",
				ResponseType:       "code",
				InsecureSkipVerify: true,
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
				Url:                "http://localhost:52345/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "none",
				ResponseType:       "code",
				InsecureSkipVerify: true,
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
				"token":         DefaultToken,
				"refresh_token": RefreshToken,
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
				Url:                "http://localhost:52345/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "none",
				ResponseType:       "code",
				InsecureSkipVerify: true,
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
				"token":         DefaultToken,
				"refresh_token": RefreshToken,
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
				Url:                "http://localhost:52345/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "none",
				ResponseType:       "code",
				InsecureSkipVerify: true,
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
				"token":         DefaultToken,
				"refresh_token": RefreshToken,
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
				Url:                "http://localhost:52345/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "none",
				ResponseType:       "code",
				InsecureSkipVerify: true,
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
				Url:                "http://localhost:52345/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "none",
				ResponseType:       "code",
				InsecureSkipVerify: true,
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
				"token":         DefaultToken,
				"refresh_token": RefreshToken,
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
				Url:                "http://localhost:52345/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "json",
				ResponseType:       "code",
				InsecureSkipVerify: true,
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
				"token":         DefaultToken,
				"refresh_token": RefreshToken,
				"client_id":     "test",
				"expires":       float64(36000),
			},
			err: errors.New("fail to authorize by oAuth: Cannot parse access token response to json: http return code error: 400"),
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
			err: errors.New("fail to authorize by oAuth: Cannot parse refresh token response to json: http return code error: 400"),
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
			err: errors.New("fail to authorize by oAuth: fail to parse the expire time for access token: template: sink:1: unexpected . after term \".\""),
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
			err: errors.New("fail to authorize by oAuth: fail to parse the header for refresh token request RefreshToken: template: sink:1: unexpected . after term \".\""),
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
	server := mockAuthServer()
	server.Start()

	defer server.Close()
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
	r := &PullSource{}
	server := mockAuthServer()
	server.Start()
	defer server.Close()
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

	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"device_id": "device1", "humidity": 60.0, "temperature": 25.5}, map[string]interface{}{}),
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestPullIncremental(t *testing.T) {
	r := &PullSource{}
	server := mockAuthServer()
	server.Start()
	defer server.Close()
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

	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "device0", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}),
		api.NewDefaultSourceTuple(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "device1", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}),
		api.NewDefaultSourceTuple(map[string]interface{}{"code": float64(200), "data": map[string]interface{}{"device_id": "device2", "humidity": 60.0, "temperature": 25.5}}, map[string]interface{}{}),
	}
	mock.TestSourceOpen(r, exp, t)
}
