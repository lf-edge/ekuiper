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

package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/jwt"
)

func genToken(signKeyName, issuer string, aud []string) string {
	tkStr, _ := jwt.CreateToken(signKeyName, issuer, aud)
	return tkStr
}

func Test_AUTH(t *testing.T) {
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := Auth(nextHandler)

	type args struct {
		th string
	}
	tests := []struct {
		name     string
		args     args
		req      *http.Request
		res      *httptest.ResponseRecorder
		wantCode int
	}{
		{
			name:     "token right",
			args:     args{th: genToken("sample_key", "sample_key.pub", []string{"neuron", "eKuiper"})},
			req:      httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9081/streams", nil),
			res:      httptest.NewRecorder(),
			wantCode: 200,
		},

		{
			name:     "audience not right",
			args:     args{th: genToken("sample_key", "sample_key.pub", []string{"Neuron"})},
			req:      httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9081/streams", nil),
			res:      httptest.NewRecorder(),
			wantCode: 401,
		},
		{
			name:     "no token",
			args:     args{th: ""},
			req:      httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9081/streams", nil),
			res:      httptest.NewRecorder(),
			wantCode: 401,
		},
		{
			name:     "no need token path",
			args:     args{th: ""},
			req:      httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9081/ping", nil),
			res:      httptest.NewRecorder(),
			wantCode: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.req.Header.Set("Authorization", tt.args.th)
			handler.ServeHTTP(tt.res, tt.req)

			res := tt.res.Result()

			data, err := io.ReadAll(res.Body)
			if err != nil {
				t.Errorf("expected error to be nil got %v", err)
			}

			if !reflect.DeepEqual(tt.wantCode, tt.res.Code) {
				t.Errorf("expect %d, actual %d, result %s", tt.wantCode, tt.res.Code, string(data))
			}

			_ = res.Body.Close()
		})
	}
}
