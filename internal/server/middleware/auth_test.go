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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/jwt"
)

func genTokenWithKey(t *testing.T, privateKey *rsa.PrivateKey, issuer string, aud []string) string {
	t.Helper()
	tk := &jwt.Token{}
	tk.Issuer = issuer
	tk.Audience = aud
	tk.ExpiresAt = gojwt.NewNumericDate(time.Now().Add(10 * time.Minute))
	token := gojwt.NewWithClaims(gojwt.SigningMethodRS256, tk)
	tkStr, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("failed to sign test token: %v", err)
	}
	return tkStr
}

func configurePublicKey(t *testing.T, issuer string, publicKey *rsa.PublicKey) {
	t.Helper()
	baseDir := t.TempDir()
	t.Setenv(conf.KuiperBaseKey, baseDir)
	keyDir := filepath.Join(baseDir, "etc", jwt.RSAKeyDir)
	if err := os.MkdirAll(keyDir, 0o755); err != nil {
		t.Fatalf("failed to create test key directory: %v", err)
	}
	keyBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("failed to marshal test public key: %v", err)
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: keyBytes})
	if err := os.WriteFile(filepath.Join(keyDir, issuer), publicKeyPEM, 0o644); err != nil {
		t.Fatalf("failed to write test public key: %v", err)
	}
}

func Test_AUTH(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test key: %v", err)
	}
	configurePublicKey(t, "test_issuer", &privateKey.PublicKey)

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
			args:     args{th: genTokenWithKey(t, privateKey, "test_issuer", []string{"neuron", "eKuiper"})},
			req:      httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9081/streams", nil),
			res:      httptest.NewRecorder(),
			wantCode: 200,
		},

		{
			name:     "audience not right",
			args:     args{th: genTokenWithKey(t, privateKey, "test_issuer", []string{"Neuron"})},
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
