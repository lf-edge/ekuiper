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

package jwt

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func genTokenWithKey(t *testing.T, key *rsa.PrivateKey, issuer string, aud []string, ttl time.Duration) string {
	t.Helper()
	tk := &Token{}
	tk.Issuer = issuer
	tk.Audience = aud
	tk.ExpiresAt = gojwt.NewNumericDate(time.Now().Add(ttl))
	token := gojwt.NewWithClaims(gojwt.SigningMethodRS256, tk)
	tkStr, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("failed to sign test token: %v", err)
	}
	return tkStr
}

func configurePublicKey(t *testing.T, issuer string, publicKey *rsa.PublicKey) {
	t.Helper()
	baseDir := t.TempDir()
	t.Setenv(conf.KuiperBaseKey, baseDir)
	keyDir := filepath.Join(baseDir, "etc", RSAKeyDir)
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

func TestParseToken(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test key: %v", err)
	}
	configurePublicKey(t, "test_issuer", &privateKey.PublicKey)

	type args struct {
		th string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		errContains string
	}{
		{
			name: "pass: have issuer public key",
			args: args{
				th: genTokenWithKey(t, privateKey, "test_issuer", []string{"eKuiper"}, 10*time.Minute),
			},
			wantErr: false,
		},
		{
			name:        "fail: token expired",
			errContains: gojwt.ErrTokenExpired.Error(),
			args: args{
				th: genTokenWithKey(t, privateKey, "test_issuer", []string{"eKuiper"}, -10*time.Minute),
			},
			wantErr: true,
		},
		{
			name: "fail: token sign error",
			args: args{
				th: genTokenWithKey(t, privateKey, "test_issuer", []string{"eKuiper"}, 10*time.Minute) + "badSign",
			},
			wantErr: true,
		},
		{
			name: "fail: do not have issuer's public key",
			args: args{
				th: genTokenWithKey(t, privateKey, "notexist.pub", []string{"eKuiper"}, 10*time.Minute),
			},
			wantErr: true,
		},
		{
			name: "bad token format",
			args: args{
				th: "invalid.token",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseToken(tt.args.th)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.ErrorContains(t, err, tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}

			if err != nil {
				fmt.Printf("=====================\n")
				fmt.Printf("Validate Error %s", err)
			}
		})
	}
}
