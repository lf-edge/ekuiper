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

var (
	expiredToken   = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJlS3VpcGVyIiwiZXhwIjoxNjM2MDExMzQxLCJpc3MiOiJzYW1wbGVfa2V5LnB1YiJ9.qm5Pq9VxDC10qbOM081U5NwScTOxYV_F5vyqbU9rXB2ebz4kDio_R2tgEgGyJ41lwD7gFl1quBjp_EgokPZNOoGRg5R1Ygf7iF8XJSDxYkspSCsBtZAuMCo3MCz3slQyvnr24qv3idUDhlwO6FPHGLaLHEyvrETSl1ZcECq2wvW01Tc2Jmg0-Kpp6TmEbH5aD-L0or5Bfy0ytBQ64nd2hKVaoADZZOXSt1iH2-1R35fEc_lBw7zs4QpCC2R--muoqYsYkESR08o6wIKAxRJvqeWab3C9k_g0zaPhwa7ZQ9wRzah-tc6PdotZkAyH7BCx-f7llO7UT47k0GnrhBe21g"
	badFormatToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmb28iOiJiYXIiLCJleHAiOjE1MDAwLCJpc3MiOiJ0ZXN0In0"
)

func genTokenWithKey(t *testing.T, key *rsa.PrivateKey, issuer string, aud []string) string {
	t.Helper()
	tk := &Token{}
	tk.Issuer = issuer
	tk.Audience = aud
	tk.ExpiresAt = gojwt.NewNumericDate(time.Now().Add(10 * time.Minute))
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
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "pass: have issuer public key",
			args: args{
				th: genTokenWithKey(t, privateKey, "test_issuer", []string{"eKuiper"}),
			},
			wantErr: false,
		},
		{
			name: "fail: token expired",
			args: args{
				th: expiredToken,
			},
			wantErr: true,
		},
		{
			name: "fail: token sign error",
			args: args{
				th: genTokenWithKey(t, privateKey, "test_issuer", []string{"eKuiper"}) + "badSign",
			},
			wantErr: true,
		},
		{
			name: "fail: do not have issuer's public key",
			args: args{
				th: genTokenWithKey(t, privateKey, "notexist.pub", []string{"eKuiper"}),
			},
			wantErr: true,
		},
		{
			name: "bad token format",
			args: args{
				th: badFormatToken,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseToken(tt.args.th)
			if tt.wantErr {
				assert.Error(t, err)
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
