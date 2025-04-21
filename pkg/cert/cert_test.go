// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package cert

import (
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestGenerateTLSForClient(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]any
		want    *tls.Config
		wantErr bool
	}{
		{
			name: "do not set tls",
			args: map[string]any{
				"insecureSkipVerify": true,
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
				Renegotiation:      tls.RenegotiateNever,
			},
			wantErr: false,
		},
		{
			name: "set tls version to TLS1.0",
			args: map[string]any{
				"insecureSkipVerify":   true,
				"renegotiationSupport": "freely",
				"tlsMinVersion":        "tls1.0",
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS10,
				Renegotiation:      tls.RenegotiateFreelyAsClient,
			},
			wantErr: false,
		},
		{
			name: "set tls version to TLS1.1",
			args: map[string]any{
				"insecureSkipVerify":   true,
				"renegotiationSupport": "once",
				"tlsMinVersion":        "tls1.1",
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS11,
				Renegotiation:      tls.RenegotiateOnceAsClient,
			},
			wantErr: false,
		},
		{
			name: "set tls version to TLS1.2",
			args: map[string]any{
				"insecureSkipVerify":   true,
				"renegotiationSupport": "never",
				"tlsMinVersion":        "tls1.2",
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
				Renegotiation:      tls.RenegotiateNever,
			},
			wantErr: false,
		},
		{
			name: "set tls version to TLS1.3",
			args: map[string]any{
				"insecureSkipVerify":   true,
				"renegotiationSupport": "freely",
				"tlsMinVersion":        "tls1.3",
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS13,
				Renegotiation:      tls.RenegotiateFreelyAsClient,
			},
			wantErr: false,
		},
		{
			name: "set unknown tls options for TLS version and negotiation",
			args: map[string]any{
				"insecureSkipVerify":   true,
				"renegotiationSupport": "foo",
				"tlsMinVersion":        "bar",
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
				Renegotiation:      tls.RenegotiateNever,
			},
			wantErr: false,
		},
		{
			name: "no cert/key",
			args: map[string]any{
				"insecureSkipVerify": false,
				"certificationPath":  "not_exist.crt",
				"privateKeyPath":     "not_exist.key",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "no cert/key",
			args: map[string]any{
				"insecureSkipVerify": false,
				"rootCaPath":         "not_exist.crt",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "skip check",
			args: map[string]any{
				"insecureSkipVerify": true,
				"rootCaPath":         "not_exist.crt",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "default",
			args: map[string]any{
				"insecureSkipVerify": true,
				"tls":                "default",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "default",
			args: map[string]any{
				"insecureSkipVerify": true,
				"tls":                "unknown",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "decrypt key",
			args: map[string]any{
				"insecureSkipVerify": false,
				"decrypt": map[string]any{
					"algorithm": "default",
				},
				"certificationPath": "not_exist.crt",
				"privateKeyPath":    "not_exist.key",
			},
			want:    nil,
			wantErr: true,
		},
	}
	ctx := mockContext.NewMockContext("gentls", "op1")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenTLSConfig(ctx, tt.args)
			if tt.wantErr {
				require.Error(t, err)
				fmt.Println(err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGenTLSConfig(t *testing.T) {
	ctx := mockContext.NewMockContext("gentls", "op1")
	m := map[string]interface{}{}
	c, err := GenTLSConfig(ctx, m)
	require.NoError(t, err)
	require.Nil(t, c)
}

func TestGenOptions(t *testing.T) {
	testcases := []struct {
		m       map[string]interface{}
		options *model.TlsConfigurationOptions
	}{
		{
			m:       map[string]interface{}{},
			options: nil,
		},
		{
			m: map[string]interface{}{
				"insecureSkipVerify": true,
			},
			options: &model.TlsConfigurationOptions{
				SkipCertVerify: true,
			},
		},
	}
	for _, tc := range testcases {
		opt, _, err := genTlsConfigurationOptions(tc.m)
		require.NoError(t, err)
		require.Equal(t, tc.options, opt)
	}
}
