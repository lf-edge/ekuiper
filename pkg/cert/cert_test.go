// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateTLSForClient(t *testing.T) {
	type args struct {
		Opts TlsConfigurationOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *tls.Config
		wantErr bool
	}{
		{
			name: "do not set tls",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       true,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "",
					RenegotiationSupport: "",
					TLSMinVersion:        "",
				},
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
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       false,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "",
					RenegotiationSupport: "freely",
					TLSMinVersion:        "tls1.0",
				},
			},
			want: &tls.Config{
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS10,
				Renegotiation:      tls.RenegotiateFreelyAsClient,
			},
			wantErr: false,
		},
		{
			name: "set tls version to TLS1.1",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       false,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "",
					RenegotiationSupport: "once",
					TLSMinVersion:        "tls1.1",
				},
			},
			want: &tls.Config{
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS11,
				Renegotiation:      tls.RenegotiateOnceAsClient,
			},
			wantErr: false,
		},
		{
			name: "set tls version to TLS1.2",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       false,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "",
					RenegotiationSupport: "never",
					TLSMinVersion:        "tls1.2",
				},
			},
			want: &tls.Config{
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS12,
				Renegotiation:      tls.RenegotiateNever,
			},
			wantErr: false,
		},
		{
			name: "set tls version to TLS1.3",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       false,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "",
					RenegotiationSupport: "freely",
					TLSMinVersion:        "tls1.3",
				},
			},
			want: &tls.Config{
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS13,
				Renegotiation:      tls.RenegotiateFreelyAsClient,
			},
			wantErr: false,
		},
		{
			name: "set unknown tls options for TLS version and negotiation",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       false,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "",
					RenegotiationSupport: "foo",
					TLSMinVersion:        "bar",
				},
			},
			want: &tls.Config{
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS12,
				Renegotiation:      tls.RenegotiateNever,
			},
			wantErr: false,
		},

		{
			name: "no cert/key",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       false,
					CertFile:             "not_exist.crt",
					KeyFile:              "not_exist.key",
					CaFile:               "",
					RenegotiationSupport: "",
					TLSMinVersion:        "",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "no cert/key",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       false,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "not_exist.crt",
					RenegotiationSupport: "",
					TLSMinVersion:        "",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "skip check",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       true,
					CertFile:             "",
					KeyFile:              "",
					CaFile:               "not_exist.crt",
					RenegotiationSupport: "",
					TLSMinVersion:        "",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateTLSForClient(&tt.args.Opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateTLSForClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateTLSForClient() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenTLSConfig(t *testing.T) {
	m := map[string]interface{}{}
	c, err := GenTLSConfig(m, "")
	require.NoError(t, err)
	require.Nil(t, c)

	var opts *TlsConfigurationOptions
	opts.TlsConfigLog("")
	opts = &TlsConfigurationOptions{}
	opts.TlsConfigLog("")
	opts.SkipCertVerify = true
	opts.TlsConfigLog("")
	opts.SkipCertVerify = false
	opts.CertificationRaw = "mock"
	opts.PrivateKeyRaw = "mock"
	opts.RootCARaw = "mock"
	opts.TlsConfigLog("")
}

func TestGenOptions(t *testing.T) {
	testcases := []struct {
		m       map[string]interface{}
		options *TlsConfigurationOptions
	}{
		{
			m:       map[string]interface{}{},
			options: nil,
		},
		{
			m: map[string]interface{}{
				"insecureSkipVerify": true,
			},
			options: &TlsConfigurationOptions{
				SkipCertVerify: true,
			},
		},
	}
	for _, tc := range testcases {
		opt, err := genTlsConfigurationOptions(tc.m)
		require.NoError(t, err)
		require.Equal(t, tc.options, opt)
	}
}
