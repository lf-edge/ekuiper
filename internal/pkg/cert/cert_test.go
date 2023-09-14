package cert

import (
	"crypto/tls"
	"reflect"
	"testing"
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
			name: "set custom tls options",
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
			name: "no cert/key",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify:       true,
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
			got, err := GenerateTLSForClient(tt.args.Opts)
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
