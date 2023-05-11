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
					SkipCertVerify: true,
					CertFile:       "",
					KeyFile:        "",
					CaFile:         "",
				},
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
			},
			wantErr: false,
		},
		{
			name: "no cert/key",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify: true,
					CertFile:       "not_exist.crt",
					KeyFile:        "not_exist.key",
					CaFile:         "",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "no cert/key",
			args: args{
				Opts: TlsConfigurationOptions{
					SkipCertVerify: true,
					CertFile:       "",
					KeyFile:        "",
					CaFile:         "not_exist.crt",
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
