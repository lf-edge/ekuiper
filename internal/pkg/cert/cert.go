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

package cert

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"os"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func GenTLSConfig(props map[string]interface{}) (*tls.Config, *TlsConfigurationOptions, error) {
	opts, err := GenTlsConfigurationOptions(props)
	if err != nil {
		return nil, nil, err
	}
	if opts == nil {
		return nil, nil, nil
	}
	if (len(opts.CertFile) < 1 && len(opts.KeyFile) < 1 && len(opts.CaFile) < 1) &&
		(len(opts.RawCert) < 1 && len(opts.RawKey) < 1 && len(opts.RawCA) < 1) {
		return nil, nil, nil
	}
	tc, err := GenerateTLSForClient(opts)
	if err != nil {
		return nil, nil, err
	}
	return tc, opts, nil
}

func GenTlsConfigurationOptions(props map[string]interface{}) (*TlsConfigurationOptions, error) {
	opts := &TlsConfigurationOptions{}
	if err := cast.MapToStruct(props, opts); err != nil {
		return nil, err
	}
	var err error
	if (len(opts.CertFile) < 1 && len(opts.KeyFile) < 1 && len(opts.CaFile) < 1) &&
		(len(opts.RawCert) < 1 && len(opts.RawKey) < 1 && len(opts.RawCA) < 1) {
		return nil, nil
	}
	if len(opts.RawCA) > 0 {
		opts.rawCABytes, err = base64.StdEncoding.DecodeString(opts.RawCA)
		if err != nil {
			return nil, err
		}
	}
	if len(opts.RawCert) > 0 {
		opts.rawCertBytes, err = base64.StdEncoding.DecodeString(opts.RawCert)
		if err != nil {
			return nil, err
		}
	}
	if len(opts.RawKey) > 0 {
		opts.rawKeyBytes, err = base64.StdEncoding.DecodeString(opts.RawKey)
		if err != nil {
			return nil, err
		}
	}
	return opts, nil
}

func (opts *TlsConfigurationOptions) TlsConfigLog(typ string) {
	if opts == nil {
		conf.Log.Infof("%s tls disabled", typ)
		return
	}
	if opts.SkipCertVerify {
		conf.Log.Infof("%s tls enable insecure skip verify", typ)
		return
	}
	b := bytes.NewBufferString("")
	b.WriteString(typ)
	b.WriteString(" tls enabled")
	if len(opts.CertFile) > 0 || len(opts.RawCert) > 0 {
		b.WriteString(", crt configured")
	} else {
		b.WriteString(", crt not configured")
	}
	if len(opts.KeyFile) > 0 || len(opts.RawKey) > 0 {
		b.WriteString(", key configured")
	} else {
		b.WriteString(", key not configured")
	}
	if len(opts.CaFile) > 0 || len(opts.RawCA) > 0 {
		b.WriteString(", root ca configured")
	} else {
		b.WriteString(", root ca not configured")
	}
	conf.Log.Info(b.String())
}

type TlsConfigurationOptions struct {
	SkipCertVerify       bool   `json:"insecureSkipVerify"`
	RawCert              string `json:"rawCert"`
	RawKey               string `json:"rawKey"`
	RawCA                string `json:"rawCA"`
	CertFile             string `json:"certificationPath"`
	KeyFile              string `json:"privateKeyPath"`
	CaFile               string `json:"rootCaPath"`
	TLSMinVersion        string `json:"tlsMinVersion"`
	RenegotiationSupport string `json:"renegotiationSupport"`

	rawCABytes   []byte
	rawCertBytes []byte
	rawKeyBytes  []byte
}

func getTLSMinVersion(userInput string) uint16 {
	switch userInput {
	case "tls1.0":
		return tls.VersionTLS10
	case "tls1.1":
		return tls.VersionTLS11
	case "tls1.2":
		return tls.VersionTLS12
	case "tls1.3":
		return tls.VersionTLS13
	case "":
		return tls.VersionTLS12
	default:
		conf.Log.Warnf("Unrecognized or unsupported TLS version: %s, defaulting to TLS 1.2", userInput)
		return tls.VersionTLS12
	}
}

func getRenegotiationSupport(userInput string) tls.RenegotiationSupport {
	switch userInput {
	case "never":
		return tls.RenegotiateNever
	case "once":
		return tls.RenegotiateOnceAsClient
	case "freely":
		return tls.RenegotiateFreelyAsClient
	case "":
		return tls.RenegotiateNever
	default:
		conf.Log.Warnf("Invalid renegotiation option: %s, defaulting to \"never\"", userInput)
		return tls.RenegotiateNever
	}
}

func isCertDefined(opts *TlsConfigurationOptions) bool {
	if len(opts.RawCert) == 0 && len(opts.RawKey) == 0 && len(opts.CertFile) == 0 && len(opts.KeyFile) == 0 {
		return false
	}
	return true
}

func GenerateTLSForClient(
	Opts *TlsConfigurationOptions,
) (*tls.Config, error) {
	if Opts == nil {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: Opts.SkipCertVerify,
		Renegotiation:      getRenegotiationSupport(Opts.RenegotiationSupport),
		MinVersion:         getTLSMinVersion(Opts.TLSMinVersion),
	}

	if !isCertDefined(Opts) {
		tlsConfig.Certificates = nil
	} else {
		if cert, err := buildCert(Opts); err != nil {
			return nil, err
		} else {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	if err := buildCA(Opts, tlsConfig); err != nil {
		return nil, err
	}

	return tlsConfig, nil
}

func buildCert(opts *TlsConfigurationOptions) (tls.Certificate, error) {
	if len(opts.CertFile) > 0 || len(opts.KeyFile) > 0 {
		return certLoader(opts.CertFile, opts.KeyFile)
	}
	return tls.X509KeyPair(opts.rawCertBytes, opts.rawKeyBytes)
}

func certLoader(certFilePath, keyFilePath string) (tls.Certificate, error) {
	if cp, err := conf.ProcessPath(certFilePath); err == nil {
		if kp, err1 := conf.ProcessPath(keyFilePath); err1 == nil {
			if cer, err2 := tls.LoadX509KeyPair(cp, kp); err2 != nil {
				return tls.Certificate{}, err2
			} else {
				return cer, nil
			}
		} else {
			return tls.Certificate{}, err1
		}
	} else {
		return tls.Certificate{}, err
	}
}

func buildCA(opts *TlsConfigurationOptions, tlsConfig *tls.Config) error {
	if len(opts.CaFile) > 0 {
		root, err := caLoader(opts.CaFile)
		if err != nil {
			return err
		}
		tlsConfig.RootCAs = root
		return nil
	}
	if len(opts.RawCA) > 0 {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(opts.rawCABytes)
		tlsConfig.RootCAs = pool
		return nil
	}
	return nil
}

func caLoader(caFilePath string) (*x509.CertPool, error) {
	if cp, err := conf.ProcessPath(caFilePath); err == nil {
		pool := x509.NewCertPool()
		caCrt, err1 := os.ReadFile(cp)
		if err1 != nil {
			return nil, err1
		}
		pool.AppendCertsFromPEM(caCrt)
		return pool, err1
	} else {
		return nil, err
	}
}
