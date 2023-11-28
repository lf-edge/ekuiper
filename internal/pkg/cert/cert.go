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
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type TlsConfigurationOptions struct {
	SkipCertVerify       bool   `json:"insecureSkipVerify"`
	CertFile             string `json:"certificationPath"`
	KeyFile              string `json:"privateKeyPath"`
	CaFile               string `json:"rootCaPath"`
	TLSMinVersion        string `json:"tlsMinVersion"`
	RenegotiationSupport string `json:"renegotiationSupport"`
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

func GenTLSForClientFromProps(props map[string]interface{}) (*tls.Config, error) {
	tc := &TlsConfigurationOptions{}
	if err := cast.MapToStruct(props, tc); err != nil {
		return nil, err
	}
	return GenerateTLSForClient(*tc)
}

func GenerateTLSForClient(
	Opts TlsConfigurationOptions,
) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: Opts.SkipCertVerify,
		Renegotiation:      getRenegotiationSupport(Opts.RenegotiationSupport),
		MinVersion:         getTLSMinVersion(Opts.TLSMinVersion),
	}

	if len(Opts.CertFile) <= 0 && len(Opts.KeyFile) <= 0 {
		tlsConfig.Certificates = nil
	} else {
		if cert, err := certLoader(Opts.CertFile, Opts.KeyFile); err != nil {
			return nil, err
		} else {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	if len(Opts.CaFile) > 0 {
		root, err := caLoader(Opts.CaFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = root
	}

	return tlsConfig, nil
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
