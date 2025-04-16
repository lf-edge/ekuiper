// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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
	"encoding/base64"
	"fmt"
	"os"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/modules/encryptor"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/path"
)

func GenTLSConfig(ctx api.StreamContext, props map[string]interface{}) (*tls.Config, error) {
	opts, keys, err := genTlsConfigurationOptions(props)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		return nil, nil
	}
	if opts.Tls == "default" {
		return GetDefaultTlsConf(ctx)
	} else if opts.Tls != "" {
		return nil, fmt.Errorf("unknown tls configuration type: %s", opts.Tls)
	}
	tc, err := GenerateTLSForClient(ctx, opts, keys)
	if err != nil {
		return nil, err
	}
	return tc, nil
}

func genTlsConfigurationOptions(props map[string]interface{}) (*model.TlsConfigurationOptions, *model.TlsKeys, error) {
	opts := &model.TlsConfigurationOptions{}
	if err := cast.MapToStruct(props, opts); err != nil {
		return nil, nil, err
	}
	if !opts.SkipCertVerify && (len(opts.CertFile) < 1 && len(opts.KeyFile) < 1 && len(opts.CaFile) < 1) &&
		(len(opts.CertificationRaw) < 1 && len(opts.PrivateKeyRaw) < 1 && len(opts.RootCARaw) < 1) {
		return nil, nil, nil
	}
	keys, err := opts.GenKeys()
	return opts, keys, err
}

func getTLSMinVersion(ctx api.StreamContext, userInput string) uint16 {
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
		ctx.GetLogger().Warnf("Unrecognized or unsupported TLS version: %s, defaulting to TLS 1.2", userInput)
		return tls.VersionTLS12
	}
}

func getRenegotiationSupport(ctx api.StreamContext, userInput string) tls.RenegotiationSupport {
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
		ctx.GetLogger().Warnf("Invalid renegotiation option: %s, defaulting to \"never\"", userInput)
		return tls.RenegotiateNever
	}
}

func isCertDefined(opts *model.TlsConfigurationOptions) bool {
	if len(opts.CertificationRaw) == 0 && len(opts.PrivateKeyRaw) == 0 && len(opts.CertFile) == 0 && len(opts.KeyFile) == 0 {
		return false
	}
	return true
}

func GenerateTLSForClient(ctx api.StreamContext, Opts *model.TlsConfigurationOptions, keys *model.TlsKeys) (*tls.Config, error) {
	if Opts == nil {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: Opts.SkipCertVerify,
		Renegotiation:      getRenegotiationSupport(ctx, Opts.RenegotiationSupport),
		MinVersion:         getTLSMinVersion(ctx, Opts.TLSMinVersion),
	}
	if !isCertDefined(Opts) {
		tlsConfig.Certificates = nil
	} else {
		if cert, err := buildCert(ctx, Opts, keys); err != nil {
			return nil, err
		} else {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	if err := buildCA(ctx, Opts, tlsConfig, keys); err != nil {
		return nil, err
	}

	return tlsConfig, nil
}

func buildCert(ctx api.StreamContext, opts *model.TlsConfigurationOptions, keys *model.TlsKeys) (tls.Certificate, error) {
	var (
		cpb, kpb []byte
		err      error
	)
	if len(opts.CertFile) > 0 || len(opts.KeyFile) > 0 {
		cpb, kpb, err = certLoader(ctx, opts.CertFile, opts.KeyFile)
	} else {
		cpb, kpb = keys.RawCertBytes, keys.RawKeyBytes
	}
	if opts.Decrypt != nil {
		var key []byte
		if opts.Decrypt.Key != "" {
			key, err = base64.StdEncoding.DecodeString(opts.Decrypt.Key)
			if err != nil {
				return tls.Certificate{}, err
			}
		}
		decryptor, e := encryptor.GetDecryptor(opts.Decrypt.Algorithm, key, opts.Decrypt.Properties)
		if e != nil {
			return tls.Certificate{}, e
		}
		cpb, e = decryptor.Decrypt(cpb)
		if err != nil {
			return tls.Certificate{}, e
		}
		kpb, e = decryptor.Decrypt(kpb)
		if e != nil {
			return tls.Certificate{}, e
		}
	}
	return tls.X509KeyPair(cpb, kpb)
}

func certLoader(ctx api.StreamContext, certFilePath, keyFilePath string) ([]byte, []byte, error) {
	cp := path.AbsPath(ctx, certFilePath)
	kp := path.AbsPath(ctx, keyFilePath)
	cpb, err := os.ReadFile(cp)
	if err != nil {
		return nil, nil, err
	}
	kpb, err := os.ReadFile(kp)
	if err != nil {
		return nil, nil, err
	}
	return cpb, kpb, nil
}

func buildCA(ctx api.StreamContext, opts *model.TlsConfigurationOptions, tlsConfig *tls.Config, keys *model.TlsKeys) error {
	if len(opts.CaFile) > 0 {
		root, err := caLoader(ctx, opts.CaFile)
		if err != nil {
			return err
		}
		tlsConfig.RootCAs = root
		return nil
	}
	if len(opts.RootCARaw) > 0 {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(keys.RawCABytes)
		tlsConfig.RootCAs = pool
		return nil
	}
	return nil
}

func caLoader(ctx api.StreamContext, caFilePath string) (*x509.CertPool, error) {
	cp := path.AbsPath(ctx, caFilePath)
	pool := x509.NewCertPool()
	caCrt, err1 := os.ReadFile(cp)
	if err1 != nil {
		return nil, err1
	}
	pool.AppendCertsFromPEM(caCrt)
	return pool, err1
}
