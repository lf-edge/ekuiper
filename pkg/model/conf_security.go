// Copyright 2025 EMQ Technologies Co., Ltd.
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

package model

import "encoding/base64"

type SecurityConf struct {
	Encryption *EncryptionConf          `yaml:"encryption,omitempty"`
	Tls        *TlsConfigurationOptions `yaml:"tls,omitempty"`
}

type EncryptionConf struct {
	Algorithm  string         `yaml:"algorithm,omitempty" json:"algorithm"`
	Properties map[string]any `yaml:"properties,omitempty" json:"properties"`
	Key        string         `yaml:"key,omitempty" json:"key"`
}

type TlsConfigurationOptions struct {
	SkipCertVerify       bool            `json:"insecureSkipVerify" yaml:"insecureSkipVerify"`
	CertificationRaw     string          `json:"certificationRaw" yaml:"certificationRaw"`
	PrivateKeyRaw        string          `json:"privateKeyRaw" yaml:"privateKeyRaw"`
	RootCARaw            string          `json:"rootCARaw" yaml:"rootCARaw"`
	CertFile             string          `json:"certificationPath" yaml:"certificationPath"`
	KeyFile              string          `json:"privateKeyPath" yaml:"privateKeyPath"`
	CaFile               string          `json:"rootCaPath" yaml:"rootCaPath"`
	TLSMinVersion        string          `json:"tlsMinVersion" yaml:"tlsMinVersion"`
	RenegotiationSupport string          `json:"renegotiationSupport" yaml:"renegotiationSupport"`
	Decrypt              *EncryptionConf `json:"decrypt" yaml:"decrypt,omitempty"`
	// whether use default tls setting
	Tls string `json:"tls"`
}

type TlsKeys struct {
	RawCABytes   []byte
	RawCertBytes []byte
	RawKeyBytes  []byte
}

func (opts *TlsConfigurationOptions) GenKeys() (*TlsKeys, error) {
	var err error
	result := &TlsKeys{}
	if !opts.SkipCertVerify && (len(opts.CertFile) < 1 && len(opts.KeyFile) < 1 && len(opts.CaFile) < 1) &&
		(len(opts.CertificationRaw) < 1 && len(opts.PrivateKeyRaw) < 1 && len(opts.RootCARaw) < 1) {
		return result, nil
	}
	if len(opts.RootCARaw) > 0 {
		result.RawCABytes, err = base64.StdEncoding.DecodeString(opts.RootCARaw)
		if err != nil {
			return result, err
		}
	}
	if len(opts.CertificationRaw) > 0 {
		result.RawCertBytes, err = base64.StdEncoding.DecodeString(opts.CertificationRaw)
		if err != nil {
			return result, err
		}
	}
	if len(opts.PrivateKeyRaw) > 0 {
		result.RawKeyBytes, err = base64.StdEncoding.DecodeString(opts.PrivateKeyRaw)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
