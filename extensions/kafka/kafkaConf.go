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

package kafka

import (
	"fmt"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/lf-edge/ekuiper/pkg/cast"
)

const (
	SASL_NONE  = "none"
	SASL_PLAIN = "plain"
	SASL_SCRAM = "scram"
)

//func (c *TLSConf) TlsConfigLog(typ string) {
//	if c == nil {
//		conf.Log.Infof("kafka %s tls not configured", typ)
//		return
//	}
//	if c.InsecureSkipVerify {
//		conf.Log.Infof("kafka %s tls enable insecure skip verify", typ)
//		return
//	}
//	b := bytes.NewBufferString("kafka ")
//	b.WriteString(typ)
//	b.WriteString(" tls enabled")
//	if len(c.CertificationPath) > 0 {
//		b.WriteString(", crt configured")
//	} else {
//		b.WriteString(", crt not configured")
//	}
//	if len(c.PrivateKeyPath) > 0 {
//		b.WriteString(", key configured")
//	} else {
//		b.WriteString(", key not configured")
//	}
//	if len(c.RootCaPath) > 0 {
//		b.WriteString(", root ca configured")
//	} else {
//		b.WriteString(", root ca not configured")
//	}
//	conf.Log.Info(b.String())
//}

//
//func (c *TLSConf) GetTlsConfig() (*tls.Config, error) {
//	if len(c.CertificationPath) == 0 && len(c.PrivateKeyPath) == 0 && len(c.RootCaPath) == 0 {
//		return nil, nil
//	}
//	return cert.GenerateTLSForClient(cert.TlsConfigurationOptions{
//		SkipCertVerify:       c.InsecureSkipVerify,
//		CertFile:             c.CertificationPath,
//		KeyFile:              c.PrivateKeyPath,
//		CaFile:               c.RootCaPath,
//		TLSMinVersion:        c.TLSMinVersion,
//		RenegotiationSupport: c.RenegotiationSupport,
//	})
//}

type SaslConf struct {
	SaslAuthType string `json:"saslAuthType"`
	SaslUserName string `json:"saslUserName"`
	SaslPassword string `json:"saslPassword"`
}

func GetSaslConf(props map[string]interface{}) (SaslConf, error) {
	sc := SaslConf{
		SaslAuthType: SASL_NONE,
	}
	if err := cast.MapToStruct(props, &sc); err != nil {
		return sc, err
	}
	return sc, nil
}

func (c SaslConf) Validate() error {
	if !(c.SaslAuthType == SASL_NONE || c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) {
		return fmt.Errorf("saslAuthType incorrect")
	}
	if (c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) && (c.SaslUserName == "" || c.SaslPassword == "") {
		return fmt.Errorf("username and password can not be empty")
	}
	return nil
}

func (c SaslConf) GetMechanism() (sasl.Mechanism, error) {
	var err error
	var mechanism sasl.Mechanism

	// sasl authentication type
	switch c.SaslAuthType {
	case SASL_PLAIN:
		mechanism = plain.Mechanism{
			Username: c.SaslUserName,
			Password: c.SaslPassword,
		}
	case SASL_SCRAM:
		mechanism, err = scram.Mechanism(scram.SHA512, c.SaslUserName, c.SaslPassword)
		if err != nil {
			return mechanism, err
		}
	default:
		mechanism = nil
	}
	return mechanism, nil
}
