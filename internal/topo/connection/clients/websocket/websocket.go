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

package websocket

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type WebSocketConnectionConfig struct {
	Addr      string `json:"addr"`
	Path      string `json:"path"`
	tlsConfig *tls.Config
}

type tlsConf struct {
	InsecureSkipVerify   bool   `json:"insecureSkipVerify"`
	CertificationPath    string `json:"certificationPath"`
	PrivateKeyPath       string `json:"privateKeyPath"`
	RootCaPath           string `json:"rootCaPath"`
	TLSMinVersion        string `json:"tlsMinVersion"`
	RenegotiationSupport string `json:"renegotiationSupport"`
}

func (c *tlsConf) isNil() bool {
	if !c.InsecureSkipVerify && len(c.PrivateKeyPath) < 1 && len(c.CertificationPath) < 1 && len(c.RootCaPath) < 1 {
		return true
	}
	return false
}

func NewWebSocketConnWrapper(props map[string]interface{}) (clients.ClientWrapper, error) {
	config := &WebSocketConnectionConfig{}
	if err := cast.MapToStruct(props, config); err != nil {
		return nil, err
	}
	tlsConfig := &tlsConf{}
	if err := cast.MapToStruct(props, tlsConfig); err != nil {
		return nil, err
	}
	if !tlsConfig.isNil() {
		tConf, err := cert.GenerateTLSForClient(cert.TlsConfigurationOptions{
			SkipCertVerify:       tlsConfig.InsecureSkipVerify,
			CertFile:             tlsConfig.CertificationPath,
			KeyFile:              tlsConfig.PrivateKeyPath,
			CaFile:               tlsConfig.RootCaPath,
			TLSMinVersion:        tlsConfig.TLSMinVersion,
			RenegotiationSupport: tlsConfig.RenegotiationSupport,
		})
		if err != nil {
			return nil, err
		}
		config.tlsConfig = tConf
	}

	if len(config.Addr) < 1 || len(config.Path) < 1 {
		return nil, fmt.Errorf("addr and path should be set")
	}
	return newWebsocketClientClientWrapper(config)
}

func GetWebsocketClientConn(addr, path string, tlsConfig *tls.Config) (*websocket.Conn, error) {
	d := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
		TLSClientConfig:  tlsConfig,
	}
	u := url.URL{Scheme: "ws", Host: addr, Path: path}
	c, _, err := d.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}
