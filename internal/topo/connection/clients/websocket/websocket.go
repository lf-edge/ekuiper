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
	Addr            string `json:"addr"`
	Path            string `json:"path"`
	MaxConnRetry    int    `json:"maxConnRetry"`
	CheckConnection bool   `json:"checkConnection"`
	tlsConfig       *tls.Config
}

func NewWebSocketConnWrapper(props map[string]interface{}) (clients.ClientWrapper, error) {
	config := &WebSocketConnectionConfig{MaxConnRetry: 3}
	if err := cast.MapToStruct(props, config); err != nil {
		return nil, err
	}
	tlsConfig, err := cert.GenTLSConfig(props, "websocket")
	if err != nil {
		return nil, err
	}
	config.tlsConfig = tlsConfig
	if len(config.Addr) > 0 && len(config.Path) > 0 {
		return newWebsocketClientClientWrapper(config)
	}
	return newWebsocketServerConnWrapper(config)
}

func GetWebsocketClientConn(addr, path string, tlsConfig *tls.Config) (*websocket.Conn, error) {
	d := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
		TLSClientConfig:  tlsConfig,
	}
	if len(addr) < 1 {
		return nil, fmt.Errorf("host should be defined")
	}
	u := url.URL{Scheme: "ws", Host: addr, Path: path}
	c, _, err := d.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}
