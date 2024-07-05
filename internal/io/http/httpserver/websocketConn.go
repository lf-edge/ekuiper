// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type WebsocketConnection struct {
	RecvTopic string
	SendTopic string
	props     map[string]any
	cfg       *wscConfig
	isServer  bool
	client    *WebsocketClient
}

type wscConfig struct {
	Datasource string `json:"datasource"`
	Addr       string `json:"addr"`
}

func (w *WebsocketConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (w *WebsocketConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
}

func (w *WebsocketConnection) Close(ctx api.StreamContext) error {
	if w.isServer {
		UnRegisterWebSocketEndpoint(w.cfg.Datasource)
	} else {
		w.client.Close(ctx)
	}
	return nil
}

func CreateWebsocketConnection(ctx api.StreamContext, props map[string]any) (modules.Connection, error) {
	return createWebsocketServerConnection(ctx, props)
}

func createWebsocketServerConnection(ctx api.StreamContext, props map[string]any) (*WebsocketConnection, error) {
	cfg := &wscConfig{}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return nil, err
	}
	wc := &WebsocketConnection{
		props:    props,
		cfg:      cfg,
		isServer: getWsType(cfg),
	}
	if wc.isServer {
		rTopic, sTopic, err := RegisterWebSocketEndpoint(ctx, cfg.Datasource)
		if err != nil {
			return nil, err
		}
		wc.RecvTopic = rTopic
		wc.SendTopic = sTopic
	} else {
		tlsConfig, err := cert.GenTLSConfig(props, "websocket")
		if err != nil {
			return nil, err
		}
		c := NewWebsocketClient(cfg.Addr, cfg.Datasource, tlsConfig)
		if err := c.Connect(); err != nil {
			return nil, err
		}
		wc.client = c
		wc.RecvTopic, wc.SendTopic = c.Run(ctx)
	}
	return wc, nil
}

func getWsType(cfg *wscConfig) bool {
	if len(cfg.Addr) < 1 {
		return true
	}
	return false
}
