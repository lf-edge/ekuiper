// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	id        string
	props     map[string]any
	cfg       *wscConfig
	isServer  bool
	client    *WebsocketClient
}

func (w *WebsocketConnection) GetId(ctx api.StreamContext) string {
	return w.id
}

func (w *WebsocketConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	cfg := &wscConfig{
		Scheme: "ws",
	}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return err
	}
	if cfg.Path == "" && len(cfg.Datasource) > 0 {
		cfg.Path = cfg.Datasource
	}
	w.cfg = cfg
	w.id = conId
	w.props = props
	w.isServer = getWsType(cfg)
	return nil
}

func (w *WebsocketConnection) Dial(ctx api.StreamContext) error {
	if w.isServer {
		rTopic, sTopic, err := RegisterWebSocketEndpoint(ctx, w.cfg.Datasource)
		if err != nil {
			return err
		}
		w.RecvTopic = rTopic
		w.SendTopic = sTopic
	} else {
		tlsConfig, err := cert.GenTLSConfig(ctx, w.props)
		if err != nil {
			return err
		}
		c := NewWebsocketClient(w.cfg.Scheme, w.cfg.Addr, w.cfg.Path, tlsConfig)
		if err := c.Connect(); err != nil {
			return err
		}
		w.client = c
		w.RecvTopic, w.SendTopic = c.Run(ctx)
	}
	return nil
}

type wscConfig struct {
	Path       string `json:"path"`
	Datasource string `json:"datasource"`
	Addr       string `json:"addr"`
	Scheme     string `json:"scheme"`
}

func (w *WebsocketConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (w *WebsocketConnection) Close(ctx api.StreamContext) error {
	if w.isServer {
		UnRegisterWebSocketEndpoint(w.cfg.Datasource)
	} else {
		w.client.Close(ctx)
	}
	return nil
}

func CreateWebsocketConnection(ctx api.StreamContext) modules.Connection {
	return &WebsocketConnection{}
}

func getWsType(cfg *wscConfig) bool {
	if len(cfg.Addr) < 1 {
		return true
	}
	return false
}
