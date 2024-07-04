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
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type WebsocketConnection struct {
	RecvTopic string
	SendTopic string
	cfg       *connectionCfg
}

func (w *WebsocketConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (w *WebsocketConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	UnRegisterWebSocketEndpoint(w.cfg.Datasource)
}

func (w *WebsocketConnection) Close(ctx api.StreamContext) error {
	return nil
}

func CreateWebsocketConnection(ctx api.StreamContext, props map[string]any) (modules.Connection, error) {
	return createWebsocketServerConnection(ctx, props)
}

func createWebsocketServerConnection(ctx api.StreamContext, props map[string]any) (*WebsocketConnection, error) {
	cfg := &connectionCfg{}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return nil, err
	}
	rTopic, sTopic, err := RegisterWebSocketEndpoint(ctx, cfg.Datasource)
	if err != nil {
		return nil, err
	}
	return &WebsocketConnection{
		RecvTopic: rTopic,
		SendTopic: sTopic,
		cfg:       cfg,
	}, nil
}
