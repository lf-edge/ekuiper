// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type HttpPushConnection struct {
	topic    string
	cfg      *connectionCfg
	endpoint string
	method   string
	id       string
}

func (h *HttpPushConnection) GetId(ctx api.StreamContext) string {
	return h.id
}

func (h *HttpPushConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	cfg := &connectionCfg{}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return err
	}
	h.cfg = cfg
	h.endpoint = cfg.Datasource
	h.method = cfg.Method
	h.id = conId
	return nil
}

func (h *HttpPushConnection) Dial(ctx api.StreamContext) error {
	topic, err := RegisterEndpoint(h.cfg.Datasource, h.cfg.Method)
	if err != nil {
		return err
	}
	h.topic = topic
	return nil
}

type connectionCfg struct {
	Datasource string `json:"datasource"`
	Method     string `json:"method"`
}

func CreateConnection(_ api.StreamContext) modules.Connection {
	return &HttpPushConnection{}
}

func (h *HttpPushConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (h *HttpPushConnection) DetachSub(ctx api.StreamContext) {
	UnregisterEndpoint(h.endpoint, h.method)
}

func (h *HttpPushConnection) Close(ctx api.StreamContext) error {
	return nil
}

func (h *HttpPushConnection) GetTopic() string {
	return h.topic
}
