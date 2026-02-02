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

type SSEConnection struct {
	RecvTopic string
	SendTopic string
	id        string
	props     map[string]any
	cfg       *sseConfig
}

type sseConfig struct {
	Path       string `json:"path"`
	Datasource string `json:"datasource"`
}

func (s *SSEConnection) GetId(ctx api.StreamContext) string {
	return s.id
}

func (s *SSEConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	cfg := &sseConfig{}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return err
	}
	if cfg.Path == "" && len(cfg.Datasource) > 0 {
		cfg.Path = cfg.Datasource
	}
	s.cfg = cfg
	s.id = conId
	s.props = props
	return nil
}

func (s *SSEConnection) Dial(ctx api.StreamContext) error {
	rTopic, sTopic, err := RegisterSSEEndpoint(ctx, s.cfg.Datasource)
	if err != nil {
		return err
	}
	s.RecvTopic = rTopic
	s.SendTopic = sTopic
	return nil
}

func (s *SSEConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (s *SSEConnection) Close(ctx api.StreamContext) error {
	if s.cfg != nil {
		UnRegisterSSEEndpoint(s.cfg.Datasource)
	}
	return nil
}

func CreateSSEConnection(ctx api.StreamContext) modules.Connection {
	return &SSEConnection{}
}
