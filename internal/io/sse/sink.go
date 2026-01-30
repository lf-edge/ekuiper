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

package sse

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

type SseConfig struct {
	Endpoint string `json:"endpoint"`
}

type SSESink struct {
	cw    *connection.ConnWrapper
	cfg   *SseConfig
	props map[string]any
	topic string
}

func (s *SSESink) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &SseConfig{}
	if err := cast.MapToStruct(configs, cfg); err != nil {
		return err
	}
	if !strings.HasPrefix(cfg.Endpoint, "/") {
		return fmt.Errorf("sse endpoint should start with /")
	}
	s.cfg = cfg
	s.props = configs
	return nil
}

func (s *SSESink) Close(ctx api.StreamContext) error {
	pubsub.RemovePub(s.topic)
	return connection.DetachConnection(ctx, buildSseEpID(s.cfg.Endpoint))
}

func (s *SSESink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	var err error
	// Connection pool will handle status change
	s.cw, err = connection.FetchConnection(ctx, buildSseEpID(s.cfg.Endpoint), "sse", s.props, sch)
	if err != nil {
		return err
	}
	conn, err := s.cw.Wait(ctx)
	if err != nil {
		return err
	}
	if conn == nil {
		return fmt.Errorf("sse endpoint not ready: %v", err)
	}
	c, ok := conn.(*httpserver.SSEConnection)
	if !ok {
		return fmt.Errorf("should use sse connection")
	}
	s.topic = c.SendTopic
	pubsub.CreatePub(s.topic)
	return err
}

func (s *SSESink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	return s.collect(ctx, item.Raw())
}

func (s *SSESink) collect(ctx api.StreamContext, data []byte) error {
	pubsub.ProduceAny(ctx, s.topic, data)
	return nil
}

func GetSink() api.Sink {
	return &SSESink{}
}

var _ api.BytesCollector = &SSESink{}

func buildSseEpID(endpoint string) string {
	return fmt.Sprintf("$$sse/%s", endpoint)
}
