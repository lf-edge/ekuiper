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

package websocket

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type WebsocketSource struct {
	topic         string
	cfg           *WebsocketConfig
	props         map[string]any
	connectionTyp string
	sourceID      string
}

type WebsocketConfig struct {
	Endpoint string `json:"datasource"`
}

func (w *WebsocketSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &WebsocketConfig{}
	if err := cast.MapToStruct(configs, cfg); err != nil {
		return err
	}
	if !strings.HasPrefix(cfg.Endpoint, "/") {
		return fmt.Errorf("websocket endpoint should start with /")
	}
	w.cfg = cfg
	w.props = configs
	return nil
}

func (w *WebsocketSource) Close(ctx api.StreamContext) error {
	pubsub.CloseSourceConsumerChannel(w.topic, w.sourceID)
	return connection.DetachConnection(ctx, w.cfg.Endpoint, w.props)
}

func (w *WebsocketSource) Connect(ctx api.StreamContext) error {
	conn, err := connection.FetchConnection(ctx, w.cfg.Endpoint, "websocket", w.props)
	if err != nil {
		return err
	}
	c, ok := conn.(*httpserver.WebsocketConnection)
	if !ok {
		return fmt.Errorf("should use websocket connection")
	}
	w.topic = c.RecvTopic
	w.sourceID = fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	return nil
}

func (w *WebsocketSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	ch := pubsub.CreateSub(w.topic, nil, w.sourceID, 1024)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-ch:
				data, ok := d.([]byte)
				if !ok {
					continue
				}
				ingest(ctx, data, nil, timex.GetNow())
			}
		}
	}()
	return nil
}

func GetSource() api.Source {
	return &WebsocketSource{}
}

var _ api.BytesSource = &WebsocketSource{}
