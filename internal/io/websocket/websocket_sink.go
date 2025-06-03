// Copyright 2024-2024 EMQ Technologies Co., Ltd.
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
)

type WebsocketSink struct {
	cw    *connection.ConnWrapper
	cfg   *WebsocketConfig
	props map[string]any
	topic string
}

func (w *WebsocketSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	configs = solveProps(configs)
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

func (w *WebsocketSink) Close(ctx api.StreamContext) error {
	pubsub.RemovePub(w.topic)
	return connection.DetachConnection(ctx, buildWebsocketEpID(w.cfg.Endpoint))
}

func (w *WebsocketSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	var err error
	// Connection pool will handle status change
	w.cw, err = connection.FetchConnection(ctx, buildWebsocketEpID(w.cfg.Endpoint), "websocket", w.props, sch)
	if err != nil {
		return err
	}
	conn, err := w.cw.Wait(ctx)
	if err != nil {
		return err
	}
	if conn == nil {
		return fmt.Errorf("websocket endpoint not ready: %v", err)
	}
	c, ok := conn.(*httpserver.WebsocketConnection)
	if !ok {
		return fmt.Errorf("should use websocket connection")
	}
	w.topic = c.SendTopic
	pubsub.CreatePub(w.topic)
	return err
}

func (w *WebsocketSink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	return w.collect(ctx, item.Raw())
}

func (w *WebsocketSink) collect(ctx api.StreamContext, data []byte) error {
	pubsub.ProduceAny(ctx, w.topic, data)
	return nil
}

func GetSink() api.Sink {
	return &WebsocketSink{}
}

var _ api.BytesCollector = &WebsocketSink{}

func buildWebsocketEpID(endpoint string) string {
	return fmt.Sprintf("$$ws/%s", endpoint)
}
