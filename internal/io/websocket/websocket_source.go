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
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type WebsocketSource struct {
	topic         string
	cfg           *WebsocketConfig
	props         map[string]any
	connectionTyp string
	sourceID      string
	// lease is the per-consumer pool handle; Release it to detach.
	lease *connection.ConnectionLease
}

type WebsocketConfig struct {
	Endpoint string `json:"datasource"`
}

func (w *WebsocketSource) Provision(ctx api.StreamContext, configs map[string]any) error {
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

func (w *WebsocketSource) Close(ctx api.StreamContext) error {
	pubsub.CloseSourceConsumerChannel(w.topic, w.sourceID)
	return w.lease.Release(ctx)
}

func (w *WebsocketSource) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	key, requireExisting := connection.ResolveConnectionKey(w.props, buildWebsocketEpID(w.cfg.Endpoint))
	lease, err := connection.FetchConnectionWithOptions(ctx, connection.FetchOptions{
		ConnectionKey:   key,
		RefID:           connection.ConsumerRefID(ctx),
		RequireExisting: requireExisting,
		Type:            "websocket",
		Props:           w.props,
		StatusHandler:   sc,
	})
	if err != nil {
		return err
	}
	w.lease = lease
	conn, err := lease.Wait(ctx)
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
				switch recv := d.(type) {
				case error:
					ingestError(ctx, errorx.NewEOF(recv.Error()))
				case []byte:
					ingest(ctx, recv, nil, timex.GetNow())
				}
			}
		}
	}()
	return nil
}

func solveProps(props map[string]any) map[string]any {
	v1, ok1 := props["path"]
	_, ok2 := props["datasource"]
	if ok1 && !ok2 {
		props["datasource"] = v1
	}
	return props
}

func GetSource() api.Source {
	return &WebsocketSource{}
}

var _ api.BytesSource = &WebsocketSource{}
