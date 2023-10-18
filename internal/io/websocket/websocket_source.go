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
	"fmt"

	"github.com/lf-edge/ekuiper/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type WebSocketSource struct {
	endpoint string
}

func (wss *WebSocketSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	topic, done, err := httpserver.RegisterWebSocketEndpoint(ctx, wss.endpoint)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	defer httpserver.UnRegisterWebSocketEndpoint(wss.endpoint)
	ch := pubsub.CreateSub(topic, nil, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
	for {
		select {
		case <-done: // http data server error
			infra.DrainError(ctx, fmt.Errorf("http data server shutdown"), errCh)
			return
		case v, opened := <-ch:
			if !opened {
				return
			}
			consumer <- v
		case <-ctx.Done():
			return
		}
	}
}

func (wss *WebSocketSource) Configure(datasource string, props map[string]interface{}) error {
	wss.endpoint = datasource
	return nil
}

func (wss *WebSocketSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing WebSocket source")
	return nil
}
