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
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type websocketServerConnWrapper struct {
	endpoint        string
	recvTopic       string
	sendTopic       string
	connSelector    string
	done            chan struct{}
	checkConnection bool

	sync.RWMutex
	isFinished bool
	refCount   int
}

func (wsw *websocketServerConnWrapper) isFinish() bool {
	wsw.RLock()
	defer wsw.RUnlock()
	return wsw.isFinished
}

func newWebsocketServerConnWrapper(config *WebSocketConnectionConfig) (clients.ClientWrapper, error) {
	if config.CheckConnection {
		recvTopic, sendTopic, done, err := httpserver.GetWebsocketEndpointCh(config.Path)
		if err != nil {
			return nil, err
		}
		wsw := &websocketServerConnWrapper{endpoint: config.Path, recvTopic: recvTopic, sendTopic: sendTopic, done: done, refCount: 1, checkConnection: true}
		return wsw, nil
	} else {
		recvTopic, sendTopic, done, err := httpserver.RegisterWebSocketEndpoint(context.Background(), config.Path)
		if err != nil {
			return nil, err
		}
		wsw := &websocketServerConnWrapper{endpoint: config.Path, recvTopic: recvTopic, sendTopic: sendTopic, done: done, refCount: 1, checkConnection: false}
		return wsw, nil
	}
}

func (wsw *websocketServerConnWrapper) Ping() error {
	return errors.New("websocket server can't ping")
}

func (wsw *websocketServerConnWrapper) process(ctx api.StreamContext, subChan []api.TopicChannel, messageErrors chan error) {
	ch := pubsub.CreateSub(wsw.recvTopic, nil, fmt.Sprintf("%s_%s_%v", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), 1024)
	defer pubsub.CloseSourceConsumerChannel(wsw.recvTopic, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-ch:
			bs, err := json.Marshal(data.Message())
			if err != nil {
				messageErrors <- err
				continue
			}
			for _, subC := range subChan {
				subC.Messages <- bs
			}
		default:
			if wsw.isFinish() {
				return
			}
		}
	}
}

func (wsw *websocketServerConnWrapper) Subscribe(c api.StreamContext, subChan []api.TopicChannel, messageErrors chan error, params map[string]interface{}) error {
	go wsw.process(c, subChan, messageErrors)
	return nil
}

func (wsw *websocketServerConnWrapper) Release(c api.StreamContext) bool {
	if wsw.isFinish() {
		return true
	}
	isFinished := false
	wsw.Lock()
	wsw.refCount--
	if wsw.refCount == 0 {
		wsw.isFinished = true
	}
	isFinished = wsw.isFinished
	wsw.Unlock()
	return isFinished
}

func (wsw *websocketServerConnWrapper) Publish(c api.StreamContext, topic string, message []byte, params map[string]interface{}) error {
	pubsub.Produce(c, wsw.sendTopic, map[string]interface{}{
		httpserver.WebsocketServerDataKey: message,
	})
	return nil
}

func (wsw *websocketServerConnWrapper) SetConnectionSelector(conSelector string) {
	wsw.connSelector = conSelector
}

func (wsw *websocketServerConnWrapper) GetConnectionSelector() string {
	return wsw.connSelector
}

func (wsw *websocketServerConnWrapper) AddRef() {
	wsw.Lock()
	defer wsw.Unlock()
	wsw.refCount++
}
