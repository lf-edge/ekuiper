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
	"sync"

	"github.com/gorilla/websocket"

	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type websocketClientWrapper struct {
	sync.Mutex
	c           *websocket.Conn
	chs         map[string][]api.TopicChannel
	errCh       map[string]chan error
	refCount    int
	conSelector string
	finished    bool
}

func newWebsocketClientClientWrapper(config *WebSocketConnectionConfig) (clients.ClientWrapper, error) {
	conn, err := GetWebsocketClientConn(config.Addr, config.Path, config.tlsConfig)
	if err != nil {
		return nil, err
	}
	cc := &websocketClientWrapper{
		c: conn, chs: make(map[string][]api.TopicChannel),
		errCh:    make(map[string]chan error),
		refCount: 1,
	}
	go cc.process()
	return cc, nil
}

func (wcw *websocketClientWrapper) getFinished() bool {
	wcw.Lock()
	defer wcw.Unlock()
	return wcw.finished
}

func (wcw *websocketClientWrapper) getChannels() (map[string][]api.TopicChannel, map[string]chan error) {
	wcw.Lock()
	defer wcw.Unlock()
	return wcw.chs, wcw.errCh
}

func (wcw *websocketClientWrapper) getID(ctx api.StreamContext) string {
	return fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}

func (wcw *websocketClientWrapper) process() {
	for {
		if wcw.getFinished() {
			return
		}
		msgTyp, data, err := wcw.c.ReadMessage()
		if err != nil {
			for _, errCh := range wcw.errCh {
				errCh <- err
			}
			continue
		}
		if msgTyp == websocket.TextMessage {
			for _, chs := range wcw.chs {
				for _, ch := range chs {
					ch.Messages <- data
				}
			}
		}
	}
}

func (wcw *websocketClientWrapper) Subscribe(ctx api.StreamContext, subChan []api.TopicChannel, messageErrors chan error, _ map[string]interface{}) error {
	wcw.Lock()
	defer wcw.Unlock()
	subId := wcw.getID(ctx)
	if _, ok := wcw.chs[subId]; ok {
		return fmt.Errorf("%s subsucribe websocket client connection duplidated", subId)
	}
	wcw.chs[subId] = subChan
	wcw.errCh[subId] = messageErrors
	return nil
}

func (wcw *websocketClientWrapper) Release(ctx api.StreamContext) bool {
	wcw.Lock()
	defer wcw.Unlock()
	subID := wcw.getID(ctx)
	delete(wcw.chs, subID)
	delete(wcw.errCh, subID)
	wcw.refCount--
	if wcw.refCount == 0 {
		wcw.finished = true
		return true
	}
	return false
}

func (wcw *websocketClientWrapper) Publish(c api.StreamContext, topic string, message []byte, params map[string]interface{}) error {
	return wcw.c.WriteMessage(websocket.TextMessage, message)
}

func (wcw *websocketClientWrapper) SetConnectionSelector(conSelector string) {
	wcw.conSelector = conSelector
}

func (wcw *websocketClientWrapper) GetConnectionSelector() string {
	return wcw.conSelector
}

func (wcw *websocketClientWrapper) AddRef() {
	wcw.Lock()
	defer wcw.Unlock()
	wcw.refCount++
}
