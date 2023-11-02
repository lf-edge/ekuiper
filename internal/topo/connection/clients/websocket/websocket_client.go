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
	"strings"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type websocketClientWrapper struct {
	sync.Mutex
	c *websocket.Conn
	// We maintained topicChannel for each source_node(ruleID_OpID_InstanceID)
	// When source_node Subscribed, each message comes from the websocket connection will be delivered into all topic Channel.
	// When source_node Released, the Topic Channel will be removed by the ID so that the websocket msg won't send data to it anymore.
	chs         map[string][]api.TopicChannel
	errCh       map[string]chan error
	refCount    int
	conSelector string
	finished    bool

	processDone bool
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

func (wcw *websocketClientWrapper) isFinished() bool {
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
	defer func() {
		wcw.processDone = true
		wcw.c.Close()
	}()
	for {
		if wcw.isFinished() {
			return
		}
		msgTyp, data, err := wcw.c.ReadMessage()
		if err != nil {
			for key, errCh := range wcw.errCh {
				select {
				case errCh <- err:
				default:
					conf.Log.Warnf("websocket client connection discard one error for %v", key)
				}
			}
			if strings.Contains(err.Error(), "close") {
				conf.Log.Info("websocket client closed")
				return
			}
			continue
		}
		if msgTyp == websocket.TextMessage {
			for key, chs := range wcw.chs {
				for _, ch := range chs {
					select {
					case ch.Messages <- data:
					default:
						conf.Log.Warnf("websocket client connection discard one msg for %v", key)
					}
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
		wcw.c.Close()
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
