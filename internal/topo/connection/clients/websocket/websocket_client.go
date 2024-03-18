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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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
	chs          map[string][]api.TopicChannel
	errCh        map[string]chan error
	refCount     int
	conSelector  string
	finished     bool
	maxConnRetry int
	config       *WebSocketConnectionConfig

	// only used for test
	sync.WaitGroup
}

func newWebsocketClientClientWrapper(config *WebSocketConnectionConfig) (clients.ClientWrapper, error) {
	conn, err := GetWebsocketClientConn(config.Addr, config.Path, config.tlsConfig)
	if err != nil {
		return nil, err
	}
	cc := &websocketClientWrapper{
		c:            conn,
		chs:          make(map[string][]api.TopicChannel),
		errCh:        make(map[string]chan error),
		refCount:     1,
		config:       config,
		maxConnRetry: config.MaxConnRetry,
	}
	cc.Add(1)
	go cc.process()
	return cc, nil
}

func (wcw *websocketClientWrapper) getConn() *websocket.Conn {
	wcw.Lock()
	defer wcw.Unlock()
	return wcw.c
}

func (wcw *websocketClientWrapper) setConn(conn *websocket.Conn) {
	wcw.Lock()
	defer wcw.Unlock()
	wcw.c = conn
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
	defer wcw.Done()
	for {
		if wcw.isFinished() {
			return
		}
		msgTyp, data, err := wcw.getConn().ReadMessage()
		if err != nil {
			if wcw.isFinished() {
				return
			}
			errMsg := err.Error()
			if strings.Contains(errMsg, "close") {
				if wcw.reconn() {
					continue
				}
				wcw.Lock()
				wcw.finished = true
				wcw.Unlock()
			}
			for key, errCh := range wcw.errCh {
				select {
				case errCh <- err:
				default:
					conf.Log.Warnf("websocket client connection discard one error for %v", key)
				}
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

func (wcw *websocketClientWrapper) Ping() error {
	return wcw.getConn().WriteMessage(websocket.PingMessage, nil)
}

func (wcw *websocketClientWrapper) Subscribe(ctx api.StreamContext, subChan []api.TopicChannel, messageErrors chan error, _ map[string]interface{}) error {
	wcw.Lock()
	defer wcw.Unlock()
	if wcw.finished {
		return errors.New("websocket client connection closed")
	}
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
	if wcw.finished {
		return true
	}
	subID := wcw.getID(ctx)
	delete(wcw.chs, subID)
	delete(wcw.errCh, subID)
	wcw.refCount--
	if wcw.refCount == 0 {
		wcw.finished = true
		wcw.c.Close()
		return true
	}
	return false
}

func (wcw *websocketClientWrapper) Publish(c api.StreamContext, topic string, message []byte, params map[string]interface{}) error {
	if wcw.isFinished() {
		return errors.New("websocket client connection closed")
	}
	return wcw.getConn().WriteMessage(websocket.TextMessage, message)
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

func (wcw *websocketClientWrapper) reconn() bool {
	if wcw.isFinished() {
		return false
	}
	conf.Log.Info("websocket client closed, try to reconnect")
	for i := 1; i <= wcw.maxConnRetry; i++ {
		conn, err := GetWebsocketClientConn(wcw.config.Addr, wcw.config.Path, wcw.config.tlsConfig)
		if err != nil {
			conf.Log.Infof("websocket client connection reconnect failed, retry: %v, err:%v", i, err)
			if i < wcw.maxConnRetry {
				time.Sleep(10 * time.Millisecond)
			}
			continue
		}
		wcw.getConn().Close()
		wcw.setConn(conn)
		conf.Log.Info("websocket client reconnect success")
		return true
	}
	conf.Log.Info("websocket client reconnect failed")
	return false
}
