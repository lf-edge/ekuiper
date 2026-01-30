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
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
)

const (
	SseTopicPrefix = "$$sse/"
)

type sseEndpointContext struct {
	wg    *sync.WaitGroup
	conns map[int64]context.CancelFunc
}

func recvSseTopic(endpoint string) string {
	return fmt.Sprintf("%s/server/recv/%s", SseTopicPrefix, endpoint)
}

func sendSseTopic(endpoint string) string {
	return fmt.Sprintf("%s/server/send/%s", SseTopicPrefix, endpoint)
}

func RegisterSSEEndpoint(ctx api.StreamContext, endpoint string) (string, string, error) {
	managerLock.RLock()
	m := manager
	managerLock.RUnlock()
	if m == nil {
		return "", "", fmt.Errorf("http server is not running")
	}
	return m.RegisterSSEEndpoint(ctx, endpoint)
}

func UnRegisterSSEEndpoint(endpoint string) {
	managerLock.RLock()
	m := manager
	managerLock.RUnlock()
	if m == nil {
		return
	}
	sctx := m.UnRegisterSSEEndpoint(endpoint)
	if sctx != nil {
		// wait all connections to close
		sctx.wg.Wait()
	}
}

func (m *GlobalServerManager) RegisterSSEEndpoint(ctx api.StreamContext, endpoint string) (string, string, error) {
	conf.Log.Infof("sse endpoint %v register", endpoint)
	m.Lock()
	defer m.Unlock()
	rTopic := recvSseTopic(endpoint)
	sTopic := sendSseTopic(endpoint)
	pubsub.CreatePub(rTopic)

	m.routes[endpoint] = func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		flusher.Flush()

		// Create a cancel context for this specific connection
		connCtx, cancel := context.WithCancel(r.Context())
		connID := int64(m.FetchInstanceID())
		wg, ok := m.AddSSEConnection(endpoint, connID, cancel)
		if !ok {
			return
		}
		defer func() {
			m.CloseSSEConnection(endpoint, connID)
			wg.Done()
		}()

		// Create a subscription to the send topic
		// The sourceID must be unique for each connection to ensure all clients receive the message
		sourceID := fmt.Sprintf("sse/send/%v", connID)
		ch := pubsub.CreateSub(sTopic, nil, sourceID, 1024)
		defer pubsub.CloseSourceConsumerChannel(sTopic, sourceID)

		conf.Log.Infof("sse client connected to %s", endpoint)

		for {
			select {
			case <-connCtx.Done():
				conf.Log.Infof("sse client disconnected from %s", endpoint)
				return
			case d, ok := <-ch:
				if !ok {
					conf.Log.Infof("sse channel closed for %s", endpoint)
					return
				}
				data, ok := d.([]byte)
				if !ok || data == nil {
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", string(data))
				flusher.Flush()
			}
		}
	}

	m.sseEndpoint[endpoint] = &sseEndpointContext{
		wg:    &sync.WaitGroup{},
		conns: make(map[int64]context.CancelFunc),
	}
	m.router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		m.RLock()
		h, ok := m.routes[endpoint]
		m.RUnlock()
		if ok {
			h(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})

	conf.Log.Infof("sse endpoint %v registered success", endpoint)
	return rTopic, sTopic, nil
}

func (m *GlobalServerManager) AddSSEConnection(endpoint string, connID int64, cancel context.CancelFunc) (*sync.WaitGroup, bool) {
	m.Lock()
	defer m.Unlock()
	sctx, ok := m.sseEndpoint[endpoint]
	if !ok {
		return nil, false
	}
	sctx.conns[connID] = cancel
	sctx.wg.Add(1)
	return sctx.wg, true
}

func (m *GlobalServerManager) CloseSSEConnection(endpoint string, connID int64) {
	m.Lock()
	defer m.Unlock()
	sctx, ok := m.sseEndpoint[endpoint]
	if !ok {
		return
	}
	delete(sctx.conns, connID)
}

func (m *GlobalServerManager) UnRegisterSSEEndpoint(endpoint string) *sseEndpointContext {
	conf.Log.Infof("sse endpoint %v unregister", endpoint)
	pubsub.RemovePub(recvSseTopic(endpoint))
	m.Lock()
	defer m.Unlock()

	sctx, ok := m.sseEndpoint[endpoint]
	if !ok {
		delete(m.routes, endpoint)
		return nil
	}
	// Cancel all active connections
	for _, cancel := range sctx.conns {
		cancel()
	}
	delete(m.sseEndpoint, endpoint)
	delete(m.routes, endpoint)
	return sctx
}
