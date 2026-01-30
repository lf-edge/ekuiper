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
	"fmt"
	"net/http"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
)

const (
	SseTopicPrefix = "$$sse/"
)

func recvSseTopic(endpoint string) string {
	return fmt.Sprintf("%s/server/recv/%s", SseTopicPrefix, endpoint)
}

func sendSseTopic(endpoint string) string {
	return fmt.Sprintf("%s/server/send/%s", SseTopicPrefix, endpoint)
}

func RegisterSSEEndpoint(ctx api.StreamContext, endpoint string) (string, string, error) {
	return manager.RegisterSSEEndpoint(ctx, endpoint)
}

func UnRegisterSSEEndpoint(endpoint string) {
	manager.UnRegisterSSEEndpoint(endpoint)
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

		// Create a subscription to the send topic
		// The sourceID must be unique for each connection to ensure all clients receive the message
		sourceID := fmt.Sprintf("sse/send/%v", m.FetchInstanceID())
		ch := pubsub.CreateSub(sTopic, nil, sourceID, 1024)
		defer pubsub.CloseSourceConsumerChannel(sTopic, sourceID)

		conf.Log.Infof("sse client connected to %s", endpoint)
		notify := r.Context().Done()
		for {
			select {
			case <-notify:
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

	m.router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		if h, ok := m.routes[endpoint]; ok {
			h(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})

	conf.Log.Infof("sse endpoint %v registered success", endpoint)
	return rTopic, sTopic, nil
}

func (m *GlobalServerManager) UnRegisterSSEEndpoint(endpoint string) {
	conf.Log.Infof("sse endpoint %v unregister", endpoint)
	pubsub.RemovePub(recvSseTopic(endpoint))
	pubsub.RemovePub(sendSseTopic(endpoint))
	m.Lock()
	defer m.Unlock()

	delete(m.routes, endpoint)
}
