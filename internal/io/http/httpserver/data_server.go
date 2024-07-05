// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type GlobalServerManager struct {
	sync.RWMutex
	instanceID        int
	endpoint          map[string]string
	server            *http.Server
	router            *mux.Router
	upgrader          websocket.Upgrader
	websocketEndpoint map[string]*websocketEndpointContext
}

var manager *GlobalServerManager

func InitGlobalServerManager(ip string, port int, tlsConf *conf.TlsConf) {
	r := mux.NewRouter()
	s := &http.Server{
		Addr: cast.JoinHostPortInt(ip, port),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 60 * 5,
		ReadTimeout:  time.Second * 60 * 5,
		IdleTimeout:  time.Second * 60,
		Handler:      handlers.CORS(handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Type", "Content-Language", "Origin", "Authorization"}), handlers.AllowedMethods([]string{"POST", "GET", "PUT", "DELETE", "HEAD"}))(r),
	}
	upgrader := websocket.Upgrader{
		ReadBufferSize:  256,
		WriteBufferSize: 256,
		WriteBufferPool: &sync.Pool{},
		// always allowed any origin
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	manager = &GlobalServerManager{
		websocketEndpoint: map[string]*websocketEndpointContext{},
		endpoint:          map[string]string{},
		server:            s,
		router:            r,
		upgrader:          upgrader,
	}
	go func(m *GlobalServerManager) {
		if tlsConf == nil {
			s.ListenAndServe()
		} else {
			s.ListenAndServeTLS(conf.Config.Source.HttpServerTls.Certfile, conf.Config.Source.HttpServerTls.Keyfile)
		}
	}(manager)
	time.Sleep(500 * time.Millisecond)
}

func ShutDown() {
	manager.Shutdown()
	manager = nil
}

func RegisterEndpoint(endpoint string, method string) (string, error) {
	return manager.RegisterEndpoint(endpoint, method)
}

func UnregisterEndpoint(endpoint string) {
	manager.UnregisterEndpoint(endpoint)
}

const (
	TopicPrefix = "$$httppush/"
)

func (m *GlobalServerManager) RegisterEndpoint(endpoint string, method string) (string, error) {
	var topic string
	var ok bool
	m.Lock()
	topic, ok = m.endpoint[endpoint]
	if ok {
		return topic, nil
	} else {
		topic = TopicPrefix + endpoint
		m.endpoint[endpoint] = topic
	}
	m.Unlock()
	pubsub.CreatePub(topic)
	m.router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			handleError(w, err, "Fail to decode data")
			return
		}
		pubsub.ProduceAny(topoContext.Background(), topic, data)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}).Methods(method)
	return topic, nil
}

func (m *GlobalServerManager) UnregisterEndpoint(endpoint string) {
	var ok bool
	m.Lock()
	_, ok = m.endpoint[endpoint]
	if !ok {
		return
	}
	delete(m.endpoint, endpoint)
	m.Unlock()
	pubsub.RemovePub(TopicPrefix + endpoint)
}

func (m *GlobalServerManager) Shutdown() {
	m.server.Shutdown(context.Background())
}

func handleError(w http.ResponseWriter, err error, prefix string) {
	message := prefix
	if message != "" {
		message += ": "
	}
	message += err.Error()
	http.Error(w, message, http.StatusBadRequest)
}
