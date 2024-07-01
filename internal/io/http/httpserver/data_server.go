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
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type GlobalServerManager struct {
	sync.RWMutex
	endpointRef map[string]int

	server *http.Server
	router *mux.Router
	cancel context.CancelFunc
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
	manager = &GlobalServerManager{
		endpointRef: map[string]int{},
		server:      s,
		router:      r,
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

func EndpointRef() map[string]int {
	return manager.EndpointRef()
}

func ShutDown() {
	manager.Shutdown()
	manager = nil
}

const (
	TopicPrefix = "$$httppush/"
)

func RegisterEndpoint(endpoint string, method string) (string, error) {
	return manager.RegisterEndpoint(endpoint, method)
}

func UnregisterEndpoint(endpoint string) {
	manager.UnregisterEndpoint(endpoint)
}

func (m *GlobalServerManager) EndpointRef() map[string]int {
	m.RLock()
	defer m.RUnlock()
	return m.endpointRef
}

func (m *GlobalServerManager) RegisterEndpoint(endpoint string, method string) (string, error) {
	var needcreate bool
	m.Lock()
	count, ok := m.endpointRef[endpoint]
	count++
	if !ok {
		needcreate = true
		m.endpointRef[endpoint] = 1
	} else {
		m.endpointRef[endpoint] = count
	}
	m.Unlock()
	topic := TopicPrefix + endpoint
	if needcreate {
		pubsub.CreatePub(topic)
	}
	m.router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		m := make(map[string]interface{})
		err := json.NewDecoder(r.Body).Decode(&m)
		if err != nil {
			handleError(w, err, "Fail to decode data")
			pubsub.ProduceError(topoContext.Background(), topic, fmt.Errorf("fail to decode data %s: %v", r.Body, err))
			return
		}
		pubsub.Produce(topoContext.Background(), topic, &xsql.Tuple{Message: m, Timestamp: timex.GetNow()})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}).Methods(method)
	return topic, nil
}

func (m *GlobalServerManager) UnregisterEndpoint(endpoint string) {
	var needRemove bool
	m.Lock()
	c, ok := m.endpointRef[endpoint]
	if ok {
		c--
		if c < 1 {
			needRemove = true
			delete(m.endpointRef, endpoint)
		} else {
			m.endpointRef[endpoint] = c
		}
	}
	m.Unlock()
	if needRemove {
		pubsub.RemovePub(TopicPrefix + endpoint)
	}
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
