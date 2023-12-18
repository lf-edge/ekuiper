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
	"strings"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// manage the global http data server

var (
	refCount      int32
	server        *http.Server
	router        *mux.Router
	done          chan struct{}
	sctx          api.StreamContext
	lock          sync.RWMutex
	upgrader      websocket.Upgrader
	wsEndpointCtx map[string]*websocketContext
)

const (
	TopicPrefix            = "$$httppush/"
	WebsocketTopicPrefix   = "$$websocket/"
	WebsocketServerDataKey = "$$websocket/server/data"
)

type websocketContext struct {
	sync.Mutex
	conns        map[*websocket.Conn]context.CancelFunc
	contextClose bool
}

func (wsctx *websocketContext) getConnCount() int {
	wsctx.Lock()
	defer wsctx.Unlock()
	return len(wsctx.conns)
}

func (wsctx *websocketContext) addConn(conn *websocket.Conn, cancel context.CancelFunc) {
	wsctx.Lock()
	defer wsctx.Unlock()
	if wsctx.contextClose {
		return
	}
	wsctx.conns[conn] = cancel
}

func (wsctx *websocketContext) removeConn(conn *websocket.Conn, endpoint string) {
	wsctx.Lock()
	defer wsctx.Unlock()
	if wsctx.contextClose {
		return
	}
	if cancel, ok := wsctx.conns[conn]; ok {
		cancel()
		delete(wsctx.conns, conn)
		conn.Close()
		conf.Log.Infof("websocket endpoint %v remove one connection", endpoint)
	}
}

func (wsctx *websocketContext) close() {
	wsctx.Lock()
	defer wsctx.Unlock()
	for conn := range wsctx.conns {
		conn.Close()
	}
	wsctx.contextClose = true
}

func init() {
	contextLogger := conf.Log.WithField("httppush_connection", 0)
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	ruleId := "$$httppush_connection"
	opId := "$$httppush_connection"
	store, err := state.CreateStore(ruleId, 0)
	if err != nil {
		ctx.GetLogger().Errorf("neuron connection create store error %v", err)
		panic(err)
	}
	sctx = ctx.WithMeta(ruleId, opId, store)
	upgrader = websocket.Upgrader{
		ReadBufferSize:  256,
		WriteBufferSize: 256,
		WriteBufferPool: &sync.Pool{},
		// always allowed any origin
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	wsEndpointCtx = make(map[string]*websocketContext)
}

func registerInit() error {
	lock.Lock()
	defer lock.Unlock()
	if server == nil {
		var err error
		server, router, err = createDataServer()
		if err != nil {
			return err
		}
	}
	refCount++
	return nil
}

func GetWebsocketEndpointCh(endpoint string) (string, string, chan struct{}, error) {
	lock.Lock()
	defer lock.Unlock()
	if server == nil {
		return "", "", nil, fmt.Errorf("http server is not initialized")
	}
	if wsCtx, ok := wsEndpointCtx[endpoint]; ok {
		if wsCtx.getConnCount() < 1 {
			return "", "", nil, fmt.Errorf("websocket endpoint %s has no connection", endpoint)
		}
		return fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint), fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint),
			done, nil
	}
	return "", "", nil, fmt.Errorf("websocket has no endpoint %s", endpoint)
}

func recvProcess(ctx api.StreamContext, c *websocket.Conn, endpoint string) {
	defer func() {
		wsEndpointCtx[endpoint].removeConn(c, endpoint)
		if r := recover(); r != nil {
			conf.Log.Infof("websocket recvProcess Process panic recovered, err:%v", r)
		}
		conf.Log.Infof("websocket endpoint %v stop recvProcess", endpoint)
	}()
	conf.Log.Infof("websocket endpoint %v start recvProcess", endpoint)
	topic := fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgType, data, err := c.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err) || strings.Contains(err.Error(), "close") {
				conf.Log.Infof("websocket endpoint %s connection get closed: %v", endpoint, err)
				return
			}
			conf.Log.Errorf("websocket endpoint %s recv error %s", endpoint, err)
			continue
		}
		conf.Log.Infof("websocket endpoint %v recv msg success", endpoint)
		switch msgType {
		case websocket.TextMessage:
			m := make(map[string]interface{})
			if err := json.Unmarshal(data, &m); err != nil {
				conf.Log.Errorf("websocket endpoint %s recv error %s", endpoint, err)
				continue
			}
			pubsub.Produce(ctx, topic, m)
		case websocket.CloseMessage:
			conf.Log.Infof("websocket endpoint %v recv close message", endpoint)
			return
		}
	}
}

func sendProcess(ctx api.StreamContext, c *websocket.Conn, endpoint string) {
	defer func() {
		wsEndpointCtx[endpoint].removeConn(c, endpoint)
		if r := recover(); r != nil {
			conf.Log.Infof("websocket sendProcess Process panic recovered, err:%v", r)
		}
		conf.Log.Infof("websocket endpoint %v stop sendProcess", endpoint)
	}()
	conf.Log.Infof("websocket endpoint %v start sendProcess", endpoint)
	topic := fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint)
	subCh := pubsub.CreateSub(topic, nil, "", 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, "")
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-subCh:
			bsV := data.Message()[WebsocketServerDataKey]
			bs, ok := bsV.([]byte)
			if !ok {
				conf.Log.Warnf("%v should send bytes", WebsocketServerDataKey)
				continue
			}
			if err := c.WriteMessage(websocket.TextMessage, bs); err != nil {
				// close sent error can't be captured by IsCloseError
				if websocket.IsCloseError(err) || strings.Contains(err.Error(), "close") {
					conf.Log.Infof("websocket endpoint %s connection get closed, err:%v", endpoint, err)
					return
				}
				conf.Log.Errorf("websocket endpoint %s send error %s", endpoint, err)
				continue
			}
		}
	}
}

func CheckWebsocketEndpoint(endpoint string) bool {
	lock.Lock()
	defer lock.Unlock()
	if server == nil {
		return false
	}
	_, ok := wsEndpointCtx[endpoint]
	return ok
}

func RegisterWebSocketEndpoint(ctx api.StreamContext, endpoint string) (string, string, chan struct{}, error) {
	conf.Log.Infof("websocket endpoint %v register", endpoint)
	lock.Lock()
	defer lock.Unlock()
	if server == nil {
		var err error
		server, router, err = createDataServer()
		if err != nil {
			return "", "", nil, err
		}
	}
	if _, ok := wsEndpointCtx[endpoint]; ok {
		conf.Log.Infof("websocker endpoint %v already registered", endpoint)
		return fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint), fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint),
			done, nil
	}
	refCount++
	wsCtx := &websocketContext{
		conns: map[*websocket.Conn]context.CancelFunc{},
	}
	wsEndpointCtx[endpoint] = wsCtx
	recvTopic := fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint)
	pubsub.CreatePub(recvTopic)
	sendTopic := fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint)
	pubsub.CreatePub(sendTopic)
	router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			conf.Log.Errorf("websocket upgrade error: %v", err)
			return
		}
		subCtx, cancel := ctx.WithCancel()
		wsCtx.addConn(c, cancel)
		conf.Log.Infof("websocket endpint %v create connection", endpoint)
		go recvProcess(subCtx, c, endpoint)
		go sendProcess(subCtx, c, endpoint)
	})
	conf.Log.Infof("websocker endpoint %v registered success", endpoint)
	return fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint), fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint),
		done, nil
}

func UnRegisterWebSocketEndpoint(endpoint string) error {
	conf.Log.Infof("websocket endpoint %v unregister", endpoint)
	lock.Lock()
	defer lock.Unlock()
	if _, ok := wsEndpointCtx[endpoint]; !ok {
		return nil
	}
	refCount--
	wsEndpointCtx[endpoint].close()
	delete(wsEndpointCtx, endpoint)
	if refCount == 0 {
		shutdown()
	}
	return nil
}

func RegisterEndpoint(endpoint string, method string, _ string) (string, chan struct{}, error) {
	err := registerInit()
	if err != nil {
		return "", nil, err
	}
	topic := TopicPrefix + endpoint
	pubsub.CreatePub(topic)
	router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		sctx.GetLogger().Debugf("receive http request: %s", r.URL.String())
		defer r.Body.Close()
		m := make(map[string]interface{})
		err := json.NewDecoder(r.Body).Decode(&m)
		if err != nil {
			handleError(w, err, "Fail to decode data")
			pubsub.ProduceError(sctx, topic, fmt.Errorf("fail to decode data %s: %v", r.Body, err))
			return
		}
		sctx.GetLogger().Debugf("httppush received message %s", m)
		pubsub.Produce(sctx, topic, m)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}).Methods(method)
	return topic, done, nil
}

func UnregisterEndpoint(endpoint string) {
	lock.Lock()
	defer lock.Unlock()
	pubsub.RemovePub(TopicPrefix + endpoint)
	refCount--
	// TODO async close server
	if refCount == 0 {
		shutdown()
	}
}

func shutdown() {
	sctx.GetLogger().Infof("shutting down http data server...")
	if server != nil {
		if err := server.Shutdown(sctx); err != nil {
			sctx.GetLogger().Errorf("shutdown: %s", err)
		}
		sctx.GetLogger().Infof("http data server exiting")
	}
	server = nil
	router = nil
}

// createDataServer creates a new http data server. Must run inside lock
func createDataServer() (*http.Server, *mux.Router, error) {
	r := mux.NewRouter()
	s := &http.Server{
		Addr: cast.JoinHostPortInt(conf.Config.Source.HttpServerIp, conf.Config.Source.HttpServerPort),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 60 * 5,
		ReadTimeout:  time.Second * 60 * 5,
		IdleTimeout:  time.Second * 60,
		Handler:      handlers.CORS(handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Type", "Content-Language", "Origin", "Authorization"}), handlers.AllowedMethods([]string{"POST", "GET", "PUT", "DELETE", "HEAD"}))(r),
	}
	done = make(chan struct{})
	go func(done chan struct{}) {
		var err error
		if conf.Config.Source.HttpServerTls == nil {
			err = s.ListenAndServe()
		} else {
			err = s.ListenAndServeTLS(conf.Config.Source.HttpServerTls.Certfile, conf.Config.Source.HttpServerTls.Keyfile)
		}
		if err != nil {
			sctx.GetLogger().Errorf("http data server error: %v", err)
			close(done)
		}
	}(done)
	sctx.GetLogger().Infof("Serving http data server on port http://%s", cast.JoinHostPortInt(conf.Config.Source.HttpServerIp, conf.Config.Source.HttpServerPort))
	return s, r, nil
}

func handleError(w http.ResponseWriter, err error, prefix string) {
	message := prefix
	if message != "" {
		message += ": "
	}
	message += err.Error()
	http.Error(w, message, http.StatusBadRequest)
}
