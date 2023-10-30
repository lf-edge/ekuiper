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
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/io/mock/context"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
)

const (
	addr = "localhost:45783"
	path = "/ws"
)

var dataCh chan interface{}

func init() {
	dataCh = make(chan interface{})
}

func TestWebSocketSink(t *testing.T) {
	factory.InitClientsFactory()
	go mockWebSocketServer()
	// wait mock server started
	time.Sleep(100 * time.Millisecond)
	wsSink := &WebSocketSink{}
	require.NoError(t, wsSink.Configure(map[string]interface{}{
		"addr": addr,
		"path": path,
	}))
	require.NoError(t, wsSink.Open(context.NewMockContext("r", "o")))
	data := map[string]interface{}{
		"a": float64(1),
	}
	require.NoError(t, wsSink.Collect(context.NewMockContext("r", "o"), data))
	v := <-dataCh
	switch nv := v.(type) {
	case error:
		require.NoError(t, nv)
	case map[string]interface{}:
		require.Equal(t, data, nv)
	default:
		t.Fatal("unknown data")
	}
}

func mockWebSocketServer() {
	http.HandleFunc(path, handler)
	http.ListenAndServe(addr, nil)
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  256,
	WriteBufferSize: 256,
	WriteBufferPool: &sync.Pool{},
}

func process(c *websocket.Conn) {
	defer c.Close()
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			dataCh <- err
			break
		}
		a := map[string]interface{}{}
		err = json.Unmarshal(message, &a)
		if err != nil {
			dataCh <- err
			break
		}
		dataCh <- a
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}

	// Process connection in a new goroutine
	go process(c)
}
