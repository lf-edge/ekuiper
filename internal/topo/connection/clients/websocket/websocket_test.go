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
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/mock/context"
	"github.com/lf-edge/ekuiper/pkg/api"
)

const (
	addr = "localhost:45784"
	path = "/ws"
)

func TestWebsocketPubSub(t *testing.T) {
	go mockWebSocketServer()
	ctx := context.NewMockContext("123", "123")
	cli, err := newWebsocketClientClientWrapper(&WebSocketConnectionConfig{Addr: addr, Path: path})
	require.NoError(t, err)
	cli.SetConnectionSelector("456")
	require.Equal(t, "456", cli.GetConnectionSelector())
	data := map[string]interface{}{"a": float64(1)}
	databytes, err := json.Marshal(data)
	require.NoError(t, err)

	dataCh := make(chan interface{})
	subs := []api.TopicChannel{
		{
			Topic:    "",
			Messages: dataCh,
		},
	}
	errCh := make(chan error)
	require.NoError(t, cli.Subscribe(ctx, subs, errCh, map[string]interface{}{}))
	err = cli.Publish(ctx, "", databytes, map[string]interface{}{})
	require.NoError(t, err)
	// assert pub
	require.Equal(t, data, <-recvDataCh)
	// assert sub
	require.Equal(t, databytes, <-dataCh)
	processDone <- struct{}{}

	cli.AddRef()
	cli.Release(ctx)
	wsCli := cli.(*websocketClientWrapper)
	require.False(t, wsCli.isFinished())
	require.False(t, wsCli.processDone)
	cli.Release(ctx)
	require.True(t, wsCli.isFinished())
	time.Sleep(100 * time.Millisecond)
	require.True(t, wsCli.processDone)
}

func mockWebSocketServer() {
	http.HandleFunc(path, handler)
	http.ListenAndServe(addr, nil)
}

var (
	recvDataCh  chan interface{}
	processDone chan struct{}
)

func init() {
	recvDataCh = make(chan interface{})
	processDone = make(chan struct{})
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  256,
	WriteBufferSize: 256,
	WriteBufferPool: &sync.Pool{},
}

func process(c *websocket.Conn) {
	defer c.Close()
	_, message, err := c.ReadMessage()
	if err != nil {
		recvDataCh <- err
		return
	}
	a := map[string]interface{}{}
	err = json.Unmarshal(message, &a)
	if err != nil {
		recvDataCh <- err
		return
	}
	recvDataCh <- a

	c.WriteMessage(websocket.TextMessage, message)
	<-processDone
}

func handler(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		conf.Log.Errorf("upgrade: %v", err)
		return
	}

	go process(c)
}
