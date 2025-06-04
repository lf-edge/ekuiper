// Copyright 2024-2024 EMQ Technologies Co., Ltd.
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
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestWebsocketConn(t *testing.T) {
	ip := "127.0.0.1"
	port := 10086
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	props := map[string]any{
		"datasource": "/e1",
	}
	conn := CreateWebsocketConnection(ctx).(*WebsocketConnection)
	err := conn.Provision(ctx, "test", props)
	require.NoError(t, err)
	err = conn.Dial(ctx)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	require.NoError(t, conn.Close(ctx))
}

func TestWebsocketClientConn(t *testing.T) {
	tc := newTC()
	s := createWServer(tc)
	defer func() {
		s.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	props := map[string]any{
		"path": "/ws",
		"addr": s.URL[len("http://"):],
	}
	conn := CreateWebsocketConnection(ctx).(*WebsocketConnection)
	err := conn.Provision(ctx, "test", props)
	require.NoError(t, err)
	err = conn.Dial(ctx)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	require.NoError(t, conn.Close(ctx))
}

func newTC() *testcase {
	ctx, cancel := context.WithCancel(context.Background())
	return &testcase{
		ctx:    ctx,
		cancel: cancel,
		recvCh: make(chan []byte, 10),
		sendCh: make(chan []byte, 10),
	}
}

type testcase struct {
	ctx    context.Context
	cancel context.CancelFunc
	recvCh chan []byte
	sendCh chan []byte
}

func createWServer(tc *testcase) *httptest.Server {
	router := http.NewServeMux()
	router.HandleFunc("/ws", tc.handler)
	server := httptest.NewServer(router)
	return server
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  256,
	WriteBufferSize: 256,
	WriteBufferPool: &sync.Pool{},
}

func (tc *testcase) recvProcess(c *websocket.Conn) {
	defer func() {
		tc.cancel()
		c.Close()
	}()
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			return
		}
		tc.recvCh <- message
	}
}

func (tc *testcase) sendProcess(c *websocket.Conn) {
	defer func() {
		tc.cancel()
		c.Close()
	}()
	for {
		select {
		case <-tc.ctx.Done():
			return
		case x := <-tc.sendCh:
			err := c.WriteMessage(websocket.TextMessage, x)
			if err != nil {
				return
			}
		}
	}
}

func (tc *testcase) handler(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	go tc.recvProcess(c)
	go tc.sendProcess(c)
}
