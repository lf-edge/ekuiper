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

package websocket

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConnection("websocket", httpserver.CreateWebsocketConnection)
}

func TestWebsocketSink(t *testing.T) {
	connection.InitConnectionManager4Test()
	ip := "127.0.0.1"
	port := 10081
	endpoint := "/e1"
	httpserver.InitGlobalServerManager(ip, port, nil)
	defer httpserver.ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	ws := &WebsocketSink{}
	props := map[string]any{
		"datasource": endpoint,
	}
	require.Error(t, ws.Provision(ctx, map[string]any{
		"datasource": "",
	}))
	require.NoError(t, ws.Provision(ctx, props))
	require.NoError(t, ws.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	expData := []byte("123")
	assertCh := make(chan struct{})
	conn, err := testx.CreateWebsocketClient(ip, port, endpoint)
	require.NoError(t, err)
	go func() {
		msgTyp, data, err := conn.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, websocket.TextMessage, msgTyp)
		require.Equal(t, expData, data)
		assertCh <- struct{}{}
	}()
	// wait goroutine start
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, ws.collect(ctx, expData))
	<-assertCh
	ws.Close(ctx)
}
