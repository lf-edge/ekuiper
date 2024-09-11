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

package websocket

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lf-edge/ekuiper/contract/v2/api"
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

func TestWebsocketSource(t *testing.T) {
	connection.InitConnectionManager4Test()
	ip := "127.0.0.1"
	port := 10081
	endpoint := "/e1"
	httpserver.InitGlobalServerManager(ip, port, nil)
	defer httpserver.ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	ws := &WebsocketSource{}
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
	recvCh := make(chan []byte, 10)
	require.NoError(t, ws.Subscribe(ctx, func(ctx api.StreamContext, payload []byte, meta map[string]any, ts time.Time) {
		recvCh <- payload
	}, func(ctx api.StreamContext, err error) {}))
	conn, err := testx.CreateWebsocketClient(ip, port, endpoint)
	require.NoError(t, err)
	data := []byte("123")
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
	require.Equal(t, data, <-recvCh)
	require.NoError(t, ws.Close(ctx))
}
