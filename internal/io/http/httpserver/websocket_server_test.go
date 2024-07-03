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
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestWebsocketServerRecvData(t *testing.T) {
	ip := "127.0.0.1"
	port := 10085
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	endpint := "/e1"
	recvTopic, err := RegisterWebSocketEndpoint(ctx, endpint)
	require.NoError(t, err)
	subCh := pubsub.CreateSub(recvTopic, nil, "test", 1024)
	defer pubsub.CloseSourceConsumerChannel(recvTopic, "test")
	conn, err := createWebsocketClient(ip, port, endpint)
	require.NoError(t, err)
	defer conn.Close()
	data := []byte("123")
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
	recvData := <-subCh
	require.Equal(t, data, recvData.([]byte))
	UnRegisterWebSocketEndpoint(endpint)
}

func TestWebsocketServerRecvDataCancel(t *testing.T) {
	ip := "127.0.0.1"
	port := 10085
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	endpint := "/e1"
	_, err := RegisterWebSocketEndpoint(ctx, endpint)
	require.NoError(t, err)
	UnRegisterWebSocketEndpoint(endpint)
}

func TestWebsocketServerRecvDataOther(t *testing.T) {
	ip := "127.0.0.1"
	port := 10085
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	endpint := "/e1"
	_, err := RegisterWebSocketEndpoint(ctx, endpint)
	require.NoError(t, err)
	conn, err := testx.CreateWebsocketClient(ip, port, endpint)
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.PingMessage, []byte("123")))
	require.NoError(t, conn.WriteMessage(websocket.CloseMessage, []byte("123")))
	wctx := manager.getEndpointConnections(endpint)
	wctx.wg.Wait()
	require.Equal(t, 0, len(wctx.conns))
}
