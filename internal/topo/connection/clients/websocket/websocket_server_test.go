// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/mock/context"
)

var (
	serverRecvCh chan map[string]interface{}
	serverPubCh  chan []byte
)

func TestWebsocketServerConn(t *testing.T) {
	conf.InitConf()
	// no endpoint, create client failed
	_, err := newWebsocketServerConnWrapper(&WebSocketConnectionConfig{Path: "/ws3", CheckConnection: true})
	require.Error(t, err)

	ctx := context.NewMockContext("123", "123")
	_, _, _, err = httpserver.RegisterWebSocketEndpoint(ctx, "/ws3")
	require.NoError(t, err)

	// no connection, create client failed
	_, err = newWebsocketServerConnWrapper(&WebSocketConnectionConfig{Path: "/ws3", CheckConnection: true})
	require.Error(t, err)

	// wait server ready
	time.Sleep(100 * time.Millisecond)

	c, err := createOneConn(t)
	require.NoError(t, err)
	c.Close()
	// wait previous connection goroutine closed
	time.Sleep(10 * time.Millisecond)
	c, err = createOneConn(t)
	require.NoError(t, err)

	serverRecvCh = make(chan map[string]interface{})
	serverPubCh = make(chan []byte)
	cli, err := newWebsocketServerConnWrapper(&WebSocketConnectionConfig{Path: "/ws3", CheckConnection: true})
	require.NoError(t, err)
	require.NotNil(t, cli)

	dataCh := make(chan interface{})
	data := map[string]interface{}{"a": float64(1)}
	subs := []api.TopicChannel{
		{
			Topic:    "",
			Messages: dataCh,
		},
	}
	errCh := make(chan error)
	go subData(t, dataCh)
	// assert sub
	require.NoError(t, cli.Subscribe(ctx, subs, errCh, map[string]interface{}{}))
	err = sendOneWebsocketMsg(c, data)
	require.NoError(t, err)
	defer c.Close()
	require.Equal(t, data, <-serverRecvCh)

	// assert pub
	go pubData(t, c)
	bs, err := json.Marshal(data)
	require.NoError(t, err)
	require.NoError(t, cli.Publish(ctx, "", bs, map[string]interface{}{}))
	require.Equal(t, bs, <-serverPubCh)

	go pubData(t, c)
	dataValue := []byte("123")
	require.NoError(t, cli.Publish(ctx, "t", dataValue, map[string]interface{}{}))
	require.Equal(t, dataValue, <-serverPubCh)

	cli.AddRef()
	require.False(t, cli.Release(ctx))
	require.True(t, cli.Release(ctx))
}

func pubData(t *testing.T, c *websocket.Conn) {
	msgTyp, msg, err := c.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, msgTyp)
	serverPubCh <- msg
}

func subData(t *testing.T, dataCh chan interface{}) {
	data := <-dataCh
	m := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(data.([]byte), &m))
	serverRecvCh <- m
}

func createOneConn(t *testing.T) (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:10081", Path: "/ws3"}
	c, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		bs, _ := io.ReadAll(resp.Body)
		t.Errorf("create connection failed, code:%v err:%v", resp.StatusCode, string(bs))
		return nil, err
	}
	return c, nil
}

func sendOneWebsocketMsg(c *websocket.Conn, data map[string]interface{}) error {
	msg, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return c.WriteMessage(websocket.TextMessage, msg)
}
