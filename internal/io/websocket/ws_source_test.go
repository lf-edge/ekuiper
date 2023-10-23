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
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestWebSocketSource(t *testing.T) {
	conf.InitConf()
	source := GetSource()
	require.NoError(t, source.Configure("/api/data", nil))
	consumer := make(chan api.SourceTuple)
	errCh := make(chan error)
	go source.Open(context.Background(), consumer, errCh)
	// wait websocket server to be ready
	time.Sleep(time.Second)
	require.NoError(t, sendOneWebsocketMsg(map[string]interface{}{
		"a": 1,
	}))
	data := <-consumer
	require.Equal(t, map[string]interface{}{
		"a": float64(1),
	}, data.Message())
}

func sendOneWebsocketMsg(data map[string]interface{}) error {
	u := url.URL{Scheme: "ws", Host: "localhost:10081", Path: "/api/data"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	defer c.Close()
	msg, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return c.WriteMessage(websocket.TextMessage, msg)
}
