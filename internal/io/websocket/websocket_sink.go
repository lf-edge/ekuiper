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
	"net/url"

	"github.com/gorilla/websocket"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type WebSocketSink struct {
	conf *WebSocketSinkConf
	conn *websocket.Conn
}

type WebSocketSinkConf struct {
	Addr string `json:"addr"`
	Path string `json:"path"`
}

func (wss *WebSocketSink) Open(ctx api.StreamContext) error {
	u := url.URL{Scheme: "ws", Host: wss.conf.Addr, Path: wss.conf.Path}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	wss.conn = c
	return nil
}

func (wss *WebSocketSink) Configure(props map[string]interface{}) error {
	conf := &WebSocketSinkConf{}
	if err := cast.MapToStruct(props, conf); err != nil {
		return err
	}
	wss.conf = conf
	return nil
}

func (wss *WebSocketSink) Collect(ctx api.StreamContext, data interface{}) error {
	decodeBytes, _, err := ctx.TransformOutput(data)
	if err != nil {
		return err
	}
	if err := wss.conn.WriteMessage(websocket.TextMessage, decodeBytes); err != nil {
		return err
	}
	return nil
}

func (wss *WebSocketSink) Close(ctx api.StreamContext) error {
	return wss.conn.Close()
}
