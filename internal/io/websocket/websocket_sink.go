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
	"fmt"

	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type WebSocketSink struct {
	cli   api.MessageClient
	props map[string]interface{}
}

type WebsocketConf struct {
	Path string `json:"path"`
}

func (c *WebsocketConf) validateSinkConf() error {
	if len(c.Path) < 1 {
		return fmt.Errorf("websocket sink conf path should be defined")
	}
	return nil
}

func (wss *WebSocketSink) Open(ctx api.StreamContext) error {
	cli, err := clients.GetClient("websocket", wss.props)
	if err != nil {
		return err
	}
	wss.cli = cli
	return nil
}

func (wss *WebSocketSink) Configure(props map[string]interface{}) error {
	wss.props = props
	c := &WebsocketConf{}
	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}
	if err := c.validateSinkConf(); err != nil {
		return err
	}
	return nil
}

func (wss *WebSocketSink) Collect(ctx api.StreamContext, data interface{}) error {
	decodeBytes, _, err := ctx.TransformOutput(data)
	if err != nil {
		return err
	}
	return wss.cli.Publish(ctx, "", decodeBytes, nil)
}

func (wss *WebSocketSink) Close(ctx api.StreamContext) error {
	clients.ReleaseClient(ctx, wss.cli)
	return nil
}
