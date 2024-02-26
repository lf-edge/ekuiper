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
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type WebSocketSink struct {
	cli   api.MessageClient
	props map[string]interface{}
	conf  *WebsocketConf
}

type WebsocketConf struct {
	Path string `json:"path"`
	Addr string `json:"addr"`
	// TODO: move this as a common config for all sinks
	SendError bool `json:"sendError"`
}

func (c *WebsocketConf) validateSinkConf() error {
	if len(c.Path) < 1 {
		return fmt.Errorf("websocket sink conf path should be defined")
	}
	return nil
}

func (wss *WebSocketSink) Ping(_ string, props map[string]interface{}) error {
	if err := wss.Configure(props); err != nil {
		return err
	}
	cli, err := clients.GetClient("websocket", wss.props)
	if err != nil {
		return err
	}
	defer clients.ReleaseClient(context.Background(), cli)
	return cli.Ping()
}

func (wss *WebSocketSink) Open(ctx api.StreamContext) error {
	cli, err := clients.GetClient("websocket", wss.props)
	if err != nil {
		return err
	}
	wss.cli = cli
	ctx.GetLogger().Infof("websocket sink is connected")
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
	wss.conf = c
	return nil
}

func (wss *WebSocketSink) Validate(props map[string]interface{}) error {
	return wss.Configure(props)
}

func (wss *WebSocketSink) Collect(ctx api.StreamContext, data interface{}) error {
	decodeBytes, _, err := ctx.TransformOutput(data)
	if err != nil {
		if wss.conf.SendError {
			_ = wss.cli.Publish(ctx, "", []byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())), nil)
		}
		return err
	}
	err = wss.cli.Publish(ctx, "", decodeBytes, nil)
	if err != nil && wss.conf.SendError {
		_ = wss.cli.Publish(ctx, "", []byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())), nil)
	}
	return err
}

func (wss *WebSocketSink) Close(ctx api.StreamContext) error {
	clients.ReleaseClient(ctx, wss.cli)
	return nil
}
