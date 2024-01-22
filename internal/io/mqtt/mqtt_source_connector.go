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

package mqtt

import (
	"fmt"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type SourceConnector struct {
	tpc string
	cfg *Conf

	cli      api.MessageClient
	messages chan any
}

type Conf struct {
	Qos       int `json:"qos"`
	BufferLen int `json:"bufferLength"`
}

func (ms *SourceConnector) Configure(topic string, props map[string]any) error {
	cfg := &Conf{
		BufferLen: 10240,
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	cli, err := clients.GetClient("mqtt", props)
	if err != nil {
		return err
	}
	ms.cli = cli
	ms.cfg = cfg
	ms.tpc = topic
	return nil
}

func (ms *SourceConnector) Ping(dataSource string, props map[string]interface{}) error {
	if err := ms.Configure(dataSource, props); err != nil {
		return err
	}
	defer func() {
		_ = ms.Close(context.Background())
	}()
	return ms.cli.Ping()
}

// Subscribe is a one time only operation for source. It connects to the mqtt broker and subscribe to the topic
func (ms *SourceConnector) Subscribe(ctx api.StreamContext) error {
	messages := make(chan any, ms.cfg.BufferLen)
	topics := []api.TopicChannel{{Topic: ms.tpc, Messages: messages}}
	subParam := map[string]interface{}{
		"qos": byte(ms.cfg.Qos),
	}
	ms.messages = messages
	return ms.cli.Subscribe(ctx, topics, nil, subParam)
}

// Open is a continuous process, it keeps reading data from mqtt broker
func (ms *SourceConnector) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	ctx.GetLogger().Infof("Successfully subscribed to topic %s.", ms.tpc)
	for {
		select {
		case <-ctx.Done():
			ctx.GetLogger().Infof("Exit subscription to mqtt messagebus topic %s.", ms.tpc)
			return
		case env := <-ms.messages:
			rcvTime := conf.GetNow()
			msg, ok := env.(pahoMqtt.Message)
			if !ok {
				ctx.GetLogger().Warnf("Received unexpected message type %[1]T(%[1]v)", env)
				continue
			}
			infra.SendThrough(ctx, api.NewDefaultRawTuple(msg.Payload(), map[string]interface{}{
				"topic":     msg.Topic(),
				"qos":       msg.Qos(),
				"messageId": msg.MessageID(),
			}, rcvTime), consumer)
		}
	}
}

func (ms *SourceConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing mqtt source connector to topic %s.", ms.tpc)
	if ms.cli != nil {
		clients.ReleaseClient(ctx, ms.cli)
	}
	return nil
}

var _ api.SourceConnector = &SourceConnector{}
