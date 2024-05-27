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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/io/connection"
	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// SourceConnector is the connector for mqtt source
// When sharing the same connection, each topic will have one single sourceConnector as the shared source node
type SourceConnector struct {
	tpc   string
	cfg   *Conf
	props map[string]any

	cli *client.Connection
}

type Conf struct {
	Topic string `json:"datasource"`
	Qos   int    `json:"qos"`
	SelId string `json:"connectionSelector"`
}

func (ms *SourceConnector) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &Conf{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	_, err = client.ValidateConfig(props)
	if err != nil {
		return err
	}
	ms.props = props
	ms.cfg = cfg
	ms.tpc = cfg.Topic
	return nil
}

func (ms *SourceConnector) Ping(ctx api.StreamContext, props map[string]interface{}) error {
	cli, err := client.CreateAnonymousConnection(context.Background(), props)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

func (ms *SourceConnector) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Connecting to mqtt server")
	var cli *client.Connection
	var err error
	if len(ms.cfg.SelId) > 0 {
		conn, err := connection.GetNameConnection(ms.cfg.SelId)
		if err != nil {
			return err
		}
		c, ok := conn.(*client.Connection)
		if !ok {
			return fmt.Errorf("connection %s should be mqtt connection", ms.cfg.SelId)
		}
		cli = c
	} else {
		id := fmt.Sprintf("%s-%s-%s-mqtt-source", ctx.GetRuleId(), ctx.GetOpId(), ms.tpc)
		conn, err := connection.CreateNonStoredConnection(ctx, id, "mqtt", ms.props)
		if err != nil {
			return err
		}
		cli = conn.(*client.Connection)
	}
	ms.cli = cli
	return err
}

// Subscribe is a one time only operation for source. It connects to the mqtt broker and subscribe to the topic
// Run open before subscribe
func (ms *SourceConnector) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	return ms.cli.Subscribe(ms.tpc, &client.SubscriptionInfo{
		Qos: byte(ms.cfg.Qos),
		Handler: func(client pahoMqtt.Client, message pahoMqtt.Message) {
			ms.onMessage(ctx, message, ingest)
		},
		ErrHandler: func(err error) {
			ingestError(ctx, err)
		},
	})
}

func (ms *SourceConnector) onMessage(ctx api.StreamContext, msg pahoMqtt.Message, ingest api.BytesIngest) {
	if msg != nil {
		ctx.GetLogger().Debugf("Received message %s from topic %s", string(msg.Payload()), msg.Topic())
	}
	rcvTime := timex.GetNow()
	ingest(ctx, msg.Payload(), map[string]interface{}{
		"topic":     msg.Topic(),
		"qos":       msg.Qos(),
		"messageId": msg.MessageID(),
	}, rcvTime)
}

func (ms *SourceConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing mqtt source connector to topic %s.", ms.tpc)
	if ms.cli != nil {
		if len(ms.cfg.SelId) < 1 {
			id := fmt.Sprintf("%s-%s-%s-mqtt-source", ctx.GetRuleId(), ctx.GetOpId(), ms.tpc)
			connection.DropNonStoredConnection(ctx, id)
		} else {
			ms.cli.DetachSub(ctx, ms.props)
		}
	}
	return nil
}

func GetSource() api.Source {
	return &SourceConnector{}
}

var _ api.BytesSource = &SourceConnector{}
