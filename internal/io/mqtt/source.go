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

	cli connection.Connection
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

func (ms *SourceConnector) Ping(props map[string]interface{}) error {
	cli, err := client.CreateClient(context.Background(), "", props)
	if err != nil {
		return err
	}
	defer cli.Close()
	return cli.Ping()
}

func (ms *SourceConnector) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Connecting to mqtt server")
	var cli connection.Connection
	var err error
	if len(ms.cfg.SelId) > 0 {
		cli, err = connection.GetNameConnection(ms.cfg.SelId)
	} else {
		cli, err = client.CreateAnonymousConnection(ctx, ms.props)
	}
	ms.cli = cli
	return err
}

// Subscribe is a one time only operation for source. It connects to the mqtt broker and subscribe to the topic
// Run open before subscribe
func (ms *SourceConnector) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	return ms.cli.Subscribe(ctx, ms.props, ingest, ingestError)
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
			ms.cli.Close()
		} else {
			ms.cli.DetachSub(ms.props)
		}
	}
	return nil
}

func GetSource() api.Source {
	return &SourceConnector{}
}

var _ api.BytesSource = &SourceConnector{}