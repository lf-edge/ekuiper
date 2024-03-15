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
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// SourceConnector is the connector for mqtt source
// When sharing the same connection, each topic will have one single sourceConnector as the shared source node
type SourceConnector struct {
	tpc   string
	cfg   *Conf
	props map[string]any

	cli      *Connection
	consumer chan<- api.SourceTuple
	stats    metric.StatManager
}

type Conf struct {
	Qos       int `json:"qos"`
	BufferLen int `json:"bufferLength"`
}

func (ms *SourceConnector) SetupStats(stats metric.StatManager) {
	ms.stats = stats
}

func (ms *SourceConnector) Configure(topic string, props map[string]any) error {
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	cfg := &Conf{
		BufferLen: 10240,
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	_, err = validateConfig(props)
	if err != nil {
		return err
	}
	ms.props = props
	ms.cfg = cfg
	ms.tpc = topic
	return nil
}

func (ms *SourceConnector) Ping(dataSource string, props map[string]interface{}) error {
	if err := ms.Configure(dataSource, props); err != nil {
		return err
	}
	cli, err := CreateClient(context.Background(), "", ms.props)
	if err != nil {
		return err
	}
	defer cli.Close()
	return cli.Ping()
}

func (ms *SourceConnector) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Connecting to mqtt server")
	cli, err := GetConnection(ctx, ms.props)
	ms.cli = cli
	return err
}

// Subscribe is a one time only operation for source. It connects to the mqtt broker and subscribe to the topic
// Run open before subscribe
func (ms *SourceConnector) Subscribe(ctx api.StreamContext) error {
	return ms.cli.Subscribe(ms.tpc, &SubscriptionInfo{
		Qos: byte(ms.cfg.Qos),
		Handler: func(client pahoMqtt.Client, message pahoMqtt.Message) {
			ms.onMessage(ctx, message)
		},
		ErrHandler: func(err error) {
			ms.onError(ctx, err)
		},
	})
}

func (ms *SourceConnector) onMessage(ctx api.StreamContext, msg pahoMqtt.Message) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	failpoint.Inject("ctxCancel", func(val failpoint.Value) {
		if val.(bool) {
			panic("shouldn't run")
		}
	})

	if ms.consumer == nil {
		// The consumer is closed, no need to process the message
		ctx.GetLogger().Debugf("The consumer is closed, skip to process the message %s from topic %s", string(msg.Payload()), msg.Topic())
		return
	}
	if msg != nil {
		ctx.GetLogger().Debugf("Received message %s from topic %s", string(msg.Payload()), msg.Topic())
	}
	rcvTime := conf.GetNow()
	select {
	case ms.consumer <- api.NewDefaultRawTuple(msg.Payload(), map[string]interface{}{
		"topic":     msg.Topic(),
		"qos":       msg.Qos(),
		"messageId": msg.MessageID(),
	}, rcvTime):
	default:
		ms.stats.IncTotalExceptions("buffer full from mqtt connector, drop message")
	}
}

func (ms *SourceConnector) onError(ctx api.StreamContext, err error) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	failpoint.Inject("ctxCancel", func(val failpoint.Value) {
		if val.(bool) {
			panic("shouldn't run")
		}
	})

	if ms.consumer == nil {
		// The consumer is closed, no need to process the message
		ctx.GetLogger().Debugf("The consumer is closed, skip to send the error")
		return
	}
	select {
	case ms.consumer <- &xsql.ErrorSourceTuple{
		Error: err,
	}:
	default:
		ms.stats.IncTotalExceptions("buffer full from mqtt connector, drop err")
	}
}

// Open is a continuous process, it keeps reading data from mqtt broker. It starts a go routine to read data and send to consumer channel
// Run open then subscribe
func (ms *SourceConnector) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ctx.GetLogger().Infof("Open connector reader")
	ms.consumer = consumer
}

func (ms *SourceConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing mqtt source connector to topic %s.", ms.tpc)
	if ms.cli != nil {
		DetachConnection(ms.cli.GetClientId(), ms.tpc)
		ms.cli = nil
	}
	return nil
}

var _ api.SourceConnector = &SourceConnector{}
