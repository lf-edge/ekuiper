// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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

package kafka

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type kafkaConnectionConf struct {
	Brokers string `json:"brokers"`

	tlsConfig *tls.Config
	mechanism sasl.Mechanism
}

func newKafkaConnectionConf(ctx api.StreamContext, props map[string]any) (*kafkaConnectionConf, error) {
	c := &kafkaConnectionConf{}
	if err := cast.MapToStruct(props, c); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	tlsConfig, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	saslConf, err := getSaslConf(props)
	if err != nil {
		return nil, err
	}
	if err := saslConf.Validate(); err != nil {
		return nil, err
	}
	mechanism, err := saslConf.GetMechanism()
	if err != nil {
		return nil, err
	}
	c.tlsConfig = tlsConfig
	c.mechanism = mechanism
	return c, nil
}

func (c *kafkaConnectionConf) validate() error {
	if strings.TrimSpace(c.Brokers) == "" {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func (c *kafkaConnectionConf) ping() error {
	hasBroker := false
	for _, broker := range strings.Split(c.Brokers, ",") {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}
		hasBroker = true
		if err := c.pingBroker(broker); err != nil {
			return err
		}
	}
	if !hasBroker {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func (c *kafkaConnectionConf) pingBroker(address string) error {
	d := &kafkago.Dialer{
		TLS:           c.tlsConfig,
		SASLMechanism: c.mechanism,
	}
	conn, err := d.Dial("tcp", address)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("found error when connecting to kafka broker %s: %s", address, err))
	}
	return conn.Close()
}

type KafkaConnection struct {
	id   string
	conf *kafkaConnectionConf
}

func init() {
	modules.RegisterConnection("kafka", CreateConnection)
}

func CreateConnection(_ api.StreamContext) modules.Connection {
	return &KafkaConnection{}
}

func (k *KafkaConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	c, err := newKafkaConnectionConf(ctx, props)
	if err != nil {
		return err
	}
	k.id = conId
	k.conf = c
	return nil
}

func (k *KafkaConnection) Dial(ctx api.StreamContext) error {
	return k.Ping(ctx)
}

func (k *KafkaConnection) GetId(_ api.StreamContext) string {
	return k.id
}

func (k *KafkaConnection) Ping(ctx api.StreamContext) error {
	if k.conf == nil {
		return fmt.Errorf("kafka connection is not provisioned")
	}
	return k.conf.ping()
}

func (k *KafkaConnection) Close(_ api.StreamContext) error {
	return nil
}

var _ modules.Connection = &KafkaConnection{}
