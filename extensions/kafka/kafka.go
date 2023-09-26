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

package kafka

import (
	"fmt"
	"strings"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/lf-edge/ekuiper/pkg/cast"
)

const (
	SASL_NONE  = "none"
	SASL_PLAIN = "plain"
	SASL_SCRAM = "scram"
)

func GenKafkaConf(props map[string]interface{}) (*KafkaConf, error) {
	conf := &KafkaConf{}
	err := cast.MapToStruct(props, conf)
	if err != nil {
		return nil, fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	return conf, nil
}

type KafkaConf struct {
	Brokers   string `json:"brokers"`
	Topic     string `json:"topic"`
	GroupID   string `json:"groupID"`
	Partition int    `json:"partition"`
	MaxBytes  int    `json:"maxBytes"`
	Offset    int64  `json:"offset"`
}

func (c *KafkaConf) ValidateSinkConf() error {
	if len(strings.Split(c.Brokers, ",")) == 0 {
		return fmt.Errorf("brokers can not be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic can not be empty")
	}
	return nil
}

func (c *KafkaConf) GetReaderConfig(topic string) kafkago.ReaderConfig {
	return kafkago.ReaderConfig{
		Brokers:   strings.Split(c.Brokers, ","),
		GroupID:   c.GroupID,
		Topic:     topic,
		Partition: c.Partition,
		MaxBytes:  c.MaxBytes,
	}
}

func (c *KafkaConf) ValidateSourceConf() error {
	if len(strings.Split(c.Brokers, ",")) == 0 {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func GenSaslConf(props map[string]interface{}) (*SaslConf, error) {
	conf := &SaslConf{}
	if err := cast.MapToStruct(props, conf); err != nil {
		return nil, err
	}
	return conf, nil
}

type SaslConf struct {
	SaslAuthType string `json:"saslAuthType"`
	SaslUserName string `json:"saslUserName"`
	SaslPassword string `json:"saslPassword"`
}

func (c *SaslConf) Validate() error {
	if !(c.SaslAuthType == SASL_NONE || c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) {
		return fmt.Errorf("saslAuthType incorrect")
	}
	if (c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) && (c.SaslUserName == "" || c.SaslPassword == "") {
		return fmt.Errorf("username and password can not be empty")
	}
	return nil
}
