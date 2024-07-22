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

package kafka

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/failpoint"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
)

type KafkaSink struct {
	writer         *kafkago.Writer
	kc             *kafkaConf
	tlsConfig      *tls.Config
	headersMap     map[string]string
	headerTemplate string
}

type kafkaConf struct {
	Brokers      string      `json:"brokers"`
	Topic        string      `json:"topic"`
	MaxAttempts  int         `json:"maxAttempts"`
	RequiredACKs int         `json:"requiredACKs"`
	Key          string      `json:"key"`
	Headers      interface{} `json:"headers"`
}

func (c *kafkaConf) validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic can not be empty")
	}
	if len(c.Brokers) < 1 {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func (k *KafkaSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	c := &kafkaConf{
		RequiredACKs: -1,
		MaxAttempts:  1,
	}
	err := cast.MapToStruct(configs, c)
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), castConfErr)
	})
	if err != nil {
		return err
	}
	err = c.validate()
	if err != nil {
		return err
	}
	sc, err := getSaslConf(configs)
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), saslConfErr)
	})
	if err != nil {
		return err
	}
	if err := sc.Validate(); err != nil {
		return err
	}
	tlsConfig, err := cert.GenTLSConfig(configs, "kafka-sink")
	if err != nil {
		return err
	}
	k.tlsConfig = tlsConfig
	k.kc = c
	err = k.setHeaders()
	if err != nil {
		return err
	}
	return k.buildKafkaWriter(sc)
}

func (k *KafkaSink) buildKafkaWriter(sc *saslConf) error {
	mechanism, err := sc.GetMechanism()
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), mechanismErr)
	})
	if err != nil {
		return err
	}
	brokers := strings.Split(k.kc.Brokers, ",")
	w := &kafkago.Writer{
		Addr:  kafkago.TCP(brokers...),
		Topic: k.kc.Topic,
		// kafka java-client default balancer
		Balancer:               &kafkago.Murmur2Balancer{},
		Async:                  false,
		AllowAutoTopicCreation: true,
		MaxAttempts:            k.kc.MaxAttempts,
		RequiredAcks:           kafkago.RequiredAcks(k.kc.RequiredACKs),
		BatchSize:              1,
		Transport: &kafkago.Transport{
			SASL: mechanism,
			TLS:  k.tlsConfig,
		},
	}
	k.writer = w
	return nil
}

func (k *KafkaSink) Close(ctx api.StreamContext) error {
	return k.writer.Close()
}

func (k *KafkaSink) Connect(ctx api.StreamContext) error {
	for _, broker := range strings.Split(k.kc.Brokers, ",") {
		err := ping(k.tlsConfig, broker)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KafkaSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return k.collect(ctx, item.ToMap())
}

func (k *KafkaSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	for _, data := range items.ToMaps() {
		err := k.collect(ctx, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KafkaSink) collect(ctx api.StreamContext, data map[string]any) error {
	ds, err := json.Marshal(data)
	if err != nil {
		return err
	}
	var messages []kafkago.Message
	msg, err := k.buildMsg(ctx, data, ds)
	if err != nil {
		return err
	}
	messages = append(messages, msg)
	return k.writer.WriteMessages(ctx, messages...)
}

func (k *KafkaSink) buildMsg(ctx api.StreamContext, item interface{}, decodedBytes []byte) (kafkago.Message, error) {
	msg := kafkago.Message{Value: decodedBytes}
	if len(k.kc.Key) > 0 {
		newKey, err := ctx.ParseTemplate(k.kc.Key, item)
		if err != nil {
			return kafkago.Message{}, fmt.Errorf("parse kafka key error: %v", err)
		}
		msg.Key = []byte(newKey)
	}
	headers, err := k.parseHeaders(ctx, item)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("parse kafka headers error: %v", err)
	}
	msg.Headers = headers
	return msg, nil
}

func (k *KafkaSink) setHeaders() error {
	if k.kc.Headers == nil {
		return nil
	}
	switch h := k.kc.Headers.(type) {
	case map[string]interface{}:
		kafkaHeaders := make(map[string]string)
		for key, value := range h {
			if sv, ok := value.(string); ok {
				kafkaHeaders[key] = sv
			}
		}
		k.headersMap = kafkaHeaders
		return nil
	case string:
		k.headerTemplate = h
		return nil
	default:
		return fmt.Errorf("kafka headers must be a map[string]string or a string")
	}
}

func (k *KafkaSink) parseHeaders(ctx api.StreamContext, data interface{}) ([]kafkago.Header, error) {
	if len(k.headersMap) > 0 {
		var kafkaHeaders []kafkago.Header
		for k, v := range k.headersMap {
			value, err := ctx.ParseTemplate(v, data)
			if err != nil {
				return nil, fmt.Errorf("parse kafka header map failed, err:%v", err)
			}
			kafkaHeaders = append(kafkaHeaders, kafkago.Header{
				Key:   k,
				Value: []byte(value),
			})
		}
		return kafkaHeaders, nil
	} else if len(k.headerTemplate) > 0 {
		headers := make(map[string]string)
		s, err := ctx.ParseTemplate(k.headerTemplate, data)
		if err != nil {
			return nil, fmt.Errorf("parse kafka header template failed, err:%v", err)
		}
		if err := json.Unmarshal([]byte(s), &headers); err != nil {
			return nil, err
		}
		var kafkaHeaders []kafkago.Header
		for key, value := range headers {
			kafkaHeaders = append(kafkaHeaders, kafkago.Header{
				Key:   key,
				Value: []byte(value),
			})
		}
		return kafkaHeaders, nil
	}
	return nil, nil
}

func GetSink() api.Sink {
	return &KafkaSink{}
}

var _ api.TupleCollector = &KafkaSink{}
