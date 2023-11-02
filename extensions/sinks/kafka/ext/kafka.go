// Copyright 2023-2023 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"strings"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/lf-edge/ekuiper/extensions/kafka"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type kafkaSink struct {
	writer         *kafkago.Writer
	c              *sinkConf
	kc             *kafkaConf
	tc             *kafka.TLSConf
	sc             kafka.SaslConf
	headersMap     map[string]string
	headerTemplate string
}

type sinkConf struct {
	Brokers string `json:"brokers"`
	Topic   string `json:"topic"`
}

type kafkaConf struct {
	MaxAttempts int         `json:"maxAttempts"`
	Key         string      `json:"key"`
	Headers     interface{} `json:"headers"`
}

func (m *kafkaSink) Configure(props map[string]interface{}) error {
	c := &sinkConf{
		Brokers: "localhost:9092",
		Topic:   "",
	}
	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}
	if len(strings.Split(c.Brokers, ",")) == 0 {
		return fmt.Errorf("brokers can not be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic can not be empty")
	}
	sc, err := kafka.GetSaslConf(props)
	if err != nil {
		return err
	}
	if err := sc.Validate(); err != nil {
		return err
	}
	m.sc = sc
	tc, err := kafka.GenTLSConf(props)
	if err != nil {
		return err
	}

	if err := cast.MapToStruct(props, tc); err != nil {
		return err
	}
	kc := &kafkaConf{
		MaxAttempts: 1,
	}
	if err := cast.MapToStruct(props, kc); err != nil {
		return err
	}
	m.kc = kc
	m.tc = tc
	m.c = c
	if err := m.setHeaders(); err != nil {
		return fmt.Errorf("set kafka header failed, err:%v", err)
	}
	m.tc.TlsConfigLog("sink")
	return nil
}

func (m *kafkaSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debug("Opening kafka sink")
	mechanism, err := m.sc.GetMechanism()
	if err != nil {
		return err
	}
	brokers := strings.Split(m.c.Brokers, ",")
	tlsConfig, err := m.tc.GetTlsConfig()
	if err != nil {
		conf.Log.Errorf("setting kafka tls config failed,err: %v", err)
		return err
	}
	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  m.c.Topic,
		Balancer:               &kafkago.LeastBytes{},
		Async:                  false,
		AllowAutoTopicCreation: true,
		MaxAttempts:            m.kc.MaxAttempts,
		RequiredAcks:           -1,
		BatchSize:              1,
		Transport: &kafkago.Transport{
			SASL: mechanism,
			TLS:  tlsConfig,
		},
	}
	m.writer = w
	return nil
}

func (m *kafkaSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	logger.Debugf("kafka sink receive %s", item)
	var messages []kafkago.Message
	switch d := item.(type) {
	case []map[string]interface{}:
		decodedBytes, _, err := ctx.TransformOutput(d)
		if err != nil {
			return fmt.Errorf("kafka sink transform data error: %v", err)
		}
		msg, err := m.buildMsg(ctx, item, decodedBytes)
		if err != nil {
			conf.Log.Errorf("build kafka msg failed, err:%v", err)
			return err
		}
		messages = append(messages, msg)
	case map[string]interface{}:
		decodedBytes, _, err := ctx.TransformOutput(d)
		if err != nil {
			return fmt.Errorf("kafka sink transform data error: %v", err)
		}
		msg, err := m.buildMsg(ctx, item, decodedBytes)
		if err != nil {
			conf.Log.Errorf("build kafka msg failed, err:%v", err)
			return err
		}
		messages = append(messages, msg)
	default:
		return fmt.Errorf("unrecognized format of %s", item)
	}
	err := m.writer.WriteMessages(ctx, messages...)
	if err != nil {
		conf.Log.Errorf("kafka sink error: %v", err)
	} else {
		conf.Log.Debug("sink kafka success")
	}
	switch err := err.(type) {
	case kafkago.Error:
		if err.Temporary() {
			return fmt.Errorf(`%s: kafka sink fails to send out the data . %v`, errorx.IOErr, err)
		} else {
			return err
		}
	case kafkago.WriteErrors:
		count := 0
		for i := range messages {
			switch err := err[i].(type) {
			case nil:
				continue

			case kafkago.Error:
				if err.Temporary() {
					count++
					continue
				}
			default:
				if strings.Contains(err.Error(), "kafka.(*Client).Produce:") {
					return fmt.Errorf(`%s: kafka sink fails to send out the data . %v`, errorx.IOErr, err)
				}
			}
		}
		if count > 0 {
			return fmt.Errorf(`%s: kafka sink fails to send out the data . %v`, errorx.IOErr, err)
		} else {
			return err
		}
	case nil:
		return nil
	default:
		return fmt.Errorf(`%s: kafka sink fails to send out the data: %v`, errorx.IOErr, err)
	}
}

func (m *kafkaSink) Close(ctx api.StreamContext) error {
	return m.writer.Close()
}

func GetSink() api.Sink {
	return &kafkaSink{}
}

func (m *kafkaSink) buildMsg(ctx api.StreamContext, item interface{}, decodedBytes []byte) (kafkago.Message, error) {
	msg := kafkago.Message{Value: decodedBytes}
	if len(m.kc.Key) > 0 {
		newKey, err := ctx.ParseTemplate(m.kc.Key, item)
		if err != nil {
			return kafkago.Message{}, fmt.Errorf("parse kafka key error: %v", err)
		}
		msg.Key = []byte(newKey)
	}
	headers, err := m.parseHeaders(ctx, item)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("parse kafka headers error: %v", err)
	}
	msg.Headers = headers
	return msg, nil
}

func (m *kafkaSink) setHeaders() error {
	if m.kc.Headers == nil {
		return nil
	}
	switch h := m.kc.Headers.(type) {
	case map[string]interface{}:
		kafkaHeaders := make(map[string]string)
		for key, value := range h {
			if sv, ok := value.(string); ok {
				kafkaHeaders[key] = sv
			}
		}
		m.headersMap = kafkaHeaders
		return nil
	case string:
		m.headerTemplate = h
		return nil
	default:
		return fmt.Errorf("kafka headers must be a map[string]string or a string")
	}
}

func (m *kafkaSink) parseHeaders(ctx api.StreamContext, data interface{}) ([]kafkago.Header, error) {
	if len(m.headersMap) > 0 {
		var kafkaHeaders []kafkago.Header
		for k, v := range m.headersMap {
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
	} else if len(m.headerTemplate) > 0 {
		headers := make(map[string]string)
		s, err := ctx.ParseTemplate(m.headerTemplate, data)
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
