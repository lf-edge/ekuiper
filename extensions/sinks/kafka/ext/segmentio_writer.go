// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/lf-edge/ekuiper/extensions/kafka"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/metrics"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type SegmentIOWriter struct {
	*kafkago.Writer
	messages []kafkago.Message
}

func (s *SegmentIOWriter) Configure(props map[string]interface{}, m *kafkaSink) error {
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
	tlsConfig, err := cert.GenTLSConfig(props, "kafka-sink")
	if err != nil {
		return err
	}
	m.tlsConfig = tlsConfig
	kc := getDefaultKafkaConf()
	if err := kc.configure(props); err != nil {
		return err
	}
	m.kc = kc
	m.c = c
	if err := m.setHeaders(); err != nil {
		return fmt.Errorf("set kafka header failed, err:%v", err)
	}
	w, err := s.buildSegmentIOWriter(m)
	if err != nil {
		return err
	}
	s.Writer = w
	return nil
}

func (s *SegmentIOWriter) Ping(props map[string]interface{}, m *kafkaSink) error {
	if err := s.Configure(props, m); err != nil {
		return err
	}
	for _, broker := range strings.Split(m.c.Brokers, ",") {
		err := s.segmentIOPing(broker, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SegmentIOWriter) SetupMeta(ctx api.StreamContext) error {
	s.Writer.RuleID = ctx.GetRuleId()
	s.Writer.OpID = ctx.GetOpId()
	return nil
}

func (s *SegmentIOWriter) CollectMessages(ctx api.StreamContext, msgs []map[string]interface{}, m *kafkaSink) error {
	messages := make([]kafkago.Message, 0, len(msgs))
	for _, msg := range msgs {
		decodedBytes, err := m.transform(ctx, msg)
		if err != nil {
			return err
		}
		kafkaMsg, err := s.buildMsg(ctx, msg, decodedBytes, m)
		if err != nil {
			conf.Log.Errorf("build kafka msg failed, err:%v", err)
			return err
		}
		messages = append(messages, kafkaMsg)
	}
	s.messages = messages
	return nil
}

func (s *SegmentIOWriter) SendMessages(ctx api.StreamContext, m *kafkaSink) error {
	err := s.Writer.WriteMessages(ctx, s.messages...)
	if err != nil {
		conf.Log.Errorf("kafka sink error: %v", err)
	}
	s.handleErr(ctx, err, len(s.messages))
	return err
}

func (s *SegmentIOWriter) Close(ctx api.StreamContext) error {
	return s.Writer.Close()
}

func (s *SegmentIOWriter) buildSegmentIOWriter(m *kafkaSink) (*kafkago.Writer, error) {
	mechanism, err := m.sc.GetMechanism()
	if err != nil {
		return nil, err
	}
	brokers := strings.Split(m.c.Brokers, ",")
	w := &kafkago.Writer{
		Addr:  kafkago.TCP(brokers...),
		Topic: m.c.Topic,
		// kafka java-client default balancer
		Balancer:               &kafkago.Murmur2Balancer{},
		Async:                  false,
		AllowAutoTopicCreation: true,
		MaxAttempts:            m.kc.MaxAttempts,
		RequiredAcks:           kafkago.RequiredAcks(m.kc.RequiredACKs),
		BatchSize:              m.kc.BatchSize,
		BatchBytes:             m.kc.BatchBytes,
		BatchTimeout:           m.kc.BatchTimeout,
		Transport: &kafkago.Transport{
			SASL: mechanism,
			TLS:  m.tlsConfig,
		},
		Compression: toCompression(m.kc.Compression),
	}
	conf.Log.Infof("kafka writer batchSize:%v, batchTimeout:%v", m.kc.BatchSize, m.kc.BatchTimeout.String())
	m.ResetStats()
	return w, nil
}

func (s *SegmentIOWriter) buildMsg(ctx api.StreamContext, item interface{}, decodedBytes []byte, m *kafkaSink) (kafkago.Message, error) {
	start := time.Now()
	defer func() {
		buildDuration := time.Since(start)
		m.LastCollectStats.TotalBuildMsgDuration += buildDuration
	}()
	msg := kafkago.Message{Value: decodedBytes}
	if len(m.kc.Key) > 0 {
		newKey, err := ctx.ParseTemplate(m.kc.Key, item)
		if err != nil {
			return kafkago.Message{}, fmt.Errorf("parse kafka key error: %v", err)
		}
		msg.Key = []byte(newKey)
	}
	headers, err := s.parseHeaders(ctx, item, m)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("parse kafka headers error: %v", err)
	}
	msg.Headers = headers
	return msg, nil
}

func (s *SegmentIOWriter) parseHeaders(ctx api.StreamContext, data interface{}, m *kafkaSink) ([]kafkago.Header, error) {
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

func (s *SegmentIOWriter) handleErr(ctx api.StreamContext, err error, count int) {
	if err == nil {
		KafkaSinkCounter.WithLabelValues(metrics.LblSuccess, LblMsg, ctx.GetRuleId(), ctx.GetOpId()).Add(float64(count))
		return
	}
	switch wErrors := err.(type) {
	case kafkago.WriteErrors:
		KafkaSinkCounter.WithLabelValues(metrics.LblException, LblMsg, ctx.GetRuleId(), ctx.GetOpId()).Add(float64(wErrors.Count()))
		KafkaSinkCounter.WithLabelValues(metrics.LblSuccess, LblMsg, ctx.GetRuleId(), ctx.GetOpId()).Add(float64(count - wErrors.Count()))
	default:
		KafkaSinkCounter.WithLabelValues(metrics.LblException, LblMsg, ctx.GetRuleId(), ctx.GetOpId()).Add(float64(count))
	}
}

func (s *SegmentIOWriter) segmentIOPing(address string, m *kafkaSink) error {
	mechanism, err := m.sc.GetMechanism()
	if err != nil {
		return err
	}
	d := &kafkago.Dialer{
		TLS:           m.tlsConfig,
		SASLMechanism: mechanism,
	}
	c, err := d.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer c.Close()
	return nil
}
