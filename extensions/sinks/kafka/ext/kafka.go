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
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type kafkaSink struct {
	writer         *kafkago.Writer
	c              *sinkConf
	tc             *tlsConf
	kc             *kafkaConf
	headers        []kafkago.Header
	headerTemplate string
}

const (
	SASL_NONE  = "none"
	SASL_PLAIN = "plain"
	SASL_SCRAM = "scram"
)

type sinkConf struct {
	Brokers      string `json:"brokers"`
	Topic        string `json:"topic"`
	SaslAuthType string `json:"saslAuthType"`
	SaslUserName string `json:"saslUserName"`
	SaslPassword string `json:"saslPassword"`
}

type tlsConf struct {
	InsecureSkipVerify   bool   `json:"insecureSkipVerify"`
	CertificationPath    string `json:"certificationPath"`
	PrivateKeyPath       string `json:"privateKeyPath"`
	RootCaPath           string `json:"rootCaPath"`
	TLSMinVersion        string `json:"tlsMinVersion"`
	RenegotiationSupport string `json:"renegotiationSupport"`
}

type kafkaConf struct {
	MaxAttempts int         `json:"maxAttempts"`
	BatchSize   int         `json:"batchSize"`
	Key         string      `json:"key"`
	Headers     interface{} `json:"headers"`
}

func (m *kafkaSink) Configure(props map[string]interface{}) error {
	c := &sinkConf{
		Brokers:      "localhost:9092",
		Topic:        "",
		SaslAuthType: SASL_NONE,
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
	if !(c.SaslAuthType == SASL_NONE || c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) {
		return fmt.Errorf("saslAuthType incorrect")
	}
	if (c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) && (c.SaslUserName == "" || c.SaslPassword == "") {
		return fmt.Errorf("username and password can not be empty")
	}
	tc := &tlsConf{}
	if err := cast.MapToStruct(props, tc); err != nil {
		return err
	}
	kc := &kafkaConf{
		MaxAttempts: 1,
		BatchSize:   1,
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
	return nil
}

func (m *kafkaSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debug("Opening kafka sink")

	var err error
	var mechanism sasl.Mechanism

	// sasl authentication type
	switch m.c.SaslAuthType {
	case SASL_PLAIN:
		mechanism = plain.Mechanism{
			Username: m.c.SaslUserName,
			Password: m.c.SaslPassword,
		}
	case SASL_SCRAM:
		mechanism, err = scram.Mechanism(scram.SHA512, m.c.SaslUserName, m.c.SaslPassword)
		if err != nil {
			return err
		}
	default:
		mechanism = nil
	}
	brokers := strings.Split(m.c.Brokers, ",")
	tlsConfig, err := cert.GenerateTLSForClient(cert.TlsConfigurationOptions{
		SkipCertVerify:       m.tc.InsecureSkipVerify,
		CertFile:             m.tc.CertificationPath,
		KeyFile:              m.tc.PrivateKeyPath,
		CaFile:               m.tc.RootCaPath,
		TLSMinVersion:        m.tc.TLSMinVersion,
		RenegotiationSupport: m.tc.RenegotiationSupport,
	})
	if err != nil {
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
		BatchSize:              m.kc.BatchSize,
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
			}
		}
		if count == len(messages) {
			return fmt.Errorf(`%s: kafka sink fails to send out the data . %v`, errorx.IOErr, err)
		}
	}
	return err
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
		msg.Key = []byte(m.kc.Key)
	}
	if len(m.headers) > 0 {
		msg.Headers = m.headers
	} else if len(m.headerTemplate) > 0 {
		headers, err := parseHeaders(ctx, item, m.headerTemplate)
		if err != nil {
			return kafkago.Message{}, fmt.Errorf("parse kafka headers error: %v", err)
		}
		msg.Headers = headers
	}
	return msg, nil
}

func (m *kafkaSink) setHeaders() error {
	if m.kc.Headers == nil {
		return nil
	}
	switch h := m.kc.Headers.(type) {
	case map[string]interface{}:
		var kafkaHeaders []kafkago.Header
		for key, value := range h {
			kafkaHeaders = append(kafkaHeaders, kafkago.Header{
				Key:   key,
				Value: []byte(value.(string)),
			})
		}
		m.headers = kafkaHeaders
		return nil
	case string:
		m.headerTemplate = h
		return nil
	default:
		return fmt.Errorf("kafka headers must be a map or a string")
	}
}

func parseHeaders(ctx api.StreamContext, data interface{}, headerTemplate string) ([]kafkago.Header, error) {
	headers := make(map[string]string)
	s, err := ctx.ParseTemplate(headerTemplate, data)
	if err != nil {
		return nil, err
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
