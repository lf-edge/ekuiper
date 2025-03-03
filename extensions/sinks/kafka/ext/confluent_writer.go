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
	"strconv"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type ConfluentWriter struct {
	enableSSL  bool
	enableSASL bool
	producer   *confluentkafka.Producer
	messages   []*confluentkafka.Message
}

func (c *ConfluentWriter) Configure(props map[string]interface{}, m *kafkaSink) error {
	if err := m.configureProps(props); err != nil {
		return err
	}
	config := &confluentkafka.ConfigMap{
		"bootstrap.servers":                     m.c.Brokers,
		"acks":                                  strconv.FormatInt(int64(m.kc.RequiredACKs), 10),
		"enable.idempotence":                    false,
		"max.in.flight.requests.per.connection": 1,
		"linger.ms":                             100,
		"queue.buffering.max.messages":          100000,
		"queue.buffering.max.kbytes":            1048576,
		"batch.num.messages":                    m.kc.BatchSize,
		"batch.size":                            m.kc.BatchBytes,
		"compression.type":                      getCompression(m),
		"retries":                               m.kc.MaxAttempts,
	}
	if err := c.setupSasl(m, config); err != nil {
		return err
	}
	if err := c.setupTLS(props, m, config); err != nil {
		return err
	}
	if c.enableSSL && c.enableSASL {
		if err := config.SetKey("security.protocol", "sasl_ssl"); err != nil {
			return err
		}
	}
	producer, err := confluentkafka.NewProducer(config)
	if err != nil {
		return err
	}
	c.producer = producer
	return nil
}

// We don't use this client to ping kafka server
func (c *ConfluentWriter) Ping(props map[string]interface{}, m *kafkaSink) error {
	return nil
}

// TODO: support it later
func (c *ConfluentWriter) SetupMeta(ctx api.StreamContext) error {
	return nil
}

func (c *ConfluentWriter) CollectMessages(ctx api.StreamContext, msgs []map[string]interface{}, m *kafkaSink) error {
	messages := make([]*confluentkafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		mesg, err := c.buildMsg(ctx, msg, m)
		if err != nil {
			return err
		}
		messages = append(messages, mesg)
	}
	c.messages = messages
	return nil
}

func (c *ConfluentWriter) buildMsg(ctx api.StreamContext, msg map[string]interface{}, m *kafkaSink) (*confluentkafka.Message, error) {
	topic := m.c.Topic
	decodedBytes, err := m.transform(ctx, msg)
	if err != nil {
		return nil, err
	}
	message := &confluentkafka.Message{
		TopicPartition: confluentkafka.TopicPartition{Topic: &topic, Partition: confluentkafka.PartitionAny},
		Value:          decodedBytes,
	}
	newKey, err := ctx.ParseTemplate(m.kc.Key, msg)
	if err != nil {
		return nil, err
	}
	message.Key = []byte(newKey)
	if err := c.parseHeader(ctx, msg, m, message); err != nil {
		return nil, err
	}
	return message, nil
}

func (c *ConfluentWriter) parseHeader(ctx api.StreamContext, data map[string]interface{}, m *kafkaSink, msg *confluentkafka.Message) error {
	var kafkaHeaders []confluentkafka.Header
	if len(m.headersMap) > 0 {
		var kafkaHeaders []confluentkafka.Header
		for k, v := range m.headersMap {
			value, err := ctx.ParseTemplate(v, data)
			if err != nil {
				return fmt.Errorf("parse kafka header map failed, err:%v", err)
			}
			kafkaHeaders = append(kafkaHeaders, confluentkafka.Header{
				Key:   k,
				Value: []byte(value),
			})
		}
	} else if len(m.headerTemplate) > 0 {
		headers := make(map[string]string)
		s, err := ctx.ParseTemplate(m.headerTemplate, data)
		if err != nil {
			return fmt.Errorf("parse kafka header template failed, err:%v", err)
		}
		if err := json.Unmarshal([]byte(s), &headers); err != nil {
			return err
		}
		for key, value := range headers {
			kafkaHeaders = append(kafkaHeaders, confluentkafka.Header{
				Key:   key,
				Value: []byte(value),
			})
		}
	}
	msg.Headers = kafkaHeaders
	return nil
}

func (c *ConfluentWriter) SendMessages(ctx api.StreamContext, m *kafkaSink) error {
	for _, msg := range c.messages {
		if err := c.producer.Produce(msg, nil); err != nil {
			return err
		}
	}
	c.producer.Flush(1000)
	return nil
}

func (c *ConfluentWriter) Close(ctx api.StreamContext) error {
	c.producer.Close()
	return nil
}

func getCompression(m *kafkaSink) string {
	if len(m.kc.Compression) < 1 {
		return "none"
	}
	return m.kc.Compression
}

func (c *ConfluentWriter) setupSasl(m *kafkaSink, config *confluentkafka.ConfigMap) error {
	if len(m.sc.SaslAuthType) > 0 {
		c.enableSASL = true
		if err := config.Set("security.protocol=sasl_plaintext"); err != nil {
			return err
		}
		if err := config.Set(fmt.Sprintf("sasl.mechanisms=%v", m.sc.SaslAuthType)); err != nil {
			return err
		}
		if err := config.Set(fmt.Sprintf("sasl.username=%v", m.sc.SaslUserName)); err != nil {
			return err
		}
		if err := config.Set(fmt.Sprintf("sasl.password=%v", m.sc.SaslPassword)); err != nil {
			return err
		}
	}
	return nil
}

func (c *ConfluentWriter) setupTLS(props map[string]interface{}, m *kafkaSink, config *confluentkafka.ConfigMap) error {
	if m.tlsConfig != nil {
		c.enableSSL = true
		opt, err := cert.GenTlsConfigurationOptions(props)
		if err != nil {
			return err
		}
		if err := config.Set("security.protocol=ssl"); err != nil {
			return err
		}
		if len(opt.CaFile) > 0 {
			if err := config.Set(fmt.Sprintf("ssl.ca.location=%v", opt.CaFile)); err != nil {
				return err
			}
		}
		if len(opt.KeyFile) > 0 {
			if err := config.Set("ssl.key.location"); err != nil {
				return err
			}
		}
		if len(opt.CertFile) > 0 {
			if err := config.Set(fmt.Sprintf("ssl.certificate.location=%v", opt.CertFile)); err != nil {
				return err
			}
		}
		if len(opt.RootCARaw) > 0 {
			if err := config.Set(fmt.Sprintf("ssl.ca.pem=%v", opt.RootCARaw)); err != nil {
				return err
			}
		}
		if len(opt.KeyFile) > 0 {
			if err := config.Set(fmt.Sprintf("ssl.key.pem=%v", opt.PrivateKeyRaw)); err != nil {
				return err
			}
		}
		if len(opt.CertificationRaw) > 0 {
			if err := config.Set(fmt.Sprintf("ssl.certificate.pem=%v", opt.CertificationRaw)); err != nil {
				return err
			}
		}
	}
	return nil
}
