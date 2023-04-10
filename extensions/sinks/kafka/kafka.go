// Copyright 2023 carlclone@gmail.com
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

package main

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"strings"
	"time"
)

type kafkaSink struct {
	writer *kafkago.Writer
	c      *sinkConf
}

const (
	AT_MOST_ONCE  = "AT_MOST_ONCE"
	AT_LEAST_ONCE = "AT_LEAST_ONCE"

	SASL_NONE  = "none"
	SASL_PLAIN = "plain"
	SASL_SCRAM = "scram"
)

type sinkConf struct {
	Brokers           string `json:"brokers"`
	Topic             string `json:"topic"`
	DeliveryGuarantee string `json:"deliveryGuarantee"`
	SaslAuthType      string `json:"saslAuthType"`
	SaslUserName      string `json:"saslUserName"`
	SaslPassword      string `json:"saslPassword"`
}

func (m *kafkaSink) Configure(props map[string]interface{}) error {
	c := &sinkConf{
		Brokers:           "localhost:9092",
		Topic:             "",
		DeliveryGuarantee: AT_LEAST_ONCE,
		SaslAuthType:      SASL_NONE,
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
	if !(c.DeliveryGuarantee == AT_MOST_ONCE || c.DeliveryGuarantee == AT_LEAST_ONCE) {
		return fmt.Errorf("deliveryGuarantee incorrect")
	}
	if !(c.SaslAuthType == SASL_NONE || c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) {
		return fmt.Errorf("saslAuthType incorrect")
	}
	if (c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) && (c.SaslUserName == "" || c.SaslPassword == "") {
		return fmt.Errorf("username and password can not be empty")
	}

	m.c = c
	return nil
}

func (m *kafkaSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debug("Opening kafka sink")
	var maxAttempts int
	var err error
	var mechanism sasl.Mechanism
	//delivery guarantee
	switch m.c.DeliveryGuarantee {
	case AT_MOST_ONCE:
		maxAttempts = 1
	case AT_LEAST_ONCE:
		maxAttempts = 10
	default:
		maxAttempts = 10
	}
	//sasl authentication type
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
	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  m.c.Topic,
		Balancer:               &kafkago.LeastBytes{},
		Async:                  false,
		AllowAutoTopicCreation: true,
		MaxAttempts:            maxAttempts,
		RequiredAcks:           -1,
		Transport: &kafkago.Transport{
			SASL: mechanism,
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
		for _, el := range d {
			decodedBytes, _, err := ctx.TransformOutput(el)
			if err != nil {
				return fmt.Errorf("kafka sink transform data error: %v", err)
			}
			messages = append(messages, kafkago.Message{Value: decodedBytes})
		}
	case map[string]interface{}:
		decodedBytes, _, err := ctx.TransformOutput(d)
		if err != nil {
			return fmt.Errorf("kafka sink transform data error: %v", err)
		}
		messages = append(messages, kafkago.Message{Value: decodedBytes})
	default:
		return fmt.Errorf("unrecognized format of %s", item)
	}

	vctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err := m.kafkaWriteWithBackoff(vctx, ctx.GetLogger().Errorf, 100*time.Millisecond, time.Second, messages...)
	if err != nil {
		return err
	}
	logger.Debug("insert data into kafka success")
	return nil
}

func (m *kafkaSink) kafkaWriteWithBackoff(ctx context.Context, log func(string, ...interface{}), interval, maxInterval time.Duration, messages ...kafkago.Message) error {
	var berr error
	tries := 0

retry:
	for {
		tries++

		err := m.writer.WriteMessages(ctx, messages...)
		switch err := err.(type) {
		case nil:
			return nil

		case kafkago.Error:
			berr = err
			if !err.Temporary() {
				break retry
			}

		case kafkago.WriteErrors:
			if m.c.DeliveryGuarantee == AT_MOST_ONCE {
				return err
			}

			var remaining []kafkago.Message
			for i, m := range messages {
				switch err := err[i].(type) {
				case nil:
					continue

				case kafkago.Error:
					if err.Temporary() {
						remaining = append(remaining, m)
						continue
					}
				}

				return fmt.Errorf("failed to deliver messages: %v", err)
			}

			messages = remaining
			berr = err

		default:
			if berr == nil || err != context.DeadlineExceeded {
				berr = err
			}
			break retry
		}

		log("temporary write error: %v", err)

		interval *= 2
		if interval > maxInterval {
			interval = maxInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			break retry
		}
	}

	return fmt.Errorf("failed to deliver messages after %d tries: %v", tries, berr)
}

func (m *kafkaSink) Close(ctx api.StreamContext) error {
	return m.writer.Close()
}

func Kafka() api.Sink {
	return &kafkaSink{}
}
