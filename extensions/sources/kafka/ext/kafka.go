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
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/lf-edge/ekuiper/extensions/kafka"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type KafkaSource struct {
	reader    *kafkago.Reader
	offset    int64
	tlsConfig *tls.Config
	sc        *kafkaSourceConf
}

type kafkaSourceConf struct {
	Brokers     string `json:"brokers"`
	Topic       string `json:"topic"`
	GroupID     string `json:"groupID"`
	Partition   int    `json:"partition"`
	MaxAttempts int    `json:"maxAttempts"`
	MaxBytes    int    `json:"maxBytes"`
}

func (s *KafkaSource) Ping(d string, props map[string]interface{}) error {
	if err := s.Configure(d, props); err != nil {
		return err
	}
	for _, broker := range strings.Split(s.sc.Brokers, ",") {
		err := ping(s.tlsConfig, broker)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *kafkaSourceConf) validate() error {
	if len(strings.Split(c.Brokers, ",")) == 0 {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func (c *kafkaSourceConf) GetReaderConfig(topic string) kafkago.ReaderConfig {
	return kafkago.ReaderConfig{
		Brokers:     strings.Split(c.Brokers, ","),
		GroupID:     c.GroupID,
		Topic:       topic,
		Partition:   c.Partition,
		MaxBytes:    c.MaxBytes,
		MaxAttempts: c.MaxAttempts,
	}
}

func getSourceConf(props map[string]interface{}) (*kafkaSourceConf, error) {
	c := &kafkaSourceConf{
		MaxBytes:    1e6,
		MaxAttempts: 3,
	}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	return c, nil
}

func (s *KafkaSource) Configure(topic string, props map[string]interface{}) error {
	if len(topic) < 1 {
		conf.Log.Error("DataSource which indicates the topic should be defined")
		return fmt.Errorf("DataSource which indicates the topic should be defined")
	}
	kConf, err := getSourceConf(props)
	if err != nil {
		conf.Log.Errorf("kafka source config error: %v", err)
		return err
	}
	if err := kConf.validate(); err != nil {
		return err
	}
	tlsConfig, err := cert.GenTLSConfig(props, "kafka-source")
	if err != nil {
		conf.Log.Errorf("kafka tls conf error: %v", err)
		return err
	}
	saslConf, err := kafka.GetSaslConf(props)
	if err != nil {
		conf.Log.Errorf("kafka sasl error: %v", err)
		return err
	}
	if err := saslConf.Validate(); err != nil {
		conf.Log.Errorf("kafka validate sasl error: %v", err)
		return err
	}
	mechanism, err := saslConf.GetMechanism()
	if err != nil {
		conf.Log.Errorf("kafka sasl mechanism error: %v", err)
		return err
	}
	readerConfig := kConf.GetReaderConfig(topic)
	conf.Log.Infof("topic: %s, brokers: %v", readerConfig.Topic, readerConfig.Brokers)
	readerConfig.Dialer = &kafkago.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
	reader := kafkago.NewReader(readerConfig)
	s.reader = reader
	s.sc = kConf
	if err := s.reader.SetOffset(kafkago.LastOffset); err != nil {
		return err
	}
	conf.Log.Infof("kafka source got configured.")
	return nil
}

func (s *KafkaSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	defer s.reader.Close()
	logger := ctx.GetLogger()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := s.reader.ReadMessage(ctx)
		if err != nil {
			logger.Errorf("Recv kafka error %v", err)
			errCh <- err
			return
		}
		s.offset = msg.Offset
		dataList, err := ctx.DecodeIntoList(msg.Value)
		if err != nil {
			logger.Errorf("unmarshal kafka message value err: %v", err)
			errCh <- err
			return
		}
		for _, data := range dataList {
			rcvTime := conf.GetNow()
			consumer <- api.NewDefaultSourceTupleWithTime(data, nil, rcvTime)
		}
	}
}

func (s *KafkaSource) Close(_ api.StreamContext) error {
	return nil
}

func (s *KafkaSource) Rewind(offset interface{}) error {
	conf.Log.Infof("set kafka source offset: %v", offset)
	offsetV := s.offset //nolint:staticcheck
	switch v := offset.(type) {
	case int64:
		offsetV = v
	case int:
		offsetV = int64(v)
	case float64:
		offsetV = int64(v)
	default:
		return fmt.Errorf("%v can't be set as offset", offset)
	}
	if err := s.reader.SetOffset(offsetV); err != nil {
		conf.Log.Errorf("kafka offset error: %v", err)
		return fmt.Errorf("set kafka offset failed, err:%v", err)
	}
	return nil
}

func (s *KafkaSource) ResetOffset(input map[string]interface{}) error {
	return errors.New("kafka source not support reset offset")
}

func (s *KafkaSource) GetOffset() (interface{}, error) {
	return s.offset, nil
}

func GetSource() api.Source {
	return &KafkaSource{}
}

func ping(tlsConfig *tls.Config, address string) error {
	d := &kafkago.Dialer{TLS: tlsConfig}
	c, err := d.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer c.Close()
	return nil
}
