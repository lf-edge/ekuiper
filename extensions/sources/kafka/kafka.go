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

package main

import (
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/lf-edge/ekuiper/extensions/kafka"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type kafkaSource struct {
	reader *kafkago.Reader
}

func (s *kafkaSource) Configure(topic string, props map[string]interface{}) error {
	kConf, err := kafka.GenKafkaConf(props)
	if err != nil {
		return err
	}
	if err := kConf.ValidateSourceConf(); err != nil {
		return err
	}
	if len(topic) < 1 {
		return fmt.Errorf("DataSource which indicates the topic should be defined")
	}
	reader := kafkago.NewReader(kConf.GetReaderConfig(topic))
	if kConf.Offset != 0 {
		if err := reader.SetOffset(kConf.Offset); err != nil {
			return fmt.Errorf("set kafka offset failed, err:%v", err)
		}
	}
	s.reader = reader
	conf.Log.Infof("kafka source got configured.")
	return nil
}

func (s *kafkaSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			logger.Errorf("Recv kafka  error %v", err)
			errCh <- err
			return
		}
		if err := s.reader.CommitMessages(ctx, msg); err != nil {
			logger.Errorf("commit kafka  error %v", err)
			errCh <- err
			return
		}
		data := make(map[string]interface{})
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			logger.Errorf("unmarshal kafka message value err: %v", err)
			errCh <- err
			return
		}
		rcvTime := conf.GetNow()
		consumer <- api.NewDefaultSourceTupleWithTime(data, nil, rcvTime)
	}
}

func (s *kafkaSource) Close(_ api.StreamContext) error {
	return s.reader.Close()
}

func Kafka() api.Source {
	return &kafkaSource{}
}
