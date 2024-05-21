// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package memory

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type config struct {
	Topic        string   `json:"topic"`
	DataTemplate string   `json:"dataTemplate"`
	RowkindField string   `json:"rowkindField"`
	KeyField     string   `json:"keyField"`
	Fields       []string `json:"fields"`
	DataField    string   `json:"dataField"`
	ResendTopic  string   `json:"resendDestination"`
}

type sink struct {
	topic        string
	hasTransform bool
	keyField     string
	rowkindField string
	fields       []string
	dataField    string
	resendTopic  string
	meta         map[string]any
}

func (s *sink) Provision(_ api.StreamContext, props map[string]any) error {
	cfg := &config{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return err
	}
	if strings.ContainsAny(cfg.Topic, "#+") {
		return fmt.Errorf("invalid memory topic %s: wildcard found", cfg.Topic)
	}
	s.topic = cfg.Topic
	if cfg.DataTemplate != "" {
		s.hasTransform = true
	}
	s.dataField = cfg.DataField
	s.fields = cfg.Fields
	s.rowkindField = cfg.RowkindField
	s.keyField = cfg.KeyField
	if s.rowkindField != "" && s.keyField == "" {
		return fmt.Errorf("keyField is required when rowkindField is set")
	}
	s.resendTopic = cfg.ResendTopic
	if s.resendTopic == "" {
		s.resendTopic = s.topic
	}
	s.meta = map[string]any{
		"topic": cfg.Topic,
	}
	return nil
}

func (s *sink) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening memory sink: %v", s.topic)
	pubsub.CreatePub(s.topic)
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data api.MessageTuple) error {
	topic := s.topic
	if dp, ok := data.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(topic)
		if transformed {
			topic = temp
		}
	}
	ctx.GetLogger().Debugf("publishing to topic %s", topic)
	pubsub.Produce(ctx, topic, &xsql.Tuple{Message: data.ToMap(), Metadata: s.meta, Timestamp: timex.GetNow()})
	return nil
}

func (s *sink) CollectList(ctx api.StreamContext, tuples api.MessageTupleList) error {
	topic := s.topic
	if dp, ok := tuples.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(topic)
		if transformed {
			topic = temp
		}
	}
	result := make([]*xsql.Tuple, tuples.Len())
	tuples.RangeOfTuples(func(index int, tuple api.MessageTuple) bool {
		result[index] = &xsql.Tuple{Message: tuple.ToMap(), Metadata: s.meta, Timestamp: timex.GetNow()}
		return true
	})
	pubsub.ProduceList(ctx, topic, result)
	return nil
}

//func (s *sink) CollectResend(ctx api.StreamContext, data interface{}) error {
//	ctx.GetLogger().Debugf("resend %+v", data)
//	return s.collectWithTopic(ctx, data, s.resendTopic)
//}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing memory sink")
	pubsub.RemovePub(s.topic)
	return nil
}

func GetSink() api.TupleCollector {
	return &sink{}
}
