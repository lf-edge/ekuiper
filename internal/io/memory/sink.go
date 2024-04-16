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
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
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
	return nil
}

func (s *sink) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening memory sink: %v", s.topic)
	pubsub.CreatePub(s.topic)
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data api.ReadonlyMessage) error {
	topic, err := ctx.ParseTemplate(s.topic, data)
	if err != nil {
		return err
	}
	return s.publish(ctx, topic, data)
}

func (s *sink) CollectList(ctx api.StreamContext, data []api.ReadonlyMessage) error {
	// TODO topic template
	//topic, err := ctx.ParseTemplate(s.topic, data)
	//if err != nil {
	//	return err
	//}
	pubsub.ProduceList(ctx, s.topic, data)
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

func (s *sink) publish(ctx api.StreamContext, topic string, mess api.ReadonlyMessage) error {
	if s.rowkindField != "" {
		c, ok := mess.Get(s.rowkindField)
		var rowkind string
		if !ok {
			rowkind = ast.RowkindUpsert
		} else {
			rowkind, ok = c.(string)
			if !ok {
				return fmt.Errorf("rowkind field %s is not a string in data %v", s.rowkindField, mess)
			}
			if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete && rowkind != ast.RowkindUpsert {
				return fmt.Errorf("invalid rowkind %s", rowkind)
			}
		}
		key, ok := mess.Get(s.keyField)
		if !ok {
			return fmt.Errorf("key field %s not found in data %v", s.keyField, mess)
		}
		pubsub.ProduceUpdatable(ctx, topic, mess, rowkind, key)
	} else {
		pubsub.Produce(ctx, topic, mess)
	}
	return nil
}

var (
	_ api.MessageCollector = &sink{}
)
