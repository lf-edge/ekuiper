// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/topo/memory/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"strings"
)

type config struct {
	Topic        string `json:"topic"`
	DataTemplate string `json:"dataTemplate"`
	RowkindField string `json:"rowkindField"`
	KeyField     string `json:"keyField"`
}

type sink struct {
	topic        string
	hasTransform bool
	keyField     string
	rowkindField string
}

func (s *sink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening memory sink: %v", s.topic)
	pubsub.CreatePub(s.topic)
	return nil
}

func (s *sink) Configure(props map[string]interface{}) error {
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
	s.rowkindField = cfg.RowkindField
	s.keyField = cfg.KeyField
	if s.rowkindField != "" && s.keyField == "" {
		return fmt.Errorf("keyField is required when rowkindField is set")
	}
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data interface{}) error {
	ctx.GetLogger().Debugf("receive %+v", data)
	topic, err := ctx.ParseTemplate(s.topic, data)
	if err != nil {
		return err
	}
	if s.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(data)
		if err != nil {
			return err
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(jsonBytes, &m)
		if err != nil {
			return fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
		}
		data = m
	}
	switch d := data.(type) {
	case []map[string]interface{}:
		for _, el := range d {
			err := s.publish(ctx, topic, el)
			if err != nil {
				return fmt.Errorf("fail to publish data %v for error %v", d, err)
			}
		}
	case map[string]interface{}:
		err := s.publish(ctx, topic, d)
		if err != nil {
			return fmt.Errorf("fail to publish data %v for error %v", d, err)
		}
	default:
		return fmt.Errorf("unrecognized format of %s", data)
	}
	return nil
}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing memory sink")
	pubsub.RemovePub(s.topic)
	return nil
}

func (s *sink) publish(ctx api.StreamContext, topic string, el map[string]interface{}) error {
	if s.rowkindField != "" {
		c, ok := el[s.rowkindField]
		var rowkind string
		if !ok {
			rowkind = ast.RowkindUpsert
		} else {
			rowkind, ok = c.(string)
			if !ok {
				return fmt.Errorf("rowkind field %s is not a string in data %v", s.rowkindField, el)
			}
			if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete && rowkind != ast.RowkindUpsert {
				return fmt.Errorf("invalid rowkind %s", rowkind)
			}
		}
		key, ok := el[s.keyField]
		if !ok {
			return fmt.Errorf("key field %s not found in data %v", s.keyField, el)
		}
		pubsub.ProduceUpdatable(ctx, topic, el, rowkind, key)
	} else {
		pubsub.Produce(ctx, topic, el)
	}
	return nil
}
