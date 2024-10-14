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
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type config struct {
	Topic        string `json:"topic"`
	RowkindField string `json:"rowkindField"`
	KeyField     string `json:"keyField"`
}

type sink struct {
	topic        string
	keyField     string
	rowkindField string
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
	s.rowkindField = cfg.RowkindField
	s.keyField = cfg.KeyField
	if s.rowkindField != "" && s.keyField == "" {
		return fmt.Errorf("keyField is required when rowkindField is set")
	}
	s.meta = map[string]any{
		"topic": cfg.Topic,
	}
	return nil
}

func (s *sink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Debugf("Opening memory sink: %v", s.topic)
	pubsub.CreatePub(s.topic)
	sch(api.ConnectionConnected, "")
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
	var spanCtx api.StreamContext
	if dt, ok := data.(xsql.HasTracerCtx); ok {
		spanCtx = dt.GetTracerCtx()
	}
	var (
		t   pubsub.MemTuple = &xsql.Tuple{Message: data.ToMap(), Metadata: s.meta, Timestamp: timex.GetNow(), Ctx: spanCtx}
		err error
	)
	if s.rowkindField != "" {
		t, err = s.wrapUpdatable(t)
		if err != nil {
			return err
		}
	}
	pubsub.Produce(ctx, topic, t)
	return nil
}

func (s *sink) wrapUpdatable(el pubsub.MemTuple) (pubsub.MemTuple, error) {
	c, ok := el.Value(s.rowkindField, "")
	var rowkind string
	if !ok {
		rowkind = ast.RowkindUpsert
	} else {
		rowkind, ok = c.(string)
		if !ok {
			return nil, fmt.Errorf("rowkind field %s is not a string in data %v", s.rowkindField, el)
		}
		if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete && rowkind != ast.RowkindUpsert {
			return nil, fmt.Errorf("invalid rowkind %s", rowkind)
		}
	}
	key, ok := el.Value(s.keyField, "")
	if !ok {
		return nil, fmt.Errorf("key field %s not found in data %v", s.keyField, el.ToMap())
	}
	return &pubsub.UpdatableTuple{
		MemTuple: el,
		Rowkind:  rowkind,
		Keyval:   key,
	}, nil
}

func (s *sink) CollectList(ctx api.StreamContext, tuples api.MessageTupleList) error {
	topic := s.topic
	var spanCtx api.StreamContext
	if dt, ok := tuples.(xsql.HasTracerCtx); ok {
		spanCtx = dt.GetTracerCtx()
	}
	if dp, ok := tuples.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(topic)
		if transformed {
			topic = temp
		}
	}
	result := make([]pubsub.MemTuple, tuples.Len())
	tuples.RangeOfTuples(func(index int, tuple api.MessageTuple) bool {
		t := &xsql.Tuple{Message: tuple.ToMap(), Metadata: s.meta, Timestamp: timex.GetNow(), Ctx: spanCtx}
		if s.rowkindField != "" {
			st, err := s.wrapUpdatable(t)
			if err != nil {
				ctx.GetLogger().Errorf("cannot convert %v to updatable %v", t, err)
			} else {
				result[index] = st
				return true
			}
		}
		result[index] = t
		return true
	})
	pubsub.ProduceList(ctx, topic, result)
	return nil
}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing memory sink")
	pubsub.RemovePub(s.topic)
	return nil
}

func GetSink() api.TupleCollector {
	return &sink{}
}
