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

package xsql

import (
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

type SliceTuple struct {
	// This is immutable, it is shared by all rules with the shared source. It is accessed by source schema
	SourceContent model.SliceVal
	// After project, this is set by combining selected source field, alias field and expression field. It is accessed by sink schema
	SinkContent model.SliceVal
	// Save the calculated fields which will not sink. Currently, these are analytic result
	TempCalContent model.SliceVal
	Timestamp      time.Time
	ctx            api.StreamContext
	// TODO remove later?
	schemaMap map[string]int
	Props     map[string]string
}

func (s *SliceTuple) GetTimestamp() time.Time {
	return s.Timestamp
}

func (s *SliceTuple) IsWatermark() bool {
	return false
}

func (s *SliceTuple) DynamicProps(template string) (string, bool) {
	v, ok := s.Props[template]
	return v, ok
}

func (s *SliceTuple) AllProps() map[string]string {
	return s.Props
}

func (s *SliceTuple) ValueByIndex(index, sourceIndex int) (any, bool) {
	if sourceIndex >= 0 {
		if len(s.SourceContent) > sourceIndex {
			return s.SourceContent[sourceIndex], true
		}
		return nil, false
	}
	if len(s.SinkContent) > index {
		val := s.SinkContent[index]
		if val != nil {
			return val, true
		}
	}
	return nil, false
}

// SetByIndex set sink result
func (s *SliceTuple) SetByIndex(index int, value any) {
	if len(s.SinkContent) <= index {
		s.SinkContent = append(s.SinkContent, make(model.SliceVal, index+1-len(s.SinkContent))...)
	}
	s.SinkContent[index] = value
}

// SetTempByIndex set analytic result. Separate it from sink to save memory in window
func (s *SliceTuple) SetTempByIndex(index int, value any) {
	if len(s.TempCalContent) <= index {
		s.TempCalContent = append(s.TempCalContent, make(model.SliceVal, index+1-len(s.TempCalContent))...)
	}
	s.TempCalContent[index] = value
}

func (s *SliceTuple) TempByIndex(index int) any {
	if len(s.TempCalContent) > index {
		val := s.TempCalContent[index]
		if val != nil {
			return val
		}
	}
	return nil
}

func (s *SliceTuple) Compact(len int) {
	s.SourceContent = s.SinkContent[:len]
	s.SinkContent = nil
	s.TempCalContent = nil
}

func (s *SliceTuple) GetTracerCtx() api.StreamContext {
	return s.ctx
}

func (s *SliceTuple) SetTracerCtx(ctx api.StreamContext) {
	s.ctx = ctx
}

func (s *SliceTuple) Value(key, _ string) (any, bool) {
	panic("calling slice tuple value func")
}

func (s *SliceTuple) Meta(key, table string) (any, bool) {
	// TODO implement me
	panic("implement me")
}

func (s *SliceTuple) AliasValue(name string) (any, bool) {
	panic("calling slice tuple alias value func, alias should be transform to index")
}

func (s *SliceTuple) AppendAlias(key string, value any) bool {
	panic("calling slice tuple append alias func, alias should be transform to index")
}

func (s *SliceTuple) All(_ string) (map[string]any, bool) {
	s.ctx.GetLogger().Warnf("calling slice tuple all func")
	// do nothing
	return nil, false
}

func (s *SliceTuple) Del(_ string) {
	// do nothing
	s.ctx.GetLogger().Warnf("calling slice tuple del func")
}

func (s *SliceTuple) Set(col string, value any) {
	panic("calling slice tuple set func")
}

func (s *SliceTuple) ToMap() map[string]any {
	s.ctx.GetLogger().Warnf("calling slice tuple to map func")
	if s.schemaMap != nil {
		result := make(map[string]any, len(s.schemaMap))
		for k, index := range s.schemaMap {
			result[k] = s.SourceContent[index]
		}
	}
	return nil
}

func (s *SliceTuple) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool, except []string, sendNil bool) {
	panic("pick should convert to index")
}

func (s *SliceTuple) Clone() Row {
	newS := &SliceTuple{ctx: s.ctx, SourceContent: s.SourceContent, Timestamp: s.Timestamp}
	newS.SinkContent = make(model.SliceVal, len(s.SinkContent))
	for i, v := range s.SinkContent {
		newS.SinkContent[i] = v
	}
	newS.TempCalContent = make(model.SliceVal, len(s.TempCalContent))
	for i, v := range s.TempCalContent {
		newS.TempCalContent[i] = v
	}
	return newS
}

func (s *SliceTuple) FuncValue(key string) (any, bool) {
	switch key {
	case "event_time":
		return s.Timestamp.UnixMilli(), true
	default:
		return nil, false
	}
}

var (
	_ Row                 = &SliceTuple{}
	_ Event               = &SliceTuple{}
	_ model.IndexValuer   = &SliceTuple{}
	_ api.HasDynamicProps = &SliceTuple{}
)
