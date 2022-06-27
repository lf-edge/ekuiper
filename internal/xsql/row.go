// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

/*
 *  Interfaces definition
 */

// TODO how to be more efficient?
type Wildcarder interface {
	// All Value returns the value and existence flag for a given key.
	All(stream string) (Message, bool)
}

type Event interface {
	GetTimestamp() int64
	IsWatermark() bool
}

type Row interface {
	Valuer
	AliasValuer
	Wildcarder
	// Set Only for some ops like functionOp
	Set(col string, value interface{})
	// GetEmitter returns the emitter of the row
	GetEmitter() string
	// Clone when broadcast to make sure each row are dealt single threaded
	Clone() Row
	// ToMap converts the row to a map to export to other systems
	ToMap() map[string]interface{}
}

/*
 *  Message definition
 */

// Message is a valuer that substitutes values for the mapped interface. It is the basic type for data events.
type Message map[string]interface{}

var _ Valuer = Message{}

type Metadata Message

// Alias will not need to convert cases
type Alias struct {
	AliasMap map[string]interface{}
}

/*
 *   All row types definitions, watermark, barrier
 */

// Tuple The input row, produced by the source
type Tuple struct {
	Emitter   string
	Message   Message // immutable
	Timestamp int64
	Metadata  Metadata // immutable
	Alias
}

var _ Row = &Tuple{}

// JoinTuple is a row produced by a join operation
type JoinTuple struct {
	Tuples []Row
	Alias
}

var _ Row = &JoinTuple{}

// GroupedTuples is a collection of tuples grouped by a key
type GroupedTuples struct {
	Content []Row
	*WindowRange
	Alias
}

var _ Row = &GroupedTuples{}

/*
 *   Implementations
 */

func ToMessage(input interface{}) (Message, bool) {
	var result Message
	switch m := input.(type) {
	case Message:
		result = m
	case Metadata:
		result = Message(m)
	case map[string]interface{}:
		result = m
	default:
		return nil, false
	}
	return result, true
}

func (m Message) Value(key, _ string) (interface{}, bool) {
	if v, ok := m[key]; ok {
		return v, ok
	} else if conf.Config == nil || conf.Config.Basic.IgnoreCase {
		//Only when with 'SELECT * FROM ...'  and 'schemaless', the key in map is not convert to lower case.
		//So all of keys in map should be convert to lowercase and then compare them.
		return m.getIgnoreCase(key)
	} else {
		return nil, false
	}
}

func (m Message) getIgnoreCase(key interface{}) (interface{}, bool) {
	if k, ok := key.(string); ok {
		for mk, v := range m {
			if strings.EqualFold(k, mk) {
				return v, true
			}
		}
	}
	return nil, false
}

func (m Message) Meta(key, table string) (interface{}, bool) {
	if key == "*" {
		return map[string]interface{}(m), true
	}
	return m.Value(key, table)
}

// MetaData implementation

func (m Metadata) Value(key, table string) (interface{}, bool) {
	msg := Message(m)
	return msg.Value(key, table)
}

func (m Metadata) Meta(key, table string) (interface{}, bool) {
	if key == "*" {
		return map[string]interface{}(m), true
	}
	msg := Message(m)
	return msg.Meta(key, table)
}

// Alias implementation

func (a *Alias) AppendAlias(key string, value interface{}) bool {
	if a.AliasMap == nil {
		a.AliasMap = make(map[string]interface{})
	}
	a.AliasMap[key] = value
	return true
}

func (a *Alias) AliasValue(key string) (interface{}, bool) {
	if a.AliasMap == nil {
		return nil, false
	}
	v, ok := a.AliasMap[key]
	return v, ok
}

// Tuple implementation

func (t *Tuple) Value(key, table string) (interface{}, bool) {
	r, ok := t.AliasValue(key)
	if ok {
		return r, ok
	}
	return t.Message.Value(key, table)
}

func (t *Tuple) Meta(key, table string) (interface{}, bool) {
	if key == "*" {
		return map[string]interface{}(t.Metadata), true
	}
	return t.Metadata.Value(key, table)
}

func (t *Tuple) Set(col string, value interface{}) {
	//TODO implement me
	panic("implement me")
}

func (t *Tuple) Clone() Row {
	c := &Tuple{
		Emitter:   t.Emitter,
		Timestamp: t.Timestamp,
	}
	if t.Message != nil {
		m := Message{}
		for k, v := range t.Message {
			m[k] = v
		}
		c.Message = m
	}
	if t.Metadata != nil {
		md := Metadata{}
		for k, v := range t.Metadata {
			md[k] = v
		}
		c.Metadata = md
	}
	return c
}

func (t *Tuple) ToMap() map[string]interface{} {
	return t.Message
}

func (t *Tuple) GetEmitter() string {
	return t.Emitter
}

func (t *Tuple) All(string) (Message, bool) {
	return t.Message, true
}

func (t *Tuple) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	return []interface{}{Eval(expr, MultiValuer(t, v, &WildcardValuer{t}))}
}

func (t *Tuple) GetTimestamp() int64 {
	return t.Timestamp
}

func (t *Tuple) GetMetadata() Metadata {
	return t.Metadata
}

func (t *Tuple) IsWatermark() bool {
	return false
}

// JoinTuple implementation

func (jt *JoinTuple) AddTuple(tuple Row) {
	jt.Tuples = append(jt.Tuples, tuple)
}

func (jt *JoinTuple) AddTuples(tuples []Row) {
	for _, t := range tuples {
		jt.Tuples = append(jt.Tuples, t)
	}
}

func (jt *JoinTuple) doGetValue(key, table string, isVal bool) (interface{}, bool) {
	tuples := jt.Tuples
	if table == "" {
		if len(tuples) > 1 {
			for _, tuple := range tuples { //TODO support key without modifier?
				v, ok := getTupleValue(tuple, key, isVal)
				if ok {
					return v, ok
				}
			}
			conf.Log.Debugf("Wrong key: %s not found", key)
			return nil, false
		} else {
			return getTupleValue(tuples[0], key, isVal)
		}
	} else {
		//TODO should use hash here
		for _, tuple := range tuples {
			if tuple.GetEmitter() == table {
				return getTupleValue(tuple, key, isVal)
			}
		}
		return nil, false
	}
}

func getTupleValue(tuple Row, key string, isVal bool) (interface{}, bool) {
	if isVal {
		return tuple.Value(key, "")
	} else {
		return tuple.Meta(key, "")
	}
}

func (jt *JoinTuple) Value(key, table string) (interface{}, bool) {
	r, ok := jt.AliasValue(key)
	if ok {
		return r, ok
	}
	return jt.doGetValue(key, table, true)
}

func (jt *JoinTuple) Meta(key, table string) (interface{}, bool) {
	return jt.doGetValue(key, table, false)
}

func (jt *JoinTuple) All(stream string) (Message, bool) {
	if stream != "" {
		for _, t := range jt.Tuples {
			if t.GetEmitter() == stream {
				return t.ToMap(), true
			}
		}
	} else {
		return jt.ToMap(), true
	}
	return nil, false
}

// TODO deal with cascade
func (jt *JoinTuple) Clone() Row {
	ts := make([]Row, len(jt.Tuples))
	for i, t := range jt.Tuples {
		ts[i] = t.Clone()
	}
	return &JoinTuple{Tuples: ts}
}

func (jt *JoinTuple) Set(col string, value interface{}) {
	//TODO implement me
	panic("implement me")
}

func (jt *JoinTuple) ToMap() map[string]interface{} {
	m := make(map[string]interface{})
	for i := len(jt.Tuples) - 1; i >= 0; i-- {
		for k, v := range jt.Tuples[i].ToMap() {
			m[k] = v
		}
	}
	return m
}

func (jt *JoinTuple) GetEmitter() string {
	return "$$JOIN"
}

// GroupedTuple implementation

func (s *GroupedTuples) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	var result []interface{}
	for _, t := range s.Content {
		result = append(result, Eval(expr, MultiValuer(t, &WindowRangeValuer{WindowRange: s.WindowRange}, v, &WildcardValuer{t})))
	}
	return result
}

func (s *GroupedTuples) Value(key, table string) (interface{}, bool) {
	return s.Content[0].Value(key, table)
}

func (s *GroupedTuples) Meta(key, table string) (interface{}, bool) {
	return s.Content[0].Meta(key, table)
}

func (s *GroupedTuples) Set(col string, value interface{}) {
	//TODO implement me
	panic("implement me")
}

func (s *GroupedTuples) Clone() Row {
	//TODO implement me
	panic("implement me")
}

func (s *GroupedTuples) ToMap() map[string]interface{} {
	//TODO implement me
	panic("implement me")
}

func (s *GroupedTuples) All(stream string) (Message, bool) {
	return s.Content[0].All(stream)
}

func (s *GroupedTuples) GetEmitter() string {
	return "$$GROUP"
}
