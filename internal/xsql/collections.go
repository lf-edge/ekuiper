// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"sort"
	"strings"
)

/**********************************
**	Various Data Types for SQL transformation
 */

type AggregateData interface {
	AggregateEval(expr ast.Expr, v CallValuer) []interface{}
}

// Message is a valuer that substitutes values for the mapped interface.
type Message map[string]interface{}

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

// Value returns the value for a key in the Message.
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

func (m Message) AppendAlias(k string, v interface{}) bool {
	conf.Log.Debugf("append alias %s:%v\n", k, v)
	return false
}

func (m Message) AliasValue(_ string) (interface{}, bool) {
	return nil, false
}

type Event interface {
	GetTimestamp() int64
	IsWatermark() bool
}

type Metadata Message

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

// Alias alias will not need to convert cases
type Alias struct {
	AliasMap map[string]interface{}
}

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

type Tuple struct {
	Emitter   string
	Message   Message // immutable
	Timestamp int64
	Metadata  Metadata // immutable
	Alias
}

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

func (t *Tuple) All(string) (interface{}, bool) {
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

func (t *Tuple) Clone() DataValuer {
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

type WindowTuples struct {
	Emitter string
	Tuples  []Tuple
}

type WindowRangeValuer struct {
	*WindowRange
}

func (r *WindowRangeValuer) Value(_, _ string) (interface{}, bool) {
	return nil, false
}

func (r *WindowRangeValuer) Meta(_, _ string) (interface{}, bool) {
	return nil, false
}

func (r *WindowRangeValuer) AppendAlias(_ string, _ interface{}) bool {
	return false
}

func (r *WindowRangeValuer) AliasValue(_ string) (interface{}, bool) {
	return nil, false
}

type WindowRange struct {
	WindowStart int64
	WindowEnd   int64
}

func (r *WindowRange) FuncValue(key string) (interface{}, bool) {
	switch key {
	case "window_start":
		return r.WindowStart, true
	case "window_end":
		return r.WindowEnd, true
	default:
		return nil, false
	}
}

type WindowTuplesSet struct {
	Content []WindowTuples
	*WindowRange
}

func (w WindowTuplesSet) GetBySrc(src string) []Tuple {
	for _, me := range w.Content {
		if me.Emitter == src {
			return me.Tuples
		}
	}
	return nil
}

func (w WindowTuplesSet) Len() int {
	if len(w.Content) > 0 {
		return len(w.Content[0].Tuples)
	}
	return 0
}
func (w WindowTuplesSet) Swap(i, j int) {
	if len(w.Content) > 0 {
		s := w.Content[0].Tuples
		s[i], s[j] = s[j], s[i]
	}
}
func (w WindowTuplesSet) Index(i int) Valuer {
	if len(w.Content) > 0 {
		s := w.Content[0].Tuples
		return &(s[i])
	}
	return nil
}

func (w WindowTuplesSet) AddTuple(tuple *Tuple) WindowTuplesSet {
	found := false
	for i, t := range w.Content {
		if t.Emitter == tuple.Emitter {
			t.Tuples = append(t.Tuples, *tuple)
			found = true
			w.Content[i] = t
			break
		}
	}

	if !found {
		ets := &WindowTuples{Emitter: tuple.Emitter}
		ets.Tuples = append(ets.Tuples, *tuple)
		w.Content = append(w.Content, *ets)
	}
	return w
}

//Sort by tuple timestamp
func (w WindowTuplesSet) Sort() {
	for _, t := range w.Content {
		tuples := t.Tuples
		sort.SliceStable(tuples, func(i, j int) bool {
			return tuples[i].Timestamp < tuples[j].Timestamp
		})
		t.Tuples = tuples
	}
}

func (w WindowTuplesSet) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	var result []interface{}
	if len(w.Content) != 1 { //should never happen
		return nil
	}
	for _, t := range w.Content[0].Tuples {
		result = append(result, Eval(expr, MultiValuer(&t, v, &WildcardValuer{&t})))
	}
	return result
}

type JoinTuple struct {
	Tuples []Tuple
	Alias
}

func (jt *JoinTuple) AddTuple(tuple Tuple) {
	jt.Tuples = append(jt.Tuples, tuple)
}

func (jt *JoinTuple) AddTuples(tuples []Tuple) {
	for _, t := range tuples {
		jt.Tuples = append(jt.Tuples, t)
	}
}

func getTupleValue(tuple Tuple, key string, isVal bool) (interface{}, bool) {
	if isVal {
		return tuple.Value(key, "")
	} else {
		return tuple.Meta(key, "")
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
			if tuple.Emitter == table {
				return getTupleValue(tuple, key, isVal)
			}
		}
		return nil, false
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

func (jt *JoinTuple) All(stream string) (interface{}, bool) {
	if stream != "" {
		for _, t := range jt.Tuples {
			if t.Emitter == stream {
				return t.Message, true
			}
		}
	} else {
		var r Message = make(map[string]interface{})
		for _, t := range jt.Tuples {
			for k, v := range t.Message {
				if _, ok := r[k]; !ok {
					r[k] = v
				}
			}
		}
		return r, true
	}
	return nil, false
}

func (jt *JoinTuple) Clone() DataValuer {
	ts := make([]Tuple, len(jt.Tuples))
	for i, t := range jt.Tuples {
		ts[i] = *(t.Clone().(*Tuple))
	}
	return &JoinTuple{Tuples: ts}
}

type JoinTupleSets struct {
	Content []JoinTuple
	*WindowRange
}

func (s *JoinTupleSets) Len() int           { return len(s.Content) }
func (s *JoinTupleSets) Swap(i, j int)      { s.Content[i], s.Content[j] = s.Content[j], s.Content[i] }
func (s *JoinTupleSets) Index(i int) Valuer { return &(s.Content[i]) }

func (s *JoinTupleSets) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	var result []interface{}
	for _, t := range s.Content {
		result = append(result, Eval(expr, MultiValuer(&t, v, &WildcardValuer{&t})))
	}
	return result
}

type GroupedTuples struct {
	Content []DataValuer
	*WindowRange
}

func (s GroupedTuples) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	var result []interface{}
	for _, t := range s.Content {
		result = append(result, Eval(expr, MultiValuer(t, v, &WildcardValuer{t})))
	}
	return result
}

type GroupedTuplesSet []GroupedTuples

func (s GroupedTuplesSet) Len() int           { return len(s) }
func (s GroupedTuplesSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s GroupedTuplesSet) Index(i int) Valuer { return s[i].Content[0] }
