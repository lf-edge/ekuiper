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
	"github.com/lf-edge/ekuiper/pkg/ast"
	"sort"
)

/**********************************
**	Various Data Types for SQL transformation
 */

type AggregateData interface {
	AggregateEval(expr ast.Expr, v CallValuer) []interface{}
}

type Event interface {
	GetTimestamp() int64
	IsWatermark() bool
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
		result = append(result, Eval(expr, MultiValuer(&t, &WindowRangeValuer{WindowRange: w.WindowRange}, v, &WildcardValuer{&t})))
	}
	return result
}

func getTupleValue(tuple Tuple, key string, isVal bool) (interface{}, bool) {
	if isVal {
		return tuple.Value(key, "")
	} else {
		return tuple.Meta(key, "")
	}
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
		result = append(result, Eval(expr, MultiValuer(&t, &WindowRangeValuer{WindowRange: s.WindowRange}, v, &WildcardValuer{&t})))
	}
	return result
}

type GroupedTuplesSet []GroupedTuples

func (s GroupedTuplesSet) Len() int           { return len(s) }
func (s GroupedTuplesSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s GroupedTuplesSet) Index(i int) Valuer { return s[i].Content[0] }
