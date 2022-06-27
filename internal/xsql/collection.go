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

/*
 *   Collection interfaces
 */

// AggregateData Could be a tuple or collection
type AggregateData interface {
	AggregateEval(expr ast.Expr, v CallValuer) []interface{}
}

type SortingData interface {
	Len() int
	Swap(i, j int)
	Index(i int) Row
}

// Collection A collection of rows as a table. It is used for window, join, group by, etc.
type Collection interface {
	SortingData
	// Range through each row. For grouped collection, each row is an aggregation of groups
	Range(func(i int, r Row) (bool, error)) error
	// GroupRange through each group. For non-grouped collection, the whole data is a single group
	GroupRange(func(i int, rows AggregateData, firstRow Row) (bool, error)) error
	Filter(indexes []int) Collection
	GetWindowRange() *WindowRange
}

// MergedCollection is a collection of rows that are from different sources
type MergedCollection interface {
	Collection
	GetBySrc(emitter string) []Row
}

/*
 *   Collection types definitions
 */

type WindowTuples struct {
	Content []Row // immutable
	*WindowRange
	Alias
	contentBySrc map[string][]Row // volatile, temporary cache
}

var _ MergedCollection = &WindowTuples{}

type JoinTuples struct {
	Content []*JoinTuple
	*WindowRange
	Alias
}

var _ Collection = &JoinTuples{}

type GroupedTuplesSet struct {
	Groups []*GroupedTuples
	*WindowRange
}

var _ Collection = &GroupedTuplesSet{}

/*
 *   Collection implementations
 */

func (w *WindowTuples) Index(index int) Row {
	return w.Content[index]
}

func (w *WindowTuples) Len() int {
	return len(w.Content)
}

func (w *WindowTuples) Swap(i, j int) {
	w.Content[i], w.Content[j] = w.Content[j], w.Content[i]
}

func (w *WindowTuples) GetBySrc(emitter string) []Row {
	if w.contentBySrc == nil {
		w.contentBySrc = make(map[string][]Row)
		for _, t := range w.Content {
			e := t.GetEmitter()
			if _, hasEmitter := w.contentBySrc[e]; !hasEmitter {
				w.contentBySrc[e] = make([]Row, 0)
			}
			w.contentBySrc[e] = append(w.contentBySrc[e], t)
		}
	}
	return w.contentBySrc[emitter]
}

func (w *WindowTuples) GetWindowRange() *WindowRange {
	return w.WindowRange
}

func (w *WindowTuples) Range(f func(i int, r Row) (bool, error)) error {
	for i, r := range w.Content {
		b, e := f(i, r)
		if e != nil {
			return e
		}
		if !b {
			break
		}
	}
	return nil
}

func (w *WindowTuples) GroupRange(f func(i int, rows AggregateData, firstRow Row) (bool, error)) error {
	_, err := f(0, w, w.Content[0])
	return err
}

func (w *WindowTuples) AddTuple(tuple *Tuple) *WindowTuples {
	w.Content = append(w.Content, tuple)
	return w
}

//Sort by tuple timestamp
func (w *WindowTuples) Sort() {
	sort.SliceStable(w.Content, func(i, j int) bool {
		return w.Content[i].(Event).GetTimestamp() < w.Content[j].(Event).GetTimestamp()
	})
}

func (w *WindowTuples) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	var result []interface{}
	for _, t := range w.Content {
		result = append(result, Eval(expr, MultiValuer(t, &WindowRangeValuer{WindowRange: w.WindowRange}, v, &WildcardValuer{t})))
	}
	return result
}

// Filter the tuples by the given predicate
func (w *WindowTuples) Filter(indexes []int) Collection {
	newC := make([]Row, 0, len(indexes))
	for _, i := range indexes {
		newC = append(newC, w.Content[i])
	}
	w.Content = newC
	return w
}

func (s *JoinTuples) Len() int        { return len(s.Content) }
func (s *JoinTuples) Swap(i, j int)   { s.Content[i], s.Content[j] = s.Content[j], s.Content[i] }
func (s *JoinTuples) Index(i int) Row { return s.Content[i] }

func (s *JoinTuples) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	var result []interface{}
	for _, t := range s.Content {
		result = append(result, Eval(expr, MultiValuer(t, &WindowRangeValuer{WindowRange: s.WindowRange}, v, &WildcardValuer{t})))
	}
	return result
}

func (s *JoinTuples) GetWindowRange() *WindowRange {
	return s.WindowRange
}

func (s *JoinTuples) Range(f func(i int, r Row) (bool, error)) error {
	for i, r := range s.Content {
		b, e := f(i, r)
		if e != nil {
			return e
		}
		if !b {
			break
		}
	}
	return nil
}

func (s *JoinTuples) GroupRange(f func(i int, rows AggregateData, firstRow Row) (bool, error)) error {
	_, err := f(0, s, s.Content[0])
	return err
}

// Filter the tuples by the given predicate
func (s *JoinTuples) Filter(indexes []int) Collection {
	newC := make([]*JoinTuple, 0, len(indexes))
	for _, i := range indexes {
		newC = append(newC, s.Content[i])
	}
	s.Content = newC
	return s
}

func (s *GroupedTuplesSet) Len() int        { return len(s.Groups) }
func (s *GroupedTuplesSet) Swap(i, j int)   { s.Groups[i], s.Groups[j] = s.Groups[j], s.Groups[i] }
func (s *GroupedTuplesSet) Index(i int) Row { return s.Groups[i] }

func (s *GroupedTuplesSet) GetWindowRange() *WindowRange {
	return s.WindowRange
}

func (s *GroupedTuplesSet) Range(f func(i int, r Row) (bool, error)) error {
	for i, r := range s.Groups {
		b, e := f(i, r)
		if e != nil {
			return e
		}
		if !b {
			break
		}
	}
	return nil
}

func (s *GroupedTuplesSet) GroupRange(f func(i int, rows AggregateData, firstRow Row) (bool, error)) error {
	for i, r := range s.Groups {
		b, e := f(i, r, r)
		if e != nil {
			return e
		}
		if !b {
			break
		}
	}
	return nil
}

// Filter clone and return the filtered set
func (s *GroupedTuplesSet) Filter(groups []int) Collection {
	newC := make([]*GroupedTuples, 0, len(groups))
	for _, i := range groups {
		newC = append(newC, s.Groups[i])
	}
	s.Groups = newC
	return s
}

/*
 *  WindowRange definitions. It should be immutable
 */

type WindowRangeValuer struct {
	*WindowRange
}

func (w WindowRangeValuer) Value(_, _ string) (interface{}, bool) {
	return nil, false
}

func (w WindowRangeValuer) Meta(_, _ string) (interface{}, bool) {
	return nil, false
}

type WindowRange struct {
	windowStart int64
	windowEnd   int64
}

func NewWindowRange(windowStart int64, windowEnd int64) *WindowRange {
	return &WindowRange{windowStart, windowEnd}
}

func (r *WindowRange) FuncValue(key string) (interface{}, bool) {
	switch key {
	case "window_start":
		return r.windowStart, true
	case "window_end":
		return r.windowEnd, true
	default:
		return nil, false
	}
}
