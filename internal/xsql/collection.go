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
	// GroupRange through each group. For non-grouped collection, the whole data is a single group
	GroupRange(func(i int, aggRow CollectionRow) (bool, error)) error
	// Range through each row. For grouped collection, each row is an aggregation of groups
	Range(func(i int, r Row) (bool, error)) error
	Filter(indexes []int) Collection
	GetWindowRange() *WindowRange
	Clone() Collection
	// ToAggMaps returns the aggregated data as a map
	ToAggMaps() []map[string]interface{}
	// ToRowMaps returns all the data in the collection
	ToRowMaps() []map[string]interface{}
}

type SingleCollection interface {
	Collection
	CollectionRow
}

type GroupedCollection interface {
	Collection
}

// MergedCollection is a collection of rows that are from different sources
type MergedCollection interface {
	Collection
	GetBySrc(emitter string) []TupleRow
}

/*
 *   Collection types definitions
 */

type WindowTuples struct {
	Content []TupleRow // immutable
	*WindowRange
	contentBySrc map[string][]TupleRow // volatile, temporary cache]

	AffiliateRow
	cachedMap map[string]interface{}
}

var _ MergedCollection = &WindowTuples{}
var _ SingleCollection = &WindowTuples{}

// Window Tuples is also an aggregate row
var _ CollectionRow = &WindowTuples{}

type JoinTuples struct {
	Content []*JoinTuple
	*WindowRange

	AffiliateRow
	cachedMap map[string]interface{}
}

var _ SingleCollection = &JoinTuples{}
var _ CollectionRow = &JoinTuples{}

type GroupedTuplesSet struct {
	Groups []*GroupedTuples
	*WindowRange
}

var _ GroupedCollection = &GroupedTuplesSet{}

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
	w.cachedMap = nil
	w.Content[i], w.Content[j] = w.Content[j], w.Content[i]
}

func (w *WindowTuples) GetBySrc(emitter string) []TupleRow {
	if w.contentBySrc == nil {
		w.contentBySrc = make(map[string][]TupleRow)
		for _, t := range w.Content {
			e := t.GetEmitter()
			if _, hasEmitter := w.contentBySrc[e]; !hasEmitter {
				w.contentBySrc[e] = make([]TupleRow, 0)
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

func (w *WindowTuples) GroupRange(f func(i int, aggRow CollectionRow) (bool, error)) error {
	_, err := f(0, w)
	return err
}

func (w *WindowTuples) AddTuple(tuple *Tuple) *WindowTuples {
	w.Content = append(w.Content, tuple)
	return w
}

//Sort by tuple timestamp
func (w *WindowTuples) Sort() {
	w.cachedMap = nil
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
	w.cachedMap = nil
	newC := make([]TupleRow, 0, len(indexes))
	for _, i := range indexes {
		newC = append(newC, w.Content[i])
	}
	w.Content = newC
	return w
}

func (w *WindowTuples) Value(key, table string) (interface{}, bool) {
	r, ok := w.AffiliateRow.Value(key, table)
	if ok {
		return r, ok
	}
	return w.Content[0].Value(key, table)
}

func (w *WindowTuples) Meta(key, table string) (interface{}, bool) {
	return w.Content[0].Meta(key, table)
}

func (w *WindowTuples) All(stream string) (Message, bool) {
	return w.ToMap(), true
}

func (w *WindowTuples) ToMap() map[string]interface{} {
	if w.cachedMap == nil {
		m := make(map[string]interface{})
		for k, v := range w.Content[0].ToMap() {
			m[k] = v
		}
		w.cachedMap = m
	}
	w.AffiliateRow.MergeMap(w.cachedMap)
	return w.cachedMap
}

func (w *WindowTuples) Clone() Collection {
	ts := make([]TupleRow, len(w.Content))
	for i, t := range w.Content {
		ts[i] = t.Clone()
	}
	c := &WindowTuples{
		Content:      ts,
		WindowRange:  w.WindowRange,
		AffiliateRow: w.AffiliateRow.Clone(),
	}
	return c
}

func (w *WindowTuples) ToAggMaps() []map[string]interface{} {
	return []map[string]interface{}{w.ToMap()}
}

func (w *WindowTuples) ToRowMaps() []map[string]interface{} {
	r := make([]map[string]interface{}, len(w.Content))
	for i, t := range w.Content {
		r[i] = t.ToMap()
	}
	return r
}

func (s *JoinTuples) Len() int { return len(s.Content) }
func (s *JoinTuples) Swap(i, j int) {
	s.cachedMap = nil
	s.Content[i], s.Content[j] = s.Content[j], s.Content[i]
}
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

func (s *JoinTuples) GroupRange(f func(i int, aggRow CollectionRow) (bool, error)) error {
	_, err := f(0, s)
	return err
}

// Filter the tuples by the given predicate
func (s *JoinTuples) Filter(indexes []int) Collection {
	newC := make([]*JoinTuple, 0, len(indexes))
	for _, i := range indexes {
		newC = append(newC, s.Content[i])
	}
	s.Content = newC
	s.cachedMap = nil
	return s
}

func (s *JoinTuples) Value(key, table string) (interface{}, bool) {
	r, ok := s.AffiliateRow.Value(key, table)
	if ok {
		return r, ok
	}
	return s.Content[0].Value(key, table)
}

func (s *JoinTuples) Meta(key, table string) (interface{}, bool) {
	return s.Content[0].Meta(key, table)
}

func (s *JoinTuples) All(stream string) (Message, bool) {
	return s.ToMap(), true
}

func (s *JoinTuples) ToMap() map[string]interface{} {
	if s.cachedMap == nil {
		m := make(map[string]interface{})
		for k, v := range s.Content[0].ToMap() {
			m[k] = v
		}
		s.cachedMap = m
	}
	s.AffiliateRow.MergeMap(s.cachedMap)
	return s.cachedMap
}

func (s *JoinTuples) Clone() Collection {
	ts := make([]*JoinTuple, len(s.Content))
	for i, t := range s.Content {
		ts[i] = t.Clone().(*JoinTuple)
	}
	c := &JoinTuples{
		Content:      ts,
		WindowRange:  s.WindowRange,
		AffiliateRow: s.AffiliateRow.Clone(),
	}
	return c
}

func (s *JoinTuples) ToAggMaps() []map[string]interface{} {
	return []map[string]interface{}{s.ToMap()}
}

func (s *JoinTuples) ToRowMaps() []map[string]interface{} {
	r := make([]map[string]interface{}, len(s.Content))
	for i, t := range s.Content {
		r[i] = t.ToMap()
	}
	return r
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

func (s *GroupedTuplesSet) GroupRange(f func(i int, aggRow CollectionRow) (bool, error)) error {
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

// Filter clone and return the filtered set
func (s *GroupedTuplesSet) Filter(groups []int) Collection {
	newC := make([]*GroupedTuples, 0, len(groups))
	for _, i := range groups {
		newC = append(newC, s.Groups[i])
	}
	s.Groups = newC
	return s
}

func (s *GroupedTuplesSet) Clone() Collection {
	ng := make([]*GroupedTuples, len(s.Groups))
	for i, g := range s.Groups {
		ng[i] = g.Clone().(*GroupedTuples)
	}
	return &GroupedTuplesSet{
		Groups:      ng,
		WindowRange: s.WindowRange,
	}
}

func (s *GroupedTuplesSet) ToAggMaps() []map[string]interface{} {
	return s.ToRowMaps()
}

func (s *GroupedTuplesSet) ToRowMaps() []map[string]interface{} {
	r := make([]map[string]interface{}, len(s.Groups))
	for i, t := range s.Groups {
		r[i] = t.ToMap()
	}
	return r
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
