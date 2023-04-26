// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	RangeProjectSet(f func(i int, r CloneAbleRow) ([]CloneAbleRow, error)) ([]Collection, error)
	// GroupRange through each group. For non-grouped collection, the whole data is a single group
	GroupRange(func(i int, aggRow CollectionRow) (bool, error)) error
	// Range through each row. For grouped collection, each row is an aggregation of groups
	Range(func(i int, r ReadonlyRow) (bool, error)) error
	// RangeSet range through each row by cloneing the row
	RangeSet(func(i int, r Row) (bool, error)) error
	Filter(indexes []int) Collection
	GetWindowRange() *WindowRange
	Clone() Collection
	// ToMaps returns the data as a map
	ToMaps() []map[string]interface{}
}

type SingleCollection interface {
	Collection
	CollectionRow
	SetIsAgg(isAgg bool)
	// ToAggMaps returns the aggregated data as a map
	ToAggMaps() []map[string]interface{}
	// ToRowMaps returns all the data in the collection
	ToRowMaps() []map[string]interface{}
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
	isAgg     bool
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
	isAgg     bool
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

func (w *WindowTuples) Range(f func(i int, r ReadonlyRow) (bool, error)) error {
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

func (w *WindowTuples) RangeProjectSet(f func(i int, r CloneAbleRow) ([]CloneAbleRow, error)) ([]Collection, error) {
	newCollections := make([]Collection, 0)
	for i, oldTupleRow := range w.Content {
		newTuples, err := f(i, oldTupleRow.Clone())
		if err != nil {
			return nil, err
		}
		for _, newTuple := range newTuples {
			ns := w.Clone().(*WindowTuples)
			ns.Content = []TupleRow{newTuple.(TupleRow)}
			newCollections = append(newCollections, ns)
		}
	}
	return newCollections, nil
}

func (w *WindowTuples) RangeSet(f func(i int, r Row) (bool, error)) error {
	for i, r := range w.Content {
		rc := r.Clone()
		b, e := f(i, rc)
		if e != nil {
			return e
		}
		if !b {
			break
		}
		w.Content[i] = rc.(TupleRow)
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

// Sort by tuple timestamp
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
	if len(w.Content) > 0 {
		return w.Content[0].Value(key, table)
	}
	return nil, false
}

func (w *WindowTuples) Meta(key, table string) (interface{}, bool) {
	if len(w.Content) > 0 {
		return w.Content[0].Value(key, table)
	}
	return nil, false
}

func (w *WindowTuples) All(_ string) (Message, bool) {
	return w.ToMap(), true
}

func (w *WindowTuples) ToMap() map[string]interface{} {
	if w.cachedMap == nil {
		m := make(map[string]interface{})
		if len(w.Content) > 0 {
			for k, v := range w.Content[0].ToMap() {
				m[k] = v
			}
		}
		w.cachedMap = m
	}
	w.AffiliateRow.MergeMap(w.cachedMap)
	return w.cachedMap
}

func (w *WindowTuples) Clone() Collection {
	ts := make([]TupleRow, len(w.Content))
	for i, t := range w.Content {
		ts[i] = t.Clone().(TupleRow)
	}
	c := &WindowTuples{
		Content:      ts,
		WindowRange:  w.WindowRange,
		AffiliateRow: w.AffiliateRow.Clone(),
		isAgg:        w.isAgg,
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

func (w *WindowTuples) ToMaps() []map[string]interface{} {
	if w.isAgg {
		return w.ToAggMaps()
	} else {
		return w.ToRowMaps()
	}
}

func (w *WindowTuples) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool) {
	cols = w.AffiliateRow.Pick(cols)
	for i, t := range w.Content {
		tc := t.Clone()
		tc.Pick(allWildcard, cols, wildcardEmitters)
		w.Content[i] = tc.(TupleRow)
	}
}

func (w *WindowTuples) SetIsAgg(_ bool) {
	w.isAgg = true
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

func (s *JoinTuples) Range(f func(i int, r ReadonlyRow) (bool, error)) error {
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

func (s *JoinTuples) RangeProjectSet(f func(i int, r CloneAbleRow) ([]CloneAbleRow, error)) ([]Collection, error) {
	newCollections := make([]Collection, 0)
	for i, oldJoinTuple := range s.Content {
		newTuples, err := f(i, oldJoinTuple.Clone())
		if err != nil {
			return nil, err
		}
		for _, newJoinTuple := range newTuples {
			ns := s.Clone().(*JoinTuples)
			ns.Content = []*JoinTuple{newJoinTuple.(*JoinTuple)}
			newCollections = append(newCollections, ns)
		}
	}
	return newCollections, nil
}

func (s *JoinTuples) RangeSet(f func(i int, r Row) (bool, error)) error {
	for i, r := range s.Content {
		rc := r.Clone()
		b, e := f(i, rc)
		if e != nil {
			return e
		}
		if !b {
			break
		}
		s.Content[i] = rc.(*JoinTuple)
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

func (s *JoinTuples) All(_ string) (Message, bool) {
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
		isAgg:        s.isAgg,
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

func (s *JoinTuples) ToMaps() []map[string]interface{} {
	if s.isAgg {
		return s.ToAggMaps()
	} else {
		return s.ToRowMaps()
	}
}

func (s *JoinTuples) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool) {
	cols = s.AffiliateRow.Pick(cols)
	for i, t := range s.Content {
		tc := t.Clone().(*JoinTuple)
		tc.Pick(allWildcard, cols, wildcardEmitters)
		s.Content[i] = tc
	}
}

func (s *JoinTuples) SetIsAgg(_ bool) {
	s.isAgg = true
}

func (s *GroupedTuplesSet) Len() int        { return len(s.Groups) }
func (s *GroupedTuplesSet) Swap(i, j int)   { s.Groups[i], s.Groups[j] = s.Groups[j], s.Groups[i] }
func (s *GroupedTuplesSet) Index(i int) Row { return s.Groups[i] }

func (s *GroupedTuplesSet) GetWindowRange() *WindowRange {
	return s.WindowRange
}

func (s *GroupedTuplesSet) Range(f func(i int, r ReadonlyRow) (bool, error)) error {
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

func (s *GroupedTuplesSet) RangeProjectSet(f func(i int, r CloneAbleRow) ([]CloneAbleRow, error)) ([]Collection, error) {
	newCollections := make([]Collection, 0)
	for i, groupTuples := range s.Groups {
		newTuples, err := f(i, groupTuples.Clone())
		if err != nil {
			return nil, err
		}
		for _, newTuple := range newTuples {
			ns := s.Clone().(*GroupedTuplesSet)
			ns.Groups = []*GroupedTuples{newTuple.(*GroupedTuples)}
			newCollections = append(newCollections, ns)
		}
	}
	return newCollections, nil
}

func (s *GroupedTuplesSet) RangeSet(f func(i int, r Row) (bool, error)) error {
	for i, r := range s.Groups {
		rc := r.Clone()
		b, e := f(i, rc)
		if e != nil {
			return e
		}
		if !b {
			break
		}
		s.Groups[i] = rc.(*GroupedTuples)
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

func (s *GroupedTuplesSet) ToMaps() []map[string]interface{} {
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
