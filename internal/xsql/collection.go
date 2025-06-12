// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

/*
 *   Collection interfaces
 */

// AggregateData Could be a tuple or collection
type AggregateData interface {
	AggregateEval(expr ast.Expr, v CallValuer) []interface{}
}

type SortingData interface {
	HasTracerCtx
	Len() int
	Swap(i, j int)
	Index(i int) Row
}

// Collection A collection of rows as a table. It is used for window, join, group by, etc.
type Collection interface {
	HasTracerCtx
	api.MessageTupleList
	SortingData
	// GroupRange through each group. For non-grouped collection, the whole data is a single group
	GroupRange(func(i int, aggRow CollectionRow) (bool, error)) error
	// Range through each row. For grouped collection, each row is an aggregation of groups
	Range(func(i int, r ReadonlyRow) (bool, error)) error
	// RangeSet range through each row by cloning the row
	RangeSet(func(i int, r Row) (bool, error)) error
	Filter(indexes []int) Collection
	GetWindowRange() *WindowRange
	// ToMaps returns the data as a map
	ToMaps() []map[string]interface{}
	// SetIsAgg Set by project, indicate if the collection is used in an aggregate context which will affect ToMaps output
	SetIsAgg(isAgg bool)
	// ToAggMaps returns the aggregated data as a map
	ToAggMaps() []map[string]interface{}
	// ToRowMaps returns all the data in the collection
	ToRowMaps() []map[string]interface{}
	// GetBySrc returns the rows by the given emitter
	GetBySrc(emitter string) []Row
	// Clone the collection
	Clone() Collection
}

/*
 *   Collection types definitions
 */

type WindowTuples struct {
	Ctx     api.StreamContext
	Content []Row // immutable
	*WindowRange
	contentBySrc map[string][]Row // volatile, temporary cache]

	AffiliateRow
	cachedMap map[string]interface{}
	isAgg     bool
}

var (
	_ Collection    = &WindowTuples{}
	_ CollectionRow = &WindowTuples{}
)

type JoinTuples struct {
	Ctx     api.StreamContext
	Content []*JoinTuple
	*WindowRange

	AffiliateRow
	cachedMap map[string]interface{}
	isAgg     bool
}

func (s *JoinTuples) GetTracerCtx() api.StreamContext {
	return s.Ctx
}

func (s *JoinTuples) SetTracerCtx(ctx api.StreamContext) {
	s.Ctx = ctx
}

var (
	_ Collection    = &JoinTuples{}
	_ CollectionRow = &JoinTuples{}
)

type GroupedTuplesSet struct {
	Ctx    api.StreamContext
	Groups []*GroupedTuples
	*WindowRange
}

func (s *GroupedTuplesSet) GetTracerCtx() api.StreamContext {
	return s.Ctx
}

func (s *GroupedTuplesSet) SetTracerCtx(ctx api.StreamContext) {
	s.Ctx = ctx
}

var _ Collection = &GroupedTuplesSet{}

/*
 *   Collection implementations
 */

func (w *WindowTuples) GetTracerCtx() api.StreamContext {
	return w.Ctx
}

func (w *WindowTuples) SetTracerCtx(ctx api.StreamContext) {
	w.Ctx = ctx
}

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

func (w *WindowTuples) GetBySrc(emitter string) []Row {
	if w.contentBySrc == nil {
		w.contentBySrc = make(map[string][]Row)
		for _, t := range w.Content {
			if et, ok := t.(EmittedData); ok {
				e := et.GetEmitter()
				if _, hasEmitter := w.contentBySrc[e]; !hasEmitter {
					w.contentBySrc[e] = make([]Row, 0)
				}
				w.contentBySrc[e] = append(w.contentBySrc[e], t)
			}
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

func (w *WindowTuples) RangeOfTuples(f func(index int, tuple api.MessageTuple) bool) {
	for i, r := range w.Content {
		if !f(i, r) {
			break
		}
	}
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
		w.Content[i] = rc.(Row)
	}
	return nil
}

func (w *WindowTuples) GroupRange(f func(i int, aggRow CollectionRow) (bool, error)) error {
	_, err := f(0, w)
	return err
}

func (w *WindowTuples) AddTuple(tuple Row) *WindowTuples {
	w.Content = append(w.Content, tuple)
	return w
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
	newC := make([]Row, 0, len(indexes))
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

func (w *WindowTuples) All(_ string) (map[string]interface{}, bool) {
	if len(w.Content) == 0 {
		m := make(map[string]interface{})
		return m, true
	}
	return w.Content[0].All("")
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
	ts := make([]Row, len(w.Content))
	for i, t := range w.Content {
		ts[i] = t.Clone()
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

func (w *WindowTuples) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool, except []string, sendNil bool) {
	cols = w.AffiliateRow.Pick(cols)
	for i, t := range w.Content {
		tc := t.Clone()
		tc.Pick(allWildcard, cols, wildcardEmitters, except, sendNil)
		w.Content[i] = tc
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

func (s *JoinTuples) RangeOfTuples(f func(index int, tuple api.MessageTuple) bool) {
	for i, r := range s.Content {
		if !f(i, r) {
			break
		}
	}
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

func (s *JoinTuples) All(_ string) (map[string]interface{}, bool) {
	return s.Content[0].All("")
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

func (s *JoinTuples) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool, except []string, sendNil bool) {
	cols = s.AffiliateRow.Pick(cols)
	for i, t := range s.Content {
		tc := t.Clone().(*JoinTuple)
		tc.Pick(allWildcard, cols, wildcardEmitters, except, sendNil)
		s.Content[i] = tc
	}
}

func (s *JoinTuples) SetIsAgg(_ bool) {
	s.isAgg = true
}

// GetBySrc to be implemented to support join after join
func (s *JoinTuples) GetBySrc(_ string) []Row {
	return nil
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

func (s *GroupedTuplesSet) RangeOfTuples(f func(index int, tuple api.MessageTuple) bool) {
	for i, r := range s.Groups {
		if !f(i, r) {
			break
		}
	}
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

func (s *GroupedTuplesSet) ToMaps() []map[string]any {
	r := make([]map[string]interface{}, len(s.Groups))
	for i, t := range s.Groups {
		r[i] = t.ToMap()
	}
	return r
}

func (s *GroupedTuplesSet) SetIsAgg(_ bool) {
	// do nothing
}

func (s *GroupedTuplesSet) ToAggMaps() []map[string]any {
	return s.ToMaps()
}

func (s *GroupedTuplesSet) ToRowMaps() []map[string]any {
	return s.ToMaps()
}

// GetBySrc to be implemented to support join after join
func (s *GroupedTuplesSet) GetBySrc(_ string) []Row {
	return nil
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
	windowStart   int64
	windowEnd     int64
	windowTrigger int64
}

func NewWindowRange(windowStart int64, windowEnd int64, windowTrigger int64) *WindowRange {
	return &WindowRange{windowStart, windowEnd, windowTrigger}
}

func (r *WindowRange) FuncValue(key string) (interface{}, bool) {
	switch key {
	case "window_start":
		return r.windowStart, true
	case "window_end":
		return r.windowEnd, true
	case "event_time":
		return r.windowTrigger, true
	default:
		return nil, false
	}
}

type TransformedTupleList struct {
	Ctx     api.StreamContext
	Content []api.MessageTuple
	Maps    []map[string]any
	Props   map[string]string
}

func (l *TransformedTupleList) GetTracerCtx() api.StreamContext {
	return l.Ctx
}

func (l *TransformedTupleList) SetTracerCtx(ctx api.StreamContext) {
	l.Ctx = ctx
}

func (l *TransformedTupleList) DynamicProps(template string) (string, bool) {
	v, ok := l.Props[template]
	return v, ok
}

func (l *TransformedTupleList) AllProps() map[string]string {
	return l.Props
}

func (l *TransformedTupleList) ToMaps() []map[string]any {
	if l.Maps == nil {
		l.Maps = make([]map[string]any, len(l.Content))
		for i, t := range l.Content {
			l.Maps[i] = t.ToMap()
		}
	}
	return l.Maps
}

func (l *TransformedTupleList) Clone() *TransformedTupleList {
	ng := make([]api.MessageTuple, len(l.Content))
	for i, g := range l.Content {
		switch gt := g.(type) {
		case Row:
			ng[i] = gt.Clone()
		default:
			ng[i] = g
		}
	}
	return &TransformedTupleList{
		Ctx:     l.Ctx,
		Content: ng,
		Props:   l.Props,
	}
}

func (l *TransformedTupleList) RangeOfTuples(f func(index int, tuple api.MessageTuple) bool) {
	for i, v := range l.Content {
		if !f(i, v) {
			break
		}
	}
}

func (l *TransformedTupleList) Len() int {
	return len(l.Content)
}

var (
	_ api.MessageTupleList = &TransformedTupleList{}
	_ api.HasDynamicProps  = &TransformedTupleList{}
)
