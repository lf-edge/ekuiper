// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"strings"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

// The original message map may be big. Make sure it is immutable so that never make a copy of it.
// The tuple clone should be cheap.

/*
 * Interfaces definition
 */

type Wildcarder interface {
	// All Value returns the value and existence flag for a given key.
	All(table string) (map[string]any, bool)
}

type Event interface {
	GetTimestamp() time.Time
	IsWatermark() bool
}

type EventRow interface {
	Row
	Event
}

type ReadonlyRow interface {
	HasTracerCtx
	Valuer
	AliasValuer
	Wildcarder
}

// RawRow is the basic data type for logical row. It could be a row or a collection row.
type RawRow interface {
	ReadonlyRow
	// Del Only for some ops like functionOp * and Alias
	Del(col string)
	// Set Only for some ops like functionOp *
	Set(col string, value interface{})
	// ToMap converts the row to a map to export to other systems *
	ToMap() map[string]interface{}
	// Pick the columns and discard others. It replaces the underlying message with a new value. There are 3 types to pick: column, alias and anonymous expressions.
	// cols is a list [columnname, tablename]
	Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool, except []string, sendNil bool)
}

type Row interface {
	RawRow
	Clone() Row
}

type HasTracerCtx interface {
	GetTracerCtx() api.StreamContext
	SetTracerCtx(ctx api.StreamContext)
}

type MetaData interface {
	MetaData() Metadata
}

// EmittedData is data that is produced by a specific source
type EmittedData interface {
	// GetEmitter returns the emitter of the row
	GetEmitter() string
}

// CollectionRow is the aggregation row of a non-grouped collection. Thinks of it as a single group.
// The row data is immutable
type CollectionRow interface {
	RawRow
	AggregateData
	// Clone when broadcast to make sure each row are dealt single threaded
	// Clone() CollectionRow
}

type ControlTuple interface {
	ControlType() string
}

// AffiliateRow part of other row types do help calculation of newly added cols
type AffiliateRow struct {
	lock     sync.RWMutex
	CalCols  map[string]interface{} // mutable and must be cloned when broadcast
	AliasMap map[string]interface{}
}

func (d *AffiliateRow) AppendAlias(key string, value interface{}) bool {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.AliasMap == nil {
		d.AliasMap = make(map[string]interface{})
	}
	d.AliasMap[key] = value
	return true
}

func (d *AffiliateRow) AliasValue(key string) (interface{}, bool) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.AliasMap == nil {
		return nil, false
	}
	v, ok := d.AliasMap[key]
	return v, ok
}

func (d *AffiliateRow) Value(key, table string) (interface{}, bool) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if table == "" {
		r, ok := d.AliasValue(key)
		if ok {
			return r, ok
		}
		r, ok = d.CalCols[key]
		if ok {
			return r, ok
		}
	}
	return nil, false
}

func (d *AffiliateRow) Set(col string, value interface{}) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.CalCols == nil {
		d.CalCols = make(map[string]interface{})
	}
	d.CalCols[col] = value
}

func (d *AffiliateRow) Del(col string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.CalCols != nil {
		delete(d.CalCols, col)
	}
	if d.AliasMap != nil {
		delete(d.AliasMap, col)
	}
}

func (d *AffiliateRow) Clone() AffiliateRow {
	d.lock.RLock()
	defer d.lock.RUnlock()
	nd := &AffiliateRow{}
	if d.CalCols != nil && len(d.CalCols) > 0 {
		nd.CalCols = make(map[string]interface{}, len(d.CalCols))
		for k, v := range d.CalCols {
			nd.CalCols[k] = v
		}
	}
	if d.AliasMap != nil && len(d.AliasMap) > 0 {
		nd.AliasMap = make(map[string]interface{}, len(d.AliasMap))
		for k, v := range d.AliasMap {
			nd.AliasMap[k] = v
		}
	}
	return *nd //nolint:govet
}

func (d *AffiliateRow) IsEmpty() bool {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return len(d.CalCols) == 0 && len(d.AliasMap) == 0
}

func (d *AffiliateRow) MergeMap(cachedMap map[string]interface{}) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	for k, v := range d.CalCols {
		// Do not write out the internal fields
		if !strings.HasPrefix(k, "$$") {
			cachedMap[k] = v
		}
	}
	for k, v := range d.AliasMap {
		cachedMap[k] = v
	}
}

func (d *AffiliateRow) Pick(cols [][]string) [][]string {
	d.lock.Lock()
	defer d.lock.Unlock()
	if len(cols) > 0 {
		newAliasMap := make(map[string]interface{})
		newCalCols := make(map[string]interface{})
		newCols := make([][]string, 0, len(cols))
		for _, a := range cols {
			if a[1] == "" || a[1] == string(ast.DefaultStream) {
				if v, ok := d.AliasMap[a[0]]; ok {
					newAliasMap[a[0]] = v
					continue
				}
				if v, ok := d.CalCols[a[0]]; ok {
					newCalCols[a[0]] = v
					continue
				}
			}
			newCols = append(newCols, a)
		}
		d.AliasMap = newAliasMap
		d.CalCols = newCalCols
		return newCols
	} else {
		d.AliasMap = nil
		d.CalCols = nil
		return cols
	}
}

/*
 *  Message definition
 */

// Message is a valuer that substitutes values for the mapped interface. It is the basic type for data events.
type Message map[string]interface{}

func (m Message) Get(key string) (value any, ok bool) {
	v, o := m[key]
	return v, o
}

func (m Message) Range(f func(key string, value any) bool) {
	for k, v := range m {
		exit := f(k, v)
		if exit {
			break
		}
	}
}

func (m Message) ToMap() map[string]any {
	return m
}

var _ Valuer = Message{}

type Metadata Message

// Alias will not need to convert cases
type Alias struct {
	AliasMap map[string]interface{}
}

/*
 * All row types definitions, watermark, barrier
 */

type RawTuple struct {
	Ctx       api.StreamContext
	Emitter   string
	Timestamp time.Time
	Rawdata   []byte
	Metadata  Metadata // immutable
	Props     map[string]string
}

func (r *RawTuple) GetTracerCtx() api.StreamContext {
	return r.Ctx
}

func (r *RawTuple) SetTracerCtx(ctx api.StreamContext) {
	r.Ctx = ctx
}

func (r *RawTuple) Replace(new []byte) {
	r.Rawdata = new
}

func (r *RawTuple) DynamicProps(template string) (string, bool) {
	v, ok := r.Props[template]
	return v, ok
}

func (r *RawTuple) AllProps() map[string]string {
	return r.Props
}

func (r *RawTuple) Raw() []byte {
	return r.Rawdata
}

func (r *RawTuple) Meta(key, table string) (any, bool) {
	v, ok := r.Metadata[key]
	return v, ok
}

var (
	_ api.RawTuple        = &RawTuple{}
	_ api.HasDynamicProps = &RawTuple{}
)

// Tuple The input row, produced by the source
type Tuple struct {
	Ctx       api.StreamContext
	Emitter   string
	Message   Message // the original pointer is immutable & big; may be cloned.
	Timestamp time.Time
	Metadata  Metadata // immutable
	Props     map[string]string

	AffiliateRow
	lock      sync.Mutex             // lock for the cachedMap, because it is possible to access by multiple sinks
	cachedMap map[string]interface{} // clone of the row and cached for performance
}

func (t *Tuple) GetTracerCtx() api.StreamContext {
	return t.Ctx
}

func (t *Tuple) SetTracerCtx(ctx api.StreamContext) {
	t.Ctx = ctx
}

func (t *Tuple) Created() time.Time {
	return t.Timestamp
}

func (t *Tuple) AllMeta() map[string]any {
	return t.Metadata
}

var (
	_ Row          = &Tuple{}
	_ MetaData     = &Tuple{}
	_ api.MetaInfo = &Tuple{}
)

// JoinTuple is a row produced by a join operation
type JoinTuple struct {
	Ctx    api.StreamContext
	Tuples []Row // The content is immutable, but the slice may be added or removed
	AffiliateRow
	lock      sync.Mutex
	cachedMap map[string]interface{} // clone of the row and cached for performance of toMap
}

func (jt *JoinTuple) GetTracerCtx() api.StreamContext {
	return jt.Ctx
}

func (jt *JoinTuple) SetTracerCtx(ctx api.StreamContext) {
	jt.Ctx = ctx
}

var _ Row = &JoinTuple{}

// GroupedTuples is a collection of tuples grouped by a key
type GroupedTuples struct {
	Ctx     api.StreamContext
	Content []Row
	*WindowRange
	AffiliateRow
	lock      sync.Mutex
	cachedMap map[string]interface{} // clone of the row and cached for performance of toMap
}

func (s *GroupedTuples) GetTracerCtx() api.StreamContext {
	return s.Ctx
}

func (s *GroupedTuples) SetTracerCtx(ctx api.StreamContext) {
	s.Ctx = ctx
}

var _ CollectionRow = &GroupedTuples{}

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
		// Only when with 'SELECT * FROM ...'  and 'schemaless', the key in map is not convert to lower case.
		// So all keys in map should be converted to lowercase and then compare them.
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

// Tuple implementation

func (t *Tuple) Value(key, table string) (interface{}, bool) {
	r, ok := t.AffiliateRow.Value(key, table)
	if ok {
		return r, ok
	}
	return t.Message.Value(key, table)
}

func (t *Tuple) All(string) (map[string]any, bool) {
	return t.Message, true
}

func (t *Tuple) Clone() Row {
	return &Tuple{
		Emitter:      t.Emitter,
		Timestamp:    t.Timestamp,
		Message:      t.Message,
		Metadata:     t.Metadata,
		AffiliateRow: t.AffiliateRow.Clone(),
	}
}

// ToMap should only use in sink.
func (t *Tuple) ToMap() map[string]interface{} {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.AffiliateRow.IsEmpty() {
		return t.Message
	}
	if t.cachedMap == nil { // clone the message
		m := make(map[string]any, len(t.Message))
		for k, v := range t.Message {
			m[k] = v
		}
		t.cachedMap = m
		t.AffiliateRow.MergeMap(t.cachedMap)
	}
	return t.cachedMap
}

func (t *Tuple) Meta(key, table string) (interface{}, bool) {
	if key == "*" {
		return map[string]interface{}(t.Metadata), true
	}
	return t.Metadata.Value(key, table)
}

func (t *Tuple) MetaData() Metadata {
	return t.Metadata
}

func (t *Tuple) GetEmitter() string {
	return t.Emitter
}

func (t *Tuple) DynamicProps(template string) (string, bool) {
	r, ok := t.Props[template]
	return r, ok
}

func (t *Tuple) AllProps() map[string]string {
	return t.Props
}

func (t *Tuple) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	return []interface{}{Eval(expr, MultiValuer(t, v, &WildcardValuer{t}))}
}

func (t *Tuple) GetTimestamp() time.Time {
	return t.Timestamp
}

func (t *Tuple) IsWatermark() bool {
	return false
}

func (t *Tuple) FuncValue(key string) (interface{}, bool) {
	switch key {
	case "event_time":
		return t.Timestamp.UnixMilli(), true
	default:
		return nil, false
	}
}

func (t *Tuple) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool, except []string, sendNil bool) {
	// invalidate cache, will calculate again
	t.cachedMap = nil
	cols = t.AffiliateRow.Pick(cols)
	if !allWildcard && wildcardEmitters[t.Emitter] {
		allWildcard = true
	}
	if !allWildcard {
		if len(cols) > 0 {
			pickedMap := make(map[string]any, len(cols))
			for _, colTab := range cols {
				if colTab[1] == t.Emitter || colTab[1] == "" || colTab[1] == string(ast.DefaultStream) {
					if v, ok := t.Message.Value(colTab[0], colTab[1]); ok {
						pickedMap[colTab[0]] = v
					} else if sendNil {
						pickedMap[colTab[0]] = nil
					}
				}
			}
			t.Message = pickedMap
		} else {
			t.Message = make(map[string]interface{})
		}
	} else if len(except) > 0 {
		pickedMap := make(map[string]interface{})
		for key, mess := range t.Message {
			if !contains(except, key) {
				pickedMap[key] = mess
			}
		}
		t.Message = pickedMap
	}
}

// JoinTuple implementation

func (jt *JoinTuple) AddTuple(tuple Row) {
	jt.Tuples = append(jt.Tuples, tuple)
}

func (jt *JoinTuple) AddTuples(tuples []Row) {
	jt.Tuples = append(jt.Tuples, tuples...)
}

func (jt *JoinTuple) doGetValue(key, table string, isVal bool) (interface{}, bool) {
	tuples := jt.Tuples
	if table == "" {
		if len(tuples) > 1 {
			for _, tuple := range tuples { // TODO support key without modifier?
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
		// TODO should use hash here
		for _, tuple := range tuples {
			if et, ok := tuple.(EmittedData); ok && et.GetEmitter() == table {
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

func (jt *JoinTuple) GetEmitter() string {
	return "$$JOIN"
}

func (jt *JoinTuple) Value(key, table string) (interface{}, bool) {
	r, ok := jt.AffiliateRow.Value(key, table)
	if ok {
		return r, ok
	}
	return jt.doGetValue(key, table, true)
}

func (jt *JoinTuple) Meta(key, table string) (interface{}, bool) {
	return jt.doGetValue(key, table, false)
}

func (jt *JoinTuple) All(stream string) (map[string]interface{}, bool) {
	if stream != "" {
		for _, t := range jt.Tuples {
			if et, ok := t.(EmittedData); ok && et.GetEmitter() == stream {
				return t.All("")
			}
		}
	}
	result := make(map[string]interface{})
	for _, t := range jt.Tuples {
		if m, ok := t.All(""); ok {
			for k, v := range m {
				result[k] = v
			}
		}
	}
	return result, true
}

func (jt *JoinTuple) Clone() Row {
	ts := make([]Row, len(jt.Tuples))
	for i, t := range jt.Tuples {
		ts[i] = t.Clone().(Row)
	}
	c := &JoinTuple{
		Tuples:       ts,
		AffiliateRow: jt.AffiliateRow.Clone(),
	}
	return c
}

func (jt *JoinTuple) ToMap() map[string]interface{} {
	jt.lock.Lock()
	defer jt.lock.Unlock()
	if jt.cachedMap == nil { // clone the message
		m := make(map[string]interface{})
		for i := len(jt.Tuples) - 1; i >= 0; i-- {
			for k, v := range jt.Tuples[i].ToMap() {
				m[k] = v
			}
		}
		jt.cachedMap = m
		jt.AffiliateRow.MergeMap(jt.cachedMap)
	}
	return jt.cachedMap
}

func (jt *JoinTuple) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool, except []string, sendNil bool) {
	cols = jt.AffiliateRow.Pick(cols)
	if !allWildcard {
		if len(cols) > 0 {
			for i, tuple := range jt.Tuples {
				if et, ok := tuple.(EmittedData); ok {
					if _, ok := wildcardEmitters[et.GetEmitter()]; ok {
						continue
					}
				}
				nt := tuple.Clone().(Row)
				nt.Pick(allWildcard, cols, wildcardEmitters, except, sendNil)
				jt.Tuples[i] = nt
			}
		} else {
			jt.Tuples = jt.Tuples[:0]
		}
	}
	jt.cachedMap = nil
}

func (jt *JoinTuple) AggregateEval(expr ast.Expr, v CallValuer) []interface{} {
	return []interface{}{Eval(expr, MultiValuer(jt, v, &WildcardValuer{jt}))}
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
	r, ok := s.AffiliateRow.Value(key, table)
	if ok {
		return r, ok
	}
	return s.Content[0].Value(key, table)
}

func (s *GroupedTuples) Meta(key, table string) (interface{}, bool) {
	return s.Content[0].Meta(key, table)
}

func (s *GroupedTuples) All(_ string) (map[string]interface{}, bool) {
	return s.Content[0].All("")
}

func (s *GroupedTuples) ToMap() map[string]interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.cachedMap == nil {
		m := make(map[string]interface{})
		for k, v := range s.Content[0].ToMap() {
			m[k] = v
		}
		s.cachedMap = m
		s.AffiliateRow.MergeMap(s.cachedMap)
	}
	return s.cachedMap
}

func (s *GroupedTuples) Clone() Row {
	ts := make([]Row, len(s.Content))
	for i, t := range s.Content {
		ts[i] = t
	}
	c := &GroupedTuples{
		Content:      ts,
		WindowRange:  s.WindowRange,
		AffiliateRow: s.AffiliateRow.Clone(),
	}
	return c
}

func (s *GroupedTuples) Pick(allWildcard bool, cols [][]string, wildcardEmitters map[string]bool, except []string, sendNil bool) {
	cols = s.AffiliateRow.Pick(cols)
	sc := s.Content[0].Clone()
	sc.Pick(allWildcard, cols, wildcardEmitters, except, sendNil)
	s.Content[0] = sc
}
