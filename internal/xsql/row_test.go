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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
)

// Row valuer, wildcarder test
// WindowTuples, JoinTuples, GroupTuples are collectionRow
func TestCollectionRow(t *testing.T) {
	var tests = []struct {
		rowC     CollectionRow
		value    []string
		wildcard []string
		result   []interface{} // result of valuers and wildcards
	}{
		{
			rowC:     &Tuple{Emitter: "a", Message: map[string]interface{}{"a": 1, "b": "2"}, Timestamp: conf.GetNowInMilli(), Metadata: nil},
			value:    []string{"a", "b"},
			wildcard: []string{""},
			result:   []interface{}{1, "2", Message{"a": 1, "b": "2"}},
		}, {
			rowC:     &Tuple{Emitter: "a", Message: map[string]interface{}{"a": 1, "b": "2"}, AffiliateRow: AffiliateRow{CalCols: map[string]interface{}{"a": 4, "c": 3}, AliasMap: map[string]interface{}{"b": "b1"}}},
			value:    []string{"a", "b", "c"},
			wildcard: []string{""},
			result:   []interface{}{4, "b1", 3, Message{"a": 4, "b": "b1", "c": 3}},
		}, {
			rowC: &JoinTuple{Tuples: []TupleRow{
				&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}},
				&Tuple{Emitter: "src2", Message: Message{"a": 2, "c": "w2"}},
			}},
			value:    []string{"a", "src2.a", "b", "c"},
			wildcard: []string{"", "src1"},
			result:   []interface{}{1, 2, "v1", "w2", Message{"a": 1, "b": "v1", "c": "w2"}, Message{"a": 1, "b": "v1"}},
		}, {
			rowC: &JoinTuple{Tuples: []TupleRow{
				&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}},
				&Tuple{Emitter: "src2", Message: Message{"a": 2, "c": "w2"}},
			}, AffiliateRow: AffiliateRow{CalCols: map[string]interface{}{"a": 4, "d": 3}, AliasMap: map[string]interface{}{"d": 4}}},
			value:    []string{"a", "src2.a", "b", "c", "d"},
			wildcard: []string{"", "src1"},
			result:   []interface{}{4, 2, "v1", "w2", 4, Message{"a": 4, "b": "v1", "c": "w2", "d": 4}, Message{"a": 1, "b": "v1"}},
		}, {
			rowC:     &GroupedTuples{Content: []TupleRow{&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}}, &Tuple{Emitter: "src1", Message: Message{"a": 2, "b": "v2"}}}},
			value:    []string{"a", "b"},
			wildcard: []string{""},
			result:   []interface{}{1, "v1", Message{"a": 1, "b": "v1"}},
		}, {
			rowC:     &GroupedTuples{Content: []TupleRow{&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}}, &Tuple{Emitter: "src1", Message: Message{"a": 2, "b": "v2"}}}, AffiliateRow: AffiliateRow{CalCols: map[string]interface{}{"a": 4, "d": 3}, AliasMap: map[string]interface{}{"d": 4}}},
			value:    []string{"a", "b", "d"},
			wildcard: []string{""},
			result:   []interface{}{4, "v1", 4, Message{"a": 4, "b": "v1", "d": 4}},
		}, {
			rowC:     &WindowTuples{Content: []TupleRow{&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}}, &Tuple{Emitter: "src1", Message: Message{"a": 2, "b": "v2"}}}, AffiliateRow: AffiliateRow{CalCols: map[string]interface{}{"a": 4, "d": 3}, AliasMap: map[string]interface{}{"d": 4}}},
			value:    []string{"a", "b", "d"},
			wildcard: []string{""},
			result:   []interface{}{4, "v1", 4, Message{"a": 4, "b": "v1", "d": 4}},
		}, {
			rowC: &JoinTuples{Content: []*JoinTuple{{Tuples: []TupleRow{
				&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}, AffiliateRow: AffiliateRow{CalCols: map[string]interface{}{"b": "v2", "$$lag_a": 1}}},
				&Tuple{Emitter: "src2", Message: Message{"a": 2, "c": "w2"}},
			}}}, AffiliateRow: AffiliateRow{CalCols: map[string]interface{}{"a": 4, "d": 3}, AliasMap: map[string]interface{}{"d": 4}}},
			value:    []string{"a", "b", "d"},
			wildcard: []string{""},
			result:   []interface{}{4, "v2", 4, Message{"a": 4, "b": "v2", "c": "w2", "d": 4}},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	var ok bool
	for i, tt := range tests {
		result := make([]interface{}, len(tt.value)+len(tt.wildcard))
		for j, v := range tt.value {
			var key, table string
			strs := strings.Split(v, ".")
			if len(strs) > 1 {
				key = strs[1]
				table = strs[0]
			} else {
				key = strs[0]
				table = ""
			}
			result[j], ok = tt.rowC.Value(key, table)
			if !ok {
				t.Errorf("%d.%d.%d: %s", i, j, 0, "Value() failed.")
				continue
			}
		}
		for j, v := range tt.wildcard {
			result[len(tt.value)+j], ok = tt.rowC.All(v)
			if !ok {
				t.Errorf("%d.%d.%d: %s", i, j, 1, "Wildcard() failed.")
				continue
			}
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, result)
		}
	}
}

func TestTupleRow(t *testing.T) {
	// boradcast(clone) -> set -> broadcast -> set -> compare
	var tests = []struct {
		rowO TupleRow
		// The multiple values to set or alias; The first value is set in the first broadcast. the next values are set in the second broadcast.
		set    [][]map[string]interface{}
		result [][]map[string]interface{}
	}{
		{
			rowO: &Tuple{Emitter: "a", Message: map[string]interface{}{"a": 1, "b": "2"}, Timestamp: conf.GetNowInMilli(), Metadata: nil},
			set: [][]map[string]interface{}{
				{
					{"a": 2, "c": "3", "@d": 4},
					{"a": 3},
					{"a": 4, "b": "5"},
					{"@d": 5, "e": 5},
				},
				{
					{"c": "4"},
					{"d": "4"},
					{"a": 5, "b": "6"},
					{"a": 6, "@b": "7"},
				},
			},
			result: [][]map[string]interface{}{
				{
					{"a": 3, "b": "2", "c": "3", "d": 4},
					{"a": 4, "b": "5", "c": "3", "d": 4},
					{"a": 2, "b": "2", "c": "3", "d": 5, "e": 5},
				}, {
					{"a": 1, "b": "2", "c": "4", "d": "4"},
					{"a": 5, "b": "6", "c": "4"},
					{"a": 6, "b": "7", "c": "4"},
				},
			},
		}, {
			rowO: &JoinTuple{Tuples: []TupleRow{
				&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}},
				&Tuple{Emitter: "src2", Message: Message{"a": 2, "c": "w2"}},
			}},
			set: [][]map[string]interface{}{
				{
					{"a": 2, "c": "3", "@d": 4},
					{"a": 3},
					{"a": 4, "b": "5"},
					{"@d": 5, "e": 5},
				},
				{
					{"e": "4"},
					{"d": "4"},
					{"a": 5, "b": "6"},
					{"a": 6, "@b": "7"},
				},
			},
			result: [][]map[string]interface{}{
				{
					{"a": 3, "b": "v1", "c": "3", "d": 4},
					{"a": 4, "b": "5", "c": "3", "d": 4},
					{"a": 2, "b": "v1", "c": "3", "d": 5, "e": 5},
				}, {
					{"a": 1, "b": "v1", "c": "w2", "d": "4", "e": "4"},
					{"a": 5, "b": "6", "c": "w2", "e": "4"},
					{"a": 6, "b": "7", "c": "w2", "e": "4"},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		var (
			wg     sync.WaitGroup
			result = make([][]map[string]interface{}, len(tt.set))
		)
		for si, set := range tt.set {
			wg.Add(1)
			go func(si int, set []map[string]interface{}) {
				nr := tt.rowO.Clone()
				for k, v := range set[0] {
					if strings.HasPrefix(k, "@") {
						nr.AppendAlias(k[1:], v)
					} else {
						nr.Set(k, v)
					}
				}
				var wg2 sync.WaitGroup
				result[si] = make([]map[string]interface{}, len(set)-1)
				for j := 1; j < len(set); j++ {
					wg2.Add(1)
					go func(j int) {
						nnr := nr.Clone()
						for k, v := range set[j] {
							if strings.HasPrefix(k, "@") {
								nnr.AppendAlias(k[1:], v)
							} else {
								nnr.Set(k, v)
							}
						}
						result[si][j-1] = nnr.ToMap()
						wg2.Done()
					}(j)
				}
				wg2.Wait()
				wg.Done()
			}(si, set)
		}
		wg.Wait()
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, result)
		}
	}
}
