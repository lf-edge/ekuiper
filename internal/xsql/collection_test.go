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
	"github.com/lf-edge/ekuiper/internal/conf"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestCollectionAgg(t *testing.T) {
	// broadcast -> range func -> broadcast -> group aggregate -> map
	var tests = []struct {
		collO     Collection
		set       [][]map[string]interface{}
		interMaps [][]map[string]interface{}
		result    [][][]map[string]interface{}
	}{
		{
			collO: &WindowTuples{Content: []TupleRow{
				&Tuple{Emitter: "a", Message: map[string]interface{}{"a": 1, "b": "2"}, Timestamp: conf.GetNowInMilli(), Metadata: nil},
				&Tuple{Emitter: "a", Message: map[string]interface{}{"a": 2, "b": "4"}, Timestamp: conf.GetNowInMilli(), Metadata: nil},
				&Tuple{Emitter: "a", Message: map[string]interface{}{"a": 3, "b": "6"}, Timestamp: conf.GetNowInMilli(), Metadata: nil},
			}},
			set: [][]map[string]interface{}{
				{
					{"a": 4, "c": "3", "@d": 4},
					{"sum": 12},
					{"avg": 4},
				},
				{
					{"c": "4"},
					{"sum": 6},
					{"avg": 2},
				},
			},
			interMaps: [][]map[string]interface{}{
				{
					{"a": 4, "b": "2", "c": "3", "d": 4},
					{"a": 4, "b": "4", "c": "3", "d": 4},
					{"a": 4, "b": "6", "c": "3", "d": 4},
				}, {
					{"a": 1, "b": "2", "c": "4"},
					{"a": 2, "b": "4", "c": "4"},
					{"a": 3, "b": "6", "c": "4"},
				},
			},
			result: [][][]map[string]interface{}{
				{
					{
						{"a": 4, "b": "2", "c": "3", "d": 4, "sum": 12},
					},
					{
						{"a": 4, "b": "2", "c": "3", "d": 4, "avg": 4},
					},
				}, {
					{
						{"a": 1, "b": "2", "c": "4", "sum": 6},
					},
					{
						{"a": 1, "b": "2", "c": "4", "avg": 2},
					},
				},
			},
		}, {
			collO: &JoinTuples{Content: []*JoinTuple{
				{
					Tuples: []TupleRow{
						&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}},
						&Tuple{Emitter: "src2", Message: Message{"a": 2, "c": "w2"}},
					},
				}, {
					Tuples: []TupleRow{
						&Tuple{Emitter: "src1", Message: Message{"a": 3, "b": "v2"}},
						&Tuple{Emitter: "src2", Message: Message{"a": 4, "c": "w1"}},
					},
				},
			}},
			set: [][]map[string]interface{}{
				{
					{"a": 4, "c": "3", "@d": 4},
					{"sum": 12},
					{"avg": 4},
				},
				{
					{"c": "4"},
					{"sum": 6},
					{"avg": 2},
				},
			},
			interMaps: [][]map[string]interface{}{
				{
					{"a": 4, "b": "v1", "c": "3", "d": 4},
					{"a": 4, "b": "v2", "c": "3", "d": 4},
				}, {
					{"a": 1, "b": "v1", "c": "4"},
					{"a": 3, "b": "v2", "c": "4"},
				},
			},
			result: [][][]map[string]interface{}{
				{
					{
						{"a": 4, "b": "v1", "c": "3", "d": 4, "sum": 12},
					},
					{
						{"a": 4, "b": "v1", "c": "3", "d": 4, "avg": 4},
					},
				}, {
					{
						{"a": 1, "b": "v1", "c": "4", "sum": 6},
					},
					{
						{"a": 1, "b": "v1", "c": "4", "avg": 2},
					},
				},
			},
		}, {
			collO: &GroupedTuplesSet{Groups: []*GroupedTuples{
				{
					Content: []TupleRow{
						&Tuple{Emitter: "src1", Message: Message{"a": 1, "b": "v1"}},
						&Tuple{Emitter: "src1", Message: Message{"a": 2, "b": "w2"}},
					},
				}, {
					Content: []TupleRow{
						&Tuple{Emitter: "src1", Message: Message{"a": 3, "b": "v2"}},
						&Tuple{Emitter: "src1", Message: Message{"a": 4, "b": "w1"}},
					},
				},
			}},
			set: [][]map[string]interface{}{
				{
					{"a": 4, "c": "3", "@d": 4},
					{"sum": 12},
					{"avg": 4},
				},
				{
					{"c": "4"},
					{"sum": 6},
					{"avg": 2},
				},
			},
			interMaps: [][]map[string]interface{}{
				{
					{"a": 4, "b": "v1", "c": "3", "d": 4},
					{"a": 4, "b": "v2", "c": "3", "d": 4},
				}, {
					{"a": 1, "b": "v1", "c": "4"},
					{"a": 3, "b": "v2", "c": "4"},
				},
			},
			result: [][][]map[string]interface{}{
				{
					{
						{"a": 4, "b": "v1", "c": "3", "d": 4, "sum": 12},
						{"a": 4, "b": "v2", "c": "3", "d": 4, "sum": 12},
					},
					{
						{"a": 4, "b": "v1", "c": "3", "d": 4, "avg": 4},
						{"a": 4, "b": "v2", "c": "3", "d": 4, "avg": 4},
					},
				}, {
					{
						{"a": 1, "b": "v1", "c": "4", "sum": 6},
						{"a": 3, "b": "v2", "c": "4", "sum": 6},
					},
					{
						{"a": 1, "b": "v1", "c": "4", "avg": 2},
						{"a": 3, "b": "v2", "c": "4", "avg": 2},
					},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		var (
			wg        sync.WaitGroup
			intermaps = make([][]map[string]interface{}, len(tt.set))
			result    = make([][][]map[string]interface{}, len(tt.set))
		)
		for si, set := range tt.set {
			wg.Add(1)
			go func(si int, set []map[string]interface{}) {
				nr := tt.collO.Clone()
				nr.Range(func(_ int, row Row) (bool, error) {
					for k, v := range set[0] {
						if strings.HasPrefix(k, "@") {
							row.AppendAlias(k[1:], v)
						} else {
							row.Set(k, v)
						}
					}
					return true, nil
				})
				intermaps[si] = nr.ToRowMaps()
				var wg2 sync.WaitGroup
				result[si] = make([][]map[string]interface{}, len(set)-1)
				for j := 1; j < len(set); j++ {
					wg2.Add(1)
					go func(j int) {
						nnr := nr.Clone()
						nnr.GroupRange(func(_ int, aggRow CollectionRow) (bool, error) {
							for k, v := range set[j] {
								if strings.HasPrefix(k, "@") {
									aggRow.AppendAlias(k[1:], v)
								} else {
									aggRow.Set(k, v)
								}
							}
							return true, nil
						})
						result[si][j-1] = nnr.ToAggMaps()
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
