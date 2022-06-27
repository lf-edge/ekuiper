// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package node

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"reflect"
	"testing"
)

var fivet = []*xsql.Tuple{
	{
		Message: map[string]interface{}{
			"f1": "v1",
		},
	},
	{
		Message: map[string]interface{}{
			"f2": "v2",
		},
	},
	{
		Message: map[string]interface{}{
			"f3": "v3",
		},
	},
	{
		Message: map[string]interface{}{
			"f4": "v4",
		},
	},
	{
		Message: map[string]interface{}{
			"f5": "v5",
		},
	},
}

func TestNewTupleList(t *testing.T) {
	_, e := NewTupleList(nil, 0)
	es1 := "Window size should not be less than zero."
	if !reflect.DeepEqual(es1, e.Error()) {
		t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", es1, e)
	}

	_, e = NewTupleList(nil, 2)
	es1 = "The tuples should not be nil or empty."
	if !reflect.DeepEqual(es1, e.Error()) {
		t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", es1, e)
	}

}

func TestCountWindow(t *testing.T) {
	var tests = []struct {
		tuplelist     TupleList
		expWinCount   int
		winTupleSets  []xsql.WindowTuples
		expRestTuples []*xsql.Tuple
	}{
		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   5,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuples{
				{
					Content: []xsql.TupleRow{

						&xsql.Tuple{
							Message: map[string]interface{}{
								"f1": "v1",
							},
						},
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f2": "v2",
							},
						},
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f3": "v3",
							},
						},
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f4": "v4",
							},
						},
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f5": "v5",
							},
						},
					},
				},
			},
			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f2": "v2",
					},
				},
				{
					Message: map[string]interface{}{
						"f3": "v3",
					},
				},
				{
					Message: map[string]interface{}{
						"f4": "v4",
					},
				},
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   3,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuples{
				{
					Content: []xsql.TupleRow{
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f3": "v3",
							},
						},
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f4": "v4",
							},
						},
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f5": "v5",
							},
						},
					},
				},
			},
			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f4": "v4",
					},
				},
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   2,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuples{
				{
					Content: []xsql.TupleRow{
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f4": "v4",
							},
						},
						&xsql.Tuple{
							Message: map[string]interface{}{
								"f5": "v5",
							},
						},
					},
				},
			},

			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   6,
			},
			expWinCount:  0,
			winTupleSets: nil,
			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f1": "v1",
					},
				},
				{
					Message: map[string]interface{}{
						"f2": "v2",
					},
				},
				{
					Message: map[string]interface{}{
						"f3": "v3",
					},
				},
				{
					Message: map[string]interface{}{
						"f4": "v4",
					},
				},
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		if tt.expWinCount == 0 {
			if tt.tuplelist.hasMoreCountWindow() {
				t.Errorf("%d \n Should not have more count window.", i)
			}
		} else {
			for j := 0; j < tt.expWinCount; j++ {
				if !tt.tuplelist.hasMoreCountWindow() {
					t.Errorf("%d \n Expect more element, but cannot find more element.", i)
				}
				cw := tt.tuplelist.nextCountWindow()
				if !reflect.DeepEqual(tt.winTupleSets[j].Content, cw.Content) {
					t.Errorf("%d. \nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.winTupleSets[j], cw)
				}
			}

			rest := tt.tuplelist.getRestTuples()
			if !reflect.DeepEqual(tt.expRestTuples, rest) {
				t.Errorf("%d. \nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.expRestTuples, rest)
			}
		}
	}
}
