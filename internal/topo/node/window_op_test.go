// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"reflect"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
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

func TestTime(t *testing.T) {
	tests := []struct {
		interval int
		unit     ast.Token
		end      time.Time
	}{
		{
			interval: 10,
			unit:     ast.MS,
			end:      time.UnixMilli(1658218371340),
		}, {
			interval: 500,
			unit:     ast.MS,
			end:      time.UnixMilli(1658218371500),
		}, {
			interval: 1,
			unit:     ast.SS,
			end:      time.UnixMilli(1658218372000),
		}, {
			interval: 40, // 40 seconds
			unit:     ast.SS,
			end:      time.UnixMilli(1658218400000),
		}, {
			interval: 1,
			unit:     ast.MI,
			end:      time.UnixMilli(1658218380000),
		}, {
			interval: 3,
			unit:     ast.MI,
			end:      time.UnixMilli(1658218500000),
		}, {
			interval: 1,
			unit:     ast.HH,
			end:      time.UnixMilli(1658221200000),
		}, {
			interval: 2,
			unit:     ast.HH,
			end:      time.UnixMilli(1658224800000),
		}, {
			interval: 5, // 5 hours
			unit:     ast.HH,
			end:      time.UnixMilli(1658232000000),
		}, {
			interval: 1, // 1 day
			unit:     ast.DD,
			end:      time.UnixMilli(1658246400000),
		}, {
			interval: 7, // 1 week
			unit:     ast.DD,
			end:      time.UnixMilli(1658764800000),
		},
	}
	// Set the global timezone
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		fmt.Println("Error loading location:", err)
		return
	}
	time.Local = location

	fmt.Println(time.UnixMilli(1658218371337).String())
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		ae := getAlignedWindowEndTime(time.UnixMilli(1658218371337), tt.interval, tt.unit)
		if tt.end.UnixMilli() != ae.UnixMilli() {
			t.Errorf("%d for interval %d. error mismatch:\n  exp=%s(%d)\n  got=%s(%d)\n\n", i, tt.interval, tt.end, tt.end.UnixMilli(), ae, ae.UnixMilli())
		}
	}
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
	tests := []struct {
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
					t.Errorf("%d. \nresult mismatch:\n\nexp=%#v\n\ngot=%#v", i, tt.winTupleSets[j], cw) //nolint:govet
				}
			}

			rest := tt.tuplelist.getRestTuples()
			if !reflect.DeepEqual(tt.expRestTuples, rest) {
				t.Errorf("%d. \nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.expRestTuples, rest)
			}
		}
	}
}
