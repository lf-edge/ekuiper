// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

var fivet = []xsql.EventRow{
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
	// time.Local = location
	// Use In(location) instead of setting global time.Local to avoid race
	baseTime := time.UnixMilli(1658218371337).In(location)
	fmt.Println(baseTime.String())
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		ae := getAlignedWindowEndTime(baseTime, tt.interval, tt.unit)
		if tt.end.UnixMilli() != ae.UnixMilli() {
			t.Errorf("%d for interval %d. error mismatch:\n  exp=%s(%d)\n  got=%s(%d)\n\n", i, tt.interval, tt.end, tt.end.UnixMilli(), ae, ae.UnixMilli())
		}
	}
}

func TestNewTupleList(t *testing.T) {
	_, e := NewTupleList(nil, nil, 0, nil)
	es1 := "Window size should not be less than zero."
	if !reflect.DeepEqual(es1, e.Error()) {
		t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", es1, e)
	}

	_, e = NewTupleList(nil, nil, 2, nil)
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
		expRestTuples []xsql.EventRow
	}{
		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   5,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuples{
				{
					Content: []xsql.Row{
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
			expRestTuples: []xsql.EventRow{
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

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   3,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuples{
				{
					Content: []xsql.Row{
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
			expRestTuples: []xsql.EventRow{
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

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   2,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuples{
				{
					Content: []xsql.Row{
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

			expRestTuples: []xsql.EventRow{
				&xsql.Tuple{
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
			expRestTuples: []xsql.EventRow{
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

				cw, err := tt.tuplelist.nextCountWindow()
				require.NoError(t, err)

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

func TestGCInputsForConditionNotMatch(t *testing.T) {
	o := &WindowOperator{
		defaultSinkNode: &defaultSinkNode{
			defaultNode: &defaultNode{
				name: "1",
			},
		},
		window: &WindowConfig{
			Length: time.Second,
			Type:   ast.SLIDING_WINDOW,
		},
		isOverlapWindow: true,
	}
	tuples := []xsql.EventRow{
		&xsql.Tuple{
			Timestamp: time.UnixMilli(3000),
		},
		&xsql.Tuple{
			Timestamp: time.UnixMilli(4000),
		},
		&xsql.Tuple{
			Timestamp: time.UnixMilli(5000),
		},
	}
	o.triggerTime = time.UnixMilli(1)
	inputs := o.gcInputs(tuples, time.UnixMilli(4500), context.Background())
	require.Equal(t, []xsql.EventRow{
		&xsql.Tuple{
			Timestamp: time.UnixMilli(4000),
		},
		&xsql.Tuple{
			Timestamp: time.UnixMilli(5000),
		},
	}, inputs)
}

func TestCollectConditionMatch(t *testing.T) {
	fv, _ := xsql.NewFunctionValuersForOp(context.Background())
	row := &xsql.Tuple{Message: map[string]any{"a": 1}}
	field := &ast.FieldRef{Name: "a", StreamName: ast.DefaultStream}
	missing := &ast.FieldRef{Name: "b", StreamName: ast.DefaultStream}
	tests := []struct {
		name      string
		condition ast.Expr
		match     bool
		err       string
	}{
		{name: "no condition", condition: nil, match: true},
		{name: "true", condition: &ast.BinaryExpr{OP: ast.EQ, LHS: field, RHS: &ast.IntegerLiteral{Val: 1}}, match: true},
		{name: "false", condition: &ast.BinaryExpr{OP: ast.GT, LHS: field, RHS: &ast.IntegerLiteral{Val: 1}}, match: false},
		{name: "nil is false", condition: missing, match: false},
		// same error as FilterOp: a non-boolean result is not coerced to false
		{name: "non-boolean", condition: field, err: "run Where error: invalid condition that returns non-bool value int(1)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := collectConditionMatch(fv, row, tt.condition)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

// TestCollectEndsSpanOfFilteredRow verifies that a row rejected by the collect
// filter has its trace span removed from tupleSpanMap. The row never enters the
// window inputs, so the gc/emit paths that normally clean the map never see it.
func TestCollectEndsSpanOfFilteredRow(t *testing.T) {
	o, err := NewWindowOp("window", WindowConfig{
		Type:   ast.SLIDING_WINDOW,
		Length: time.Second,
		CollectCondition: &ast.BinaryExpr{
			OP:  ast.GT,
			LHS: &ast.FieldRef{Name: "a", StreamName: ast.DefaultStream},
			RHS: &ast.IntegerLiteral{Val: 1},
		},
	}, &def.RuleOption{BufferLength: 10})
	require.NoError(t, err)

	ctx := context.Background()
	ctx.EnableTracer(true)
	fv, _ := xsql.NewFunctionValuersForOp(ctx)

	kept := &xsql.Tuple{Message: map[string]any{"a": 2}}
	filtered := &xsql.Tuple{Message: map[string]any{"a": 1}}
	o.tupleSpanMap[kept] = noop.Span{}
	o.tupleSpanMap[filtered] = noop.Span{}

	inputs := o.collect(ctx, fv, nil, kept)
	inputs = o.collect(ctx, fv, inputs, filtered)

	require.Equal(t, []xsql.EventRow{kept}, inputs)
	require.Contains(t, o.tupleSpanMap, xsql.EventRow(kept), "the span of a collected row is ended by the gc/emit paths")
	require.NotContains(t, o.tupleSpanMap, xsql.EventRow(filtered), "the span of a filtered row must not stay in tupleSpanMap")
}
