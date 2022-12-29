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

package node

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"testing"
	"time"
)

func TestTuple(t *testing.T) {
	inputs := []*xsql.Tuple{
		{
			Message: map[string]interface{}{
				"f1": "v1",
				"f2": 45.6,
			},
		}, {
			Message: map[string]interface{}{
				"f1": "v2",
				"f2": 46.6,
			},
		}, {
			Message: map[string]interface{}{
				"f1": "v2",
				"f2": 26.6,
			},
		}, {
			Message: map[string]interface{}{
				"f1": "v2",
				"f2": 54.3,
			},
		}, {
			Message: map[string]interface{}{
				"f1": "v1",
				"f2": 36.6,
			},
		}, {
			Message: map[string]interface{}{
				"f1": "v1",
				"f2": 76.6,
			},
		}, {
			Message: map[string]interface{}{
				"f1": "v2",
				"f2": 41.2,
			},
		}, {
			Message: map[string]interface{}{
				"f1": "v2",
				"f2": 86.6,
			},
		},
	}
	outputs := [][]*xsql.Tuple{
		{ // f2 > 40
			{
				Message: map[string]interface{}{
					"f1": "v1",
					"f2": 45.6,
				},
			}, {
				Message: map[string]interface{}{
					"f1": "v2",
					"f2": 46.6,
				},
			}, {
				Message: map[string]interface{}{
					"f1": "v2",
					"f2": 54.3,
				},
			}, {
				Message: map[string]interface{}{
					"f1": "v1",
					"f2": 76.6,
				},
			}, {
				Message: map[string]interface{}{
					"f1": "v2",
					"f2": 41.2,
				},
			}, {
				Message: map[string]interface{}{
					"f1": "v2",
					"f2": 86.6,
				},
			},
		},
		{ // f1 == v1
			{
				Message: map[string]interface{}{
					"f1": "v1",
					"f2": 45.6,
				},
			}, {
				Message: map[string]interface{}{
					"f1": "v1",
					"f2": 36.6,
				},
			}, {
				Message: map[string]interface{}{
					"f1": "v1",
					"f2": 76.6,
				},
			},
		},
		{ // f1 == v2 && f2 < 40
			{
				Message: map[string]interface{}{
					"f1": "v2",
					"f2": 26.6,
				},
			},
		},
	}

	sn, err := NewSwitchNode("test", &SwitchConfig{
		Cases: []ast.Expr{
			&ast.BinaryExpr{
				LHS: &ast.FieldRef{Name: "f2"},
				OP:  ast.GT,
				RHS: &ast.NumberLiteral{Val: 40},
			},
			&ast.BinaryExpr{
				LHS: &ast.FieldRef{Name: "f1"},
				OP:  ast.EQ,
				RHS: &ast.StringLiteral{Val: "v1"},
			},
			&ast.BinaryExpr{
				LHS: &ast.BinaryExpr{
					LHS: &ast.FieldRef{Name: "f1"},
					OP:  ast.EQ,
					RHS: &ast.StringLiteral{Val: "v2"},
				},
				OP: ast.AND,
				RHS: &ast.BinaryExpr{
					LHS: &ast.FieldRef{Name: "f2"},
					OP:  ast.LT,
					RHS: &ast.NumberLiteral{Val: 40},
				},
			},
		},
		StopAtFirstMatch: false,
	}, &api.RuleOption{})
	if err != nil {
		t.Fatalf("Failed to create switch node: %v", err)
	}
	contextLogger := conf.Log.WithField("rule", "TestSwitchTuple")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	errCh := make(chan error)
	output1 := make(chan interface{}, 10)
	output2 := make(chan interface{}, 10)
	output3 := make(chan interface{}, 10)
	sn.outputNodes[0].AddOutput(output1, "output1")
	sn.outputNodes[1].AddOutput(output2, "output2")
	sn.outputNodes[2].AddOutput(output3, "output3")
	go sn.Exec(ctx, errCh)
	go func() {
		for i, input := range inputs {
			select {
			case sn.input <- input:
				fmt.Println("send input", i)
			case <-time.After(time.Second):
				t.Fatalf("Timeout sending input %d", i)
			}
		}
	}()
	actualOuts := make([][]*xsql.Tuple, 3)
outterFor:
	for {
		select {
		case err := <-errCh:
			t.Fatalf("Error received: %v", err)
		case out1 := <-output1:
			actualOuts[0] = append(actualOuts[0], out1.(*xsql.Tuple))
		case out2 := <-output2:
			actualOuts[1] = append(actualOuts[1], out2.(*xsql.Tuple))
		case out3 := <-output3:
			actualOuts[2] = append(actualOuts[2], out3.(*xsql.Tuple))
		case <-time.After(100 * time.Millisecond):
			break outterFor
		}
	}
	if !reflect.DeepEqual(actualOuts, outputs) {
		t.Errorf("Expected: %v, actual: %v", outputs, actualOuts)
	}
}

func TestCollection(t *testing.T) {
	inputs := []*xsql.WindowTuples{
		{
			Content: []xsql.TupleRow{
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v1",
						"f2": 45.6,
					},
				},
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v1",
						"f2": 65.6,
					},
				},
			},
		}, {
			Content: []xsql.TupleRow{
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v2",
						"f2": 46.6,
					},
				},
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v2",
						"f2": 26.6,
					},
				},
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v2",
						"f2": 54.3,
					},
				},
			},
		}, {
			Content: []xsql.TupleRow{
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v1",
						"f2": 36.6,
					},
				},
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v1",
						"f2": 76.6,
					},
				},
				&xsql.Tuple{
					Message: map[string]interface{}{
						"f1": "v2",
						"f2": 41.2,
					},
				},
			},
		},
	}
	outputs := [][]*xsql.WindowTuples{
		{ // avg(f2) > 50
			{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v1",
							"f2": 45.6,
						},
					},
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v1",
							"f2": 65.6,
						},
					},
				},
			}, {
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v1",
							"f2": 36.6,
						},
					},
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v1",
							"f2": 76.6,
						},
					},
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v2",
							"f2": 41.2,
						},
					},
				},
			},
		},
		{ // else
			{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v2",
							"f2": 46.6,
						},
					},
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v2",
							"f2": 26.6,
						},
					},
					&xsql.Tuple{
						Message: map[string]interface{}{
							"f1": "v2",
							"f2": 54.3,
						},
					},
				},
			},
		},
	}

	sn, err := NewSwitchNode("test", &SwitchConfig{
		Cases: []ast.Expr{
			&ast.BinaryExpr{
				LHS: &ast.Call{
					Name:     "avg",
					FuncId:   0,
					FuncType: ast.FuncTypeAgg,
					Args:     []ast.Expr{&ast.FieldRef{Name: "f2"}},
				},
				OP:  ast.GT,
				RHS: &ast.NumberLiteral{Val: 50},
			},
			&ast.BinaryExpr{
				LHS: &ast.Call{
					Name:     "avg",
					FuncId:   0,
					FuncType: ast.FuncTypeAgg,
					Args:     []ast.Expr{&ast.FieldRef{Name: "f2"}},
				},
				OP:  ast.LTE,
				RHS: &ast.NumberLiteral{Val: 50},
			},
		},
		StopAtFirstMatch: true,
	}, &api.RuleOption{})
	if err != nil {
		t.Fatalf("Failed to create switch node: %v", err)
	}
	contextLogger := conf.Log.WithField("rule", "TestSwitchWindow")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	errCh := make(chan error)
	output1 := make(chan interface{}, 10)
	output2 := make(chan interface{}, 10)
	sn.outputNodes[0].AddOutput(output1, "output1")
	sn.outputNodes[1].AddOutput(output2, "output2")
	go sn.Exec(ctx, errCh)
	go func() {
		for i, input := range inputs {
			select {
			case sn.input <- input:
				fmt.Println("send input", i)
			case <-time.After(time.Second):
				t.Fatalf("Timeout sending input %d", i)
			}
		}
	}()
	actualOuts := make([][]*xsql.WindowTuples, 2)
outterFor:
	for {
		select {
		case err := <-errCh:
			t.Fatalf("Error received: %v", err)
		case out1 := <-output1:
			actualOuts[0] = append(actualOuts[0], out1.(*xsql.WindowTuples))
		case out2 := <-output2:
			actualOuts[1] = append(actualOuts[1], out2.(*xsql.WindowTuples))
		case <-time.After(100 * time.Millisecond):
			break outterFor
		}
	}
	if !reflect.DeepEqual(actualOuts, outputs) {
		t.Errorf("Expected: %v, actual: %v", outputs, actualOuts)
	}
}
