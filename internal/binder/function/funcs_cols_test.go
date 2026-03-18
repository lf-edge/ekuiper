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

package function

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestValidation(t *testing.T) {
	f, ok := builtins["changed_cols"]
	if !ok {
		t.Fatal("builtin not found")
	}
	tests := []struct {
		args []ast.Expr
		err  error
	}{
		{
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
			},
			err: fmt.Errorf("expect more than two args but got 1"),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
			},
			err: fmt.Errorf("expect more than two args but got 2"),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
				&ast.StringLiteral{Val: "baz"},
			},
			err: fmt.Errorf("Expect bool type for parameter 2"),
		}, {
			args: []ast.Expr{
				&ast.IntegerLiteral{Val: 20},
				&ast.BooleanLiteral{Val: true},
				&ast.StringLiteral{Val: "baz"},
			},
			err: fmt.Errorf("Expect string type for parameter 1"),
		}, {
			args: []ast.Expr{
				&ast.FieldRef{
					StreamName: "demo",
					Name:       "a",
					AliasRef:   nil,
				},
				&ast.BooleanLiteral{Val: true},
				&ast.StringLiteral{Val: "baz"},
			},
			err: nil,
		},
	}
	for i, tt := range tests {
		err := f.val(nil, tt.args)
		if !reflect.DeepEqual(err, tt.err) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, err, tt.err)
		}
	}
}

func TestExec(t *testing.T) {
	f, ok := builtins["changed_cols"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 1)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 3
			args: []interface{}{
				"ab_",
				true,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				IndexValues: []interface{}{nil, nil, "baz", 44},
				Keys:        []string{"a", "b", "col1", "col2"},
			},
		}, { // 4
			args: []interface{}{
				"ab_",
				true,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				Keys: []string{"a", "b", "col1", "col2"},
			},
		}, { // 5
			args: []interface{}{
				"cd_",
				true,
				"baz",
				45,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				IndexValues: []interface{}{nil, nil, nil, 45},
				Keys:        []string{"a", "b", "col1", "col2"},
			},
		}, 		{ // 6
			args: []interface{}{
				"ab_",
				true,
				"foo",
				46,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				IndexValues: []interface{}{nil, nil, "foo", 46},
				Keys:        []string{"a", "b", "col1", "col2"},
			},
		}, { // 7
			args: []interface{}{
				"ab_",
				true,
				"foo",
				46,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				Keys: []string{"a", "b", "col1", "col2"},
			},
		}, { // 8
			args: []interface{}{
				"ab_",
				true,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				IndexValues: []interface{}{nil, nil, "baz", 44},
				Keys:        []string{"a", "b", "col1", "col2"},
			},
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}
