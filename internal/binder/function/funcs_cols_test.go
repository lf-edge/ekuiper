// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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
	var nilResult ResultCols
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
			},
			result: fmt.Errorf("the last arg is not the key list but got baz"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
				[]string{"baz"},
			},
			result: fmt.Errorf("expect more than two args but got 2"),
		}, { // 2
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				[]string{"baz"},
			},
			result: fmt.Errorf("second arg is not a bool but got bar"),
		}, { // 3
			args: []interface{}{
				"ab_",
				true,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"ab_col1": "baz",
				"ab_col2": 44,
			},
		}, { // 4
			args: []interface{}{
				"ab_",
				true,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: nilResult,
		}, { // 5
			args: []interface{}{
				"cd_",
				true,
				"baz",
				45,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"cd_col2": 45,
			},
		}, { // 6
			args: []interface{}{
				"ab_",
				true,
				"foo",
				46,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"ab_col1": "foo",
				"ab_col2": 46,
			},
		}, { // 7
			args: []interface{}{
				"ab_",
				true,
				"foo",
				46,
				[]string{"a", "b", "col1", "col2"},
			},
			result: nilResult,
		}, { // 8
			args: []interface{}{
				"ab_",
				true,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"ab_col1": "baz",
				"ab_col2": 44,
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

func TestExecIgnoreNull(t *testing.T) {
	f, ok := builtins["changed_cols"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 1)
	var nilResult ResultCols
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
			},
			result: fmt.Errorf("the last arg is not the key list but got baz"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
				[]string{"baz"},
			},
			result: fmt.Errorf("expect more than two args but got 2"),
		}, { // 2
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				[]string{"baz"},
			},
			result: fmt.Errorf("second arg is not a bool but got bar"),
		}, { // 3
			args: []interface{}{
				"ab_",
				false,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"ab_col1": "baz",
				"ab_col2": 44,
			},
		}, { // 4
			args: []interface{}{
				"ab_",
				false,
				nil,
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"ab_col1": nil,
			},
		}, { // 5
			args: []interface{}{
				"cd_",
				false,
				"baz",
				45,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"cd_col1": "baz",
				"cd_col2": 45,
			},
		}, { // 6
			args: []interface{}{
				"ab_",
				true,
				"foo",
				46,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"ab_col1": "foo",
				"ab_col2": 46,
			},
		}, { // 7
			args: []interface{}{
				"ab_",
				true,
				"foo",
				46,
				[]string{"a", "b", "col1", "col2"},
			},
			result: nilResult,
		}, { // 8
			args: []interface{}{
				"ab_",
				true,
				"baz",
				44,
				[]string{"a", "b", "col1", "col2"},
			},
			result: ResultCols{
				"ab_col1": "baz",
				"ab_col2": 44,
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

func TestChangedValueEqual(t *testing.T) {
	tests := []struct {
		name string
		v1   any
		v2   any
		want bool
	}{
		{name: "nil", want: true},
		{name: "one nil", v1: int64(1), want: false},
		{name: "string", v1: "value", v2: "value", want: true},
		{name: "different string", v1: "value", v2: "other", want: false},
		{name: "int64", v1: int64(1), v2: int64(1), want: true},
		{name: "different numeric types", v1: int64(1), v2: int(1), want: false},
		{name: "float64", v1: float64(1.5), v2: float64(1.5), want: true},
		{name: "bool", v1: true, v2: true, want: true},
		{name: "int", v1: int(1), v2: int(1), want: true},
		{name: "slice fallback", v1: []int{1, 2}, v2: []int{1, 2}, want: true},
		{name: "map fallback", v1: map[string]any{"a": 1}, v2: map[string]any{"a": 2}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := changedValueEqual(tt.v1, tt.v2); got != tt.want {
				t.Errorf("changedValueEqual(%v, %v) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

func BenchmarkChangedColsStable(b *testing.B) {
	contextLogger := conf.Log.WithField("rule", "BenchmarkChangedColsStable")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, err := state.CreateStore("BenchmarkChangedColsStable", def.AtMostOnce)
	if err != nil {
		b.Fatal(err)
	}
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("BenchmarkChangedColsStable", "changed_cols", tempStore), 1)
	args := []any{"p_", true, "same", int64(42), true}
	keys := []string{"prefix", "ignore", "string", "integer", "boolean"}
	if _, err := changedFunc(fctx, args, keys); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := changedFunc(fctx, args, keys); err != nil {
			b.Fatal(err)
		}
	}
}
