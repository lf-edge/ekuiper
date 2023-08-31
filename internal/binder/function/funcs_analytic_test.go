// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestChangedColValidation(t *testing.T) {
	f, ok := builtins["changed_col"]
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
			err: fmt.Errorf("Expect 2 arguments but found 1."),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
			},
			err: fmt.Errorf("Expect boolean type for parameter 1"),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
				&ast.StringLiteral{Val: "baz"},
			},
			err: fmt.Errorf("Expect 2 arguments but found 3."),
		}, {
			args: []ast.Expr{
				&ast.BooleanLiteral{Val: true},
				&ast.StringLiteral{Val: "baz"},
			},
		},
	}
	for i, tt := range tests {
		err := f.val(nil, tt.args)
		if !reflect.DeepEqual(err, tt.err) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, err, tt.err)
		}
	}
}

func TestChangedColExec(t *testing.T) {
	f, ok := builtins["changed_col"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				true,
				"bar",
				true,
				"self",
			},
			result: "bar",
		}, { // 2
			args: []interface{}{
				true,
				"bar",
				true,
				"self",
			},
			result: nil,
		}, { // 3
			args: []interface{}{
				true,
				"baz",
				true,
				"self",
			},
			result: "baz",
		}, { // 4
			args: []interface{}{
				false,
				nil,
				true,
				"self",
			},
			result: nil,
		}, { // 5
			args: []interface{}{
				false,
				"baz",
				true,
				"self",
			},
			result: "baz",
		}, { // 6
			args: []interface{}{
				true,
				"foo",
				true,
				"self",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestChangedColPartition(t *testing.T) {
	f, ok := builtins["changed_col"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				true,
				"bar",
				true,
				"2",
			},
			result: "bar",
		}, { // 2
			args: []interface{}{
				true,
				"bar",
				true,
				"1",
			},
			result: "bar",
		}, { // 3
			args: []interface{}{
				true,
				"baz",
				true,
				"2",
			},
			result: "baz",
		}, { // 4
			args: []interface{}{
				false,
				nil,
				true,
				"1",
			},
			result: nil,
		}, { // 5
			args: []interface{}{
				false,
				"baz",
				true,
				"2",
			},
			result: nil,
		}, { // 6
			args: []interface{}{
				true,
				"foo",
				true,
				"1",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestChangedColPartitionWithWhen(t *testing.T) {
	f, ok := builtins["changed_col"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				true,
				"bar",
				true,
				"2",
			},
			result: "bar",
		}, { // 2
			args: []interface{}{
				true,
				"bar",
				true,
				"1",
			},
			result: "bar",
		}, { // 3
			args: []interface{}{
				true,
				"baz",
				true,
				"2",
			},
			result: "baz",
		}, { // 3.1 copy of 3 with baz changed to bar and when condition false
			args: []interface{}{
				true,
				"bar",
				false,
				"2",
			},
			result: nil,
		}, { // 4
			args: []interface{}{
				false,
				nil,
				true,
				"1",
			},
			result: nil,
		}, { // 5
			args: []interface{}{
				false,
				"baz",
				true,
				"2",
			},
			result: nil,
		}, { // 6
			args: []interface{}{
				true,
				"foo",
				true,
				"1",
			},
			result: "foo",
		}, { // 7
			args: []interface{}{
				true,
				"bar",
				false,
				"1",
			},
			result: nil,
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestHadChangedValidation(t *testing.T) {
	f, ok := builtins["had_changed"]
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
			err: fmt.Errorf("expect more than one arg but got 1"),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
				&ast.StringLiteral{Val: "baz"},
			},
			err: fmt.Errorf("Expect bool type for parameter 1"),
		}, {
			args: []ast.Expr{
				&ast.IntegerLiteral{Val: 20},
				&ast.BooleanLiteral{Val: true},
				&ast.StringLiteral{Val: "baz"},
			},
			err: fmt.Errorf("Expect bool type for parameter 1"),
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

func TestHadChangedExec(t *testing.T) {
	f, ok := builtins["had_changed"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 1)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				true,
				"self",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
				true,
				"self",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 2
			args: []interface{}{
				true,
				"bar",
				20,
				true,
				"self",
			},
			result: true,
		}, { // 3
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"self",
			},
			result: true,
		}, { // 4
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"self",
			},
			result: false,
		}, { // 5
			args: []interface{}{
				true,
				"foo",
				44,
				true,
				"self",
			},
			result: true,
		}, { // 6
			args: []interface{}{
				true,
				"foo",
				nil,
				true,
				"self",
			},
			result: false,
		}, { // 7
			args: []interface{}{
				true,
				"foo",
				44,
				true,
				"self",
			},
			result: false,
		}, { // 8
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"self",
			},
			result: true,
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestHadChangedExecAllowNull(t *testing.T) {
	f, ok := builtins["had_changed"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 1)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				true,
				"self",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
				true,
				"self",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 2
			args: []interface{}{
				false,
				"bar",
				20,
				true,
				"self",
			},
			result: true,
		}, { // 3
			args: []interface{}{
				false,
				"baz",
				nil,
				true,
				"self",
			},
			result: true,
		}, { // 4
			args: []interface{}{
				false,
				"baz",
				44,
				true,
				"self",
			},
			result: true,
		}, { // 5
			args: []interface{}{
				false,
				nil,
				44,
				true,
				"self",
			},
			result: true,
		}, { // 6
			args: []interface{}{
				false,
				"baz",
				44,
				true,
				"self",
			},
			result: true,
		}, { // 7
			args: []interface{}{
				false,
				"baz",
				44,
				true,
				"self",
			},
			result: false,
		}, { // 8
			args: []interface{}{
				false,
				nil,
				nil,
				true,
				"self",
			},
			result: true,
		}, { // 9
			args: []interface{}{
				false,
				"baz",
				44,
				true,
				"self",
			},
			result: true,
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestHadChangedPartition(t *testing.T) {
	f, ok := builtins["had_changed"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 1)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				true,
				"1",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
				true,
				"1",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 2
			args: []interface{}{
				true,
				"bar",
				20,
				true,
				"3",
			},
			result: true,
		}, { // 3
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"2",
			},
			result: true,
		}, { // 4
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"2",
			},
			result: false,
		}, { // 5
			args: []interface{}{
				true,
				"foo",
				44,
				true,
				"3",
			},
			result: true,
		}, { // 6
			args: []interface{}{
				true,
				"foo",
				nil,
				true,
				"1",
			},
			result: true,
		}, { // 7
			args: []interface{}{
				true,
				"foo",
				44,
				true,
				"2",
			},
			result: true,
		}, { // 8
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"3",
			},
			result: true,
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestHadChangedPartitionWithWhen(t *testing.T) {
	f, ok := builtins["had_changed"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 1)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				true,
				"1",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
				true,
				"1",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 2
			args: []interface{}{
				true,
				"bar",
				20,
				true,
				"3",
			},
			result: true,
		}, { // 3
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"2",
			},
			result: true,
		}, { // 4
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"2",
			},
			result: false,
		}, { // 5
			args: []interface{}{
				true,
				"baz",
				44,
				true,
				"2",
			},
			result: false,
		}, { // 6
			args: []interface{}{
				true,
				"foo",
				45,
				false,
				"2",
			},
			result: false,
		}, { // 7
			args: []interface{}{
				true,
				"foo",
				nil,
				true,
				"1",
			},
			result: true,
		}, { // 8
			args: []interface{}{
				true,
				"foo",
				44,
				true,
				"2",
			},
			result: true,
		}, { // 9
			args: []interface{}{
				true,
				"baz",
				44,
				false,
				"3",
			},
			result: false,
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLagExec(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: nil,
		},
		{ // 2
			args: []interface{}{
				"bar",
				true,
				"self",
			},
			result: "foo",
		},
		{ // 3
			args: []interface{}{
				"bar",
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLagPartition(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"foo",
				true,
				"1",
			},
			result: nil,
		},
		{ // 2
			args: []interface{}{
				"bar",
				true,
				"1",
			},
			result: "foo",
		},
		{ // 3
			args: []interface{}{
				"bar",
				true,
				"2",
			},
			result: nil,
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"1",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"2",
			},
			result: "bar",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLagExecWithWhen(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: nil,
		},
		{ // 2
			args: []interface{}{
				"bar",
				false,
				"self",
			},
			result: "foo",
		},
		{ // 3
			args: []interface{}{
				"bar",
				true,
				"self",
			},
			result: "foo",
		},
		{ // 4
			args: []interface{}{
				"foo",
				false,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: "bar",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLagPartitionWithWhen(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"foo",
				true,
				"1",
			},
			result: nil,
		},
		{ // 2
			args: []interface{}{
				"bar",
				false,
				"1",
			},
			result: "foo",
		},
		{ // 3
			args: []interface{}{
				"bar",
				true,
				"2",
			},
			result: nil,
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"1",
			},
			result: "foo",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"2",
			},
			result: "bar",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLagExecIndexWithDefaultValue(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"bar",
				2,
				"no result",
				true,
				"self",
			},
			result: "no result",
		},
		{ // 2
			args: []interface{}{
				"bar",
				2,
				"no result",
				true,
				"self",
			},
			result: "no result",
		},
		{ // 3
			args: []interface{}{
				"foo",
				2,
				"no result",
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
				"no result",
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
				"no result",
				true,
				"self",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLagExecIndex(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"bar",
				2,
				true,
				"self",
			},
			result: nil,
		},
		{ // 2
			args: []interface{}{
				"bar",
				2,
				true,
				"self",
			},
			result: nil,
		},
		{ // 3
			args: []interface{}{
				"foo",
				2,
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
				true,
				"self",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLatestExec(t *testing.T) {
	f, ok := builtins["latest"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				nil,
				true,
				"self",
			},
			result: nil,
		},
		{ // 1
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: "foo",
		},
		{ // 2
			args: []interface{}{
				nil,
				true,
				"self",
			},
			result: "foo",
		},
		{ // 3
			args: []interface{}{
				"bar",
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				nil,
				true,
				"self",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLatestExecWithWhen(t *testing.T) {
	f, ok := builtins["latest"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: "foo",
		},
		{ // 2
			args: []interface{}{
				nil,
				true,
				"self",
			},
			result: "foo",
		},
		{ // 3
			args: []interface{}{
				"bar",
				false,
				"self",
			},
			result: "foo",
		},
		{ // 4
			args: []interface{}{
				nil,
				true,
				"self",
			},
			result: "foo",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"self",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestLatestPartition(t *testing.T) {
	f, ok := builtins["latest"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 1
			args: []interface{}{
				"foo",
				true,
				"2",
			},
			result: "foo",
		},
		{ // 2
			args: []interface{}{
				nil,
				"dd",
				true,
				"1",
			},
			result: "dd",
		},
		{ // 3
			args: []interface{}{
				"bar",
				true,
				"1",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				nil,
				true,
				"2",
			},
			result: "foo",
		},
		{ // 4
			args: []interface{}{
				"foo",
				true,
				"1",
			},
			result: "foo",
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestAccumulateAgg(t *testing.T) {
	tests := []struct {
		name     string
		results  []interface{}
		testargs []interface{}
	}{
		{
			name: "acc_count",
			testargs: []interface{}{
				"1",
				float64(1),
				float32(1),
				1,
				int32(1),
				int64(1),
			},
			results: []interface{}{
				1, 2, 3, 4, 5, 6,
			},
		},
		{
			name: "acc_avg",
			testargs: []interface{}{
				"1",
				float64(1),
				float32(1),
				1,
				int32(1),
				int64(1),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(1),
				float64(1),
				float64(1),
				float64(1),
				float64(1),
			},
		},
		{
			name: "acc_max",
			testargs: []interface{}{
				"1",
				float64(1),
				float32(2),
				3,
				int32(4),
				int64(5),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(1),
				float64(2),
				float64(3),
				float64(4),
				float64(5),
			},
		},
		{
			name: "acc_min",
			testargs: []interface{}{
				"1",
				float64(5),
				float32(4),
				3,
				int32(2),
				int64(1),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(5),
				float64(4),
				float64(3),
				float64(2),
				float64(1),
			},
		},
		{
			name: "acc_sum",
			testargs: []interface{}{
				"1",
				float64(1),
				float32(1),
				1,
				int32(1),
				int64(1),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(1),
				float64(2),
				float64(3),
				float64(4),
				float64(5),
			},
		},
	}
	for _, test := range tests {
		f, ok := builtins[test.name]
		require.True(t, ok)
		contextLogger := conf.Log.WithField("rule", "testExec")
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
		fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
		for i, arg := range test.testargs {
			result, _ := f.exec(fctx, []interface{}{arg, true, fmt.Sprintf("%s_key", test.name)})
			require.Equal(t, test.results[i], result)
		}
	}

	tests2 := []struct {
		name   string
		result interface{}
	}{
		{
			"acc_sum",
			float64(0),
		},
		{
			"acc_max",
			0,
		},
		{
			"acc_min",
			0,
		},
		{
			"acc_avg",
			float64(0),
		},
		{
			"acc_count",
			0,
		},
	}
	for _, test := range tests2 {
		f, ok := builtins[test.name]
		require.True(t, ok)
		contextLogger := conf.Log.WithField("rule", "testExec")
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
		fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
		result, b := f.exec(fctx, []interface{}{1, false, fmt.Sprintf("%s_key", test.name)})
		require.True(t, b)
		require.Equal(t, test.result, result)
	}
}
