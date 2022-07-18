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
	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"testing"
)

func TestChangedColValidation(t *testing.T) {
	f, ok := builtins["changed_col"]
	if !ok {
		t.Fatal("builtin not found")
	}
	var tests = []struct {
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
	var tests = []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 1
			args: []interface{}{
				true,
				"bar",
			},
			result: "bar",
		}, { // 2
			args: []interface{}{
				true,
				"bar",
			},
			result: nil,
		}, { // 3
			args: []interface{}{
				true,
				"baz",
			},
			result: "baz",
		}, { // 4
			args: []interface{}{
				false,
				nil,
			},
			result: nil,
		}, { // 5
			args: []interface{}{
				false,
				"baz",
			},
			result: "baz",
		}, { // 6
			args: []interface{}{
				true,
				"foo",
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

func TestToMap(t *testing.T) {
	f, ok := builtins["object_construct"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	var tests = []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
			},
			result: map[string]interface{}{
				"foo": "bar",
			},
		}, { // 1
			args: []interface{}{
				true,
				"bar",
			},
			result: fmt.Errorf("key true is not a string"),
		}, { // 2
			args: []interface{}{
				"key1",
				"bar",
				"key2",
				"foo",
			},
			result: map[string]interface{}{
				"key1": "bar",
				"key2": "foo",
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

func TestHadChangedValidation(t *testing.T) {
	f, ok := builtins["had_changed"]
	if !ok {
		t.Fatal("builtin not found")
	}
	var tests = []struct {
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
	var tests = []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 2
			args: []interface{}{
				true,
				"bar",
				20,
			},
			result: true,
		}, { // 3
			args: []interface{}{
				true,
				"baz",
				44,
			},
			result: true,
		}, { // 4
			args: []interface{}{
				true,
				"baz",
				44,
			},
			result: false,
		}, { // 5
			args: []interface{}{
				true,
				"foo",
				44,
			},
			result: true,
		}, { // 6
			args: []interface{}{
				true,
				"foo",
				nil,
			},
			result: false,
		}, { // 7
			args: []interface{}{
				true,
				"foo",
				44,
			},
			result: false,
		}, { // 8
			args: []interface{}{
				true,
				"baz",
				44,
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
	var tests = []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bar",
			},
			result: fmt.Errorf("first arg is not a bool but got foo"),
		}, { // 2
			args: []interface{}{
				false,
				"bar",
				20,
			},
			result: true,
		}, { // 3
			args: []interface{}{
				false,
				"baz",
				nil,
			},
			result: true,
		}, { // 4
			args: []interface{}{
				false,
				"baz",
				44,
			},
			result: true,
		}, { // 5
			args: []interface{}{
				false,
				nil,
				44,
			},
			result: true,
		}, { // 6
			args: []interface{}{
				false,
				"baz",
				44,
			},
			result: true,
		}, { // 7
			args: []interface{}{
				false,
				"baz",
				44,
			},
			result: false,
		}, { // 8
			args: []interface{}{
				false,
				nil,
				nil,
			},
			result: true,
		}, { // 9
			args: []interface{}{
				false,
				"baz",
				44,
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

func TestLagExec(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	var tests = []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args:   []interface{}{},
			result: fmt.Errorf("expect one two or three args but got 0"),
		}, { // 1
			args: []interface{}{
				"foo",
			},
			result: nil,
		},
		{ // 2
			args: []interface{}{
				"bar",
			},
			result: "foo",
		},
		{ // 3
			args: []interface{}{
				"bar",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
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

func TestLagExecIndexWithDefaultValue(t *testing.T) {
	f, ok := builtins["lag"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	var tests = []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				"baw",
			},
			result: fmt.Errorf("expect one two or three args but got 4"),
		}, { // 1
			args: []interface{}{
				"bar",
				2,
				"no result",
			},
			result: "no result",
		},
		{ // 2
			args: []interface{}{
				"bar",
				2,
				"no result",
			},
			result: "no result",
		},
		{ // 3
			args: []interface{}{
				"foo",
				2,
				"no result",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
				"no result",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
				"no result",
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
	var tests = []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
				"baz",
				"baw",
			},
			result: fmt.Errorf("expect one two or three args but got 4"),
		}, { // 1
			args: []interface{}{
				"bar",
				2,
			},
			result: nil,
		},
		{ // 2
			args: []interface{}{
				"bar",
				2,
			},
			result: nil,
		},
		{ // 3
			args: []interface{}{
				"foo",
				2,
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				"foo",
				2,
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
