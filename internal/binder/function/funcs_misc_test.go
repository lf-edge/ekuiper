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
	"github.com/lf-edge/ekuiper/internal/keyedstate"
	"github.com/lf-edge/ekuiper/internal/testx"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func init() {
	testx.InitEnv()
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
	tests := []struct {
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

func TestCoalesceExec(t *testing.T) {
	f, ok := builtins["coalesce"]
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
				"bar",
				"2",
			},
			result: "foo",
		},
		{ // 2
			args: []interface{}{
				nil,
				"dd",
				"1",
			},
			result: "dd",
		},
		{ // 3
			args: []interface{}{
				"bar",
				nil,
				"1",
			},
			result: "bar",
		},
		{ // 4
			args: []interface{}{
				nil,
				nil,
				"2",
			},
			result: "2",
		},
		{ // 4
			args: []interface{}{
				nil,
				nil,
				nil,
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

func TestToJson(t *testing.T) {
	f, ok := builtins["to_json"]
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
		{ // 0
			args: []interface{}{
				"foo",
			},
			result: `"foo"`,
		}, { // 1
			args: []interface{}{
				nil,
			},
			result: "null",
		}, { // 2
			args: []interface{}{
				map[string]interface{}{
					"key1": "bar",
					"key2": "foo",
				},
			},
			result: `{"key1":"bar","key2":"foo"}`,
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestFromJson(t *testing.T) {
	f, ok := builtins["parse_json"]
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
		{ // 0
			args: []interface{}{
				`"foo"`,
			},
			result: "foo",
		}, { // 1
			args: []interface{}{
				"null",
			},
			result: nil,
		}, { // 2
			args: []interface{}{
				`{"key1":"bar","key2":"foo"}`,
			},
			result: map[string]interface{}{
				"key1": "bar",
				"key2": "foo",
			},
		}, { // 3
			args: []interface{}{
				"key1",
			},
			result: fmt.Errorf("fail to parse json: invalid character 'k' looking for beginning of value"),
		}, { // 4
			args: []interface{}{
				`[{"key1":"bar","key2":"foo"}]`,
			},
			result: []interface{}{
				map[string]interface{}{
					"key1": "bar",
					"key2": "foo",
				},
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

func TestDelay(t *testing.T) {
	f, ok := builtins["delay"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	err := f.val(fctx, []ast.Expr{&ast.StringLiteral{Val: "abc"}})
	if err == nil {
		t.Fatal("expect error")
	}
	err = f.val(fctx, []ast.Expr{&ast.StringLiteral{Val: "1s"}, &ast.StringLiteral{Val: "1s"}})
	if err == nil {
		t.Fatal("expect error")
	}
	err = f.val(fctx, []ast.Expr{&ast.IntegerLiteral{Val: 1000}, &ast.StringLiteral{Val: "1s"}})
	if err != nil {
		t.Fatal("expect no error")
	}

	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				10,
				"bar",
			},
			result: "bar",
		}, { // 1
			args: []interface{}{
				"bar",
				"bar",
			},
			result: fmt.Errorf("cannot convert string(bar) to int"),
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestKeyedStateValidation(t *testing.T) {
	f, ok := builtins["get_keyed_state"]
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
			err: fmt.Errorf("Expect 3 arguments but found 1."),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
			},
			err: fmt.Errorf("Expect 3 arguments but found 2."),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
				&ast.StringLiteral{Val: "barz"},
			},
			err: fmt.Errorf("expect one of following value for the 2nd parameter: bigint, float, string, boolean, datetime"),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bigint"},
				&ast.StringLiteral{Val: "barz"},
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

func TestKeyedStateExec(t *testing.T) {
	keyedstate.InitKeyedStateKV()

	f, ok := builtins["get_keyed_state"]
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
			},
			result: fmt.Errorf("the args must be two or three"),
		}, { // 1
			args: []interface{}{
				"foo",
				"bigint",
				"baz",
				"bar",
			},
			result: fmt.Errorf("the args must be two or three"),
		}, { // 2
			args: []interface{}{
				"foo",
				"float",
				20.0,
			},
			result: 20.0,
		},
	}

	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
	_ = keyedstate.ClearKeyedState()
}

func TestMiscFuncNil(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerMiscFunc()
	for name, function := range builtins {
		switch name {
		case "compress", "decompress", "newuuid", "tstamp", "rule_id", "window_start", "window_end",
			"json_path_query", "json_path_query_first", "coalesce", "meta", "json_path_exists":
			continue
		case "isnull":
			v, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b)
			require.Equal(t, v, true)
		case "cardinality":
			v, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b)
			require.Equal(t, v, 0)
		case "to_json":
			v, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b)
			require.Equal(t, v, "null")
		case "parse_json":
			v, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b)
			require.Equal(t, v, nil)
			v, b = function.exec(fctx, []interface{}{"null"})
			require.True(t, b)
			require.Equal(t, v, nil)
		default:
			v, b := function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, v, fmt.Sprintf("%v failed", name))
		}
	}
}
