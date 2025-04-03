// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/keyedstate"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	testx.InitEnv("function")
}

func TestCoalesceExec(t *testing.T) {
	f, ok := builtins["coalesce"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
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

func TestToSeconds(t *testing.T) {
	f, ok := builtins["to_seconds"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				time.Unix(1e9, 0),
			},
			result: int64(1e9),
		}, { // 1
			args: []interface{}{
				nil,
			},
			result: errors.New("unsupported type to convert to timestamp <nil>"),
		},
	}
	for _, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		assert.Equal(t, tt.result, result)
	}
}

func TestToJson(t *testing.T) {
	f, ok := builtins["to_json"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
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
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
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

func TestConvertTZ(t *testing.T) {
	f, ok := builtins["convert_tz"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	loc, _ := time.LoadLocation("Asia/Shanghai")

	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
				"UTC",
			},
			result: time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
		}, { // 1
			args: []interface{}{
				time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
				"Asia/Shanghai",
			},
			result: time.Date(2022, time.April, 13, 14, 22, 32, 233000000, loc),
		}, { // 2
			args: []interface{}{
				time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
				"Unknown",
			},
			result: errors.New("unknown time zone Unknown"),
		}, { // 3
			args: []interface{}{
				true,
				"UTC",
			},
			result: errors.New("unsupported type to convert to timestamp true"),
		},
	}
	for _, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		assert.Equal(t, tt.result, result)
	}

	vtests := []struct {
		args    []ast.Expr
		wantErr bool
	}{
		{
			[]ast.Expr{&ast.TimeLiteral{Val: 0}, &ast.StringLiteral{Val: "0"}},
			false,
		},
		{
			[]ast.Expr{&ast.StringLiteral{Val: "0"}},
			true,
		},
		{
			[]ast.Expr{&ast.NumberLiteral{Val: 0}, &ast.NumberLiteral{Val: 0}},
			true,
		},
		{
			[]ast.Expr{&ast.NumberLiteral{Val: 0}, &ast.TimeLiteral{Val: 0}},
			true,
		},
		{
			[]ast.Expr{&ast.NumberLiteral{Val: 0}, &ast.BooleanLiteral{Val: true}},
			true,
		},
		{
			[]ast.Expr{&ast.StringLiteral{Val: "0"}, &ast.NumberLiteral{Val: 0}},
			true,
		},
		{
			[]ast.Expr{&ast.BooleanLiteral{Val: true}, &ast.NumberLiteral{Val: 0}},
			true,
		},
	}
	for _, vtt := range vtests {
		err := f.val(fctx, vtt.args)
		if vtt.wantErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
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
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
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
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
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

func TestHexIntFunctions(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		name   string
		args   []interface{}
		result interface{}
	}{
		{
			name: "hex2dec",
			args: []interface{}{
				"0x10",
			},
			result: int64(16),
		},
		{
			name: "dec2hex",
			args: []interface{}{
				16,
			},
			result: "0x10",
		},
	}
	for i, tt := range tests {
		f, ok := builtins[tt.name]
		if !ok {
			t.Fatal(fmt.Sprintf("builtin %v not found", tt.name))
		}
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestMiscFuncNil(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerMiscFunc()
	for name, function := range builtins {
		switch name {
		case "compress", "decompress", "newuuid", "tstamp", "rule_id", "rule_start", "window_start", "window_end", "event_time",
			"json_path_query", "json_path_query_first", "coalesce", "meta", "json_path_exists", "bypass", "get_keyed_state":
			continue
		case "isnull":
			v, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b)
			require.Equal(t, v, true)
		case "cardinality":
			v, b := function.check([]interface{}{nil})
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

func TestCast(t *testing.T) {
	f, ok := builtins["cast"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"Ynl0ZWE=",
				"bytea",
			},
			result: []byte("bytea"),
		},
		{ // 1
			args: []interface{}{
				[]byte("bytea"),
				"bytea",
			},
			result: []byte("bytea"),
		},
		{ // 2
			args: []interface{}{
				1,
				"bytea",
			},
			result: fmt.Errorf("cannot convert int(1) to bytea"),
		},
		{ // 3
			args: []interface{}{
				101.5,
				"bigint",
			},
			result: 101,
		},
		{ // 4
			args: []interface{}{
				1,
				"boolean",
			},
			result: true,
		},
		{ // 5
			args: []interface{}{
				1,
				"float",
			},
			result: float64(1),
		},
		{ // 6
			args: []interface{}{
				1,
				"string",
			},
			result: "1",
		},
	}
	for _, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		assert.Equal(t, tt.result, result)
	}

	vtests := []struct {
		args    []ast.Expr
		wantErr bool
	}{
		{
			[]ast.Expr{&ast.FieldRef{Name: "foo"}, &ast.StringLiteral{Val: "bytea"}},
			false,
		},
		{
			[]ast.Expr{&ast.FieldRef{Name: "foo"}},
			true,
		},
		{
			[]ast.Expr{&ast.FieldRef{Name: "foo"}, &ast.StringLiteral{Val: "bigint"}},
			false,
		},
		{
			[]ast.Expr{&ast.FieldRef{Name: "foo"}, &ast.StringLiteral{Val: "float"}},
			false,
		},
		{
			[]ast.Expr{&ast.FieldRef{Name: "foo"}, &ast.StringLiteral{Val: "string"}},
			false,
		},
		{
			[]ast.Expr{&ast.FieldRef{Name: "foo"}, &ast.StringLiteral{Val: "boolean"}},
			false,
		},
		{
			[]ast.Expr{&ast.FieldRef{Name: "foo"}, &ast.StringLiteral{Val: "test"}},
			true,
		},
	}
	for _, vtt := range vtests {
		err := f.val(fctx, vtt.args)
		if vtt.wantErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestProps(t *testing.T) {
	f, ok := builtins["props"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	e, ok := f.exec(fctx, []any{12})
	require.False(t, ok)
	err, ok := e.(error)
	require.True(t, ok)
	require.EqualError(t, err, "invalid input 12: must be property name of string type")
	tt := timex.GetNowInMilli()
	et, ok := f.exec(fctx, []any{"et"})
	require.True(t, ok)
	require.Equal(t, strconv.FormatInt(tt, 10), et)
}
