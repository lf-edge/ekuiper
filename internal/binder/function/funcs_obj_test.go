// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

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
		}, { // 3
			args: []interface{}{
				"key1",
				nil,
				"key2",
				"foo",
				"key3",
				nil,
			},
			result: map[string]interface{}{
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

func TestToMapVal(t *testing.T) {
	f, ok := builtins["object_construct"]
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
			err: fmt.Errorf("the args must be key value pairs"),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
			},
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
				&ast.StringLiteral{Val: "baz"},
			},
			err: fmt.Errorf("the args must be key value pairs"),
		}, {
			args: []ast.Expr{
				&ast.BooleanLiteral{Val: true},
				&ast.StringLiteral{Val: "baz"},
			},
			err: fmt.Errorf("Expect string type for parameter 1"),
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			err := f.val(nil, tt.args)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestObjectFunctions(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		name   string
		args   []interface{}
		result interface{}
	}{
		{
			name: "keys",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
				},
			},
			result: []string{"a", "b"},
		},
		{
			name:   "keys",
			args:   []interface{}{1, 2},
			result: fmt.Errorf("the argument should be map[string]interface{}"),
		},
		{
			name: "values",
			args: []interface{}{
				map[string]interface{}{
					"a": "c",
					"b": "d",
				},
			},
			result: []interface{}{"c", "d"},
		},
		{
			name:   "values",
			args:   []interface{}{1, 2},
			result: fmt.Errorf("the argument should be map[string]interface{}"),
		},
		{
			name: "object",
			args: []interface{}{
				[]interface{}{"a", "b"},
				[]interface{}{1, 2},
			},
			result: map[string]interface{}{
				"a": 1,
				"b": 2,
			},
		},
		{
			name: "object",
			args: []interface{}{
				1,
				[]interface{}{1, 2},
			},
			result: fmt.Errorf("first argument should be []string"),
		},
		{
			name: "object",
			args: []interface{}{
				[]interface{}{1, 2},
				[]interface{}{1, 2},
			},
			result: fmt.Errorf("first argument should be []string"),
		},
		{
			name: "object",
			args: []interface{}{
				[]interface{}{1, 2},
				1,
			},
			result: fmt.Errorf("second argument should be []interface{}"),
		},
		{
			name: "object",
			args: []interface{}{
				[]interface{}{"a", "b"},
				[]interface{}{1, 2, 3},
			},
			result: fmt.Errorf("the length of the arguments should be same"),
		},
		{
			name: "zip",
			args: []interface{}{
				[]interface{}{
					[]interface{}{"a", 1},
					[]interface{}{"b", 2},
				},
			},
			result: map[string]interface{}{
				"a": 1,
				"b": 2,
			},
		},
		{
			name: "zip",
			args: []interface{}{
				1,
			},
			result: fmt.Errorf("each argument should be [][2]interface{}"),
		},
		{
			name: "zip",
			args: []interface{}{
				[]interface{}{
					1, 2,
				},
			},
			result: fmt.Errorf("each argument should be [][2]interface{}"),
		},
		{
			name: "zip",
			args: []interface{}{
				[]interface{}{
					[]interface{}{"a", 1, 3},
					[]interface{}{"b", 2, 4},
				},
			},
			result: fmt.Errorf("each argument should be [][2]interface{}"),
		},
		{
			name: "zip",
			args: []interface{}{
				[]interface{}{
					[]interface{}{1, 3},
					[]interface{}{2, 4},
				},
			},
			result: fmt.Errorf("the first element in the list item should be string"),
		},
		{
			name: "items",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
				},
			},
			result: []interface{}{
				[]interface{}{"a", 1},
				[]interface{}{"b", 2},
			},
		},
		{
			name: "items",
			args: []interface{}{
				1,
			},
			result: fmt.Errorf("first argument should be map[string]interface{}"),
		},
		{
			name: "element_at",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
				},
				"a",
			},
			result: 1,
		},
		{
			name: "element_at",
			args: []interface{}{
				"1",
				"a",
			},
			result: fmt.Errorf("first argument should be []interface{} or map[string]interface{}"),
		},
		{
			name: "element_at",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
				},
				2,
			},
			result: fmt.Errorf("second argument should be string"),
		},
		{
			name: "object_concat",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
				},
				map[string]interface{}{
					"b": 3,
					"c": 4,
				},
				map[string]interface{}{
					"a": 2,
					"d": 1,
				},
			},
			result: map[string]interface{}{
				"a": 2,
				"b": 3,
				"c": 4,
				"d": 1,
			},
		},
		{
			name: "object_concat",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
				},
				map[string]interface{}{
					"b": 3,
					"c": 4,
				},
				[]interface{}{
					1,
					2,
				},
			},
			result: fmt.Errorf("the argument should be map[string]interface{}, got %v", []interface{}{1, 2}),
		},
		{
			name: "object_concat",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
				},
				map[string]interface{}{
					"b": 3,
					"c": 4,
				},
				nil,
			},
			result: map[string]interface{}{
				"a": 1,
				"b": 3,
				"c": 4,
			},
		},
		{
			name: "erase",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				"a",
			},
			result: map[string]interface{}{
				"b": 2,
				"c": 3,
			},
		},
		{
			name: "erase",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				[]string{
					"a",
					"b",
				},
			},
			result: map[string]interface{}{
				"c": 3,
			},
		},
		{
			name: "erase",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				[]string{
					"a",
					"b",
				},
				"c",
			},
			result: fmt.Errorf("the argument number should be 2, got 3"),
		},
		{
			name: "object_pick",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				[]string{
					"a",
					"c",
				},
			},
			result: map[string]interface{}{
				"a": 1,
				"c": 3,
			},
		},
		{
			name: "object_pick",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				"a",
			},
			result: map[string]interface{}{
				"a": 1,
			},
		},
		{
			name: "object_pick",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				"d",
			},
			result: map[string]interface{}{},
		},
		{
			name: "obj_to_kvpair_array",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
				},
			},
			result: []map[string]interface{}{
				{kvPairKName: "a", kvPairVName: 1},
			},
		},
		{
			name: "obj_to_kvpair_array",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": []string{"foo", "bar"},
				},
			},
			result: []map[string]interface{}{
				{kvPairKName: "a", kvPairVName: 1},
				{kvPairKName: "b", kvPairVName: []string{"foo", "bar"}},
			},
		},
		{
			name: "object_size",
			args: []interface{}{
				map[string]interface{}{
					"a": 1,
					"b": []string{"foo", "bar"},
				},
			},
			result: 2,
		},
		{
			name: "object_size",
			args: []interface{}{
				nil,
			},
			result: 0,
		},
	}
	fe := funcExecutor{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := fe.ExecWithName(tt.args, fctx, tt.name)
			switch r := result.(type) {
			case []string:
				sort.Strings(r)
				assert.Equal(t, tt.result, result)
			case []interface{}:
				assert.ElementsMatch(t, tt.result, result)
			default:
				assert.Equal(t, tt.result, result)
			}
		})
	}
}

// pick with split 56.25 ns/op, split 35 ns/op
// pick raw 15.57 ns/op
// pick with contain 22 ns/op
// exec with contain 187 ns/op
// exec nothing 31.34 ns/op
// TODO reduce function call overhead
func BenchmarkPick(t *testing.B) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	// res := make(map[string]any)
	arg := map[string]interface{}{"k1": map[string]any{"temp": 23, "hum": 34}, "k2": "2", "k3": map[string]any{"temp": 23, "hum": 34}, "k4": map[string]any{"embed": map[string]any{"ee": 23, "ff": map[string]any{"gg": 23}}}}
	f, ok := builtins["object_pick"]
	require.True(t, ok)
	for i := 0; i < t.N; i++ {
		f.exec(fctx, []interface{}{arg, "k2"})
		// pick(fctx, res, arg, "k2")
	}
}

func TestObjectFunctionsNil(t *testing.T) {
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerObjectFunc()
	for name, function := range builtins {
		if function.check != nil {
			r, b := function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		}
	}
}

func TestObjectFuncArgNil(t *testing.T) {
	registerObjectFunc()
	tests := []struct {
		funcName string
		args     []interface{}
		result   interface{}
	}{
		{
			funcName: "object_pick",
			args: []interface{}{
				map[string]interface{}{"k1": nil, "k2": "2"},
				"k1",
			},
			result: map[string]interface{}{
				"k1": nil,
			},
		},
		{
			funcName: "erase",
			args: []interface{}{
				map[string]interface{}{"k1": nil, "k2": "2"},
				"k1",
			},
			result: map[string]interface{}{
				"k2": "2",
			},
		},
		{
			funcName: "object_construct",
			args: []interface{}{
				nil, "v1", "k2", "v2",
			},
			result: map[string]interface{}{
				"k2": "v2",
			},
		},
		{
			funcName: "object_concat",
			args: []interface{}{
				map[string]interface{}{"k1": "v1"},
				nil,
				map[string]interface{}{"k2": "v2"},
			},
			result: map[string]interface{}{
				"k1": "v1",
				"k2": "v2",
			},
		},
		{
			funcName: "items",
			args: []interface{}{
				map[string]interface{}{"k2": nil},
			},
			result: []interface{}{[]interface{}{"k2", nil}},
		},
		{
			funcName: "zip",
			args: []interface{}{
				[]interface{}{[]interface{}{"k1", "v1"}, nil, []interface{}{"k2", "v2"}},
			},
			result: map[string]interface{}{"k1": "v1", "k2": "v2"},
		},
		{
			funcName: "object",
			args: []interface{}{
				[]interface{}{"k1"},
				[]interface{}{nil},
			},
			result: map[string]interface{}{"k1": nil},
		},
		{
			funcName: "values",
			args: []interface{}{
				map[string]interface{}{"k": nil},
			},
			result: []interface{}{nil},
		},
	}
	for _, tt := range tests {
		f, ok := builtins[tt.funcName]
		require.True(t, ok)
		r, ok := f.exec(nil, tt.args)
		require.True(t, ok)
		require.Equal(t, tt.result, r, tt.funcName)
	}
}

func TestObjectPick(t *testing.T) {
	registerObjectFunc()
	tests := []struct {
		name   string
		args   []interface{}
		result interface{}
	}{
		{
			name: "pick one",
			args: []interface{}{
				map[string]interface{}{"k1": 23, "k2": "2"},
				"k2",
			},
			result: map[string]interface{}{
				"k2": "2",
			},
		},
		{
			name: "pick one with embed",
			args: []interface{}{
				map[string]interface{}{"k1": map[string]any{"temp": 23, "hum": 34}, "k2": "2"},
				"k1.temp",
			},
			result: map[string]interface{}{
				"k1": map[string]any{"temp": 23},
			},
		},
		{
			name: "pick one with nil",
			args: []interface{}{
				map[string]interface{}{"k1": nil, "k2": "2"},
				"k1",
			},
			result: map[string]interface{}{
				"k1": nil,
			},
		},
		{
			name: "pick with invalid arg",
			args: []interface{}{
				map[string]interface{}{"k1": nil, "k2": "2"},
				nil,
			},
			result: map[string]interface{}{},
		},
		{
			name: "pick nil map",
			args: []interface{}{
				nil,
				"k1",
			},
			result: nil,
		},
		{
			name: "pick multiple",
			args: []interface{}{
				map[string]interface{}{"k1": map[string]any{"temp": 23, "hum": 34}, "k2": "2", "k3": map[string]any{"temp": 23, "hum": 34}, "k4": map[string]any{"embed": map[string]any{"ee": 23, "ff": map[string]any{"gg": 23}}}},
				"k1.temp", "k2", "k3.hum", "k4.embed.ff",
			},
			result: map[string]interface{}{
				"k1": map[string]any{"temp": 23},
				"k2": "2",
				"k3": map[string]any{"hum": 34},
				"k4": map[string]any{"embed": map[string]any{"ff": map[string]any{"gg": 23}}},
			},
		},
		{
			name: "pick multiple by array",
			args: []interface{}{
				map[string]interface{}{"k1": map[string]any{"temp": 23, "hum": 34}, "k2": "2", "k3": map[string]any{"temp": 23, "hum": 34}, "k4": map[string]any{"embed": map[string]any{"ee": 23, "ff": map[string]any{"gg": 23}}}},
				[]any{"k1.temp", "k2", "k3.hum", "k4.embed.ff"},
			},
			result: map[string]interface{}{
				"k1": map[string]any{"temp": 23},
				"k2": "2",
				"k3": map[string]any{"hum": 34},
				"k4": map[string]any{"embed": map[string]any{"ff": map[string]any{"gg": 23}}},
			},
		},
		{
			name: "pick multiple by string array",
			args: []interface{}{
				map[string]interface{}{"k1": map[string]any{"temp": 23, "hum": 34}, "k2": "2", "k3": map[string]any{"temp": 23, "hum": 34}, "k4": map[string]any{"embed": map[string]any{"ee": 23, "ff": map[string]any{"gg": 23}}}},
				[]string{"k1.temp", "k2", "k3.hum", "k4.embed.ff"},
			},
			result: map[string]interface{}{
				"k1": map[string]any{"temp": 23},
				"k2": "2",
				"k3": map[string]any{"hum": 34},
				"k4": map[string]any{"embed": map[string]any{"ff": map[string]any{"gg": 23}}},
			},
		},
		{
			name: "pick multiple with invalid arg",
			args: []interface{}{
				map[string]interface{}{"k1": map[string]any{"temp": 23, "hum": 34}, "k2": "2", "k3": map[string]any{"temp": 23, "hum": 34}, "k4": map[string]any{"embed": map[string]any{"ee": 23, "ff": map[string]any{"gg": 23}}}},
				"k1.temp", "k2", 123, "k4.embed.ff",
			},
			result: map[string]interface{}{
				"k1": map[string]any{"temp": 23},
				"k2": "2",
				"k4": map[string]any{"embed": map[string]any{"ff": map[string]any{"gg": 23}}},
			},
		},
		{
			name: "pick multiple by array with invalid arg",
			args: []interface{}{
				map[string]interface{}{"k1": map[string]any{"temp": 23, "hum": 34}, "k2": "2", "k3": map[string]any{"temp": 23, "hum": 34}, "k4": map[string]any{"embed": map[string]any{"ee": 23, "ff": map[string]any{"gg": 23}}}},
				[]any{"k1.temp", "k2", 123, "k4.embed.ff"},
			},
			result: map[string]interface{}{
				"k1": map[string]any{"temp": 23},
				"k2": "2",
				"k4": map[string]any{"embed": map[string]any{"ff": map[string]any{"gg": 23}}},
			},
		},
	}
	for _, tt := range tests {
		f, ok := builtins["object_pick"]
		require.True(t, ok)
		contextLogger := conf.Log.WithField("rule", "testExec")
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
		fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
		t.Run(tt.name, func(t *testing.T) {
			r, ok := f.exec(fctx, tt.args)
			require.True(t, ok)
			require.Equal(t, tt.result, r)
		})
	}
}

func TestVal(t *testing.T) {
	tests := []struct {
		name string
		args []ast.Expr
		err  error
	}{
		{
			name: "object_pick",
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
			},
			err: fmt.Errorf("At least has 2 argument but found 1."),
		}, {
			name: "object_size",
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.StringLiteral{Val: "bar"},
			},
			err: fmt.Errorf("Expect 1 arguments but found 2."),
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			f, ok := builtins[tt.name]
			assert.True(t, ok)
			err := f.val(nil, tt.args)
			assert.Equal(t, tt.err, err)
		})
	}
}
