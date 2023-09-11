// Copyright 2023 EMQ Technologies Co., Ltd.
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
				},
				"a",
			},
			result: map[string]interface{}{
				"b": 2,
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
