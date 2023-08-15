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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestArrayCommonFunctions(t *testing.T) {
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
			name: "array_create",
			args: []interface{}{
				1, "2", 3,
			},
			result: []interface{}{
				1, "2", 3,
			},
		},
		{
			name: "array_position",
			args: []interface{}{
				1, 2,
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_position",
			args: []interface{}{
				[]interface{}{3, 2, 1},
				1,
			},
			result: 2,
		},
		{
			name: "array_position",
			args: []interface{}{
				[]interface{}{3, 2, 1},
				4,
			},
			result: -1,
		},
		{
			name: "length",
			args: []interface{}{
				[]interface{}{1, 2, 3},
			},
			result: 3,
		},
		{
			name: "element_at",
			args: []interface{}{
				1, 2,
			},
			result: fmt.Errorf("first argument should be []interface{} or map[string]interface{}"),
		},
		{
			name: "element_at",
			args: []interface{}{
				[]interface{}{1, 2, 3}, 4,
			},
			result: errorArrayIndex,
		},
		{
			name: "element_at",
			args: []interface{}{
				[]interface{}{1, 2, 3}, -4,
			},
			result: errorArrayIndex,
		},
		{
			name: "element_at",
			args: []interface{}{
				[]interface{}{1, 2, 3}, 1,
			},
			result: 2,
		},
		{
			name: "element_at",
			args: []interface{}{
				[]interface{}{1, 2, 3}, -1,
			},
			result: 3,
		},
		{
			name: "array_contains",
			args: []interface{}{
				1, 2,
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_contains",
			args: []interface{}{
				[]interface{}{1, 2}, 2,
			},
			result: true,
		},
		{
			name: "array_contains",
			args: []interface{}{
				[]interface{}{1, 2}, 3,
			},
			result: false,
		},
		{
			name: "array_remove",
			args: []interface{}{
				[]interface{}{3, 1, 2}, 1,
			},
			result: []interface{}{3, 2},
		},
		{
			name: "array_remove",
			args: []interface{}{
				[]interface{}{'a', 'b', 'c'}, 'c',
			},
			result: []interface{}{'a', 'b'},
		},
		{
			name: "array_remove",
			args: []interface{}{
				[]interface{}{1, 2, 3, 4, 3}, 3,
			},
			result: []interface{}{1, 2, 4},
		},
		{
			name: "array_remove",
			args: []interface{}{
				[]interface{}{3, 3, 3}, 3,
			},
			result: []interface{}{},
		},
		{
			name: "array_last_position",
			args: []interface{}{
				[]interface{}{5, nil, 5}, 5,
			},
			result: 2,
		},
		{
			name: "array_last_position",
			args: []interface{}{
				[]interface{}{5, nil, 5}, "hello",
			},
			result: -1,
		},
		{
			name: "array_last_position",
			args: []interface{}{
				[]interface{}{5, nil, 7}, 5,
			},
			result: 0,
		},
		{
			name: "array_last_position",
			args: []interface{}{
				1, 2,
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_last_position",
			args: []interface{}{
				[]interface{}{5, "hello", nil}, nil,
			},
			result: 2,
		},
		{
			name: "array_contains_any",
			args: []interface{}{
				[]interface{}{1, 2, 3}, []interface{}{0, 2, 4},
			},
			result: true,
		},
		{
			name: "array_contains_any",
			args: []interface{}{
				[]interface{}{1, 2, 3}, []interface{}{4, "hello", 6},
			},
			result: false,
		},
		{
			name: "array_contains_any",
			args: []interface{}{
				[]interface{}{1, 2, 3}, []interface{}{},
			},
			result: false,
		},
		{
			name: "array_contains_any",
			args: []interface{}{
				[]interface{}{1, 2, 3, 4}, 1,
			},
			result: errorArraySecondArgumentNotArrayError,
		},
		{
			name: "array_intersect",
			args: []interface{}{
				[]interface{}{1, 1, 2, 3}, []interface{}{1, 3, 4},
			},
			result: []interface{}{1, 3},
		},
		{
			name: "array_intersect",
			args: []interface{}{
				[]interface{}{"hello", "ekuiper", 2, 3}, []interface{}{"ekuiper", 3, 4},
			},
			result: []interface{}{"ekuiper", 3},
		},
		{
			name: "array_intersect",
			args: []interface{}{
				[]interface{}{"hello", "ekuiper", 2, 3}, "ekuiper",
			},
			result: errorArraySecondArgumentNotArrayError,
		},
		{
			name: "array_intersect",
			args: []interface{}{
				"1", []interface{}{1, 2, 3},
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_union",
			args: []interface{}{
				[]interface{}{1, 1, 2, 3}, []interface{}{1, 3, 4},
			},
			result: []interface{}{1, 2, 3, 4},
		},
		{
			name: "array_union",
			args: []interface{}{
				[]interface{}{"hello", "ekuiper", 2, 3}, []interface{}{"ekuiper", 3, 4},
			},
			result: []interface{}{"hello", "ekuiper", 2, 3, 4},
		},
		{
			name: "array_union",
			args: []interface{}{
				[]interface{}{1, 1, 2, 3}, "ekuiper",
			},
			result: errorArraySecondArgumentNotArrayError,
		},
		{
			name: "array_union",
			args: []interface{}{
				"1", []interface{}{1, 2, 3},
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_max",
			args: []interface{}{
				[]interface{}{1},
			},
			result: int64(1),
		},
		{
			name: "array_max",
			args: []interface{}{
				[]interface{}{1, nil, 3},
			},
			result: int64(3),
		},
		{
			name: "array_max",
			args: []interface{}{
				[]interface{}{1, "4", 3},
			},
			result: errors.New("requires int64 but found string(4)"),
		},
		{
			name: "array_max",
			args: []interface{}{
				[]interface{}{1, "a4a", 3},
			},
			result: errors.New("requires int64 but found string(a4a)"),
		},
		{
			name: "array_max",
			args: []interface{}{
				[]interface{}{1.2, 4.2, 3.0},
			},
			result: 4.2,
		},
		{
			name: "array_max",
			args: []interface{}{
				[]interface{}{1, 3.2, 4.1, 2},
			},
			result: int64(4),
		},
		{
			name: "array_min",
			args: []interface{}{
				[]interface{}{1, nil, 3},
			},
			result: int64(1),
		},
		{
			name: "array_min",
			args: []interface{}{
				[]interface{}{1, "0", 3},
			},
			result: errors.New("requires int64 but found string(0)"),
		},
		{
			name: "array_min",
			args: []interface{}{
				[]interface{}{1.2, 4.2, 3.0},
			},
			result: 1.2,
		},
		{
			name: "array_min",
			args: []interface{}{
				[]interface{}{1, "a4a", 3},
			},
			result: errors.New("requires int64 but found string(a4a)"),
		},
		{
			name: "array_min",
			args: []interface{}{
				[]interface{}{1, 3.2, 4.1, 2},
			},
			result: int64(1),
		},
		{
			name: "array_except",
			args: []interface{}{
				[]interface{}{1, 2, 3}, []interface{}{1, 3, 4},
			},
			result: []interface{}{2},
		},
		{
			name: "array_except",
			args: []interface{}{
				[]interface{}{1, 2, 3}, []interface{}{4, 5, 6},
			},
			result: []interface{}{1, 2, 3},
		},
		{
			name: "array_except",
			args: []interface{}{
				[]interface{}{1, 2, 1, 2, 3}, []interface{}{1, 2, 1, 1, 2, 2, 4},
			},
			result: []interface{}{3},
		},
		{
			name: "array_except",
			args: []interface{}{
				[]interface{}{1, 1, 1, 1, 3}, []interface{}{4},
			},
			result: []interface{}{1, 3},
		},
		{
			name: "repeat",
			args: []interface{}{
				1, 5,
			},
			result: []interface{}{1, 1, 1, 1, 1},
		},
		{
			name: "repeat",
			args: []interface{}{
				1, "hellow",
			},
			result: errorArraySecondArgumentNotIntError,
		},
		{
			name: "repeat",
			args: []interface{}{
				"hello", 3,
			},
			result: []interface{}{"hello", "hello", "hello"},
		},
		{
			name: "repeat",
			args: []interface{}{
				"rockset", 0,
			},
			result: []interface{}{},
		},
		{
			name: "sequence",
			args: []interface{}{
				1, 5,
			},
			result: []interface{}{1, 2, 3, 4, 5},
		},
		{
			name: "sequence",
			args: []interface{}{
				"1", 10, 2,
			},
			result: errorArrayFirstArgumentNotIntError,
		},
		{
			name: "sequence",
			args: []interface{}{
				1, "6", 2,
			},
			result: errorArraySecondArgumentNotIntError,
		},
		{
			name: "sequence",
			args: []interface{}{
				1, 7, "1",
			},
			result: errorArrayThirdArgumentNotIntError,
		},
		{
			name: "sequence",
			args: []interface{}{
				1, 10, 2,
			},
			result: []interface{}{1, 3, 5, 7, 9},
		},
		{
			name: "sequence",
			args: []interface{}{
				10, 1, -3,
			},
			result: []interface{}{10, 7, 4, 1},
		},
		{
			name: "array_cardinality",
			args: []interface{}{
				[]interface{}{1, 2, 3},
			},
			result: 3,
		},
		{
			name: "array_cardinality",
			args: []interface{}{
				1, 2, 3,
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_flatten",
			args: []interface{}{
				[]interface{}{
					[]interface{}{1, 2, 3},
				},
			},
			result: []interface{}{1, 2, 3},
		},
		{
			name: "array_flatten",
			args: []interface{}{
				1, 2,
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_flatten",
			args: []interface{}{
				[]interface{}{1, 2, 3}, 4,
			},
			result: errorArrayNotArrayElementError,
		},
		{
			name: "array_flatten",
			args: []interface{}{
				[]interface{}{
					[]interface{}{1, 2, 3},
					[]interface{}{4, 5, 6},
				},
			},
			result: []interface{}{1, 2, 3, 4, 5, 6},
		},
		{
			name: "array_distinct",
			args: []interface{}{
				[]interface{}{1, 2, 3},
			},
			result: []interface{}{1, 2, 3},
		},
		{
			name: "array_distinct",
			args: []interface{}{
				1, 1,
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_distinct",
			args: []interface{}{
				[]interface{}{1, 1, 1},
			},
			result: []interface{}{1},
		},
		{
			name: "array_distinct",
			args: []interface{}{
				[]interface{}{1, 2, 2, 1},
			},
			result: []interface{}{1, 2},
		},
		{
			name: "array_map",
			args: []interface{}{
				"round", []interface{}{0, 0.4, 1.2},
			},
			result: []interface{}{0.0, 0.0, 1.0},
		},
		{
			name: "array_map",
			args: []interface{}{
				123, []interface{}{1, 2, 3},
			},
			result: errorArrayFirstArgumentNotStringError,
		},
		{
			name: "array_map",
			args: []interface{}{
				"round", 1,
			},
			result: errorArraySecondArgumentNotArrayError,
		},
		{
			name: "array_map",
			args: []interface{}{
				"abs", []interface{}{0, -0.4, 1.2},
			},
			result: []interface{}{0, 0.4, 1.2},
		},
		{
			name: "array_map",
			args: []interface{}{
				"pow", []interface{}{0, -0.4, 1.2},
			},
			result: fmt.Errorf("validate function arguments failed."),
		},
		{
			name: "array_map",
			args: []interface{}{
				"avg", []interface{}{0, -0.4, 1.2},
			},
			result: fmt.Errorf("first argument should be a scalar function."),
		},
		{
			name: "array_map",
			args: []interface{}{
				"ceil", []interface{}{0, -1, 1.2},
			},
			result: []interface{}{0.0, -1.0, 2.0},
		},
		{
			name: "array_map",
			args: []interface{}{
				"power", []interface{}{1, 2, 3},
			},
			result: fmt.Errorf("validate function arguments failed."),
		},
		{
			name: "array_join",
			args: []interface{}{
				"a", "",
			},
			result: errorArrayFirstArgumentNotArrayError,
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", "c"}, 123, "a",
			},
			result: errorArraySecondArgumentNotStringError,
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", "c"}, ":", 123,
			},
			result: errorArrayThirdArgumentNotStringError,
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{123, "b", "c"}, ":", "a",
			},
			result: errorArrayNotStringElementError,
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", "c"}, "",
			},
			result: "abc",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", nil, "b"}, ":",
			},
			result: "a:b",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", "c"}, ":",
			},
			result: "a:b:c",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", "c"}, ":,%",
			},
			result: "a:,%b:,%c",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", nil, "c"}, ":", "nullReplacementStr",
			},
			result: "a:nullReplacementStr:c",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", nil, "c"}, ":", "nullReplacementStr",
			},
			result: "a:nullReplacementStr:c",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", "c"}, ":", "a",
			},
			result: "a:b:c",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", "c"}, ":",
			},
			result: "a:b:c",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{nil, nil, nil}, ",", "nullReplacementStr",
			},
			result: "nullReplacementStr,nullReplacementStr,nullReplacementStr",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{nil, nil, nil}, ",",
			},
			result: "",
		},
		{
			name: "array_join",
			args: []interface{}{
				[]interface{}{"a", "b", nil}, ",",
			},
			result: "a,b",
		},
		{
			name: "array_concat",
			args: []interface{}{
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{"3"},
				[]interface{}{nil},
			},
			result: []interface{}{
				1, 2, "3", nil,
			},
		},
		{
			name: "array_concat",
			args: []interface{}{
				[]interface{}{1},
				nil,
			},
			result: nil,
		},
	}

	fe := funcExecutor{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := fe.ExecWithName(tt.args, fctx, tt.name)
			assert.Equal(t, tt.result, result)
		})
	}
}

func TestArrayShuffle(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		name   string
		args   []interface{}
		result []interface{}
	}{
		{
			name: "array_shuffle",
			args: []interface{}{
				[]interface{}{1, 2, 3},
			},
			result: []interface{}{
				[]interface{}{1, 2, 3}, []interface{}{1, 3, 2}, []interface{}{2, 1, 3}, []interface{}{2, 3, 1}, []interface{}{3, 1, 2}, []interface{}{3, 2, 1},
			},
		},
		{
			name: "array_shuffle",
			args: []interface{}{
				1,
			},
			result: []interface{}{
				errorArrayFirstArgumentNotArrayError,
			},
		},
	}

	for i, tt := range tests {
		f, ok := builtins[tt.name]
		if !ok {
			t.Fatal(fmt.Sprintf("builtin %v not found", tt.name))
		}
		result, _ := f.exec(fctx, tt.args)
		flag := false
		for _, actual := range tt.result {
			if reflect.DeepEqual(result, actual) {
				flag = true
				break
			}
		}

		if !flag {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant in:\t%v", i, result, tt.result)
		}
	}
}

func TestArraySort(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		name   string
		args   []interface{}
		result []interface{}
	}{
		{
			name: "array_sort",
			args: []any{
				[]any{3, 2, 1},
			},

			result: []interface{}{1, 2, 3},
		},
		{
			name: "array_sort",
			args: []any{
				[]any{3, 1.6, -0.83},
			},

			result: []interface{}{-0.83, 1.6, 3},
		},
		{
			name: "array_sort",
			args: []any{
				[]any{"abc", 3, "def", 1.6, -0.83},
			},
			result: []interface{}{-0.83, 1.6, 3, "abc", "def"},
		},
	}

	for i, tt := range tests {
		f, ok := builtins[tt.name]
		if !ok {
			t.Fatal(fmt.Sprintf("builtin %v not found", tt.name))
		}
		result, _ := f.exec(fctx, tt.args)
		flag := false
		if reflect.DeepEqual(result, tt.result) {
			flag = true
		}

		if !flag {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant in:\t%v", i, result, tt.result)
		}
	}
}

func TestArrayFuncNil(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerArrayFunc()
	for mathFuncName, mathFunc := range builtins {
		switch mathFuncName {
		case "array_create":
			r, b := mathFunc.exec(fctx, []interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.Equal(t, r, nil, fmt.Sprintf("%v failed", mathFuncName))
			r, b = mathFunc.exec(fctx, []interface{}{nil, 1})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.Equal(t, r, []interface{}{1}, fmt.Sprintf("%v failed", mathFuncName))
		case "array_position", "array_last_position":
			r, b := mathFunc.exec(fctx, []interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.Equal(t, r, -1, fmt.Sprintf("%v failed", mathFuncName))
		case "array_contains", "array_contains_any":
			r, b := mathFunc.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.False(t, r.(bool), fmt.Sprintf("%v failed", mathFuncName))
		case "array_union":
			r, b := mathFunc.exec(fctx, []interface{}{[]interface{}{1}, nil})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.Equal(t, r, []interface{}{1}, fmt.Sprintf("%v failed", mathFuncName))
		case "array_cardinality":
			r, b := mathFunc.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.Equal(t, r, 0, fmt.Sprintf("%v failed", mathFuncName))
		default:
			r, b := mathFunc.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.Nil(t, r, fmt.Sprintf("%v failed", mathFuncName))
		}
	}
}

func TestArrayFuncVal(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []ast.Expr
		err      error
	}{
		{
			name:     "array sort failure",
			funcName: "array_sort",
			args: []ast.Expr{
				&ast.BooleanLiteral{Val: true},
				&ast.BooleanLiteral{Val: true},
			},
			err: fmt.Errorf("Expect 1 arguments but found 2."),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, ok := builtins[tt.funcName]
			assert.True(t, ok)
			err := f.val(nil, tt.args)
			assert.Equal(t, tt.err, err)
		})
	}
}
