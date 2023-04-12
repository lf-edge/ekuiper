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
	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"testing"
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
func TestCoalesceExec(t *testing.T) {
	f, ok := builtins["coalesce"]
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
	var tests = []struct {
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
	var tests = []struct {
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

	var tests = []struct {
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
