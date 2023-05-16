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

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
)

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
			name: "item",
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
			name: "item",
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
	}
	for i, tt := range tests {
		f, ok := builtins[tt.name]
		if !ok {
			t.Fatal(fmt.Sprintf("builtin %v not found", tt.name))
		}
		result, _ := f.exec(fctx, tt.args)
		switch r := result.(type) {
		case []string:
			sort.Strings(r)
			if !reflect.DeepEqual(r, tt.result) {
				t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, r, tt.result)
			}
		case []interface{}:
			rr := make([]interface{}, len(r))
			copy(rr, r)
			rr[0] = r[1]
			rr[1] = r[0]
			if !reflect.DeepEqual(r, tt.result) && !reflect.DeepEqual(rr, tt.result) {
				t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, r, tt.result)
			}
		default:
			if !reflect.DeepEqual(result, tt.result) {
				t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
			}
		}
	}
}
