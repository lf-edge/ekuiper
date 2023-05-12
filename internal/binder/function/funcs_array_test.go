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
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestArrayFunctions(t *testing.T) {
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
			result: errorArrayArgumentError,
		},
		{
			name: "array_position",
			args: []interface{}{
				[]interface{}{3, 2, 1},
				1,
			},
			result: 3,
		},
		{
			name: "array_position",
			args: []interface{}{
				[]interface{}{3, 2, 1},
				4,
			},
			result: 0,
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
				[]interface{}{1, 2, 3}, 0,
			},
			result: fmt.Errorf("index should be larger or smaller than 0"),
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
			result: 1,
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
			result: errorArrayArgumentError,
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
