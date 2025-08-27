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

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestExtractFunc(t *testing.T) {
	f, ok := builtins["extract"]
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
				map[string]interface{}{"k": "v"},
			},
			result: []interface{}{map[string]interface{}{"k": "v"}},
		},
	}
	for i, tt := range tests {
		result, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestUnnestFunctions(t *testing.T) {
	f, ok := builtins["unnest"]
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
				[]interface{}{1, 2, 3},
			},
			result: []interface{}{1, 2, 3},
		},
		{
			args: []interface{}{
				[]interface{}{
					map[string]int{
						"a": 1,
						"b": 2,
					},
					map[string]int{
						"a": 3,
						"b": 4,
					},
				},
			},
			result: []interface{}{
				map[string]int{
					"a": 1,
					"b": 2,
				},
				map[string]int{
					"a": 3,
					"b": 4,
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

func TestUnnestFunctionsNil(t *testing.T) {
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerSetReturningFunc()
	for name, function := range builtins {
		r, b := function.check([]interface{}{nil})
		require.True(t, b, fmt.Sprintf("%v failed", name))
		require.Nil(t, r, fmt.Sprintf("%v failed", name))
	}
}
