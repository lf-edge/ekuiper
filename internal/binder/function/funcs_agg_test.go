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
	"reflect"
	"testing"
)

func TestAggExec(t *testing.T) {
	fAvg, ok := builtins["avg"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fMax, ok := builtins["max"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fMin, ok := builtins["min"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	var tests = []struct {
		args []interface{}
		avg  interface{}
		max  interface{}
		min  interface{}
	}{
		{ // 0
			args: []interface{}{
				[]interface{}{
					"foo",
					"bar",
					"self",
				},
			},
			avg: fmt.Errorf("run avg function error: found invalid arg string(foo)"),
			max: "self",
			min: "bar",
		}, { // 1
			args: []interface{}{
				[]interface{}{
					int64(100),
					int64(150),
					int64(200),
				},
			},
			avg: int64(150),
			max: int64(200),
			min: int64(100),
		}, { // 2
			args: []interface{}{
				[]interface{}{
					float64(100),
					float64(150),
					float64(200),
				},
			},
			avg: float64(150),
			max: float64(200),
			min: float64(100),
		}, { // 3
			args: []interface{}{
				[]interface{}{
					100, 150, 200,
				},
			},
			avg: int64(150),
			max: int64(200),
			min: int64(100),
		},
	}
	for i, tt := range tests {
		rAvg, _ := fAvg.exec(fctx, tt.args)
		if !reflect.DeepEqual(rAvg, tt.avg) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rAvg, tt.avg)
		}
		rMax, _ := fMax.exec(fctx, tt.args)
		if !reflect.DeepEqual(rMax, tt.max) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rMax, tt.max)
		}
		rMin, _ := fMin.exec(fctx, tt.args)
		if !reflect.DeepEqual(rMin, tt.min) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rMin, tt.min)
		}
	}
}
