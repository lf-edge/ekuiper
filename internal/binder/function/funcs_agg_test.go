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
	fStddev, ok := builtins["stddev"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fStddevs, ok := builtins["stddevs"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fVar, ok := builtins["var"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fVars, ok := builtins["vars"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	var tests = []struct {
		args    []interface{}
		avg     interface{}
		max     interface{}
		min     interface{}
		stddev  interface{}
		stddevs interface{}
		var1    interface{}
		vars    interface{}
	}{
		{ // 0
			args: []interface{}{
				[]interface{}{
					"foo",
					"bar",
					"self",
				},
			},
			avg:     fmt.Errorf("run avg function error: found invalid arg string(foo)"),
			max:     "self",
			min:     "bar",
			stddev:  fmt.Errorf("requires float64 slice but found []interface {}([foo bar self])"),
			stddevs: fmt.Errorf("requires float64 slice but found []interface {}([foo bar self])"),
			var1:    fmt.Errorf("requires float64 slice but found []interface {}([foo bar self])"),
			vars:    fmt.Errorf("requires float64 slice but found []interface {}([foo bar self])"),
		}, { // 1
			args: []interface{}{
				[]interface{}{
					int64(100),
					int64(150),
					int64(200),
				},
			},
			avg:     int64(150),
			max:     int64(200),
			min:     int64(100),
			stddev:  40.824829046386306,
			stddevs: float64(50),
			var1:    1666.6666666666667,
			vars:    float64(2500),
		}, { // 2
			args: []interface{}{
				[]interface{}{
					float64(100),
					float64(150),
					float64(200),
				},
			},
			avg:     float64(150),
			max:     float64(200),
			min:     float64(100),
			stddev:  40.824829046386306,
			stddevs: float64(50),
			var1:    1666.6666666666667,
			vars:    float64(2500),
		}, { // 3
			args: []interface{}{
				[]interface{}{
					100, 150, 200,
				},
			},
			avg:     int64(150),
			max:     int64(200),
			min:     int64(100),
			stddev:  40.824829046386306,
			stddevs: float64(50),
			var1:    1666.6666666666667,
			vars:    float64(2500),
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
		rStddev, _ := fStddev.exec(fctx, tt.args)
		if !reflect.DeepEqual(rStddev, tt.stddev) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rStddev, tt.stddev)
		}
		rStddevs, _ := fStddevs.exec(fctx, tt.args)
		if !reflect.DeepEqual(rStddevs, tt.stddevs) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rStddevs, tt.stddevs)
		}
		rVar, _ := fVar.exec(fctx, tt.args)
		if !reflect.DeepEqual(rVar, tt.var1) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rVar, tt.var1)
		}
		rVars, _ := fVars.exec(fctx, tt.args)
		if !reflect.DeepEqual(rVars, tt.vars) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rVars, tt.vars)
		}
	}
}

func TestPercentileExec(t *testing.T) {
	pCont, ok := builtins["percentile_cont"]
	if !ok {
		t.Fatal("builtin not found")
	}
	pDisc, ok := builtins["percentile_disc"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	var tests = []struct {
		args  []interface{}
		pCont interface{}
		pDisc interface{}
	}{
		{ // 0
			args: []interface{}{
				[]interface{}{
					"foo",
					"bar",
					"self",
				},
				0.25,
			},
			pCont: fmt.Errorf("requires float64 slice but found []interface {}([foo bar self])"),
			pDisc: fmt.Errorf("requires float64 slice but found []interface {}([foo bar self])"),
		}, { // 1
			args: []interface{}{
				[]interface{}{
					int64(100),
					int64(150),
					int64(200),
				},
			},
			pCont: fmt.Errorf("Expect 2 arguments but found 1."),
			pDisc: fmt.Errorf("Expect 2 arguments but found 1."),
		}, { // 2
			args: []interface{}{
				[]interface{}{
					int64(100),
					int64(150),
					int64(200),
				},
				0.5,
			},
			pCont: float64(125),
			pDisc: float64(150),
		}, { // 3
			args: []interface{}{
				[]interface{}{
					float64(100),
					float64(150),
					float64(200),
				},
				0.5,
			},
			pCont: float64(125),
			pDisc: float64(150),
		}, { // 4
			args: []interface{}{
				[]interface{}{
					100, 150, 200,
				},
				0.5,
			},
			pCont: float64(125),
			pDisc: float64(150),
		},
	}
	for i, tt := range tests {
		rCont, _ := pCont.exec(fctx, tt.args)
		if !reflect.DeepEqual(rCont, tt.pCont) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rCont, tt.pCont)
		}
		rDisc, _ := pDisc.exec(fctx, tt.args)
		if !reflect.DeepEqual(rDisc, tt.pDisc) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, rDisc, tt.pCont)
		}
	}
}
