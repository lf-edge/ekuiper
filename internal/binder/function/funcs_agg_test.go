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
	tests := []struct {
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
		}, { // 4
			args: []interface{}{
				[]interface{}{},
			},
			avg:     nil,
			max:     nil,
			min:     nil,
			stddev:  nil,
			stddevs: nil,
			var1:    nil,
			vars:    nil,
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
	tests := []struct {
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
				[]interface{}{0.25, 0.25, 0.25},
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
				[]interface{}{0.5, 0.5, 0.5},
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
				[]interface{}{0.5, 0.5, 0.5},
			},
			pCont: float64(125),
			pDisc: float64(150),
		}, { // 4
			args: []interface{}{
				[]interface{}{
					100, 150, 200,
				},
				[]interface{}{0.5, 0.5, 0.5},
			},
			pCont: float64(125),
			pDisc: float64(150),
		}, { // 5
			args: []interface{}{
				[]interface{}{},
				[]interface{}{},
			},
			pCont: nil,
			pDisc: nil,
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

func TestConcatExec(t *testing.T) {
	fcon, ok := builtins["merge_agg"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		name   string
		args   []interface{}
		result any
	}{
		{ // 0
			name: "concat wildcard",
			args: []interface{}{
				[]interface{}{
					map[string]interface{}{
						"foo": "bar",
						"a":   123,
					},
					map[string]interface{}{
						"foo1": "bar",
						"a":    243,
					},
					map[string]interface{}{
						"foo": "bar1",
						"a":   342,
					},
				},
			},
			result: map[string]interface{}{
				"foo":  "bar1",
				"a":    342,
				"foo1": "bar",
			},
		}, { // 1
			name: "concat int column",
			args: []interface{}{
				[]interface{}{
					int64(100),
					int64(150),
					int64(200),
				},
			},
			result: nil,
		}, { // 2
			name: "concat empty",
			args: []interface{}{
				[]interface{}{},
			},
			result: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, ok := fcon.exec(fctx, tt.args)
			assert.True(t, ok, "failed to execute concat")
			assert.Equal(t, tt.result, r)
		})
	}
}

func TestAggFuncNil(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerAggFunc()
	for name, function := range builtins {
		switch name {
		case "avg":
			r, b := function.exec(fctx, []interface{}{[]interface{}{nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, nil, fmt.Sprintf("%v failed", name))
			r, b = function.exec(fctx, []interface{}{[]interface{}{1, nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, int64(1), fmt.Sprintf("%v failed", name))
			r, b = function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		case "count":
			r, b := function.exec(fctx, []interface{}{[]interface{}{nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, 0, fmt.Sprintf("%v failed", name))
			r, b = function.exec(fctx, []interface{}{[]interface{}{1, nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, 1, fmt.Sprintf("%v failed", name))
			r, b = function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		case "max":
			r, b := function.exec(fctx, []interface{}{[]interface{}{nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
			r, b = function.exec(fctx, []interface{}{[]interface{}{1, 2, nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, int64(2), fmt.Sprintf("%v failed", name))
			r, b = function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		case "min":
			r, b := function.exec(fctx, []interface{}{[]interface{}{nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
			r, b = function.exec(fctx, []interface{}{[]interface{}{1, 2, nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, int64(1), fmt.Sprintf("%v failed", name))
			r, b = function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		case "sum":
			r, b := function.exec(fctx, []interface{}{[]interface{}{nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
			r, b = function.exec(fctx, []interface{}{[]interface{}{1, 2, nil}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, int64(3), fmt.Sprintf("%v failed", name))
			r, b = function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		case "collect":
			r, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		case "merge_agg":
			r, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		case "last_value":
			r, b := function.exec(fctx, []interface{}{[]interface{}{nil}, []interface{}{false}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
			r, b = function.exec(fctx, []interface{}{[]interface{}{1, 2, nil}, []interface{}{true}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, 2, fmt.Sprintf("%v failed", name))
			r, b = function.exec(fctx, []interface{}{[]interface{}{1, 2, nil}, []interface{}{false}})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, r, nil, fmt.Sprintf("%v failed", name))
			r, b = function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		default:
			r, b := function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		}
	}
}

func TestLastValue(t *testing.T) {
	f, ok := builtins["last_value"]
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
		{
			args: []interface{}{
				[]interface{}{
					"foo",
					"bar",
					"self",
				},
				[]interface{}{
					true,
					true,
					true,
				},
			},
			result: "self",
		},
		{
			args: []interface{}{
				[]interface{}{
					"foo",
					"bar",
					"self",
				},
				[]interface{}{
					false,
					false,
					false,
				},
			},
			result: "self",
		},
		{
			args: []interface{}{
				[]interface{}{
					int64(100),
					float64(3.14),
					1,
				},
				[]interface{}{
					true,
					true,
					true,
				},
			},
			result: 1,
		},
		{
			args: []interface{}{
				[]interface{}{
					int64(100),
					float64(3.14),
					1,
				},
				[]interface{}{
					false,
					false,
					false,
				},
			},
			result: 1,
		},
		{
			args: []interface{}{
				[]interface{}{
					int64(100),
					float64(3.14),
					nil,
				},
				[]interface{}{
					true,
					true,
					true,
				},
			},
			result: float64(3.14),
		},
		{
			args: []interface{}{
				[]interface{}{
					int64(100),
					float64(3.14),
					nil,
				},
				[]interface{}{
					false,
					false,
					false,
				},
			},
			result: nil,
		},
		{
			args: []interface{}{
				[]interface{}{
					nil,
					nil,
					nil,
				},
				[]interface{}{
					true,
					true,
					true,
				},
			},
			result: nil,
		},
		{
			args: []interface{}{
				[]interface{}{
					nil,
					nil,
					nil,
				},
				[]interface{}{
					false,
					false,
					false,
				},
			},
			result: nil,
		},
		{
			args: []interface{}{
				1,
				true,
			},
			result: fmt.Errorf("the first argument to the aggregate function should be []interface but found int(1)"),
		},
		{
			args: []interface{}{
				[]interface{}{1},
				true,
			},
			result: fmt.Errorf("the second argument to the aggregate function should be []interface but found bool(true)"),
		},
		{
			args: []interface{}{
				[]interface{}{1},
				[]interface{}{1},
			},
			result: fmt.Errorf("the second parameter requires bool but found int(1)"),
		},
		{
			args: []interface{}{
				[]interface{}{},
				[]interface{}{true},
			},
			result: nil,
		},
		{
			args: []interface{}{
				[]interface{}{},
				true,
			},
			result: nil,
		},
	}

	for i, tt := range tests {
		r, _ := f.exec(fctx, tt.args)
		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, r, tt.result)
		}
	}
}

func TestLastValueValidation(t *testing.T) {
	f, ok := builtins["last_value"]
	if !ok {
		t.Fatal("builtin not found")
	}
	tests := []struct {
		args []ast.Expr
		err  error
	}{
		{
			args: []ast.Expr{
				&ast.BooleanLiteral{Val: true},
			},
			err: fmt.Errorf("Expect 2 arguments but found 1."),
		}, {
			args: []ast.Expr{
				&ast.FieldRef{Name: "foo"},
				&ast.FieldRef{Name: "bar"},
			},
			err: fmt.Errorf("Expect bool type for parameter 2"),
		}, {
			args: []ast.Expr{
				&ast.StringLiteral{Val: "foo"},
				&ast.BooleanLiteral{Val: true},
			},
		},
	}
	for i, tt := range tests {
		err := f.val(nil, tt.args)
		if !reflect.DeepEqual(err, tt.err) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, err, tt.err)
		}
	}
}
