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
	"math"
	"math/cmplx"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestFuncMath(t *testing.T) {
	fAbs, ok := builtins["abs"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fCeil, ok := builtins["ceil"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fExp, ok := builtins["exp"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fFloor, ok := builtins["floor"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fLn, ok := builtins["ln"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fLog10, ok := builtins["log"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fSqrt, ok := builtins["sqrt"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fPow, ok := builtins["power"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fBitAnd, ok := builtins["bitand"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fBitOr, ok := builtins["bitor"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fAcos, ok := builtins["acos"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fAsin, ok := builtins["asin"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fAtan, ok := builtins["atan"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fAtan2, ok := builtins["atan2"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fBitXor, ok := builtins["bitxor"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fBitNot, ok := builtins["bitnot"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fCos, ok := builtins["cos"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fCosh, ok := builtins["cosh"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fMod, ok := builtins["mod"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fPi, ok := builtins["pi"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fRound, ok := builtins["round"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fSign, ok := builtins["sign"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fSin, ok := builtins["sin"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fSinh, ok := builtins["sinh"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fTan, ok := builtins["tan"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fTanh, ok := builtins["tanh"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fCot, ok := builtins["cot"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fRadians, ok := builtins["radians"]
	if !ok {
		t.Fatal("builtin not found")
	}
	fDegrees, ok := builtins["degrees"]
	if !ok {
		t.Fatal("builtin not found")
	}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args []interface{}
		res  []interface{}
	}{
		{ // 0
			args: []interface{}{
				-10, 2,
			},
			res: []interface{}{
				10,
				float64(-10),
				math.Exp(-10),
				nil,
				nil,
				nil,
				float64(100),
				2,
				-10,
				nil,
				nil,
				math.Atan(-10),
				math.Atan2(-10, 2),
				-12,
				9,
				math.Cos(-10),
				math.Cosh(-10),
				float64(0),
				float64(-10),
				-1,
				math.Sin(-10),
				math.Sinh(-10),
				math.Tan(-10),
				math.Tanh(-10),
				math.Floor(-10),
				math.Pi,
				real(cmplx.Cot(-10)),
				radians(-10),
				degrees(-10),
			},
		}, { // 1
			args: []interface{}{
				10, 2,
			},
			res: []interface{}{
				10,
				float64(10),
				math.Exp(10),
				math.Log(10),
				math.Log10(10),
				math.Sqrt(10),
				float64(100),
				2,
				10,
				nil,
				nil,
				math.Atan(10),
				math.Atan2(10, 2),
				8,
				-11,
				math.Cos(10),
				math.Cosh(10),
				float64(0),
				float64(10),
				1,
				math.Sin(10),
				math.Sinh(10),
				math.Tan(10),
				math.Tanh(10),
				math.Floor(10),
				math.Pi,
				real(cmplx.Cot(10)),
				radians(10),
				degrees(10),
			},
		}, { // 2
			args: []interface{}{
				-10.5, 2,
			},
			res: []interface{}{
				float64(10.5),
				float64(-10),
				math.Exp(-10.5),
				nil,
				nil,
				nil,
				110.25,
				fmt.Errorf("Expect int type for the first operand but got -10.5"),
				fmt.Errorf("Expect int type for the first operand but got -10.5"),
				nil,
				nil,
				math.Atan(-10.5),
				math.Atan2(-10.5, 2),
				fmt.Errorf("Expect int type for the first operand but got -10.5"),
				fmt.Errorf("Expect int type for operand but got -10.5"),
				math.Cos(-10.5),
				math.Cosh(-10.5),
				-0.5,
				-10.5,
				-1,
				math.Sin(-10.5),
				math.Sinh(-10.5),
				math.Tan(-10.5),
				math.Tanh(-10.5),
				math.Floor(-10.5),
				math.Pi,
				real(cmplx.Cot(-10.5)),
				radians(-10.5),
				degrees(-10.5),
			},
		}, { // 3
			args: []interface{}{
				10.5, 2,
			},
			res: []interface{}{
				10.5,
				float64(11),
				math.Exp(10.5),
				math.Log(10.5),
				math.Log10(10.5),
				math.Sqrt(10.5),
				110.25,
				fmt.Errorf("Expect int type for the first operand but got 10.5"),
				fmt.Errorf("Expect int type for the first operand but got 10.5"),
				nil,
				nil,
				math.Atan(10.5),
				math.Atan2(10.5, 2),
				fmt.Errorf("Expect int type for the first operand but got 10.5"),
				fmt.Errorf("Expect int type for operand but got 10.5"),
				math.Cos(10.5),
				math.Cosh(10.5),
				0.5,
				10.5,
				1,
				math.Sin(10.5),
				math.Sinh(10.5),
				math.Tan(10.5),
				math.Tanh(10.5),
				math.Floor(10.5),
				math.Pi,
				real(cmplx.Cot(10.5)),
				radians(10.5),
				degrees(10.5),
			},
		}, { // 4
			args: []interface{}{
				0, 2,
			},
			res: []interface{}{
				0,
				float64(0),
				float64(1),
				math.Inf(-1),
				math.Inf(-1),
				float64(0),
				float64(0),
				0,
				2,
				math.Acos(0),
				math.Asin(0),
				math.Atan(0),
				math.Atan2(0, 2),
				2,
				-1,
				math.Cos(0),
				math.Cosh(0),
				float64(0),
				float64(0),
				0,
				math.Sin(0),
				math.Sinh(0),
				math.Tan(0),
				math.Tanh(0),
				float64(0),
				math.Pi,
				fmt.Errorf("out-of-range error"),
				radians(0),
				degrees(0),
			},
		},
	}
	for i, tt := range tests {
		rAbs, _ := fAbs.exec(fctx, tt.args)
		if !reflect.DeepEqual(rAbs, tt.res[0]) {
			t.Errorf("%d.0 abs result mismatch,\ngot:\t%v \nwant:\t%v", i, rAbs, tt.res[0])
		}
		rCeil, _ := fCeil.exec(fctx, tt.args)
		if !reflect.DeepEqual(rCeil, tt.res[1]) {
			t.Errorf("%d.1 ceil result mismatch,\ngot:\t%v \nwant:\t%v", i, rCeil, tt.res[1])
		}
		rExp, _ := fExp.exec(fctx, tt.args)
		if !reflect.DeepEqual(rExp, tt.res[2]) {
			t.Errorf("%d.2 exp result mismatch,\ngot:\t%v \nwant:\t%v", i, rExp, tt.res[2])
		}
		rLn, _ := fLn.exec(fctx, tt.args)
		if !reflect.DeepEqual(rLn, tt.res[3]) {
			t.Errorf("%d.3 ln result mismatch,\ngot:\t%v \nwant:\t%v", i, rLn, tt.res[3])
		}
		rLog10, _ := fLog10.exec(fctx, tt.args[:1])
		if !reflect.DeepEqual(rLog10, tt.res[4]) {
			t.Errorf("%d.4 log result mismatch,\ngot:\t%v \nwant:\t%v", i, rLog10, tt.res[4])
		}
		rSqrt, _ := fSqrt.exec(fctx, tt.args)
		if !reflect.DeepEqual(rSqrt, tt.res[5]) {
			t.Errorf("%d.5 sqrt result mismatch,\ngot:\t%v \nwant:\t%v", i, rSqrt, tt.res[5])
		}
		rPow, _ := fPow.exec(fctx, tt.args)
		if !reflect.DeepEqual(rPow, tt.res[6]) {
			t.Errorf("%d.6 power result mismatch,\ngot:\t%v \nwant:\t%v", i, rPow, tt.res[6])
		}
		rBitAnd, _ := fBitAnd.exec(fctx, tt.args)
		if !reflect.DeepEqual(rBitAnd, tt.res[7]) {
			t.Errorf("%d.7 bitand result mismatch,\ngot:\t%v \nwant:\t%v", i, rBitAnd, tt.res[7])
		}
		rBitOr, _ := fBitOr.exec(fctx, tt.args)
		if !reflect.DeepEqual(rBitOr, tt.res[8]) {
			t.Errorf("%d.8 bitor result mismatch,\ngot:\t%v \nwant:\t%v", i, rBitOr, tt.res[8])
		}
		rAcos, _ := fAcos.exec(fctx, tt.args)
		if !reflect.DeepEqual(rAcos, tt.res[9]) {
			t.Errorf("%d.9 acos result mismatch,\ngot:\t%v \nwant:\t%v", i, rAcos, tt.res[9])
		}
		rAsin, _ := fAsin.exec(fctx, tt.args)
		if !reflect.DeepEqual(rAsin, tt.res[10]) {
			t.Errorf("%d.10 asin result mismatch,\ngot:\t%v \nwant:\t%v", i, rAsin, tt.res[10])
		}
		rAtan, _ := fAtan.exec(fctx, tt.args)
		if !reflect.DeepEqual(rAtan, tt.res[11]) {
			t.Errorf("%d.11 atan result mismatch,\ngot:\t%v \nwant:\t%v", i, rAtan, tt.res[11])
		}
		rAtan2, _ := fAtan2.exec(fctx, tt.args)
		if !reflect.DeepEqual(rAtan2, tt.res[12]) {
			t.Errorf("%d.12 atan2 result mismatch,\ngot:\t%v \nwant:\t%v", i, rAtan2, tt.res[12])
		}
		rBitXor, _ := fBitXor.exec(fctx, tt.args)
		if !reflect.DeepEqual(rBitXor, tt.res[13]) {
			t.Errorf("%d.13 bitxor result mismatch,\ngot:\t%v \nwant:\t%v", i, rBitXor, tt.res[13])
		}
		rBitNot, _ := fBitNot.exec(fctx, tt.args)
		if !reflect.DeepEqual(rBitNot, tt.res[14]) {
			t.Errorf("%d.14 bitnot result mismatch,\ngot:\t%v \nwant:\t%v", i, rBitNot, tt.res[14])
		}
		rCos, _ := fCos.exec(fctx, tt.args)
		if !reflect.DeepEqual(rCos, tt.res[15]) {
			t.Errorf("%d.15 cos result mismatch,\ngot:\t%v \nwant:\t%v", i, rCos, tt.res[15])
		}
		rCosh, _ := fCosh.exec(fctx, tt.args)
		if !reflect.DeepEqual(rCosh, tt.res[16]) {
			t.Errorf("%d.16 cosh result mismatch,\ngot:\t%v \nwant:\t%v", i, rCosh, tt.res[16])
		}
		rMod, _ := fMod.exec(fctx, tt.args)
		if !reflect.DeepEqual(rMod, tt.res[17]) {
			t.Errorf("%d.17 mod result mismatch,\ngot:\t%v \nwant:\t%v", i, rMod, tt.res[17])
		}
		rRound, _ := fRound.exec(fctx, tt.args)
		if !reflect.DeepEqual(rRound, tt.res[18]) {
			t.Errorf("%d.18 round result mismatch,\ngot:\t%v \nwant:\t%v", i, rRound, tt.res[18])
		}
		rSign, _ := fSign.exec(fctx, tt.args)
		if !reflect.DeepEqual(rSign, tt.res[19]) {
			t.Errorf("%d.19 sign result mismatch,\ngot:\t%v \nwant:\t%v", i, rSign, tt.res[19])
		}
		rSin, _ := fSin.exec(fctx, tt.args)
		if !reflect.DeepEqual(rSin, tt.res[20]) {
			t.Errorf("%d.20 sin result mismatch,\ngot:\t%v \nwant:\t%v", i, rSin, tt.res[20])
		}
		rSinh, _ := fSinh.exec(fctx, tt.args)
		if !reflect.DeepEqual(rSinh, tt.res[21]) {
			t.Errorf("%d.21 sinh result mismatch,\ngot:\t%v \nwant:\t%v", i, rSinh, tt.res[21])
		}
		rTan, _ := fTan.exec(fctx, tt.args)
		if !reflect.DeepEqual(rTan, tt.res[22]) {
			t.Errorf("%d.22 tan result mismatch,\ngot:\t%v \nwant:\t%v", i, rTan, tt.res[22])
		}
		rTanh, _ := fTanh.exec(fctx, tt.args)
		if !reflect.DeepEqual(rTanh, tt.res[23]) {
			t.Errorf("%d.23 tanh result mismatch,\ngot:\t%v \nwant:\t%v", i, rTanh, tt.res[23])
		}
		rFloor, _ := fFloor.exec(fctx, tt.args)
		if !reflect.DeepEqual(rFloor, tt.res[24]) {
			t.Errorf("%d.24 exp result mismatch,\ngot:\t%v \nwant:\t%v", i, rFloor, tt.res[24])
		}
		rPi, _ := fPi.exec(fctx, tt.args)
		if !reflect.DeepEqual(rPi, tt.res[25]) {
			t.Errorf("%d.25 exp result mismatch,\ngot:\t%v \nwant:\t%v", i, rPi, tt.res[25])
		}
		rCot, _ := fCot.exec(fctx, tt.args)
		if !reflect.DeepEqual(rCot, tt.res[26]) {
			t.Errorf("%d.26 cot result mismatch,\ngot:\t%v \nwant:\t%v", i, rCot, tt.res[26])
		}
		rRadians, _ := fRadians.exec(fctx, tt.args)
		if !reflect.DeepEqual(rRadians, tt.res[27]) {
			t.Errorf("%d.27 radians result mismatch,\ngot:\t%v \nwant:\t%v", i, rCot, tt.res[27])
		}
		rDegrees, _ := fDegrees.exec(fctx, tt.args)
		if !reflect.DeepEqual(rDegrees, tt.res[28]) {
			t.Errorf("%d.28 degrees result mismatch,\ngot:\t%v \nwant:\t%v", i, rCot, tt.res[28])
		}
	}
}

func TestFuncMathNil(t *testing.T) {
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerMathFunc()
	for mathFuncName, mathFunc := range builtins {
		switch mathFuncName {
		case "rand":
			continue
		default:
			r, b := mathFunc.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", mathFuncName))
			require.Nil(t, r, fmt.Sprintf("%v failed", mathFuncName))
		}
	}
}

func TestRadians(t *testing.T) {
	cases := []struct {
		degrees float64
		want    float64
	}{
		{90, math.Pi / 2},
		{180, math.Pi},
		{45, math.Pi / 4},
		{0, 0},
	}

	for _, c := range cases {
		got := radians(c.degrees)
		if got != c.want {
			t.Errorf("radians(%f) == %f, want %f", c.degrees, got, c.want)
		}
	}
}

func TestDegrees(t *testing.T) {
	cases := []struct {
		radians float64
		want    float64
	}{
		{math.Pi / 2, 90},
		{math.Pi, 180},
		{math.Pi / 4, 45},
		{0, 0},
	}

	for _, c := range cases {
		got := degrees(c.radians)
		if got != c.want {
			t.Errorf("degrees(%f) == %f, want %f", c.radians, got, c.want)
		}
	}
}

func TestGetValidPrefix(t *testing.T) {
	v := []struct {
		s    string
		base int64
		ret  string
	}{
		{"-123456D1f", 5, "-1234"},
		{"+12azD", 16, "12a"},
		{"+", 12, ""},
	}
	for _, tt := range v {
		r := getValidPrefix(tt.s, tt.base)
		require.Equal(t, tt.ret, r)
	}
}

func TestConvFunc(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerMathFunc()

	fConv := builtins["conv"]
	cases := []struct {
		args     []interface{}
		expected interface{}
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{"a", 16, 2}, "1010", false, false},
		{[]interface{}{"6E", 18, 8}, "172", false, false},
		{[]interface{}{"-17", 10, -18}, "-H", false, false},
		{[]interface{}{"-17", 10, 18}, "2D3FGB0B9CG4BD1H", false, false},
		{[]interface{}{"+18aZ", 7, 36}, "1", false, false},
		{[]interface{}{"18446744073709551615", -10, 16}, "7FFFFFFFFFFFFFFF", false, false},
		{[]interface{}{"12F", -10, 16}, "C", false, false},
		{[]interface{}{"  FF ", 16, 10}, "255", false, false},
		{[]interface{}{"aa", 10, 2}, "0", false, false},
		{[]interface{}{" A", -10, 16}, "0", false, false},
		{[]interface{}{"random_str", 10, 8}, "0", false, false},
		{[]interface{}{"a6a", 10, 8}, "0", false, false},
		{[]interface{}{"a6a", 1, 8}, nil, true, false},
	}
	for _, c := range cases {
		got, _ := fConv.exec(fctx, []interface{}{c.args[0], c.args[1], c.args[2]})
		if c.getErr {
			require.Error(t, got.(error))
			continue
		}
		if got != c.expected {
			t.Errorf("%s:Expected %s, but got %s", c.args[0], c.expected, got)
		}
	}
}

func TestRoundFunc(t *testing.T) {
	ctx := mockContext.NewMockContext("testRound", "op1")
	fctx := kctx.NewDefaultFuncContext(ctx, 2)
	registerMathFunc()

	f := builtins["round"]
	cases := []struct {
		name   string
		args   []any
		valErr string
		runErr string
		exp    float64
	}{
		{name: "round int", args: []any{16, 2}, exp: 16.0},
		{name: "trunc ceiling", args: []any{25.987, 2}, exp: 25.99},
		{name: "trunc floor", args: []any{25.919, 1}, exp: 25.9},
		{name: "trunc mul ceil", args: []any{9.9999, 3}, exp: 10},
		{name: "negative", args: []any{56788.34, -3}, exp: 57000},
		{name: "overflow v", args: []any{1.8e+306 + 0.56784, 2}, exp: 1.8e+306 + 0.57},
		{name: "val error", args: []any{1, 2, 3}, exp: 1.8e+306 + 0.57, valErr: "Expect 1 or 2 arguments only"},
		{name: "type 1 error", args: []any{"1", "2"}, exp: 1.8e+306 + 0.57, valErr: "Expect number - float or int type for parameter 1"},
		{name: "type 2 error", args: []any{1, "2"}, exp: 1.8e+306 + 0.57, valErr: "Expect number - float or int type for parameter 2"},
		{name: "rt 2 error", args: []any{1, []string{"sa"}}, exp: 1.8e+306 + 0.57, runErr: "The second argument must be an integer: cannot convert []string([sa]) to int"},
		{name: "rt 1 error", args: []any{[]string{"sa"}, 23}, exp: 1.8e+306 + 0.57, runErr: "cannot convert []string([sa]) to float64"},
		{name: "round 1 arg", args: []any{10.5}, exp: 11},
		{name: "round 1 negative", args: []any{-10.5}, exp: -11},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ee := make([]ast.Expr, len(c.args))
			for i, arg := range c.args {
				switch at := arg.(type) {
				case int:
					ee[i] = &ast.IntegerLiteral{Val: int64(at)}
				case float64:
					ee[i] = &ast.NumberLiteral{Val: at}
				case string:
					ee[i] = &ast.StringLiteral{Val: at}
				default:
					ee[i] = &ast.NumberLiteral{Val: 0.0}
				}
			}
			err := f.val(fctx, ee)
			if c.valErr != "" {
				require.EqualError(t, err, c.valErr)
				return
			} else {
				require.NoError(t, err)
			}
			got, re := f.exec(fctx, c.args)
			if c.runErr != "" {
				require.False(t, re)
				eee := got.(error)
				require.EqualError(t, eee, c.runErr)
			} else {
				require.True(t, re)
				require.Equal(t, c.exp, got)
			}
		})
	}
}
