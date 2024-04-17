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

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestStrFuncNil(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerStrFunc()
	for name, function := range builtins {
		switch name {
		case "concat":
			r, b := function.exec(fctx, []interface{}{"1", nil, "2"})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, "12", r)
		case "endswith", "regexp_matches", "startswith":
			r, b := function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, false, r)
		case "indexof":
			r, b := function.exec(fctx, []interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, -1, r)
		case "length", "numbytes":
			r, b := function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Equal(t, 0, r)
		default:
			r, b := function.check([]interface{}{nil})
			require.True(t, b, fmt.Sprintf("%v failed", name))
			require.Nil(t, r, fmt.Sprintf("%v failed", name))
		}
	}
}

func TestSplitValueFunctions(t *testing.T) {
	f, ok := builtins["split_value"]
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
		ok     bool
	}{
		{ // 0
			args:   []interface{}{"a/b/c", "/", 0},
			result: "a",
			ok:     true,
		},
		{ // 0
			args:   []interface{}{"a/b/c", "/", -1},
			result: "c",
			ok:     true,
		},
		{ // 0
			args:   []interface{}{"a/b/c", "/", 3},
			result: errors.New("3 out of index array (size = 3)"),
			ok:     false,
		},
		{ // 0
			args:   []interface{}{"a/b/c", "/", -4},
			result: errors.New("-4 out of index array (size = 3)"),
			ok:     false,
		},
	}
	for _, tt := range tests {
		result, ok := f.exec(fctx, tt.args)
		require.Equal(t, tt.ok, ok)
		require.Equal(t, tt.result, result)
	}
}

func TestStrFunc(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	oldBuiltins := builtins
	defer func() {
		builtins = oldBuiltins
	}()
	builtins = map[string]builtinFunc{}
	registerStrFunc()

	testFormat(t, fctx)
	testFormatLocale(t, fctx)
}

func testFormat(t *testing.T, fctx *kctx.DefaultFuncContext) {
	fFormat := builtins["format"]
	cases := []struct {
		x    float64
		d    int
		want interface{}
	}{
		{12332.123456, 4, "12332.1235"},
		{12332.1, 4, "12332.1000"},
		{12332.2, 0, "12332"},
		{12332.2, 2, "12332.20"},
		{12332.2, -1, fmt.Errorf("the decimal places must greater or equal than 0")},
	}
	for _, c := range cases {
		got, _ := fFormat.exec(fctx, []interface{}{c.x, c.d})
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("formatNumber(%f, %d) == %s, want %s", c.x, c.d, got, c.want)
		}
	}
}

func testFormatLocale(t *testing.T, fctx *kctx.DefaultFuncContext) {
	fFormat := builtins["format"]
	cases := []struct {
		number    interface{}
		precision interface{}
		locale    string
		ret       interface{}
	}{
		{12332.123456, 4, "en_US", "12,332.1235"},
		{12332.1, 4, "en_US", "12,332.1000"},
		{12332.2, 0, "en_US", "12,332"},
		{12332.2, 2, "en_US", "12,332.20"},

		{12332.12341111111111111111111111111111111111111, 4, "en_US", "12,332.1234"},
		{12332.1234567, 4, "en_US", "12,332.1235"},
		{12332.2, 2, "de_DE", "12.332,20"},
		{12332.2, 2, "zh_CN", "12,332.20"},
		{12332.2, 2, "zh_HK", "12,332.20"},
		{-123456.7899, 2, "de_CH", "-123’456.79"},
		{98765.4321, 2, "not_exist_locale", errors.New("not support for the specific locale:not_exist_locale")},
		{98765.4321, 2, "ff", errors.New("not support for the specific locale:ff")},

		{12332.123456, 4, "ar_AE", "12,332.1235"},
		{12332.1, 4, "ar_BH", "12,332.1000"},
		{12332.2, 0, "ar_DZ", "12.332"},
		{12332.2, 2, "ar_EG", "12,332.20"},
		{12332.12341111111111111111111111111111111111111, 4, "ar_IN", "١٢٬٣٣٢٫١٢٣٤"},
		{12332.1234567, 4, "ar_IQ", "12,332.1235"},
		{12332.2, 2, "ar_JO", "12,332.20"},
		{12332.2, 2, "ar_KW", "12,332.20"},
		{12332.2, 2, "ar_LB", "12.332,20"},
		{12332.2, 2, "ar_LY", "12.332,20"},
		{12332.2, 2, "ar_MA", "12.332,20"},
		{12332.2, 2, "ar_OM", "12,332.20"},
		{12332.2, 2, "ar_QA", "12,332.20"},
		{12332.2, 2, "ar_SA", "12,332.20"},
		{12332.2, 2, "ar_SD", "12,332.20"},
		{12332.2, 2, "ar_SY", "12,332.20"},
		{12332.2, 2, "ar_TN", "12.332,20"},
		{12332.2, 2, "ar_YE", "12,332.20"},

		{12332.123456, 4, "be_BY", "12 332,1235"},
		{12332.1, 4, "bg_BG", "12 332,1000"},

		{12332.2, 0, "ca_ES", "12.332"},
		{12332.2, 2, "cs_CZ", "12 332,20"},

		{12332.2, 2, "da_DK", "12.332,20"},
		{12332.2, 2, "de_AT", "12 332,20"},
		{12332.2, 2, "de_BE", "12.332,20"},
		{12332.2, 2, "de_CH", "12’332.20"},
		{12332.2, 2, "de_DE", "12.332,20"},
		{12332.2, 2, "de_LU", "12.332,20"},

		{12332.2, 2, "el_GR", "12.332,20"},
		{12332.123456, 4, "en_AU", "12,332.1235"},
		{12332.1, 4, "en_CA", "12,332.1000"},
		{12332.2, 0, "en_GB", "12,332"},
		{12332.2, 2, "en_IN", "12,332.20"},
		{12332.2, 2, "en_NZ", "12,332.20"},
		{12332.2, 2, "en_PH", "12,332.20"},
		{12332.2, 2, "en_US", "12,332.20"},
		{12332.2, 2, "en_ZA", "12 332,20"},
		{12332.2, 2, "en_ZW", "12,332.20"},
		{12332.2, 2, "es_AR", "12.332,20"},
		{12332.2, 2, "es_BO", "12.332,20"},
		{12332.2, 2, "es_CL", "12.332,20"},
		{12332.2, 2, "es_CO", "12.332,20"},
		{12332.2, 2, "es_CR", "12 332,20"},
		{12332.2, 2, "es_DO", "12,332.20"},
		{12332.2, 2, "es_EC", "12.332,20"},
		{12332.2, 2, "es_ES", "12.332,20"},
		{12332.2, 2, "es_GT", "12,332.20"},
		{12332.2, 2, "es_HN", "12,332.20"},
		{12332.2, 2, "es_MX", "12,332.20"},
		{12332.2, 2, "es_NI", "12,332.20"},
		{12332.2, 2, "es_PA", "12,332.20"},
		{12332.2, 2, "es_PE", "12,332.20"},
		{12332.2, 2, "es_PR", "12,332.20"},
		{12332.2, 2, "es_PY", "12.332,20"},
		{12332.2, 2, "es_SV", "12,332.20"},
		{12332.2, 2, "es_US", "12,332.20"},
		{12332.2, 2, "es_UY", "12.332,20"},
		{12332.2, 2, "es_VE", "12.332,20"},
		{12332.123456, 4, "et_EE", "12 332,1235"},
		{12332.1, 4, "eu_ES", "12.332,1000"},

		{12332.2, 2, "fi_FI", "12 332,20"},
		{12332.2, 2, "fo_FO", "12.332,20"},
		{12332.2, 2, "fr_BE", "12 332,20"},
		{12332.2, 2, "fr_CA", "12 332,20"},
		{12332.2, 2, "fr_CH", "12 332,20"},
		{12332.2, 2, "fr_FR", "12 332,20"},
		{12332.2, 2, "fr_LU", "12.332,20"},
		{12332.2, 2, "gl_ES", "12.332,20"},
		{12332.2, 2, "gu_IN", "12,332.20"},
		{12332.2, 2, "he_IL", "12,332.20"},
		{12332.2, 2, "hi_IN", "12,332.20"},
		{12332.2, 2, "hr_HR", "12.332,20"},
		{12332.2, 2, "hu_HU", "12 332,20"},
		{12332.2, 2, "id_ID", "12.332,20"},
		{12332.2, 2, "is_IS", "12.332,20"},
		{12332.2, 2, "it_CH", "12’332.20"},
		{12332.2, 2, "it_IT", "12.332,20"},
		{12332.2, 2, "ja_JP", "12,332.20"},
		{12332.2, 2, "ko_KR", "12,332.20"},
		{12332.2, 2, "lt_LT", "12 332,20"},
		{12332.2, 2, "lv_LV", "12 332,20"},
		{12332.2, 2, "mk_MK", "12.332,20"},
		{12332.2, 2, "mn_MN", "12,332.20"},
		{12332.2, 2, "ms_MY", "12,332.20"},
		{12332.2, 2, "nb_NO", "12 332,20"},
		{12332.2, 2, "nl_BE", "12.332,20"},
		{12332.2, 2, "nl_NL", "12.332,20"},
		{12332.2, 2, "no_NO", "12,332.20"},
		{12332.2, 2, "pl_PL", "12 332,20"},
		{12332.2, 2, "pt_BR", "12.332,20"},
		{12332.2, 2, "pt_PT", "12 332,20"},
		{12332.2, 2, "rm_CH", "12’332.20"},
		{12332.2, 2, "ro_RO", "12.332,20"},
		{12332.2, 2, "ru_RU", "12 332,20"},
		{12332.2, 2, "ru_UA", "12 332,20"},
		{12332.2, 2, "sk_SK", "12 332,20"},
		{12332.2, 2, "sl_SI", "12.332,20"},
		{12332.2, 2, "sq_AL", "12 332,20"},
		{12332.2, 2, "sr_RS", "12.332,20"},
		{12332.2, 2, "sv_FI", "12 332,20"},
		{12332.2, 2, "sv_SE", "12 332,20"},
		{12332.2, 2, "ta_IN", "12,332.20"},
		{12332.2, 2, "te_IN", "12,332.20"},
		{12332.2, 2, "th_TH", "12,332.20"},
		{12332.2, 2, "tr_TR", "12.332,20"},
		{12332.2, 2, "uk_UA", "12 332,20"},
		{12332.2, 2, "ur_PK", "12,332.20"},
		{12332.2, 2, "vi_VN", "12.332,20"},
		{12332.2, 2, "zh_CN", "12,332.20"},
		{12332.2, 2, "zh_HK", "12,332.20"},
		{12332.2, 2, "zh_TW", "12,332.20"},
	}
	for _, c := range cases {
		got, _ := fFormat.exec(fctx, []interface{}{c.number, c.precision, c.locale})
		if !reflect.DeepEqual(got, c.ret) {
			t.Errorf("formatNumber(%f, %d,%s) == %s, want %s", c.number, c.precision, c.locale, got, c.ret)
		}
	}
}

func TestStringFuncVal(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []ast.Expr
		err      error
	}{
		{
			name:     "format failure",
			funcName: "format",
			args: []ast.Expr{
				&ast.StringLiteral{Val: "1"},
			},
			err: fmt.Errorf("At least has 2 argument but found 1."),
		},
		{
			name:     "format failure",
			funcName: "format",
			args: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
				&ast.StringLiteral{Val: "1"},
			},
			err: fmt.Errorf("Expect integer type for parameter 2"),
		},
		{
			name:     "format success",
			funcName: "format",
			args: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
				&ast.IntegerLiteral{Val: 0},
			},
			err: nil,
		},
		{
			name:     "format failure",
			funcName: "format",
			args: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
				&ast.IntegerLiteral{Val: 0},
				&ast.IntegerLiteral{Val: 0},
			},
			err: fmt.Errorf("Expect string type for parameter 3"),
		},
		{
			name:     "format success",
			funcName: "format",
			args: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
				&ast.IntegerLiteral{Val: 0},
				&ast.StringLiteral{Val: "en"},
			},
			err: nil,
		},
	}

	registerStrFunc()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, ok := builtins[tt.funcName]
			assert.True(t, ok)
			err := f.val(nil, tt.args)
			assert.Equal(t, tt.err, err)
		})
	}
}
