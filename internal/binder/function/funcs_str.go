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
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func registerStrFunc() {
	builtins["concat"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			var b bytes.Buffer
			for _, arg := range args {
				b.WriteString(cast.ToStringAlways(arg))
			}
			return b.String(), true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) == 0 {
				return fmt.Errorf("The arguments should be at least one.")
			}
			for i, a := range args {
				if ast.IsNumericArg(a) || ast.IsTimeArg(a) || ast.IsBooleanArg(a) {
					return ProduceErrInfo(i, "string")
				}
			}
			return nil
		},
	}
	builtins["endswith"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil || args[1] == nil {
				return false, true
			}
			arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
			return strings.HasSuffix(arg0, arg1), true
		},
		val: ValidateTwoStrArg,
	}
	builtins["indexof"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil || args[1] == nil {
				return -1, true
			}
			arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
			return strings.Index(arg0, arg1), true
		},
		val: ValidateTwoStrArg,
	}
	builtins["length"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := cast.ToStringAlways(args[0])
			switch v := args[0].(type) {
			case []interface{}:
				return len(v), true
			case map[string]interface{}:
				return len(v), true
			default:
			}
			return utf8.RuneCountInString(arg0), true
		},
		val: ValidateOneArg,
	}
	builtins["lower"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			return strings.ToLower(arg0), true
		},
		val: ValidateOneStrArg,
	}
	builtins["lpad"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			arg1, err := cast.ToInt(args[1], cast.STRICT)
			if err != nil {
				return err, false
			}
			return strings.Repeat(" ", arg1) + arg0, true
		},
		val: ValidateOneStrOneInt,
	}
	builtins["ltrim"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			return strings.TrimLeftFunc(arg0, unicode.IsSpace), true
		},
		val: ValidateOneStrArg,
	}
	builtins["numbytes"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := cast.ToStringAlways(args[0])
			return len(arg0), true
		},
		val: ValidateOneStrArg,
	}
	builtins["format_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			arg1 := cast.ToStringAlways(args[1])
			if s, err := cast.FormatTime(arg0, arg1); err == nil {
				return s, true
			} else {
				return err, false
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			if ast.IsNumericArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(1, "string")
			}
			return nil
		},
	}
	builtins["regexp_matches"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil || args[1] == nil {
				return false, true
			}
			arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
			if matched, err := regexp.MatchString(arg1, arg0); err != nil {
				return err, false
			} else {
				return matched, true
			}
		},
		val: ValidateTwoStrArg,
	}
	builtins["regexp_replace"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil || args[1] == nil || args[2] == nil {
				return nil, true
			}
			arg0, arg1, arg2 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1]), cast.ToStringAlways(args[2])
			if re, err := regexp.Compile(arg1); err != nil {
				return err, false
			} else {
				return re.ReplaceAllString(arg0, arg2), true
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(3, len(args)); err != nil {
				return err
			}
			for i := 0; i < 3; i++ {
				if ast.IsNumericArg(args[i]) || ast.IsTimeArg(args[i]) || ast.IsBooleanArg(args[i]) {
					return ProduceErrInfo(i, "string")
				}
			}
			return nil
		},
	}
	builtins["regexp_substr"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil || args[1] == nil {
				return nil, true
			}
			arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
			if re, err := regexp.Compile(arg1); err != nil {
				return err, false
			} else {
				return re.FindString(arg0), true
			}
		},
		val: ValidateTwoStrArg,
	}
	builtins["rpad"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			arg1, err := cast.ToInt(args[1], cast.STRICT)
			if err != nil {
				return err, false
			}
			return arg0 + strings.Repeat(" ", arg1), true
		},
		val: ValidateOneStrOneInt,
	}
	builtins["rtrim"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			return strings.TrimRightFunc(arg0, unicode.IsSpace), true
		},
		val: ValidateOneStrArg,
	}
	builtins["substring"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			arg1, err := cast.ToInt(args[1], cast.STRICT)
			if err != nil {
				return err, false
			}
			if arg1 < 0 {
				return fmt.Errorf("start index must be a positive number"), false
			}
			if len(args) > 2 {
				arg2, err := cast.ToInt(args[2], cast.STRICT)
				if err != nil {
					return err, false
				}
				if arg2 < 0 {
					return fmt.Errorf("end index must be a positive number"), false
				}
				if arg1 > arg2 {
					return fmt.Errorf("start index must be smaller than end index"), false
				}
				if arg1 > len(arg0) {
					return "", true
				}
				if arg2 > len(arg0) {
					return arg0[arg1:], true
				}
				return arg0[arg1:arg2], true
			} else {
				if arg1 > len(arg0) {
					return "", true
				}
				return arg0[arg1:], true
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			l := len(args)
			if l != 2 && l != 3 {
				return fmt.Errorf("the arguments for substring should be 2 or 3")
			}
			if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "string")
			}
			for i := 1; i < l; i++ {
				if ast.IsFloatArg(args[i]) || ast.IsTimeArg(args[i]) || ast.IsBooleanArg(args[i]) || ast.IsStringArg(args[i]) {
					return ProduceErrInfo(i, "int")
				}
			}

			if s, ok := args[1].(*ast.IntegerLiteral); ok {
				sv := s.Val
				if sv < 0 {
					return fmt.Errorf("The start index should not be a nagtive integer.")
				}
				if l == 3 {
					if e, ok1 := args[2].(*ast.IntegerLiteral); ok1 {
						ev := e.Val
						if ev < sv {
							return fmt.Errorf("The end index should be larger than start index.")
						}
					}
				}
			}
			return nil
		},
	}
	builtins["startswith"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return false, true
			}
			arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
			return strings.HasPrefix(arg0, arg1), true
		},
		val: ValidateTwoStrArg,
	}
	builtins["split_value"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil || args[1] == nil {
				return nil, true
			}
			arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
			ss := strings.Split(arg0, arg1)
			v, _ := cast.ToInt(args[2], cast.STRICT)
			if v > (len(ss) - 1) {
				return fmt.Errorf("%d out of index array (size = %d)", v, len(ss)), false
			} else {
				return ss[v], true
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			l := len(args)
			if l != 3 {
				return fmt.Errorf("the arguments for split_value should be 3")
			}
			if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "string")
			}
			if ast.IsNumericArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(1, "string")
			}
			if ast.IsFloatArg(args[2]) || ast.IsTimeArg(args[2]) || ast.IsBooleanArg(args[2]) || ast.IsStringArg(args[2]) {
				return ProduceErrInfo(2, "int")
			}
			if s, ok := args[2].(*ast.IntegerLiteral); ok {
				if s.Val < 0 {
					return fmt.Errorf("The index should not be a nagtive integer.")
				}
			}
			return nil
		},
	}
	builtins["trim"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			return strings.TrimSpace(arg0), true
		},
		val: ValidateOneStrArg,
	}
	builtins["upper"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			return strings.ToUpper(arg0), true
		},
		val: ValidateOneStrArg,
	}
}
