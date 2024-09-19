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
	"errors"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

var errTooManyArguments = errors.New("too many arguments")

type IntervalUnit string

// registerDateTimeFunc registers the date and time functions.
func registerDateTimeFunc() {
	builtins["now"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  execGetCurrentDateTime(false),
		val:   validFspArgs(),
	}
	builtins["current_timestamp"] = builtins["now"]
	builtins["local_time"] = builtins["now"]
	builtins["local_timestamp"] = builtins["now"]

	builtins["cur_date"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  execGetCurrentDate(),
		val:   ValidateNoArg,
	}
	builtins["current_date"] = builtins["cur_date"]

	builtins["cur_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  execGetCurrentDateTime(true),
		val:   validFspArgs(),
	}
	builtins["current_time"] = builtins["cur_time"]

	builtins["format_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
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
		check: returnNilIfHasAnyNil,
	}
	builtins["date_calc"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			arg1 := cast.ToStringAlways(args[1])

			unitSign := 1
			if len(arg1) > 0 && arg1[0] == '-' {
				unitSign = -1
				arg1 = arg1[1:]
			}

			unit, err := cast.InterfaceToDuration(cast.ToStringAlways(arg1))
			if err != nil {
				return err, false
			}

			t, err := cast.FormatTime(arg0.Add(unit*time.Duration(unitSign)), "yyyy-MM-dd HH:mm:ss")
			if err != nil {
				return err, false
			}

			return t, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}

			if !ast.IsStringArg(args[1]) {
				return ProduceErrInfo(1, "string")
			}
			return nil
		},
	}
	builtins["date_diff"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			arg1, err := cast.InterfaceToTime(args[1], "")
			if err != nil {
				return err, false
			}
			return arg1.Sub(arg0), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}

			if ast.IsNumericArg(args[1]) || ast.IsStringArg(args[1]) || ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day_name"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.Weekday().String(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day_of_month"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.Day(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day"] = builtins["day_of_month"]

	builtins["day_of_week"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.Weekday(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day_of_year"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.YearDay(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["from_days"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			days, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return err, false
			}

			if days == 0 {
				return nil, true
			}

			t := time.Unix(0, 0).Add(time.Duration(days-1) * 24 * time.Hour)
			result, err := cast.FormatTime(t, "yyyy-MM-dd")
			if err != nil {
				return err, false
			}
			return result, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "int")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["from_unix_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			seconds, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return err, false
			}

			if seconds == 0 {
				return nil, true
			}
			t := time.Unix(int64(seconds), 0).In(cast.GetConfiguredTimeZone())
			result, err := cast.FormatTime(t, "yyyy-MM-dd HH:mm:ss")
			if err != nil {
				return err, false
			}
			return result, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "int")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["hour"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Hour(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["last_day"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			year, month, _ := arg0.Date()
			lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)
			result, err := cast.FormatTime(lastDay, "yyyy-MM-dd")
			if err != nil {
				return err, false
			}
			return result, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["microsecond"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Nanosecond() / 1000, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["minute"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Minute(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["month"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return int(arg0.Month()), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["month_name"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Month().String(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(1, "datetime")
			}
			return nil
		},
	}
	builtins["second"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Second(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
}

func execGetCurrentDate() funcExe {
	return func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
		formatted, err := cast.FormatTime(time.Now(), "yyyy-MM-dd")
		if err != nil {
			return err, false
		}
		return formatted, true
	}
}

// validFspArgs returns a function that validates the 'fsp' arg.
func validFspArgs() funcVal {
	return func(ctx api.FunctionContext, args []ast.Expr) error {
		if len(args) < 1 {
			return nil
		}

		if len(args) > 1 {
			return errTooManyArguments
		}

		if !ast.IsIntegerArg(args[0]) {
			return ProduceErrInfo(0, "int")
		}

		return nil
	}
}

func execGetCurrentDateTime(timeOnly bool) funcExe {
	return func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
		fsp := 0
		switch len(args) {
		case 0:
			fsp = 0
		default:
			fsp = args[0].(int)
		}
		formatted, err := getCurrentWithFsp(fsp, timeOnly)
		if err != nil {
			return err, false
		}
		return formatted, true
	}
}

// getCurrentWithFsp returns the current date/time with the specified number of fractional seconds precision.
func getCurrentWithFsp(fsp int, timeOnly bool) (string, error) {
	format := "yyyy-MM-dd HH:mm:ss"
	now := timex.GetNow().In(cast.GetConfiguredTimeZone())
	switch fsp {
	case 1:
		format += ".S"
	case 2:
		format += ".SS"
	case 3:
		format += ".SSS"
	case 4:
		format += ".SSSS"
	case 5:
		format += ".SSSSS"
	case 6:
		format += ".SSSSSS"
	default:
	}

	formatted, err := cast.FormatTime(now, format)
	if err != nil {
		return "", err
	}

	if timeOnly {
		return strings.SplitN(formatted, " ", 2)[1], nil
	}

	return formatted, nil
}
