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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// TestDateTimeFunctions test the date and time functions.
func TestDateTimeFunctions(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	tests := []struct {
		// testCaseName represent the name of the test case
		testCaseName string
		// funcName represent the SQL function name to be tested
		funcName string
		// execArgs represent the arguments to be passed to the function
		execArgs []interface{}
		// valFunc represent the function to validate the result
		valFunc func(t interface{}) error
		// execTest represent whether to test the exec function
		execTest bool
		// valArgs represent the arguments to be passed to the builtinFunc.val
		valArgs []ast.Expr
	}{
		{
			testCaseName: "test now() with no args",
			funcName:     "now",
			execArgs:     []interface{}{},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test now() with fsp set to 1",
			funcName:     "now",
			execArgs:     []interface{}{1},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.S")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test now() with fsp set to 2",
			funcName:     "now",
			execArgs:     []interface{}{2},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SS")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test now() with fsp set to 3",
			funcName:     "now",
			execArgs:     []interface{}{3},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSS")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test now() with fsp set to 4",
			funcName:     "now",
			execArgs:     []interface{}{4},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSSS")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test now() with fsp set to 5",
			funcName:     "now",
			execArgs:     []interface{}{5},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSSSS")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test now() with fsp set to 6",
			funcName:     "now",
			execArgs:     []interface{}{6},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSSSSS")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test now() with too many args",
			funcName:     "now",
			valFunc: func(t interface{}) error {
				if !reflect.DeepEqual(t, errTooManyArguments) {
					return errors.New("mismatch error")
				}
				return nil
			},
			execTest: false,
			valArgs:  []ast.Expr{&ast.IntegerLiteral{Val: 1}, &ast.IntegerLiteral{Val: 2}},
		},
		{
			testCaseName: "test cur_date() with no args",
			funcName:     "cur_date",
			execArgs:     []interface{}{},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd")
				return err
			},
			execTest: true,
		},
		{
			testCaseName: "test cur_date() with too many args",
			funcName:     "cur_date",
			execTest:     false,
			valFunc: func(t interface{}) error {
				if !reflect.DeepEqual(t, errors.New("Expect 0 arguments but found 1.")) {
					return errors.New("mismatch error")
				}
				return nil
			},
			valArgs: []ast.Expr{&ast.IntegerLiteral{Val: 1}},
		},
		{
			testCaseName: "test cur_time() with no args",
			funcName:     "cur_time",
			execTest:     true,
			execArgs:     []interface{}{},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "HH:mm:ss")
				return err
			},
		},
		{
			testCaseName: "test cur_time() with fsp set to 1",
			funcName:     "cur_time",
			execTest:     true,
			execArgs:     []interface{}{1},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "HH:mm:ss.S")
				return err
			},
		},
		{
			testCaseName: "test format_time() with 'yyyy-MM-dd' format",
			funcName:     "format_time",
			execTest:     true,
			execArgs:     []interface{}{time.Now(), "yyyy-MM-dd"},
			valFunc: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd")
				return err
			},
		},
		{
			testCaseName: "test format_time() with 1 arg",
			funcName:     "format_time",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				if !reflect.DeepEqual(t, errors.New("Expect 2 arguments but found 1.")) {
					return errors.New("mismatch error")
				}
				return nil
			},
		},
		{
			testCaseName: "test format_time() with invalid date time arg",
			funcName:     "format_time",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
				&ast.StringLiteral{Val: "yyyy-MM-dd"},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test date_calc() for add 1 day (24h)",
			funcName:     "date_calc",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 00:00:00", "24h"},
			valFunc: func(t interface{}) error {
				parsed, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss")
				if err != nil {
					return err
				}
				if parsed.Day() != 2 {
					return fmt.Errorf("mismatch days, expect %d, got %d", 2, parsed.Day())
				}
				return nil
			},
		},
		{
			testCaseName: "test date_calc() with invalid date time arg",
			funcName:     "date_calc",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
				&ast.StringLiteral{Val: "24h"},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test date_calc() with no args",
			funcName:     "date_calc",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 2 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test date_calc() for sub 1 day (24h)",
			funcName:     "date_calc",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 00:00:00", "-24h"},
			valFunc: func(t interface{}) error {
				parsed, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss")
				if err != nil {
					return err
				}
				if parsed.Day() != 31 {
					return fmt.Errorf("mismatch days, expect %d, got %d", 31, parsed.Day())
				}
				return nil
			},
		},
		{
			testCaseName: "test date_diff with 2 args",
			funcName:     "date_diff",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 00:00:00", "2019-01-02 00:00:00"},
			valFunc: func(t interface{}) error {
				result := t.(time.Duration)
				if result.Milliseconds() != 24*3600*1000 {
					return fmt.Errorf("mismatch result, expect %d, got %d", 26*3600*1000, result.Milliseconds())
				}
				return nil
			},
		},
		{
			testCaseName: "test date_diff with no arg",
			funcName:     "date_diff",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 2 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test date_diff with invalid date time arg",
			funcName:     "date_diff",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
				&ast.IntegerLiteral{Val: 2},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_name with 1 arg",
			funcName:     "day_name",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 00:00:00"},
			valFunc: func(t interface{}) error {
				if t.(string) != "Tuesday" {
					return fmt.Errorf("mismatch day name, expect %s, got %s", "Tuesday", t.(string))
				}
				return nil
			},
		},
		{
			testCaseName: "test day_name with no arg",
			funcName:     "day_name",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_name with invalid date time arg",
			funcName:     "day_name",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_month with 1 arg",
			funcName:     "day_of_month",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 00:00:00"},
			valFunc: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch day of month, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_month with no arg",
			funcName:     "day_of_month",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_month with invalid date time arg",
			funcName:     "day_of_month",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_week with 1 arg",
			funcName:     "day_of_week",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 00:00:00"},
			valFunc: func(t interface{}) error {
				if t.(time.Weekday) != time.Tuesday {
					return fmt.Errorf("mismatch day of week, expect %d, got %d", time.Tuesday, t.(time.Weekday))
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_week with no arg",
			funcName:     "day_of_week",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_week with invalid date time arg",
			funcName:     "day_of_week",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_year with 1 arg",
			funcName:     "day_of_year",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 00:00:00"},
			valFunc: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch day of year, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_year with no arg",
			funcName:     "day_of_year",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test day_of_year with invalid date time arg",
			funcName:     "day_of_year",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test from_days with no arg",
			funcName:     "from_days",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test from_days with invalid date time arg",
			funcName:     "from_days",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect int type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test from_days with 100",
			funcName:     "from_days",
			execTest:     true,
			execArgs:     []interface{}{100},
			valFunc: func(t interface{}) error {
				if t.(string) != "1970-04-10" {
					return fmt.Errorf("mismatch date, expect %s, got %s", "2019-01-01", t.(string))
				}
				return nil
			},
		},
		{
			testCaseName: "test from_unix_time with no arg",
			funcName:     "from_unix_time",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %s", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test from_unix_time with invalid arg",
			funcName:     "from_unix_time",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.NumberLiteral{Val: 0.1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect int type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test from_unix_time with 100",
			funcName:     "from_unix_time",
			execTest:     true,
			execArgs:     []interface{}{100},
			valFunc: func(t interface{}) error {
				expect := time.Unix(100, 0)

				expectStr, err := cast.FormatTime(expect, "yyyy-MM-dd HH:mm:ss")
				if err != nil {
					return err
				}

				if t.(string) != expectStr {
					return fmt.Errorf("mismatch date, expect %s, got %s", expectStr, t.(string))
				}
				return nil
			},
		},
		{
			testCaseName: "test hour with no arg",
			funcName:     "hour",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test hour with invalid date time arg",
			funcName:     "hour",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test hour with 1 arg",
			funcName:     "hour",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 01:00:00"},
			valFunc: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch hour, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			testCaseName: "test last_day with no arg",
			funcName:     "last_day",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test last_day with invalid date time arg",
			funcName:     "last_day",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test last_day with 1 arg",
			funcName:     "last_day",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 01:00:00"},
			valFunc: func(t interface{}) error {
				if t.(string) != "2019-01-31" {
					return fmt.Errorf("mismatch date, expect %s, got %s", "2019-01-31", t.(string))
				}
				return nil
			},
		},
		{
			testCaseName: "test microsecond with no arg",
			funcName:     "microsecond",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test microsecond with invalid date time arg",
			funcName:     "microsecond",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test microsecond with 1 arg",
			funcName:     "microsecond",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 01:00:00.123456"},
			valFunc: func(t interface{}) error {
				if t.(int) != 123456 {
					return fmt.Errorf("mismatch microsecond, expect %d, got %d", 123456, t.(int))
				}
				return nil
			},
		},
		{
			testCaseName: "test minute with no arg",
			funcName:     "minute",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test minute with invalid date time arg",
			funcName:     "minute",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test minute with 1 arg",
			funcName:     "minute",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 01:23:45"},
			valFunc: func(t interface{}) error {
				if t.(int) != 23 {
					return fmt.Errorf("mismatch minute, expect %d, got %d", 23, t.(int))
				}
				return nil
			},
		},
		{
			testCaseName: "test month with no arg",
			funcName:     "month",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got: %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test month with invalid date time arg",
			funcName:     "month",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect: %s, got %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test month with 1 arg",
			funcName:     "month",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 01:23:45"},
			valFunc: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch month, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			testCaseName: "test month_name with no arg",
			funcName:     "month_name",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, expect) {
					return fmt.Errorf("mismatch error, expect %s, got %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test month_name with invalid date time arg",
			funcName:     "month_name",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, t) {
					return fmt.Errorf("mismatch error, expect %s, got %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test month_name with 1 arg",
			funcName:     "month_name",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 01:23:45"},
			valFunc: func(t interface{}) error {
				if t.(string) != "January" {
					return fmt.Errorf("mismatch month name, expect %s, got %s", "January", t.(string))
				}
				return nil
			},
		},
		{
			testCaseName: "test second with no arg",
			funcName:     "second",
			execTest:     false,
			valArgs:      []ast.Expr{},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect 1 arguments but found 0.")
				if !reflect.DeepEqual(t, t) {
					return fmt.Errorf("mismatch error, expect %s, got %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test second with invalid date time arg",
			funcName:     "second",
			execTest:     false,
			valArgs: []ast.Expr{
				&ast.IntegerLiteral{Val: 1},
			},
			valFunc: func(t interface{}) error {
				expect := errors.New("Expect datetime type for parameter 1")
				if !reflect.DeepEqual(t, t) {
					return fmt.Errorf("mismatch error, expect %s, got %v", expect, t)
				}
				return nil
			},
		},
		{
			testCaseName: "test second with 1 arg",
			funcName:     "second",
			execTest:     true,
			execArgs:     []interface{}{"2019-01-01 01:23:45"},
			valFunc: func(t interface{}) error {
				if t.(int) != 45 {
					return fmt.Errorf("mismatch second, expect %d, got %d", 45, t.(int))
				}
				return nil
			},
		},
	}

	for _, test := range tests {
		f, ok := builtins[test.funcName]
		if !ok {
			t.Fatalf("builtin '%s' not found", test.funcName)
		}

		var result interface{}
		if test.execTest {
			result, _ = f.exec(fctx, test.execArgs)
		} else {
			result = f.val(fctx, test.valArgs)
		}
		if err := test.valFunc(result); err != nil {
			t.Errorf("\n%s: %q", test.testCaseName, err)
		}
	}
}

const layout = "2006-01-02 15:04:05"

func TestTimeFunctionWithTZ(t *testing.T) {
	l1, err := time.LoadLocation("UTC")
	require.NoError(t, err)
	l2, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	err = cast.SetTimeZone("UTC")
	require.NoError(t, err)
	now := time.Now().In(l1)
	m := mockclock.GetMockClock()
	m.Set(now)
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	f, ok := builtins["now"]
	require.True(t, ok)
	result, ok := f.exec(fctx, []interface{}{})
	require.True(t, ok)
	require.Equal(t, result.(string), now.Format(layout))
	err = cast.SetTimeZone("Asia/Shanghai")
	require.NoError(t, err)
	result, ok = f.exec(fctx, []interface{}{})
	require.True(t, ok)
	require.Equal(t, result.(string), now.In(l2).Format(layout))

	err = cast.SetTimeZone("UTC")
	require.NoError(t, err)
	f, ok = builtins["from_unix_time"]
	require.True(t, ok)
	result, ok = f.exec(fctx, []interface{}{1691995105})
	require.True(t, ok)
	require.Equal(t, result.(string), "2023-08-14 06:38:25")

	err = cast.SetTimeZone("Asia/Shanghai")
	require.NoError(t, err)
	result, ok = f.exec(fctx, []interface{}{1691995105})
	require.True(t, ok)
	require.Equal(t, result.(string), "2023-08-14 14:38:25")
}

func TestValidateFsp(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	f := validFspArgs()
	err := f(fctx, []ast.Expr{})
	require.NoError(t, err)
}
