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

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestDateTimeFunctions(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	tests := []struct {
		name    string
		args    []interface{}
		val     func(t interface{}) error
		exec    bool
		valArgs []ast.Expr
	}{
		{
			name: "now",
			args: []interface{}{},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss")
				return err
			},
			exec: true,
		},
		{
			name: "now",
			args: []interface{}{1},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.S")
				return err
			},
			exec: true,
		},
		{
			name: "now",
			args: []interface{}{2},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SS")
				return err
			},
			exec: true,
		},
		{
			name: "now",
			args: []interface{}{3},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSS")
				return err
			},
			exec: true,
		},
		{
			name: "now",
			args: []interface{}{4},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSSS")
				return err
			},
			exec: true,
		},
		{
			name: "now",
			args: []interface{}{5},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSSSS")
				return err
			},
			exec: true,
		},
		{
			name: "now",
			args: []interface{}{6},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd HH:mm:ss.SSSSSS")
				return err
			},
			exec: true,
		},
		{
			name: "now",
			val: func(t interface{}) error {
				if !reflect.DeepEqual(t, errTooManyArguments) {
					return errors.New("mismatch error")
				}
				return nil
			},
			exec:    false,
			valArgs: []ast.Expr{&ast.IntegerLiteral{Val: 1}, &ast.IntegerLiteral{Val: 2}},
		},
		{
			name: "cur_date",
			args: []interface{}{},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd")
				return err
			},
			exec: true,
		},
		{
			name: "cur_date",
			exec: false,
			val: func(t interface{}) error {
				if !reflect.DeepEqual(t, errors.New("Expect 0 arguments but found 1.")) {
					return errors.New("mismatch error")
				}
				return nil
			},
			valArgs: []ast.Expr{&ast.IntegerLiteral{Val: 1}},
		},
		{
			name: "cur_time",
			exec: true,
			args: []interface{}{},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "HH:mm:ss")
				return err
			},
		},
		{
			name: "cur_time",
			exec: true,
			args: []interface{}{1},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "HH:mm:ss.S")
				return err
			},
		},
		{
			name: "format_time",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00", "yyyy-MM-dd"},
			val: func(t interface{}) error {
				_, err := cast.ParseTime(t.(string), "yyyy-MM-dd")
				return err
			},
		},
		{
			name: "date_calc",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00", "24h"},
			val: func(t interface{}) error {
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
			name: "date_calc",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00", "-24h"},
			val: func(t interface{}) error {
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
			name: "date_diff",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00", "2019-01-02 00:00:00"},
			val: func(t interface{}) error {
				result := t.(time.Duration)
				if result.Milliseconds() != 24*3600*1000 {
					return fmt.Errorf("mismatch result, expect %d, got %d", 26*3600*1000, result.Milliseconds())
				}
				return nil
			},
		},
		{
			name: "day_name",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00"},
			val: func(t interface{}) error {
				if t.(string) != "Tuesday" {
					return fmt.Errorf("mismatch day name, expect %s, got %s", "Tuesday", t.(string))
				}
				return nil
			},
		},
		{
			name: "day_of_month",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00"},
			val: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch day of month, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			name: "day_of_week",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00"},
			val: func(t interface{}) error {
				if t.(time.Weekday) != time.Tuesday {
					return fmt.Errorf("mismatch day of week, expect %d, got %d", time.Tuesday, t.(time.Weekday))
				}
				return nil
			},
		},
		{
			name: "day_of_year",
			exec: true,
			args: []interface{}{"2019-01-01 00:00:00"},
			val: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch day of year, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			name: "from_days",
			exec: true,
			args: []interface{}{100},
			val: func(t interface{}) error {
				if t.(string) != "1970-04-10" {
					return fmt.Errorf("mismatch date, expect %s, got %s", "2019-01-01", t.(string))
				}
				return nil
			},
		},
		{
			name: "from_unix_time",
			exec: true,
			args: []interface{}{100},
			val: func(t interface{}) error {
				if t.(string) != "1970-01-01 08:01:40" {
					return fmt.Errorf("mismatch date, expect %s, got %s", "1970-01-01 00:01:40", t.(string))
				}
				return nil
			},
		},
		{
			name: "hour",
			exec: true,
			args: []interface{}{"2019-01-01 01:00:00"},
			val: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch hour, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			name: "last_day",
			exec: true,
			args: []interface{}{"2019-01-01 01:00:00"},
			val: func(t interface{}) error {
				if t.(string) != "2019-01-31" {
					return fmt.Errorf("mismatch date, expect %s, got %s", "2019-01-31", t.(string))
				}
				return nil
			},
		},
		{
			name: "microsecond",
			exec: true,
			args: []interface{}{"2019-01-01 01:00:00.123456"},
			val: func(t interface{}) error {
				if t.(int) != 123456 {
					return fmt.Errorf("mismatch microsecond, expect %d, got %d", 123456, t.(int))
				}
				return nil
			},
		},
		{
			name: "minute",
			exec: true,
			args: []interface{}{"2019-01-01 01:23:45"},
			val: func(t interface{}) error {
				if t.(int) != 23 {
					return fmt.Errorf("mismatch minute, expect %d, got %d", 23, t.(int))
				}
				return nil
			},
		},
		{
			name: "month",
			exec: true,
			args: []interface{}{"2019-01-01 01:23:45"},
			val: func(t interface{}) error {
				if t.(int) != 1 {
					return fmt.Errorf("mismatch month, expect %d, got %d", 1, t.(int))
				}
				return nil
			},
		},
		{
			name: "month_name",
			exec: true,
			args: []interface{}{"2019-01-01 01:23:45"},
			val: func(t interface{}) error {
				if t.(string) != "January" {
					return fmt.Errorf("mismatch month name, expect %s, got %s", "January", t.(string))
				}
				return nil
			},
		},
		{
			name: "second",
			exec: true,
			args: []interface{}{"2019-01-01 01:23:45"},
			val: func(t interface{}) error {
				if t.(int) != 45 {
					return fmt.Errorf("mismatch second, expect %d, got %d", 45, t.(int))
				}
				return nil
			},
		},
	}

	for i, test := range tests {
		f, ok := builtins[test.name]
		if !ok {
			t.Fatalf("builtin '%s' not found", test.name)
		}

		var result interface{}
		if test.exec {
			result, _ = f.exec(fctx, test.args)
		} else {
			result = f.val(fctx, test.valArgs)
		}
		if err := test.val(result); err != nil {
			t.Errorf("%d result mismatch: %v", i, err)
		}
	}
}
