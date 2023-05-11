// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package xsql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestComparison(t *testing.T) {
	testTime, _ := cast.InterfaceToTime(1541152488442, "")
	data := []struct {
		m Message
		r []interface{}
	}{
		{ // 0
			m: map[string]interface{}{
				"a": float64(32),
				"b": float64(72),
			},
			r: []interface{}{
				false, true, errors.New("invalid operation float64(32) = string(string literal)"),
				false, true, false, true, true, false,
			},
		}, { // 1
			m: map[string]interface{}{
				"a": int64(32),
				"b": int64(72),
			},
			r: []interface{}{
				false, true, errors.New("invalid operation int64(32) = string(string literal)"),
				false, true, false, true, true, false,
			},
		}, { // 2
			m: map[string]interface{}{
				"a": "32",
				"b": "72",
			},
			r: []interface{}{
				errors.New("invalid operation string(32) > int64(72)"), errors.New("invalid operation string(32) <= int64(32)"), false,
				false, true, false, true, errors.New("between operator cannot compare string(32) and int(30)"), errors.New("between operator cannot compare string(32) and int(2)"),
			},
		}, { // 3
			m: map[string]interface{}{
				"a": []interface{}{32, 72},
				"b": []interface{}{32, 72},
			},
			r: []interface{}{
				errors.New("> is an invalid operation for []interface {}"), errors.New("<= is an invalid operation for []interface {}"), errors.New("= is an invalid operation for []interface {}"),
				errors.New(">= is an invalid operation for []interface {}"), errors.New("< is an invalid operation for []interface {}"), errors.New("= is an invalid operation for []interface {}"), errors.New("!= is an invalid operation for []interface {}"), errors.New("BETWEEN is an invalid operation for []interface {}"), errors.New("NOT BETWEEN is an invalid operation for []interface {}"),
			},
		}, { // 4
			m: map[string]interface{}{
				"a": map[string]interface{}{"c": 5},
				"b": map[string]interface{}{"d": 5},
			},
			r: []interface{}{
				errors.New("> is an invalid operation for map[string]interface {}"), errors.New("<= is an invalid operation for map[string]interface {}"), errors.New("= is an invalid operation for map[string]interface {}"),
				errors.New(">= is an invalid operation for map[string]interface {}"), errors.New("< is an invalid operation for map[string]interface {}"), errors.New("= is an invalid operation for map[string]interface {}"), errors.New("!= is an invalid operation for map[string]interface {}"), errors.New("BETWEEN is an invalid operation for map[string]interface {}"), errors.New("NOT BETWEEN is an invalid operation for map[string]interface {}"),
			},
		}, { // 5
			m: map[string]interface{}{
				"a": float64(55),
				"b": int64(55),
			},
			r: []interface{}{
				false, false, errors.New("invalid operation float64(55) = string(string literal)"),
				true, false, true, false, true, false,
			},
		}, { // 6
			m: map[string]interface{}{
				"a": testTime,
				"b": int64(1541152388442),
			},
			r: []interface{}{
				true, false, errors.New("invalid operation time.Time(2018-11-02 09:54:48.442 +0000 UTC) = string(string literal)"),
				true, false, false, true, false, true,
			},
		}, { // 7
			m: map[string]interface{}{
				"a": testTime,
				"b": "2020-02-26T02:37:21.822Z",
			},
			r: []interface{}{
				true, false, errors.New("invalid operation time.Time(2018-11-02 09:54:48.442 +0000 UTC) = string(string literal)"),
				false, true, false, true, false, false,
			},
		}, { // 8
			m: map[string]interface{}{
				"a": int64(1541152388442),
				"b": testTime,
			},
			r: []interface{}{
				true, false, errors.New("invalid operation int64(1541152388442) = string(string literal)"),
				errors.New("invalid operation int64(1541152388442) >= time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation int64(1541152388442) < time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation int64(1541152388442) = time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation int64(1541152388442) != time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), false, errors.New("between operator cannot compare int64(1541152388442) and time.Time(2018-11-02 09:54:48.442 +0000 UTC)"),
			},
		}, { // 9
			m: map[string]interface{}{
				"a": "2020-02-26T02:37:21.822Z",
				"b": testTime,
			},
			r: []interface{}{
				errors.New("invalid operation string(2020-02-26T02:37:21.822Z) > int64(72)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) <= int64(32)"), false,
				errors.New("invalid operation string(2020-02-26T02:37:21.822Z) >= time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) < time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) = time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) != time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("between operator cannot compare string(2020-02-26T02:37:21.822Z) and int(30)"), errors.New("between operator cannot compare string(2020-02-26T02:37:21.822Z) and int(2)"),
			},
		}, { // 10
			m: map[string]interface{}{
				"c": "nothing",
			},
			r: []interface{}{
				false, false, false,
				true, false, true, false, false, true,
			},
		}, { // 11
			m: map[string]interface{}{
				"a": 12,
				"c": "nothing",
			},
			r: []interface{}{
				false, true, errors.New("invalid operation int64(12) = string(string literal)"),
				false, false, false, true, false, true,
			},
		},
	}
	sqls := []string{
		"select * from src where a > 72",
		"select * from src where a <= 32",
		"select * from src where a = \"string literal\"",
		"select * from src where a >= b",
		"select * from src where a < b",
		"select * from src where a = b",
		"select * from src where a != b",
		"select * from src where a between 30 and 100",
		"select * from src where a not between 2 and b",
	}
	var conditions []ast.Expr
	for _, sql := range sqls {
		stmt, _ := NewParser(strings.NewReader(sql)).Parse()
		conditions = append(conditions, stmt.Condition)
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range conditions {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: conf.GetNowInMilli(), Metadata: nil}
			ve := &ValuerEval{Valuer: MultiValuer(tuple)}
			result := ve.Eval(c)
			if !reflect.DeepEqual(tt.r[j], result) {
				t.Errorf("%d-%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.r[j], result)
			}
		}
	}
}

func TestCalculation(t *testing.T) {
	data := []struct {
		m Message
		r []interface{}
	}{
		{
			m: map[string]interface{}{
				"a": float64(32),
				"b": float64(72),
			},
			r: []interface{}{
				float64(104), float64(96), float64(0.4444444444444444), float64(32),
			},
		}, {
			m: map[string]interface{}{
				"a": int64(32),
				"b": int64(72),
			},
			r: []interface{}{
				int64(104), int64(96), int64(0), int64(32),
			},
		}, {
			m: map[string]interface{}{
				"a": "32",
				"b": "72",
			},
			r: []interface{}{
				errors.New("invalid operation string(32) + string(72)"), errors.New("invalid operation string(32) * int64(3)"),
				errors.New("invalid operation string(32) / string(72)"), errors.New("invalid operation string(32) % string(72)"),
			},
		}, {
			m: map[string]interface{}{
				"a": float64(55),
				"b": int64(55),
			},
			r: []interface{}{
				float64(110), float64(165), float64(1), float64(0),
			},
		}, {
			m: map[string]interface{}{
				"a": int64(55),
				"b": float64(0),
			},
			r: []interface{}{
				float64(55), int64(165), errors.New("divided by zero"), errors.New("divided by zero"),
			},
		}, {
			m: map[string]interface{}{
				"c": "nothing",
			},
			r: []interface{}{
				nil, nil, nil, nil,
			},
		}, {
			m: map[string]interface{}{
				"a": 12,
				"c": "nothing",
			},
			r: []interface{}{
				nil, int64(36), nil, nil,
			},
		},
	}
	sqls := []string{
		"select a + b as t from src",
		"select a * 3 as t from src",
		"select a / b as t from src",
		"select a % b as t from src",
	}
	var projects []ast.Expr
	for _, sql := range sqls {
		stmt, _ := NewParser(strings.NewReader(sql)).Parse()
		projects = append(projects, stmt.Fields[0].Expr)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range projects {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: conf.GetNowInMilli(), Metadata: nil}
			ve := &ValuerEval{Valuer: MultiValuer(tuple)}
			result := ve.Eval(c)
			if !reflect.DeepEqual(tt.r[j], result) {
				t.Errorf("%d-%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.r[j], result)
			}
		}
	}
}

func TestCase(t *testing.T) {
	data := []struct {
		m Message
		r []interface{}
	}{
		{
			m: map[string]interface{}{
				"a": float64(32),
				"b": float64(72),
			},
			r: []interface{}{
				1, 0, 0, 1,
			},
		}, {
			m: map[string]interface{}{
				"a": int64(32),
				"b": int64(72),
			},
			r: []interface{}{
				1, 0, 0, 1,
			},
		}, {
			m: map[string]interface{}{
				"a": "32",
				"b": "72",
			},
			r: []interface{}{
				errors.New("evaluate case expression error: invalid operation string(32) = int64(32)"), errors.New("evaluate case expression error: invalid operation string(32) = int64(72)"),
				errors.New("evaluate case expression error: invalid operation string(32) > int64(70)"), errors.New("evaluate case expression error: invalid operation string(32) > int64(30)"),
			},
		}, {
			m: map[string]interface{}{
				"a": float64(55),
				"b": int64(55),
			},
			r: []interface{}{
				0, nil, 0, 1,
			},
		}, {
			m: map[string]interface{}{
				"a": int64(55),
				"b": float64(0),
			},
			r: []interface{}{0, nil, 0, 1},
		}, {
			m: map[string]interface{}{
				"c": "nothing",
			},
			r: []interface{}{
				0, nil, -1, nil,
			},
		}, {
			m: map[string]interface{}{
				"a": 12,
				"c": "nothing",
			},
			r: []interface{}{
				0, nil, -1, nil,
			},
		},
	}
	sqls := []string{
		"select CASE a WHEN 32 THEN 1 ELSE 0 END as t from src",
		"select CASE a WHEN 72 THEN 1 WHEN 32 THEN 0 END as t from src",
		"select CASE WHEN a > 70 THEN 1 WHEN a > 30 AND a < 70 THEN 0 ELSE -1 END as t from src",
		"select CASE WHEN a > 30 THEN 1 END as t from src",
	}
	var projects []ast.Expr
	for _, sql := range sqls {
		stmt, _ := NewParser(strings.NewReader(sql)).Parse()
		projects = append(projects, stmt.Fields[0].Expr)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range projects {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: conf.GetNowInMilli(), Metadata: nil}
			ve := &ValuerEval{Valuer: MultiValuer(tuple)}
			result := ve.Eval(c)
			if !reflect.DeepEqual(tt.r[j], result) {
				t.Errorf("%d-%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.r[j], result)
			}
		}
	}
}

func TestArray(t *testing.T) {
	data := []struct {
		m Message
		r []interface{}
	}{
		{
			m: map[string]interface{}{
				"a": []int64{0, 1, 2, 3, 4, 5},
			},
			r: []interface{}{
				int64(0), int64(5), int64(1), []int64{0, 1}, []int64{4, 5}, []int64{5}, []int64{0, 1, 2, 3, 4}, []int64{1, 2, 3, 4}, []int64{0, 1, 2, 3, 4, 5},
			},
		},
	}
	sqls := []string{
		"select a[0] as t from src",
		"select a[-1] as t from src",
		"select a[1] as t from src",
		"select a[:2] as t from src",
		"select a[4:] as t from src",
		"select a[-1:] as t from src",
		"select a[0:-1] as t from src",
		"select a[-5:-1] as t from src",
		"select a[:] as t from src",
	}
	var projects []ast.Expr
	for _, sql := range sqls {
		stmt, _ := NewParser(strings.NewReader(sql)).Parse()
		projects = append(projects, stmt.Fields[0].Expr)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range projects {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: conf.GetNowInMilli(), Metadata: nil}
			ve := &ValuerEval{Valuer: MultiValuer(tuple)}
			result := ve.Eval(c)
			if !reflect.DeepEqual(tt.r[j], result) {
				t.Errorf("%d-%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.r[j], result)
			}
		}
	}
}

func TestLike(t *testing.T) {
	data := []struct {
		m Message
		r []interface{}
	}{
		{
			m: map[string]interface{}{
				"a": "string1",
				"b": 12,
			},
			r: []interface{}{
				false, errors.New("LIKE operator left operand expects string, but found 12"), false, true, false, errors.New("invalid LIKE pattern, must be a string but got 12"),
			},
		}, {
			m: map[string]interface{}{
				"a": "string2",
				"b": "another",
			},
			r: []interface{}{
				false, true, true, true, false, false,
			},
		}, {
			m: map[string]interface{}{
				"a": `str\_ng`,
				"b": "str_ng",
			},
			r: []interface{}{
				false, false, true, true, true, false,
			},
		}, {
			m: map[string]interface{}{
				"a": `str_ng`,
				"b": "str_ng",
			},
			r: []interface{}{
				false, false, true, true, false, true,
			},
		},
	}
	sqls := []string{
		`select a LIKE "string" as t from src`,
		`select b LIKE "an_ther" from src`,
		`select a NOT LIKE "string1" as t from src`,
		`select a LIKE "str%" as t from src`,
		`select a LIKE "str\\_ng" as t from src`,
		`select a LIKE b as t from src`,
	}
	var projects []ast.Expr
	for _, sql := range sqls {
		stmt, err := NewParser(strings.NewReader(sql)).Parse()
		if err != nil {
			t.Errorf("%s: %s", sql, err)
			return
		}
		projects = append(projects, stmt.Fields[0].Expr)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range projects {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: conf.GetNowInMilli(), Metadata: nil}
			ve := &ValuerEval{Valuer: MultiValuer(tuple)}
			result := ve.Eval(c)
			if !reflect.DeepEqual(tt.r[j], result) {
				t.Errorf("%d-%s. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, sqls[j], tt.r[j], result)
			}
		}
	}
}
