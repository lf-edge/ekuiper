package xsql

import (
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
	"reflect"
	"strings"
	"testing"
)

func TestComparison(t *testing.T) {
	testTime, _ := common.InterfaceToTime(1541152488442, "")
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
				false, true, errors.New("invalid operation float64(32) = string(string literal)"),
				false, true, false, true,
			},
		}, {
			m: map[string]interface{}{
				"a": int64(32),
				"b": int64(72),
			},
			r: []interface{}{
				false, true, errors.New("invalid operation int64(32) = string(string literal)"),
				false, true, false, true,
			},
		}, {
			m: map[string]interface{}{
				"a": "32",
				"b": "72",
			},
			r: []interface{}{
				errors.New("invalid operation string(32) > int64(72)"), errors.New("invalid operation string(32) <= int64(32)"), false,
				false, true, false, true,
			},
		}, {
			m: map[string]interface{}{
				"a": []interface{}{32, 72},
				"b": []interface{}{32, 72},
			},
			r: []interface{}{
				errors.New("> is an invalid operation for []interface {}"), errors.New("<= is an invalid operation for []interface {}"), errors.New("= is an invalid operation for []interface {}"),
				errors.New(">= is an invalid operation for []interface {}"), errors.New("< is an invalid operation for []interface {}"), errors.New("= is an invalid operation for []interface {}"), errors.New("!= is an invalid operation for []interface {}"),
			},
		}, {
			m: map[string]interface{}{
				"a": map[string]interface{}{"c": 5},
				"b": map[string]interface{}{"d": 5},
			},
			r: []interface{}{
				errors.New("> is an invalid operation for map[string]interface {}"), errors.New("<= is an invalid operation for map[string]interface {}"), errors.New("= is an invalid operation for map[string]interface {}"),
				errors.New(">= is an invalid operation for map[string]interface {}"), errors.New("< is an invalid operation for map[string]interface {}"), errors.New("= is an invalid operation for map[string]interface {}"), errors.New("!= is an invalid operation for map[string]interface {}"),
			},
		}, {
			m: map[string]interface{}{
				"a": float64(55),
				"b": int64(55),
			},
			r: []interface{}{
				false, false, errors.New("invalid operation float64(55) = string(string literal)"),
				true, false, true, false,
			},
		}, {
			m: map[string]interface{}{
				"a": testTime,
				"b": int64(1541152388442),
			},
			r: []interface{}{
				true, false, errors.New("invalid operation time.Time(2018-11-02 09:54:48.442 +0000 UTC) = string(string literal)"),
				true, false, false, true,
			},
		}, {
			m: map[string]interface{}{
				"a": testTime,
				"b": "2020-02-26T02:37:21.822Z",
			},
			r: []interface{}{
				true, false, errors.New("invalid operation time.Time(2018-11-02 09:54:48.442 +0000 UTC) = string(string literal)"),
				false, true, false, true,
			},
		}, {
			m: map[string]interface{}{
				"a": int64(1541152388442),
				"b": testTime,
			},
			r: []interface{}{
				true, false, errors.New("invalid operation int64(1541152388442) = string(string literal)"),
				errors.New("invalid operation int64(1541152388442) >= time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation int64(1541152388442) < time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation int64(1541152388442) = time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation int64(1541152388442) != time.Time(2018-11-02 09:54:48.442 +0000 UTC)"),
			},
		}, {
			m: map[string]interface{}{
				"a": "2020-02-26T02:37:21.822Z",
				"b": testTime,
			},
			r: []interface{}{
				errors.New("invalid operation string(2020-02-26T02:37:21.822Z) > int64(72)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) <= int64(32)"), false,
				errors.New("invalid operation string(2020-02-26T02:37:21.822Z) >= time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) < time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) = time.Time(2018-11-02 09:54:48.442 +0000 UTC)"), errors.New("invalid operation string(2020-02-26T02:37:21.822Z) != time.Time(2018-11-02 09:54:48.442 +0000 UTC)"),
			},
		}, {
			m: map[string]interface{}{
				"c": "nothing",
			},
			r: []interface{}{
				false, false, false,
				true, false, true, false,
			},
		}, {
			m: map[string]interface{}{
				"a": 12,
				"c": "nothing",
			},
			r: []interface{}{
				false, true, errors.New("invalid operation int64(12) = string(string literal)"),
				false, false, false, true,
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
	}
	var conditions []Expr
	for _, sql := range sqls {
		stmt, _ := NewParser(strings.NewReader(sql)).Parse()
		conditions = append(conditions, stmt.Condition)
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range conditions {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: common.GetNowInMilli(), Metadata: nil}
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
	var projects []Expr
	for _, sql := range sqls {
		stmt, _ := NewParser(strings.NewReader(sql)).Parse()
		projects = append(projects, stmt.Fields[0].Expr)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range projects {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: common.GetNowInMilli(), Metadata: nil}
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
	var projects []Expr
	for _, sql := range sqls {
		stmt, _ := NewParser(strings.NewReader(sql)).Parse()
		projects = append(projects, stmt.Fields[0].Expr)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data)*len(sqls))
	for i, tt := range data {
		for j, c := range projects {
			tuple := &Tuple{Emitter: "src", Message: tt.m, Timestamp: common.GetNowInMilli(), Metadata: nil}
			ve := &ValuerEval{Valuer: MultiValuer(tuple)}
			result := ve.Eval(c)
			if !reflect.DeepEqual(tt.r[j], result) {
				t.Errorf("%d-%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.r[j], result)
			}
		}
	}
}
