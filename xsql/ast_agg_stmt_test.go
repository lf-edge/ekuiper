package xsql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestIsAggStatement(t *testing.T) {
	var tests = []struct {
		s    string
		agg  bool
		err  string
	}{
		{s: `SELECT avg(1) FROM tbl`,agg: true},
		{s: `SELECT sin(1) FROM tbl`,agg: false},
		{s: `SELECT sin(avg(f1)) FROM tbl`,agg: true},

		{s: `SELECT sum(f1) FROM tbl GROUP by f1`,agg: true},
		{s: `SELECT f1 FROM tbl GROUP by f1`,agg: true},

		{s: `SELECT count(f1) FROM tbl`,agg: true},
		{s: `SELECT max(f1) FROM tbl`,agg: true},
		{s: `SELECT min(f1) FROM tbl`,agg: true},
		{s: `SELECT count(f1) FROM tbl group by tumblingwindow(ss, 5)`,agg: true},

		{s: `SELECT f1 FROM tbl left join tbl2 on tbl1.f1 = tbl2.f2`,agg: false},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		isAgg := IsAggStatement(stmt)
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && (tt.agg != isAgg) {
			t.Errorf("Error: expected %t, actual %t.", tt.agg, isAgg)
		}
	}
}