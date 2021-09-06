// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"strings"
	"testing"
)

func TestIsAggStatement(t *testing.T) {
	var tests = []struct {
		s   string
		agg bool
		err string
	}{
		{s: `SELECT avg(1) FROM tbl`, agg: true},
		{s: `SELECT sin(1) FROM tbl`, agg: false},
		{s: `SELECT sin(avg(f1)) FROM tbl`, agg: true},

		{s: `SELECT sum(f1) FROM tbl GROUP by f1`, agg: true},
		{s: `SELECT f1 FROM tbl GROUP by f1`, agg: true},

		{s: `SELECT count(f1) FROM tbl`, agg: true},
		{s: `SELECT max(f1) FROM tbl`, agg: true},
		{s: `SELECT min(f1) FROM tbl`, agg: true},
		{s: `SELECT count(f1) FROM tbl group by tumblingwindow(ss, 5)`, agg: true},
		{s: `SELECT f1 FROM tbl group by tumblingwindow(ss, 5) having count(f1) > 3`, agg: false},
		{s: `SELECT f1 FROM tbl left join tbl2 on tbl1.f1 = tbl2.f2`, agg: false},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		stmt, err := NewParser(strings.NewReader(tt.s)).Parse()
		isAgg := IsAggStatement(stmt)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && (tt.agg != isAgg) {
			t.Errorf("Error: expected %t, actual %t.", tt.agg, isAgg)
		}
	}
}
