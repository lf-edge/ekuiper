package plans

import (
	"engine/xsql"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestFilterPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql  string
		data map[string]interface{}
		result interface{}
	}{
		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 AND abc < 20",
			data: map[string]interface{}{
				"a" : int64(6),
			},
			result: nil,
		},

		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 AND abc < 20",
			data: map[string]interface{}{
				"abc" : int64(6),
			},
			result: map[string]interface{}{
				"abc" : int64(6),
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 OR def = \"hello\"",
			data: map[string]interface{}{
				"abc" : int64(34),
				"def" : "hello",
			},
			result: map[string]interface{}{
				"abc" : int64(34),
				"def" : "hello",
			},
		},

	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		pp := &FilterPlan{Condition:stmt.Condition}
		result := pp.Apply(nil, tt.data)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
