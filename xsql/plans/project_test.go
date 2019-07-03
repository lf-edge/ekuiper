package plans

import (
	"encoding/json"
	"engine/xsql"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestProjectPlan_Apply1(t *testing.T) {
	var tests = []struct {
		sql  string
		data string
		result map[string]interface{}
	}{
		{
			sql: "SELECT a FROM test",
			data: `{"a": "val_a"}`,
			result: map[string]interface{}{
				"a" : "val_a",
			},
		},

		{
			sql: `SELECT "value" FROM test`,
			data: `{}`,
			result: map[string]interface{}{
				DEFAULT_FIELD_NAME_PREFIX + "0" : "value",
			},
		},

		{
			sql: `SELECT 3.4 FROM test`,
			data: `{}`,
			result: map[string]interface{}{
				DEFAULT_FIELD_NAME_PREFIX + "0" : 3.4,
			},
		},

		{
			sql: `SELECT 5 FROM test`,
			data: `{}`,
			result: map[string]interface{}{
				DEFAULT_FIELD_NAME_PREFIX + "0" : 5.0,
			},
		},

		{
			sql: `SELECT a, "value" AS b FROM test`,
			data: `{"a": "val_a"}`,
			result: map[string]interface{}{
				"a" : "val_a",
				"b" : "value",
			},
		},

		{
			sql: `SELECT a, "value" AS b, 3.14 as Pi, 0 as Zero FROM test`,
			data: `{"a": "val_a"}`,
			result: map[string]interface{}{
				"a" : "val_a",
				"b" : "value",
				"Pi" : 3.14,
				"Zero" : 0.0,
			},
		},

		{
			sql: `SELECT a->b AS ab FROM test`,
			data: `{"a": {"b" : "hello"}}`,
			result: map[string]interface{}{
				"ab" : "hello",
			},
		},

		{
			sql: `SELECT a[0]->b AS ab FROM test`,
			data: `{"a": [{"b" : "hello1"}, {"b" : "hello2"}]}`,
			result: map[string]interface{}{
				"ab" : "hello1",
			},
		},

		{
			sql: `SELECT a->c->d AS f1 FROM test`,
			data: `{"a": {"b" : "hello", "c": {"d": 35.2}}}`,
			result: map[string]interface{}{
				"f1" : 35.2,
			},
		},

		//The int type is not supported yet, the json parser returns float64 for int values
		{
			sql: `SELECT a->c->d AS f1 FROM test`,
			data: `{"a": {"b" : "hello", "c": {"d": 35}}}`,
			result: map[string]interface{}{
				"f1" : float64(35),
			},
		},

		{
			sql: "SELECT a FROM test",
			data: `{}`,
			result: map[string]interface{}{
			},
		},

		{
			sql: "SELECT * FROM test",
			data: `{}`,
			result: map[string]interface{}{
			},
		},

		{
			sql: `SELECT * FROM test`,
			data: `{"a": {"b" : "hello", "c": {"d": 35.2}}}`,
			result: map[string]interface{}{
				"a" : map[string]interface{} {
					"b" : "hello",
					"c" : map[string]interface{} {
						"d" : 35.2,
					},
				},
			},
		},

		{
			sql: `SELECT * FROM test`,
			data: `{"a": "val1", "b": 3.14}`,
			result: map[string]interface{}{
				"a" : "val1",
				"b" : 3.14,
			},
		},

		{
			sql: `SELECT 3*4 AS f1 FROM test`,
			data: `{}`,
			result: map[string]interface{}{
				"f1" : float64(12),
			},
		},

		{
			sql: `SELECT 4.5*2 AS f1 FROM test`,
			data: `{}`,
			result: map[string]interface{}{
				"f1" : float64(9),
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, _ := xsql.NewParser(strings.NewReader(tt.sql)).Parse()

		pp := &ProjectPlan{Fields:stmt.Fields}
		var input map[string]interface{}
		if err := json.Unmarshal([]byte(tt.data), &input); err != nil {
			fmt.Printf("Failed to parse the JSON data.\n")
			return
		}
		result := pp.Apply(nil, input)
		var mapRes map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte\n")
		}
	}
}
