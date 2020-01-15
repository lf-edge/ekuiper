package plans

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"strings"
	"testing"
)

func TestStrFunc_Apply1(t *testing.T) {
	var tests = []struct {
		sql    string
		data   *xsql.Tuple
		result []map[string]interface{}
	}{
		{
			sql: "SELECT concat(a, b, c) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "mya",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "myamybmyc",
			}},
		},

		{
			sql: "SELECT endswith(a, b) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "mya",
					"b": "myb",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": false,
			}},
		},
		{
			sql: "SELECT endswith(a, b) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "mya",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": true,
			}},
		},
		{
			sql: "SELECT format_time(a, \"yyyy-MM-dd T HH:mm:ss\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": common.TimeFromUnixMilli(1568854515000),
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "2019-09-19 T 00:55:15",
			}},
		},
		{
			sql: "SELECT indexof(a, \"a\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "mya",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": float64(2),
			}},
		},
		{
			sql: "SELECT length(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "中国",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": float64(2),
			}},
		},
		{
			sql: "SELECT length(c) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "中国",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": float64(3),
			}},
		},
		{
			sql: "SELECT lower(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "nycnicks",
			}},
		},
		{
			sql: "SELECT lpad(a, 2) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "  NYCNicks",
			}},
		},
		{
			sql: "SELECT ltrim(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": " \ttrimme\n ",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "trimme\n ",
			}},
		},
		{
			sql: "SELECT numbytes(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "中国",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": float64(6),
			}},
		},
		{
			sql: "SELECT numbytes(b) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "中国",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": float64(2),
			}},
		},
		{
			sql: "SELECT regexp_matches(a,\"foo.*\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "seafood",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": true,
			}},
		},
		{
			sql: "SELECT regexp_matches(b,\"foo.*\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "seafood",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": false,
			}},
		},
		{
			sql: "SELECT regexp_replace(a,\"a(x*)b\", \"REP\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "-ab-axxb-",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "-REP-REP-",
			}},
		},
		{
			sql: "SELECT regexp_substr(a,\"foo.*\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "seafood",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "food",
			}},
		},
		{
			sql: "SELECT rpad(a, 3) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "NYCNicks   ",
			}},
		},
		{
			sql: "SELECT rtrim(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": " \ttrimme\n ",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": " \ttrimme",
			}},
		},
		{
			sql: "SELECT substring(a, 3) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "Nicks",
			}},
		},
		{
			sql: "SELECT substring(a, 3, 5) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "Ni",
			}},
		},
		{
			sql: "SELECT endswith(a, b) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "mya",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": true,
			}},
		},
		{
			sql: "SELECT endswith(a, c) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "mya",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": false,
			}},
		},
		{
			sql: "SELECT trim(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": " \ttrimme\n ",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "trimme",
			}},
		},
		{
			sql: "SELECT upper(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "NYCNICKS",
			}},
		},

		{
			sql: `SELECT split_value(a,"/",0) AS a FROM test1`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "test/device001/message",
				},
			},
			result: []map[string]interface{}{{
				"a": "test",
			}},
		},

		{
			sql: `SELECT split_value(a,"/",1) AS a FROM test1`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "test/device001/message",
				},
			},
			result: []map[string]interface{}{{
				"a": "device001",
			}},
		},

		{
			sql: `SELECT split_value(a,"/",2) AS a FROM test1`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "test/device001/message",
				},
			},
			result: []map[string]interface{}{{
				"a": "message",
			}},
		},

		{
			sql: `SELECT split_value(a,"/",0) AS a, split_value(a,"/",3) AS b FROM test1`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "/test/device001/message",
				},
			},
			result: []map[string]interface{}{{
				"a": "",
				"b": "message",
			}},
		},

		{
			sql: `SELECT split_value(a,"/",3) AS a FROM test1`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "test/device001/message",
				},
			},
			result: []map[string]interface{}{map[string]interface{}{}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestStrFunc_Apply1")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectPlan{Fields: stmt.Fields}
		pp.isTest = true
		result := pp.Apply(ctx, tt.data)
		var mapRes []map[string]interface{}
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
