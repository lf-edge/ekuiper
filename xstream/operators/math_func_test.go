package operators

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

func TestMathAndConversionFunc_Apply1(t *testing.T) {
	var tests = []struct {
		sql    string
		data   *xsql.Tuple
		result []map[string]interface{}
	}{
		{
			sql: "SELECT abs(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": -1,
				},
			},
			result: []map[string]interface{}{{
				"a": float64(1), //Actually it should be 1, it's caused by json Unmarshal method, which convert int to float64
			}},
		},

		{
			sql: "SELECT abs(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": -1.1,
				},
			},
			result: []map[string]interface{}{{
				"a": 1.1,
			}},
		},

		{
			sql: "SELECT abs(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": 1.1,
				},
			},
			result: []map[string]interface{}{{
				"a": 1.1,
			}},
		},

		{
			sql: "SELECT acos(1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0),
			}},
		},

		{
			sql: "SELECT asin(1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1.5707963267948966),
			}},
		},

		{
			sql: "SELECT atan(1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0.7853981633974483),
			}},
		},

		{
			sql: "SELECT atan2(1,1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0.7853981633974483),
			}},
		},

		{
			sql: "SELECT bitand(1,1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
			}},
		},

		{
			sql: "SELECT bitand(1.0,1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: nil,
		},

		{
			sql: "SELECT bitor(1,1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
			}},
		},

		{
			sql: "SELECT bitxor(1,1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0),
			}},
		},

		{
			sql: "SELECT bitnot(1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(-2),
			}},
		},

		{
			sql: "SELECT ceil(1.6) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(2),
			}},
		},

		{
			sql: "SELECT cos(0) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
			}},
		},

		{
			sql: "SELECT cosh(0) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
			}},
		},

		{
			sql: "SELECT exp(1.2) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(3.3201169227365472),
			}},
		},

		{
			sql: "SELECT ln(1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0),
			}},
		},

		{
			sql: "SELECT log(10) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
			}},
		},

		{
			sql: "SELECT mod(10, 3) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
			}},
		},

		{
			sql: "SELECT power(10, 3) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1000),
			}},
		},

		{
			sql: "SELECT round(10.2) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(10),
			}},
		},

		{
			sql: "SELECT sign(10.2) AS a, sign(-2) as b, sign(0) as c FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
				"b": float64(-1),
				"c": float64(0),
			}},
		},

		{
			sql: "SELECT sin(0) as a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0),
			}},
		},

		{
			sql: "SELECT sinh(0) as a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0),
			}},
		},

		{
			sql: "SELECT sqrt(4) as a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(2),
			}},
		},

		{
			sql: "SELECT tan(0) as a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0),
			}},
		},

		{
			sql: "SELECT tanh(1) as a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0.7615941559557649),
			}},
		},

		{
			sql: `SELECT cast(1.2, "bigint") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(1),
			}},
		},

		{
			sql: `SELECT cast(5, "bigint") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(5),
			}},
		},

		{
			sql: `SELECT cast(1.2, "string") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": "1.2",
			}},
		},

		{
			sql: `SELECT cast(true, "string") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": "true",
			}},
		},

		{
			sql: `SELECT cast("true", "boolean") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": true,
			}},
		},

		{
			sql: `SELECT cast("1", "boolean") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": true,
			}},
		},

		{
			sql: `SELECT cast(0.0, "boolean") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": false,
			}},
		},

		{
			sql: `SELECT chr(0) as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(0),
			}},
		},

		{
			sql: `SELECT chr("a") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(97),
			}},
		},

		{
			sql: `SELECT encode("hello", "base64") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": "aGVsbG8=",
			}},
		},

		{
			sql: `SELECT trunc(3.1415, 2) as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(3.14),
			}},
		},

		{
			sql: `SELECT trunc(3, 2) as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": float64(3.00),
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestMathAndConversionFunc_Apply1")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		//fmt.Println("Running test " + strconv.Itoa(i))
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil && tt.result == nil {
			continue
		} else if err != nil && tt.result != nil {
			t.Errorf("%q", err)
			continue
		}
		pp := &ProjectOp{Fields: stmt.Fields}
		pp.isTest = true
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		result := pp.Apply(ctx, tt.data, fv, afv)
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
