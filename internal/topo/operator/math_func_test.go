// Copyright 2022 EMQ Technologies Co., Ltd.
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

package operator

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
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
				"a": 1,
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
				"a": 1,
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
				"a": 1,
			}},
		},

		{
			sql: "SELECT bitxor(1,1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": 0,
			}},
		},

		{
			sql: "SELECT bitnot(1) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": -2,
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
				"a": 1,
				"b": -1,
				"c": 0,
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
				"a": 1,
			}},
		},

		{
			sql: `SELECT cast(5, "bigint") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": 5,
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
				"a": int32(0),
			}},
		},

		{
			sql: `SELECT chr("a") as a FROM test`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: nil,
			},
			result: []map[string]interface{}{{
				"a": int32(97),
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
	contextLogger := conf.Log.WithField("rule", "TestMathAndConversionFunc_Apply1")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil && tt.result == nil {
			continue
		} else if err != nil && tt.result != nil {
			t.Errorf("%d: found error %q", i, err)
			continue
		}
		pp := &ProjectOp{SendMeta: true, IsAggregate: xsql.IsAggStatement(stmt)}
		parseStmt(pp, stmt.Fields)
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		opResult := pp.Apply(ctx, tt.data, fv, afv)
		result, err := parseResult(opResult, pp.IsAggregate)
		if err != nil {
			t.Errorf("parse result error： %s", err)
			continue
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
