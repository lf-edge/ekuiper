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

package operator

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/cast"
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
			sql: "SELECT concat(a, d, b, c) AS a FROM test",
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
			sql: "SELECT endswith(a, d) AS a FROM test",
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
			sql: "SELECT format_time(a, \"yyyy-MM-dd T HH:mm:ss\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": cast.TimeFromUnixMilli(1568854515000),
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "2019-09-19 T 00:55:15",
			}},
		},
		{
			sql: "SELECT format_time(meta(created) * 1000, \"yyyy-MM-dd T HH:mm:ss\") AS time FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "hello",
					"b": "ya",
					"c": "myc",
				},
				Metadata: xsql.Metadata{
					"created": 1.62000273e+09,
				},
			},
			result: []map[string]interface{}{{
				"time": "2021-05-03 T 00:45:30",
			}},
		},
		{
			sql: "SELECT format_time(d, \"yyyy-MM-dd T HH:mm:ss\") AS time FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "hello",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{}},
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
				"a": 2,
			}},
		},
		{
			sql: "SELECT indexof(d, \"a\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "mya",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": -1,
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
				"a": 2,
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
				"a": 3,
			}},
		},
		{
			sql: "SELECT length(d) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "中国",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": 0,
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
			sql: "SELECT lower(d) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{}},
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
				"a": 6,
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
				"a": 2,
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
			sql: "SELECT regexp_matches(d,\"foo.*\") AS a FROM test",
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
			sql: "SELECT regexp_replace(a,\"a(x*)b\", d) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "-ab-axxb-",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{}},
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
			sql: "SELECT regexp_substr(d,\"foo.*\") AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "seafood",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{}},
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
			sql: "SELECT substring(a, 3, 100) AS a FROM test",
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
			sql: "SELECT substring(a, 88, 100) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "",
			}},
		},
		{
			sql: "SELECT substring(a, 100) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "",
			}},
		},
		{
			sql: "SELECT substring(a, 100) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": "",
			}},
		},
		{
			sql: "SELECT substring(d, 3, 100) AS bc FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "NYCNicks",
					"b": "ya",
					"c": "myc",
				},
			},
			result: []map[string]interface{}{{}},
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
			sql: "SELECT endswith(d, c) AS a FROM test",
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
			sql: `SELECT split_value(d,"/",2) AS a FROM test1`,
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a": "test/device001/message",
				},
			},
			result: []map[string]interface{}{{}},
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
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestStrFunc_Apply1")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{}
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
