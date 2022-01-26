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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"strings"
	"testing"
)

func TestChangedColsFunc_Apply1(t *testing.T) {
	var tests = []struct {
		sql    string
		data   []interface{}
		result [][]map[string]interface{}
	}{
		{
			sql: `SELECT changed_cols("", true, a, b, c) FROM test`,
			data: []interface{}{
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"c": "c1",
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: xsql.Message{
						"a": "a1",
						"b": "b2",
						"c": "c2",
					},
				},
			},
			result: [][]map[string]interface{}{{{
				"a": "a1",
				"b": "b1",
				"c": "c1",
			}}, {{
				"b": "b2",
			}}, {{}}, {{
				"c": "c2",
			}}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestMiscFunc_Apply1")
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("mockRule0", "project", tempStore)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectOp{Fields: stmt.Fields}
		fv, afv := xsql.NewFunctionValuersForOp(ctx)
		r := make([][]map[string]interface{}, 0, len(tt.data))
		for _, d := range tt.data {
			result := pp.Apply(ctx, d, fv, afv)
			r = append(r, result.([]map[string]interface{}))
		}
		if !reflect.DeepEqual(tt.result, r) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, r)
		}
	}
}
