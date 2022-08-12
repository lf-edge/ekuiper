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
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"testing"
)

func TestTableProcessor_Apply(t *testing.T) {
	var tests = []struct {
		stmt   *ast.StreamStmt
		data   []byte
		result interface{}
	}{
		{
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "a", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "b", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							},
						},
					}},
				},
			},
			data: []byte(`[{"a": [{"b" : "hello1"}, {"b" : "hello2"}]},{"a": [{"b" : "hello2"}, {"b" : "hello3"}]},{"a": [{"b" : "hello3"}, {"b" : "hello4"}]}]`),
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Message: xsql.Message{
							"a": []map[string]interface{}{
								{"b": "hello1"},
								{"b": "hello2"},
							},
						},
						Emitter: "demo",
					},
					&xsql.Tuple{
						Message: xsql.Message{
							"a": []map[string]interface{}{
								{"b": "hello2"},
								{"b": "hello3"},
							},
						},
						Emitter: "demo",
					},
					&xsql.Tuple{
						Message: xsql.Message{
							"a": []map[string]interface{}{
								{"b": "hello3"},
								{"b": "hello4"},
							},
						},
						Emitter: "demo",
					},
				},
			},
		}, {
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`[{"a": {"b" : "hello", "c": {"d": 35.2}}},{"a": {"b" : "world", "c": {"d": 65.2}}}]`),
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Message: xsql.Message{
							"a": map[string]interface{}{
								"b": "hello",
								"c": map[string]interface{}{
									"d": 35.2,
								},
							},
						},
						Emitter: "demo",
					},
					&xsql.Tuple{
						Message: xsql.Message{
							"a": map[string]interface{}{
								"b": "world",
								"c": map[string]interface{}{
									"d": 65.2,
								},
							},
						},
						Emitter: "demo",
					},
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer conf.CloseLogger()
	contextLogger := conf.Log.WithField("rule", "TestPreprocessor_Apply")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &TableProcessor{isBatchInput: true, emitterName: "demo"}
		pp.streamFields = convertFields(tt.stmt.StreamFields)
		pp.output = &xsql.WindowTuples{
			Content: make([]xsql.TupleRow, 0),
		}

		var dm []map[string]interface{}
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			t.Log(e)
			t.Fail()
		} else {
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			for _, m := range dm {
				pp.Apply(ctx, &xsql.Tuple{
					Emitter: "demo",
					Message: m,
				}, fv, afv)
			}

			result := pp.Apply(ctx, &xsql.Tuple{}, fv, afv)
			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("%d. result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, result)
			}
		}

	}
}
