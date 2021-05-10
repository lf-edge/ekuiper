package operators

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"testing"
)

func TestTableProcessor_Apply(t *testing.T) {

	var tests = []struct {
		stmt   *xsql.StreamStmt
		data   []byte
		result interface{}
	}{
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.ArrayType{
						Type: xsql.STRUCT,
						FieldType: &xsql.RecType{
							StreamFields: []xsql.StreamField{
								{Name: "b", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
							},
						},
					}},
				},
			},
			data: []byte(`[{"a": [{"b" : "hello1"}, {"b" : "hello2"}]},{"a": [{"b" : "hello2"}, {"b" : "hello3"}]},{"a": [{"b" : "hello3"}, {"b" : "hello4"}]}]`),
			result: xsql.WindowTuples{
				Emitter: "demo",
				Tuples: []xsql.Tuple{
					{
						Message: xsql.Message{
							"a": []map[string]interface{}{
								{"b": "hello1"},
								{"b": "hello2"},
							},
						},
						Emitter: "demo",
					},
					{
						Message: xsql.Message{
							"a": []map[string]interface{}{
								{"b": "hello2"},
								{"b": "hello3"},
							},
						},
						Emitter: "demo",
					},
					{
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
			stmt: &xsql.StreamStmt{
				Name:         xsql.StreamName("demo"),
				StreamFields: nil,
			},
			data: []byte(`[{"a": {"b" : "hello", "c": {"d": 35.2}}},{"a": {"b" : "world", "c": {"d": 65.2}}}]`),
			result: xsql.WindowTuples{
				Emitter: "demo",
				Tuples: []xsql.Tuple{
					{
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
					{
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

	defer common.CloseLogger()
	contextLogger := common.Log.WithField("rule", "TestPreprocessor_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp := &TableProcessor{isBatchInput: true, emitterName: "demo"}
		pp.streamFields = convertFields(tt.stmt.StreamFields)
		pp.output = xsql.WindowTuples{
			Emitter: "demo",
			Tuples:  make([]xsql.Tuple, 0),
		}

		var dm []map[string]interface{}
		if e := json.Unmarshal(tt.data, &dm); e != nil {
			t.Log(e)
			t.Fail()
		} else {
			fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
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
