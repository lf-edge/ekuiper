package plans

import (
	"engine/common"
	"engine/xsql"
	"fmt"
	"reflect"
	"testing"
)

func TestPreprocessor_Apply(t *testing.T) {

	var tests = []struct {
		stmt *xsql.StreamStmt
		data []byte
		result interface{}
	}{
		//Basic type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
			},
			data: []byte(`{"a": 6}`),
			result: nil,
		},

		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
				},
			},
			data: []byte(`{"abc": 6}`),
			result: map[string]interface{}{
				"abc" : int(6),
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
				},
			},
			data: []byte(`{"abc": 34, "def" : "hello", "ghi": 50}`),
			result: map[string]interface{}{
				"abc" : float64(34),
				"def" : "hello",
			},
		},
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.BOOLEAN}},
				},
			},
			data: []byte(`{"abc": 77, "def" : "hello"}`),
			result: nil,
		},
		//Array type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "abc", FieldType: &xsql.BasicType{Type: xsql.FLOAT}},
					{Name: "def", FieldType: &xsql.BasicType{Type: xsql.BOOLEAN}},
				},
			},
			data: []byte(`{"a": {"b" : "hello"}}`),
			result: nil,
		},
		//Rec type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.RecType{
						StreamFields: []xsql.StreamField{
							{Name: "b", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello"}}`),
			result: map[string]interface{}{
				"a" : map[string]interface{}{
					"b": "hello",
				},
			},
		},

		//Array of complex type
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
			data: []byte(`{"a": [{"b" : "hello1"}, {"b" : "hello2"}]}`),
			result: map[string]interface{}{
				"a" : []map[string]interface{}{
					{"b": "hello1"},
					{"b": "hello2"},
				},
			},
		},
		//Rec of complex type
		{
			stmt: &xsql.StreamStmt{
				Name: xsql.StreamName("demo"),
				StreamFields: []xsql.StreamField{
					{Name: "a", FieldType: &xsql.RecType{
						StreamFields: []xsql.StreamField{
							{Name: "b", FieldType: &xsql.BasicType{Type: xsql.STRINGS}},
							{Name: "c", FieldType: &xsql.RecType{
								StreamFields: []xsql.StreamField{
									{Name: "d", FieldType: &xsql.BasicType{Type: xsql.BIGINT}},
								},
							}},
						},
					}},
				},
			},
			data: []byte(`{"a": {"b" : "hello", "c": {"d": 35.2}}}`),
			result: map[string]interface{}{
				"a" : map[string]interface{}{
					"b" : "hello",
					"c" : map[string]interface{}{
						"d": int(35),
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	defer common.CloseLogger()
	for i, tt := range tests {

		pp := &Preprocessor{StreamStmt:tt.stmt}
		result := pp.Apply(nil, tt.data)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.data, tt.result, result)
		}
	}
}