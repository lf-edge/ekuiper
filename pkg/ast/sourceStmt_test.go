package ast

import (
	"fmt"
	"reflect"
	"testing"
)

func TestPrintFieldType(t *testing.T) {
	var tests = []struct {
		ft      FieldType
		printed string
	}{{
		ft: &RecType{
			StreamFields: []StreamField{
				{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
				{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
			},
		},
		printed: `{"Type":"struct","Fields":[{"FieldType":"string","Name":"STREET_NAME"},{"FieldType":"bigint","Name":"NUMBER"}]}`,
	}, {
		ft: &ArrayType{
			Type: STRUCT,
			FieldType: &RecType{
				StreamFields: []StreamField{
					{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
				},
			},
		},
		printed: `{"Type":"array","ElementType":{"Type":"struct","Fields":[{"FieldType":"string","Name":"STREET_NAME"},{"FieldType":"bigint","Name":"NUMBER"}]}}`,
	}, {
		ft: &ArrayType{
			Type:      STRUCT,
			FieldType: &BasicType{Type: STRINGS},
		},
		printed: `{"Type":"array","ElementType":"string"}`,
	}, {
		ft: &BasicType{
			Type: STRINGS,
		},
		printed: `string`,
	}}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n",tt.s)
		result, _ := doPrintFieldTypeForJson(tt.ft)
		if !reflect.DeepEqual(tt.printed, result) {
			t.Errorf("%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.printed, result)
		}
	}
}
