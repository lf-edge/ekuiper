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

package ast

import (
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
	t.Logf("The test bucket size is %d.", len(tests))
	for i, tt := range tests {
		// t.Logf("Parsing SQL %q.",tt.s)
		result, _ := doPrintFieldTypeForJson(tt.ft)
		if !reflect.DeepEqual(tt.printed, result) {
			t.Errorf("%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.printed, result)
		}
	}
}

func TestToJsonFields(t *testing.T) {
	var tests = []struct {
		input  StreamFields
		output map[string]*JsonStreamField
	}{
		{
			input: StreamFields{
				{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
			},
			output: map[string]*JsonStreamField{
				"STREET_NAME": {
					Type: "string",
				},
			},
		}, {
			input: []StreamField{
				{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
				{Name: "FIRST_NAME", FieldType: &BasicType{Type: STRINGS}},
				{Name: "LAST_NAME", FieldType: &BasicType{Type: STRINGS}},
				{Name: "NICKNAMES", FieldType: &ArrayType{Type: STRINGS}},
				{Name: "data", FieldType: &BasicType{Type: BYTEA}},
				{Name: "Gender", FieldType: &BasicType{Type: BOOLEAN}},
				{Name: "ADDRESS", FieldType: &RecType{
					StreamFields: []StreamField{
						{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
						{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
					},
				}},
			},
			output: map[string]*JsonStreamField{
				"USERID":     {Type: "bigint"},
				"FIRST_NAME": {Type: "string"},
				"LAST_NAME":  {Type: "string"},
				"NICKNAMES":  {Type: "array", Items: &JsonStreamField{Type: "string"}},
				"data":       {Type: "bytea"},
				"Gender":     {Type: "boolean"},
				"ADDRESS": {Type: "struct", Properties: map[string]*JsonStreamField{
					"STREET_NAME": {Type: "string"},
					"NUMBER":      {Type: "bigint"},
				}},
			},
		}, {
			input: []StreamField{
				{Name: "ADDRESSES", FieldType: &ArrayType{
					Type: STRUCT,
					FieldType: &RecType{
						StreamFields: []StreamField{
							{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
							{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
						},
					},
				}},
			},
			output: map[string]*JsonStreamField{
				"ADDRESSES": {Type: "array", Items: &JsonStreamField{
					Type: "struct", Properties: map[string]*JsonStreamField{
						"STREET_NAME": {Type: "string"},
						"NUMBER":      {Type: "bigint"},
					}},
				},
			},
		},
	}
	t.Logf("The test bucket size is %d.", len(tests))
	for i, tt := range tests {
		result := tt.input.ToJsonSchema()
		if !reflect.DeepEqual(tt.output, result) {
			t.Errorf("%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.output, result)
		}
	}
}
