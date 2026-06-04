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
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestPrintFieldType(t *testing.T) {
	tests := []struct {
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

func TestMarshalJSON(t *testing.T) {
	tests := []struct {
		input  StreamField
		output string
		err    error
	}{
		{
			input: StreamField{
				Name:      "id",
				FieldType: &BasicType{Type: BIGINT},
				Default:   &IntegerLiteral{Val: 10},
			},
			output: `{"FieldType":"bigint","Name":"id","DefaultClause":"10"}`,
			err:    nil,
		},
		{
			input: StreamField{
				Name:      "foo",
				FieldType: &BasicType{Type: FLOAT},
				Default:   &NumberLiteral{Val: -55.34},
			},
			output: `{"FieldType":"float","Name":"foo","DefaultClause":"-55.34"}`,
			err:    nil,
		},
		{
			input: StreamField{
				Name:      "bar",
				FieldType: &BasicType{Type: STRINGS},
				Default:   &StringLiteral{Val: ""},
			},
			output: `{"FieldType":"string","Name":"bar","DefaultClause":""}`,
			err:    nil,
		},
		{
			input: StreamField{
				Name:      "mock",
				FieldType: &BasicType{Type: STRINGS},
			},
			output: `{"FieldType":"string","Name":"mock"}`,
			err:    nil,
		},
		{
			input: StreamField{
				Name:      "motion",
				FieldType: &BasicType{Type: BOOLEAN},
				Default:   &BooleanLiteral{Val: false},
			},
			output: `{"FieldType":"boolean","Name":"motion","DefaultClause":"false"}`,
			err:    nil,
		},
		{
			input: StreamField{
				Name:      "clock",
				FieldType: &BasicType{Type: DATETIME},
				Default:   &StringLiteral{Val: "something"},
			},
			output: "",
			err:    errors.New("DEFAULT clause is not supported for datetime"),
		},
	}

	t.Logf("The test bucket size is %d.", len(tests))
	for i, test := range tests {
		var got, exp map[string]any
		result, err := test.input.MarshalJSON()
		if err != nil {
			if test.err != nil {
				if err.Error() != test.err.Error() {
					t.Errorf("%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.err, err)
				}
			} else {
				t.Fatalf("failed to marshal json object %v", err)
			}
			continue
		}

		if err1 := json.Unmarshal(result, &got); err1 != nil {
			t.Fatalf("failed to unmarshal result got %v", err1)
		}

		if err2 := json.Unmarshal([]byte(test.output), &exp); err2 != nil {
			t.Fatalf("failed to unmarshal result got %v", err2)
		}

		if !reflect.DeepEqual(got, exp) {
			t.Errorf("%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.output, string(result))
		}
	}
}

func TestToJsonFields(t *testing.T) {
	defaultValues := struct {
		userId              string
		firstName, lastName string
		gender              string
	}{
		userId:    "10",
		firstName: "foo",
		lastName:  "bar",
		gender:    "true",
	}

	tests := []struct {
		input  StreamFields
		output map[string]*JsonStreamField
	}{
		{
			input: StreamFields{
				{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
				{Name: "Any_Val"},
			},
			output: map[string]*JsonStreamField{
				"STREET_NAME": {
					Type: "string",
				},
				"Any_Val": {},
			},
		}, {
			input: []StreamField{
				{Name: "USERID", FieldType: &BasicType{Type: BIGINT}, Default: &IntegerLiteral{Val: 10}},
				{Name: "FIRST_NAME", FieldType: &BasicType{Type: STRINGS}, Default: &StringLiteral{Val: "foo"}},
				{Name: "LAST_NAME", FieldType: &BasicType{Type: STRINGS}, Default: &StringLiteral{Val: "bar"}},
				{Name: "NICKNAMES", FieldType: &ArrayType{Type: STRINGS}},
				{Name: "data", FieldType: &BasicType{Type: BYTEA}},
				{Name: "Gender", FieldType: &BasicType{Type: BOOLEAN}, Default: &BooleanLiteral{Val: true}},
				{Name: "ADDRESS", FieldType: &RecType{
					StreamFields: []StreamField{
						{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
						{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
					},
				}},
			},
			output: map[string]*JsonStreamField{
				"USERID":     {Type: "bigint", DefaultValue: &defaultValues.userId},
				"FIRST_NAME": {Type: "string", DefaultValue: &defaultValues.firstName},
				"LAST_NAME":  {Type: "string", DefaultValue: &defaultValues.lastName},
				"NICKNAMES":  {Type: "array", Items: &JsonStreamField{Type: "string"}},
				"data":       {Type: "bytea"},
				"Gender":     {Type: "boolean", DefaultValue: &defaultValues.gender},
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
				"ADDRESSES": {
					Type: "array", Items: &JsonStreamField{
						Type: "struct", Properties: map[string]*JsonStreamField{
							"STREET_NAME": {Type: "string"},
							"NUMBER":      {Type: "bigint"},
						},
					},
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
