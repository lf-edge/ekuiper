// Copyright 2021 EMQ Technologies Co., Ltd.
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
