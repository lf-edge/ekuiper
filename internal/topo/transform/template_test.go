// Copyright 2023 EMQ Technologies Co., Ltd.
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

package transform

import (
	"fmt"
	"reflect"
	"testing"
)

func Test_SelectMap(t *testing.T) {
	type args struct {
		input  interface{}
		fields []string
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "test1",
			args: args{
				input: map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				fields: []string{"a", "b"},
			},
			want: map[string]interface{}{
				"a": 1,
				"b": 2,
			},
		},
		{
			name: "test2",
			args: args{
				input: []map[string]interface{}{
					{
						"a": 1,
						"b": 2,
						"c": 3,
					},
				},
				fields: []string{"a", "b"},
			},
			want: []map[string]interface{}{
				{
					"a": 1,
					"b": 2,
				},
			},
		},
		{
			name: "test3",
			args: args{
				input: []interface{}{
					map[string]interface{}{
						"a": 1,
						"b": 2,
						"c": 3,
					},
				},
				fields: []string{"a", "b"},
			},
			want: []map[string]interface{}{
				{
					"a": 1,
					"b": 2,
				},
			},
		},
		{
			name: "test4",
			args: args{
				input: []map[string]interface{}{
					{
						"a": 1,
						"b": 2,
						"c": 3,
					},
				},
				fields: nil,
			},
			want: []map[string]interface{}{
				{
					"a": 1,
					"b": 2,
					"c": 3,
				},
			},
		},
		{
			name: "test5",
			args: args{
				input:  []byte(`{"a": 1, "b": 2, "c": 3}`),
				fields: nil,
			},
			want: []byte(`{"a": 1, "b": 2, "c": 3}`),
		},
		{
			name: "test6",
			args: args{
				input:  []byte(`{"a": 1, "b": 2, "c": 3}`),
				fields: []string{"d"},
			},
			want: map[string]interface{}{
				"d": nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := SelectMap(tt.args.input, tt.args.fields); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SelectMap() = %v, want %v", got, tt.want)
				fmt.Println(reflect.TypeOf(got), reflect.TypeOf(tt.want))

			}
		})
	}
}
