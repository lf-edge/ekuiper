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
	"reflect"
	"testing"
)

func Test_selectMap(t *testing.T) {
	type args struct {
		input  map[string]interface{}
		fields []string
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
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
				input: map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				fields: nil,
			},
			want: map[string]interface{}{
				"a": 1,
				"b": 2,
				"c": 3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectMap(tt.args.input, tt.args.fields); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("selectMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_selectJson(t *testing.T) {
	type args struct {
		bytes       []byte
		fields      []string
		transformed bool
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		want1   bool
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				bytes:       []byte(`{"a":1,"b":2,"c":3}`),
				fields:      []string{"a", "b"},
				transformed: false,
			},
			want:    []byte(`{"a":1,"b":2}`),
			want1:   true,
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				bytes:       []byte(`[{"a":1,"b":2,"c":3}]`),
				fields:      []string{"a", "b"},
				transformed: true,
			},
			want:    []byte(`[{"a":1,"b":2}]`),
			want1:   true,
			wantErr: false,
		},
		{
			name: "test3",
			args: args{
				bytes:       []byte(`[{"a":1,"b":2,"c":3}]`),
				fields:      nil,
				transformed: true,
			},
			want:    []byte(`[{"a":1,"b":2,"c":3}]`),
			want1:   true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := selectJson(tt.args.bytes, tt.args.fields, tt.args.transformed)
			if (err != nil) != tt.wantErr {
				t.Errorf("selectJson() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("selectJson() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("selectJson() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
