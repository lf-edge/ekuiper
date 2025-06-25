// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

func Test_SelectMap(t *testing.T) {
	type args struct {
		input   any
		fields  []string
		exclude []string
	}
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "test1",
			args: args{
				input: map[string]any{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				fields: []string{"a", "b"},
			},
			want: map[string]any{
				"a": 1,
				"b": 2,
			},
		},
		{
			name: "test2",
			args: args{
				input: []map[string]any{
					{
						"a": 1,
						"b": 2,
						"c": 3,
					},
				},
				fields: []string{"a", "b"},
			},
			want: []map[string]any{
				{
					"a": 1,
					"b": 2,
				},
			},
		},
		{
			name: "test4",
			args: args{
				input: []map[string]any{
					{
						"a": 1,
						"b": 2,
						"c": 3,
					},
				},
				fields: nil,
			},
			want: []map[string]any{
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
			name: "test ex1",
			args: args{
				input: map[string]any{
					"a": 1,
					"b": 2,
					"c": 3,
				},
				exclude: []string{"c"},
			},
			want: map[string]any{
				"a": 1,
				"b": 2,
			},
		},
		{
			name: "test ex2",
			args: args{
				input: []map[string]any{
					{
						"a": 1,
						"b": 2,
						"c": 3,
					},
				},
				exclude: []string{"a"},
			},
			want: []map[string]any{
				{
					"b": 2,
					"c": 3,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := selectMap(tt.args.input, tt.args.fields, tt.args.exclude); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("selectMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTransItem(t *testing.T) {
	type args struct {
		input     any
		dataField string
		fields    []string
		exclude   []string
	}
	tests := []struct {
		name    string
		args    args
		want    any
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				input: map[string]any{
					"device": map[string]any{
						"device_id":          1,
						"device_temperature": 31.2,
						"device_humidity":    80,
					},
					"ts": 1625040000,
				},
				dataField: "device",
				fields:    []string{"device_temperature", "device_humidity"},
			},
			want: map[string]any{
				"device_temperature": 31.2,
				"device_humidity":    80,
			},
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				input: []map[string]any{
					{
						"device": map[string]any{
							"device_id":          1,
							"device_temperature": 31.2,
							"device_humidity":    80,
						},
						"ts": 1625040000,
					},
				},
				dataField: "device",
				fields:    []string{"device_temperature", "device_humidity"},
			},
			want: map[string]any{
				"device_temperature": 31.2,
				"device_humidity":    80,
			},
			wantErr: false,
		},
		{
			name: "test3",
			args: args{
				input: map[string]any{
					"telemetry": []map[string]any{
						{
							"temperature": 32.32,
							"humidity":    80.8,
							"f3":          "f3tagValue",
							"f4":          "f4tagValue",
							"ts":          1388082430,
						},
						{
							"temperature": 34.32,
							"humidity":    81.8,
							"f3":          "f3tagValue",
							"f4":          "f4tagValue",
							"ts":          1388082440,
						},
					},
					"device": map[string]any{
						"device_id":          1,
						"device_temperature": 31.2,
						"device_humidity":    80,
					},
				},
				dataField: "telemetry",
				fields:    []string{"temperature", "humidity"},
			},
			want: []map[string]any{
				{
					"temperature": 32.32,
					"humidity":    80.8,
				},
				{
					"temperature": 34.32,
					"humidity":    81.8,
				},
			},
			wantErr: false,
		},
		{
			name: "test4",
			args: args{
				input: []any{
					map[string]any{
						"telemetry": []map[string]any{
							{
								"temperature": 32.32,
								"humidity":    80.8,
								"ts":          1388082430,
							},
							{
								"temperature": 34.32,
								"humidity":    81.8,
								"ts":          1388082440,
							},
						},
					},
				},
				dataField: "telemetry",
				fields:    []string{"temperature", "humidity"},
			},
			want: []map[string]any{
				{
					"temperature": 32.32,
					"humidity":    80.8,
				},
				{
					"temperature": 34.32,
					"humidity":    81.8,
				},
			},
			wantErr: false,
		},
		{
			name: "test5",
			args: args{
				input: []any{
					map[string]any{
						"telemetry": []any{
							"abc", "Def",
						},
					},
				},
				dataField: "telemetry",
				fields:    []string{"temperature", "humidity"},
			},
			wantErr: true,
		},
		{
			name: "test ex 1",
			args: args{
				input: map[string]any{
					"device": map[string]any{
						"device_id":          1,
						"device_temperature": 31.2,
						"device_humidity":    80,
					},
					"ts": 1625040000,
				},
				dataField: "device",
				exclude:   []string{"device_temperature"},
			},
			want: map[string]any{
				"device_id":       1,
				"device_humidity": 80,
			},
			wantErr: false,
		},
		{
			name: "test ex2",
			args: args{
				input: []map[string]any{
					{
						"device": map[string]any{
							"device_id":          1,
							"device_temperature": 31.2,
							"device_humidity":    80,
						},
						"ts": 1625040000,
					},
				},
				dataField: "device",
				exclude:   []string{"device_id", "device_humidity"},
			},
			want: map[string]any{
				"device_temperature": 31.2,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := TransItem(tt.args.input, tt.args.dataField, tt.args.fields, tt.args.exclude)
			if (err != nil) != tt.wantErr {
				t.Errorf("TransItem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TransItem() got = %v, want %v", got, tt.want)
			}
		})
	}
}
