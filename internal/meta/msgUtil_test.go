// Copyright 2022 EMQ Technologies Co., Ltd.
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

package meta

import (
	"bytes"
	"reflect"
	"testing"
)

func TestConstructJsonArray(t *testing.T) {
	type args struct {
		jsonByteItems []fileContent
	}

	var buf1 bytes.Buffer
	buf1.Write([]byte("[]"))

	var buf2 bytes.Buffer
	buf2.Write([]byte(`[{"key": "key1"}]`))

	var buf3 bytes.Buffer
	buf3.Write([]byte(`[{"key1": "value1"},{"key2": "value2"}]`))

	tests := []struct {
		name string
		args args
		want bytes.Buffer
	}{
		{
			name: "no value",
			args: args{
				jsonByteItems: nil,
			},
			want: buf1,
		},
		{
			name: "one value",
			args: args{
				jsonByteItems: []fileContent{
					[]byte(`{"key": "key1"}`),
				},
			},
			want: buf2,
		},
		{
			name: "two value",
			args: args{
				jsonByteItems: []fileContent{
					[]byte(`{"key1": "value1"}`),
					[]byte(`{"key2": "value2"}`),
				},
			},
			want: buf3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConstructJsonArray(tt.args.jsonByteItems); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConstructJsonArray() = %v, want %v", got, tt.want)
			}
		})
	}
}
