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

package keyedstate

import (
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"testing"
)

func init() {
	testx.InitEnv()
}

func TestGetKeyedState(t *testing.T) {
	// env set up
	InitManager()

	type args struct {
		groupName string
		key       string
		value     interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "string",
			args: args{
				key:   "status",
				value: "0",
			},
			want: "0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetKeyedState(tt.args.key, tt.args.value)
			if err != nil {
				t.Errorf("SetKeyedState() = %v", err)
			}
			got, err := GetKeyedState(tt.args.key)
			if err != nil {
				t.Errorf("GetKeyedState() = %v", err)
			} else {
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("expect: %v, get: %v", tt.want, got)
				}
			}

		})
	}

	_ = ClearKeyedState()
}
