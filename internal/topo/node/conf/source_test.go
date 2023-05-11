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

package conf

import (
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestGetSourceConf(t *testing.T) {
	type args struct {
		sourceType string
		options    *ast.Options
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
	}{
		{
			name: "default",
			args: args{
				sourceType: "mqtt",
				options: &ast.Options{
					CONF_KEY: "",
				},
			},
			want: map[string]interface{}{
				"qos":    1,
				"server": "tcp://127.0.0.1:1883",
				"format": "json",
				"key":    "",
			},
		},
		{
			name: "demo_conf",
			args: args{
				sourceType: "mqtt",
				options: &ast.Options{
					CONF_KEY: "Demo_conf",
				},
			},
			want: map[string]interface{}{
				"qos":    0,
				"server": "tcp://10.211.55.6:1883",
				"format": "json",
				"key":    "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSourceConf(tt.args.sourceType, tt.args.options); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSourceConf() = %v, want %v", got, tt.want)
			}
		})
	}
}
