// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"testing"

	"github.com/stretchr/testify/assert"

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
				"server":             "tcp://127.0.0.1:1883",
				"format":             "json",
				"key":                "",
				"insecureSkipVerify": false,
				"protocolVersion":    "3.1.1",
				"qos":                1,
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
				"server":             "tcp://127.0.0.1:1883",
				"format":             "json",
				"key":                "",
				"insecureSkipVerify": false,
				"protocolVersion":    "3.1.1",
				"qos":                1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetSourceConf(tt.args.sourceType, tt.args.options)
			assert.Equal(t, tt.want, got)
		})
	}
}
