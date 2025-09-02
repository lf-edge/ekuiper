// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestGetSourceConf(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	_, err := connection.CreateNamedConnection(ctx, "test11", "mock", map[string]any{
		"a": 1,
	})
	require.NoError(t, err)
	require.NoError(t, conf.WriteCfgIntoKVStorage("sources", "mqtt", "test11", map[string]interface{}{
		"connectionSelector": "test11",
	}))
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
					CONF_KEY:   "",
					DATASOURCE: "abc",
				},
			},
			want: map[string]interface{}{
				"server":             "tcp://127.0.0.1:1883",
				"format":             "json",
				"key":                "",
				"insecureSkipVerify": false,
				"protocolVersion":    "3.1.1",
				"qos":                1,
				"datasource":         "abc",
				"delimiter":          "",
				"retainSize":         0,
				"schemaId":           "",
				"strictValidation":   false,
				"timestamp":          "",
				"timestampFormat":    "",
			},
		},
		{
			name: "demo_conf",
			args: args{
				sourceType: "mqtt",
				options: &ast.Options{
					CONF_KEY:   "Demo_conf",
					DATASOURCE: "abc",
				},
			},
			want: map[string]interface{}{
				"server":             "tcp://127.0.0.1:1883",
				"format":             "json",
				"key":                "",
				"insecureSkipVerify": false,
				"protocolVersion":    "3.1.1",
				"qos":                1,
				"datasource":         "abc",
				"delimiter":          "",
				"retainSize":         0,
				"schemaId":           "",
				"strictValidation":   false,
				"timestamp":          "",
				"timestampFormat":    "",
			},
		},
		{
			name: "connTest",
			args: args{
				sourceType: "mqtt",
				options: &ast.Options{
					CONF_KEY:   "test11",
					DATASOURCE: "abc",
				},
			},
			want: map[string]interface{}{
				"server":             "tcp://127.0.0.1:1883",
				"format":             "json",
				"key":                "",
				"insecureSkipVerify": false,
				"protocolVersion":    "3.1.1",
				"qos":                1,
				"datasource":         "abc",
				"delimiter":          "",
				"retainSize":         0,
				"schemaId":           "",
				"strictValidation":   false,
				"timestamp":          "",
				"timestampFormat":    "",
				"connectionSelector": "test11",
				"a":                  1,
			},
		},
		{
			name: "extra cover confkey",
			args: args{
				sourceType: "mqtt",
				options: &ast.Options{
					CONF_KEY:   "test11",
					DATASOURCE: "abc",
					EXTRA:      "{\"test\":3, \"qos\":2}",
				},
			},
			want: map[string]interface{}{
				"server":             "tcp://127.0.0.1:1883",
				"format":             "json",
				"key":                "",
				"insecureSkipVerify": false,
				"protocolVersion":    "3.1.1",
				"qos":                float64(2),
				"datasource":         "abc",
				"delimiter":          "",
				"retainSize":         0,
				"schemaId":           "",
				"strictValidation":   false,
				"timestamp":          "",
				"timestampFormat":    "",
				"connectionSelector": "test11",
				"a":                  1,
				"test":               float64(3),
			},
		},
		{
			name: "extra not cover option",
			args: args{
				sourceType: "mqtt",
				options: &ast.Options{
					CONF_KEY:   "test11",
					DATASOURCE: "abc",
					EXTRA:      "{\"datasource\":\"new\", \"qos\":2}",
				},
			},
			want: map[string]interface{}{
				"server":             "tcp://127.0.0.1:1883",
				"format":             "json",
				"key":                "",
				"insecureSkipVerify": false,
				"protocolVersion":    "3.1.1",
				"qos":                float64(2),
				"datasource":         "abc",
				"delimiter":          "",
				"retainSize":         0,
				"schemaId":           "",
				"strictValidation":   false,
				"timestamp":          "",
				"timestampFormat":    "",
				"connectionSelector": "test11",
				"a":                  1,
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
