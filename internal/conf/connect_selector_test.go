// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

	"github.com/stretchr/testify/require"
)

func TestConSelector_ReadCfgFromYaml(t *testing.T) {
	type fields struct {
		ConnSelectorStr string
		Type            string
		CfgKey          string
	}
	tests := []struct {
		name      string
		fields    fields
		wantProps map[string]interface{}
		wantErr   bool
	}{
		{
			name: "mqtt localConnection",
			fields: fields{
				ConnSelectorStr: "mqtt.localconnection",
				Type:            "mqtt",
				CfgKey:          "localConnection",
			},
			wantProps: map[string]interface{}{
				"username": "ekuiper",
				"password": "password",
				"server":   "tcp://127.0.0.1:1883",
			},
			wantErr: false,
		},
		{
			name: "edgex mqttMsgBus",
			fields: fields{
				ConnSelectorStr: "edgex.mqttmsgbus",
				Type:            "edgex",
				CfgKey:          "mqttMsgBus",
			},
			wantProps: map[string]interface{}{
				"protocol": "tcp",
				"port":     1883,
				"server":   "127.0.0.1",
				"type":     "mqtt",
				"optional": map[string]any{
					"KeepAlive": "50",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConSelector{
				ConnSelectorStr: tt.fields.ConnSelectorStr,
				Type:            tt.fields.Type,
				CfgKey:          tt.fields.CfgKey,
			}
			gotProps, err := c.ReadCfgFromYaml()
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCfgFromYaml() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.wantProps, gotProps)
		})
	}
}
