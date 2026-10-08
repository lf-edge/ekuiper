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

package client

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/replace"
)

func TestPrintConfSensitiveKeys(t *testing.T) {
	tests := []struct {
		key         string
		isSensitive bool
	}{
		{"password", true},
		{"Password", true},
		{"PASS", true},
		{"token", true},
		{"access_token", true},
		{"refresh_token", true},
		{"secret", true},
		{"clientid", false},
		{"username", false},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := replace.HidePasswordString(map[string]string{tt.key: "value"})
			if tt.isSensitive {
				assert.Equal(t, "***", got[tt.key])
			} else {
				assert.Equal(t, "value", got[tt.key])
			}
		})
	}
}

func TestEdgex_CfgValidate(t *testing.T) {
	tests := []struct {
		name    string
		expConf types.MessageBusConfig
		props   map[string]interface{}
		wantErr bool
	}{
		{
			name: "config pass",
			props: map[string]interface{}{
				"protocol": "tcp",
				"server":   "127.0.0.1",
				"port":     1883,
				"type":     "mqtt",
				"optional": map[string]interface{}{
					"clientid":  "client1",
					"username":  "user1",
					"KeepAlive": 500,
				},
			},
			wantErr: false,
			expConf: types.MessageBusConfig{
				Broker: types.HostInfo{
					Host:     "127.0.0.1",
					Port:     1883,
					Protocol: "tcp",
				},
				Type: "mqtt",
				Optional: map[string]string{
					"ClientId":  "client1",
					"Username":  "user1",
					"KeepAlive": "500",
				},
			},
		},
		{
			name: "config pass",
			props: map[string]interface{}{
				"protocol": "redis",
				"server":   "edgex-redis",
				"port":     6379,
				"type":     "redis",
			},
			wantErr: true,
		},
		{
			name: "config not case sensitive",
			props: map[string]interface{}{
				"Protocol": "tcp",
				"server":   "127.0.0.1",
				"Port":     1883,
				"type":     "mqtt",
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			expConf: types.MessageBusConfig{
				Broker: types.HostInfo{
					Host:     "127.0.0.1",
					Port:     1883,
					Protocol: "tcp",
				},
				Type: "mqtt",
				Optional: map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			wantErr: false,
		},
		{
			name: "config type not in mqtt/redis ",
			props: map[string]interface{}{
				"protocol": "tcp",
				"server":   "127.0.0.1",
				"port":     1883,
				"type":     "kafka",
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			wantErr: true,
		},
		{
			name: "do not have enough config items ",
			props: map[string]interface{}{
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			expConf: types.MessageBusConfig{
				Broker: types.HostInfo{
					Host:     "localhost",
					Port:     1883,
					Protocol: "tcp",
				},
				Type: "mqtt",
				Optional: map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			wantErr: false,
		},
		{
			name: "type is not right",
			props: map[string]interface{}{
				"type":     20,
				"protocol": "redis",
				"host":     "edgex-redis",
				"port":     6379,
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			wantErr: true,
		},
		{
			name: "port is not right",
			props: map[string]interface{}{
				"type":     "mqtt",
				"protocol": "redis",
				"host":     "edgex-redis",
				"port":     -1,
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			wantErr: true,
		},
		{
			name: "wrong type value",
			props: map[string]interface{}{
				"type":     "mqt",
				"protocol": "redis",
				"host":     "edgex-redis",
				"port":     6379,
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es := &Client{}
			if err := es.CfgValidate(tt.props); (err != nil) != tt.wantErr {
				t.Errorf("CfgValidate() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				if !reflect.DeepEqual(tt.expConf, es.mbconf) {
					t.Errorf("CfgValidate() expect = %v, actual %v", tt.expConf, es.mbconf)
				}
			}
		})
	}
}

// The same definition is persisted after Provision and used again on reload.
func TestEdgexCfgValidatePreservesOptional(t *testing.T) {
	for _, optional := range []any{
		map[string]string{"Username": "user", "Password": "secret", "KeepAlive": "30"},
		map[string]any{"username": "user", "password": "secret", "keepalive": 30},
	} {
		props := map[string]any{"type": "mqtt", "server": "localhost", "optional": optional}
		before, err := json.Marshal(props)
		require.NoError(t, err)
		c := &Client{}
		require.NoError(t, c.CfgValidate(props))
		after, err := json.Marshal(props)
		require.NoError(t, err)
		require.Equal(t, string(before), string(after))
		reloaded := &Client{}
		require.NoError(t, reloaded.CfgValidate(props))
		require.Equal(t, c.mbconf, reloaded.mbconf)
		// Provider-owned options must not alias a caller's string map either.
		c.mbconf.Optional["Username"] = "changed"
		after, err = json.Marshal(props)
		require.NoError(t, err)
		require.Equal(t, string(before), string(after))
	}
}
