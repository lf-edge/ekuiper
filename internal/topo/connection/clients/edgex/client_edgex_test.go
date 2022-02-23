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

//go:build edgex
// +build edgex

package edgex

import (
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"reflect"
	"testing"
)

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
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			},
			wantErr: false,
			expConf: types.MessageBusConfig{
				PublishHost: types.HostInfo{
					Host:     "127.0.0.1",
					Port:     1883,
					Protocol: "tcp",
				},
				SubscribeHost: types.HostInfo{
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
		},
		{
			name: "config pass",
			props: map[string]interface{}{
				"protocol": "redis",
				"server":   "edgex-redis",
				"port":     6379,
				"type":     "redis",
			},
			wantErr: false,
			expConf: types.MessageBusConfig{
				PublishHost: types.HostInfo{
					Host:     "edgex-redis",
					Port:     6379,
					Protocol: "redis",
				},
				SubscribeHost: types.HostInfo{
					Host:     "edgex-redis",
					Port:     6379,
					Protocol: "redis",
				},
				Type: "redis",
			},
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
				PublishHost: types.HostInfo{
					Host:     "127.0.0.1",
					Port:     1883,
					Protocol: "tcp",
				},
				SubscribeHost: types.HostInfo{
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
			name: "config type not in zero/mqtt/redis ",
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
				PublishHost: types.HostInfo{
					Host:     "localhost",
					Port:     6379,
					Protocol: "redis",
				},
				SubscribeHost: types.HostInfo{
					Host:     "localhost",
					Port:     6379,
					Protocol: "redis",
				},
				Type: "redis",
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
				"type":     "zmq",
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
			es := &EdgexClient{}
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
