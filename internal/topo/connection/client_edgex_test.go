// Copyright 2021 EMQ Technologies Co., Ltd.
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

package connection

import (
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/lf-edge/ekuiper/internal/conf"
	"testing"
)

func TestEdgex_CfgValidate(t *testing.T) {
	type fields struct {
		mbconf types.MessageBusConfig
	}
	type args struct {
		props map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "config pass",
			fields: fields{},
			args: args{props: map[string]interface{}{
				"protocol": "tcp",
				"server":   "127.0.0.1",
				"port":     int64(1883),
				"type":     "mqtt",
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			}},
			wantErr: false,
		},
		{
			name:   "config not case sensitive",
			fields: fields{},
			args: args{props: map[string]interface{}{
				"Protocol": "tcp",
				"server":   "127.0.0.1",
				"Port":     1883,
				"type":     "mqtt",
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			}},

			wantErr: false,
		},
		{
			name:   "have unwanted config items topic",
			fields: fields{},
			args: args{props: map[string]interface{}{
				"protocol": "tcp",
				"server":   "127.0.0.1",
				"port":     1883,
				"type":     "mqtt",
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
				"topic": "demo",
			}},

			wantErr: true,
		},
		{
			name:   "config type not in zero/mqtt/redis ",
			fields: fields{},
			args: args{props: map[string]interface{}{
				"protocol": "tcp",
				"server":   "127.0.0.1",
				"port":     1883,
				"type":     "kafka",
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			}},

			wantErr: true,
		},
		{
			name:   "do not have enough config items ",
			fields: fields{},
			args: args{props: map[string]interface{}{
				"protocol": "tcp",
				"type":     "mqtt",
				"optional": map[string]string{
					"ClientId": "client1",
					"Username": "user1",
				},
			}},

			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es := &EdgexClient{
				selector: &conf.ConSelector{
					ConnSelectorStr: "testSelector",
				},
				mbconf: tt.fields.mbconf,
			}
			if err := es.CfgValidate(tt.args.props); (err != nil) != tt.wantErr {
				t.Errorf("CfgValidate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
