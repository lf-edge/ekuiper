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

package mqtt

import (
	"reflect"
	"testing"
)

func TestMQTTClient_CfgValidate(t *testing.T) {
	type args struct {
		props map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "config pass",
			args: args{
				props: map[string]interface{}{
					"server": "tcp:127.0.0.1",
				},
			},
			wantErr: false,
		},
		{
			name: "config are not case sensitive",
			args: args{
				props: map[string]interface{}{
					"SERVER": "tcp:127.0.0.1",
				},
			},
			wantErr: false,
		},
		{
			name: "config server addr key error",
			args: args{
				props: map[string]interface{}{
					"server": "tcp:127.0.0.1",
				},
			},
			wantErr: false,
		},
		{
			name: "config have unwanted topic fields",
			args: args{
				props: map[string]interface{}{
					"server": "tcp:127.0.0.1",
					"topic":  "demo",
				},
			},
			wantErr: false,
		},
		{
			name: "config no server addr",
			args: args{
				props: map[string]interface{}{
					"username": "user1",
				},
			},
			wantErr: true,
		},
		{
			name: "config no server addr",
			args: args{
				props: map[string]interface{}{
					"server": "",
				},
			},
			wantErr: true,
		},
		{
			name: "config miss cert key file",
			args: args{
				props: map[string]interface{}{
					"server":            "tcp:127.0.0.1",
					"certificationPath": "./not_exist.crt",
					"privateKeyPath":    "./not_exist.key",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := &MQTTClient{}
			err := ms.CfgValidate(tt.args.props)
			if (err != nil) != tt.wantErr {
				t.Errorf("CfgValidate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMQTTClient_CfgResult(t *testing.T) {
	props := map[string]interface{}{
		"server":   "tcp:127.0.0.1:1883",
		"USERNAME": "demo",
		"Password": "password",
		"clientID": "clientid",
	}

	ms := &MQTTClient{}

	_ = ms.CfgValidate(props)

	if !reflect.DeepEqual("tcp:127.0.0.1:1883", ms.srv) {
		t.Errorf("result mismatch:\n\n got=%#v\n\n", ms.srv)
	}
	if !reflect.DeepEqual("demo", ms.uName) {
		t.Errorf("result mismatch:\n\n got=%#v\n\n", ms.uName)
	}
	if !reflect.DeepEqual("password", ms.password) {
		t.Errorf("result mismatch:\n\n got=%#v\n\n", ms.password)
	}
	if !reflect.DeepEqual("clientid", ms.clientid) {
		t.Errorf("result mismatch:\n\n got=%#v\n\n", ms.clientid)
	}
	if !reflect.DeepEqual(uint(4), ms.pVersion) {
		t.Errorf("result mismatch:\n\n got=%#v\n\n", ms.pVersion)
	}
}
