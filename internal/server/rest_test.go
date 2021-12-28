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

package server

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"reflect"
	"testing"
)

func Test_fetchPluginList(t *testing.T) {
	version = "1.4.0"
	type args struct {
		t     plugin.PluginType
		hosts string
		os    string
		arch  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			"source",
			args{
				t:     plugin.SOURCE,
				hosts: "http://127.0.0.1:8080",
				os:    "debian",
				arch:  "amd64",
			},
			nil,
		},
		{
			"sink",
			args{
				t:     plugin.SINK,
				hosts: "http://127.0.0.1:8080",
				os:    "debian",
				arch:  "amd64",
			},
			nil,
		},
		{
			"function",
			args{
				t:     plugin.FUNCTION,
				hosts: "http://127.0.0.1:8080",
				os:    "debian",
				arch:  "amd64",
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr, gotResult := fetchPluginList(tt.args.t, tt.args.hosts, tt.args.os, tt.args.arch)
			if !reflect.DeepEqual(gotErr, tt.wantErr) {
				t.Errorf("fetchPluginList() gotErr = %v, want %v", gotErr, tt.wantErr)
			}
			fmt.Printf("%v", gotResult)
		})
	}
}
