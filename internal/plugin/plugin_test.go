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

package plugin

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestDecode(t *testing.T) {
	var tests = []struct {
		t          PluginType
		j          string
		name       string
		file       string
		shellParas []string
		symbols    []string
	}{
		{
			t:    PORTABLE,
			j:    "{\"name\":\"mirror\",\"file\":\"mirror_win.zip\"}",
			name: "mirror",
			file: "mirror_win.zip",
		}, {
			t:          SINK,
			j:          `{"name":"tdengine","file":"https://packages.emqx.io/kuiper-plugins/1.3.1/debian/sinks/tdengine_amd64.zip","shellParas": ["2.0.3.1"]}`,
			name:       "tdengine",
			file:       "https://packages.emqx.io/kuiper-plugins/1.3.1/debian/sinks/tdengine_amd64.zip",
			shellParas: []string{"2.0.3.1"},
		}, {
			t:       FUNCTION,
			j:       `{"name":"image","file":"https://packages.emqx.io/kuiper-plugins/1.3.1/debian/functions/image_amd64.zip","functions": ["resize","thumbnail"]}`,
			name:    "image",
			file:    "https://packages.emqx.io/kuiper-plugins/1.3.1/debian/functions/image_amd64.zip",
			symbols: []string{"resize", "thumbnail"},
		}, {
			t: FUNCTION,
			j: "{\"name1\":\"image\",\"file1\":\"abc\"}",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		p := NewPluginByType(tt.t)
		err := json.Unmarshal([]byte(tt.j), p)
		if err != nil {
			t.Error(err)
			return
		}
		if tt.name != p.GetName() {
			t.Errorf("%d name mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.name, p.GetName())
		}
		if tt.file != p.GetFile() {
			t.Errorf("%d file mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.file, p.GetFile())
		}
		if !reflect.DeepEqual(tt.shellParas, p.GetShellParas()) {
			t.Errorf("%d shellParas mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.shellParas, p.GetShellParas())
		}
		if !reflect.DeepEqual(tt.symbols, p.GetSymbols()) {
			t.Errorf("%d symbols mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.symbols, p.GetSymbols())
		}
	}
}
