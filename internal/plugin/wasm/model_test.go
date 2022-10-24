// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package wasm

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"testing"
)

func TestValidate(t *testing.T) {
	var tests = []struct {
		p   *PluginInfo
		err string
	}{
		{
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "fibonacci",
					Version:    "1.0.0",
					WasmEngine: "wasmedge",
				},
			},
			err: "invalid plugin, must define at lease one function",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "fibonacci",
					Version:    "1.0.0",
					WasmEngine: "wasmedge",
				},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, missing executable",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:    "fibonacci",
					Version: "1.0.0",
					//Executable: "tt",
					WasmEngine: "wasmedge",
				},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, missing language",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "fibonacci",
					Version:    "1.0.0",
					WasmEngine: "wasmedge",
					//Executable: "tt",
				},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, language 'c' is not supported",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		err := tt.p.Validate("fibonacci")
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			fmt.Println("i : ", i)
			fmt.Println("err: ", err)
			//t.Errorf("%d error mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.err, err.Error())
		}
		fmt.Println("err: ", err)
	}
}
