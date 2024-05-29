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

package portable

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/internal/testx"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		p   *PluginInfo
		err string
	}{
		{
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "mirror",
					Version:    "1.0.0",
					Language:   "go",
					Executable: "mirror.exe",
				},
			},
			err: "invalid plugin, must define at lease one source, sink or function",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "wrr",
					Version:    "1.0.0",
					Language:   "go",
					Executable: "mirror.exe",
				},
				Sources: []string{"a", "b"},
			},
			err: "invalid plugin, expect name 'mirror' but got 'wrr'",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "mirror",
					Language:   "go",
					Executable: "mirror.exe",
				},
				Sinks: []string{"a", "b"},
			},
			err: "",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:     "mirror",
					Version:  "1.0.0",
					Language: "go",
				},
				Sources:   []string{"a", "b"},
				Sinks:     []string{"a", "b"},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, missing executable",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "mirror",
					Version:    "1.0.0",
					Executable: "tt",
				},
				Sources:   []string{"a", "b"},
				Sinks:     []string{"a", "b"},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, missing language",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "mirror",
					Version:    "1.0.0",
					Language:   "c",
					Executable: "tt",
				},
				Sources:   []string{"a", "b"},
				Sinks:     []string{"a", "b"},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, language 'c' is not supported",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		err := tt.p.Validate("mirror")
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d error mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.err, err.Error())
		}
	}
}
