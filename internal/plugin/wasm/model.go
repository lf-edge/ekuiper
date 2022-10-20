// Copyright erfenjiao, 630166475@qq.com.
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
)

type PluginInfo struct {
	runtime.PluginMeta
	Functions []string `json:"functions"`
}

func (p *PluginInfo) Validate(expectedName string) error {
	if p.Name != expectedName {
		return fmt.Errorf("invalid plugin, expect name '%s' but got '%s'", expectedName, p.Name)
	}
	if len(p.Functions) == 0 {
		return fmt.Errorf("invalid plugin, must define at lease one function")
	}
	if p.WasmEngine == "" {
		return fmt.Errorf("invalid WasmEngine")
	}
	return nil
}
