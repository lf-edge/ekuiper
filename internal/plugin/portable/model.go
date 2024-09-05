// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
)

type PluginInfo struct {
	runtime.PluginMeta
	Sources   []string `json:"sources"`
	Sinks     []string `json:"sinks"`
	Functions []string `json:"functions"`
}

var langMap = map[string]bool{
	"go":     true,
	"python": true,
}

// Validate TODO validate duplication of source, sink and functions
func (p *PluginInfo) Validate(expectedName string) (err error) {
	defer func() {
		failpoint.Inject("PluginInfoValidateErr", func() {
			err = errors.New("PluginInfoValidateErr")
		})
	}()
	if p.Name != expectedName {
		return fmt.Errorf("invalid plugin, expect name '%s' but got '%s'", expectedName, p.Name)
	}
	if p.Language == "" {
		return fmt.Errorf("invalid plugin, missing language")
	}
	if p.Executable == "" {
		return fmt.Errorf("invalid plugin, missing executable")
	}
	if len(p.Sources)+len(p.Sinks)+len(p.Functions) == 0 {
		return fmt.Errorf("invalid plugin, must define at lease one source, sink or function")
	}
	if l, ok := langMap[p.Language]; !ok || !l {
		return fmt.Errorf("invalid plugin, language '%s' is not supported", p.Language)
	}
	return nil
}
