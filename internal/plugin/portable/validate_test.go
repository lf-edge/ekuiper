// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
)

func validMeta(name string) runtime.PluginMeta {
	return runtime.PluginMeta{Name: name, Language: "go", Executable: "test"}
}

func TestValidateRejectsTraversalFragments(t *testing.T) {
	for _, arr := range []struct {
		name string
		pi   *PluginInfo
	}{
		{"dotdot source", &PluginInfo{PluginMeta: validMeta("p"), Sources: []string{"../../evil"}}},
		{"slash sink", &PluginInfo{PluginMeta: validMeta("p"), Sinks: []string{"a/b"}}},
		{"abs function", &PluginInfo{PluginMeta: validMeta("p"), Functions: []string{"/etc/x"}}},
		{"empty element", &PluginInfo{PluginMeta: validMeta("p"), Sources: []string{""}}},
	} {
		assert.Error(t, arr.pi.Validate("p"), arr.name)
	}

	// Clean names still pass.
	pi := &PluginInfo{PluginMeta: validMeta("p"), Sources: []string{"s1"}, Sinks: []string{"sk-1"}, Functions: []string{"f_1"}}
	assert.NoError(t, pi.Validate("p"))
}

func TestDeleteRejectsInvalidName(t *testing.T) {
	m, err := MockManager(map[string]*PluginInfo{
		"p": {PluginMeta: validMeta("p"), Sources: []string{"s1"}},
	})
	require.NoError(t, err)

	assert.Error(t, m.Delete(".."), "dotdot name must be rejected before touching the filesystem")
	assert.Error(t, m.Delete("../../etc"), "traversal name must be rejected")
	assert.Error(t, m.Delete("a/b"), "slash name must be rejected")
}
