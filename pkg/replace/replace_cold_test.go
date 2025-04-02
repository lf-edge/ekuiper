// Copyright 2025 EMQ Technologies Co., Ltd.
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

package replace

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func TestReplaceCold(t *testing.T) {
	testx.InitEnv("replaceCold")
	initConf := conf.Config.Hack.Cold
	conf.Config.Hack.Cold = true
	defer func() {
		conf.Config.Hack.Cold = initConf
	}()

	tests := []struct {
		name    string
		changed bool
		origin  string
		expect  string
	}{
		{
			name:    "changed",
			changed: true,
			origin:  "test/origin",
			expect:  "test/target",
		},
		{
			name:   "no change",
			origin: "test/origin2",
			expect: "test/origin2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin, err := os.ReadFile(tt.origin)
			require.NoError(t, err)
			expected, err := os.ReadFile(tt.expect)
			require.NoError(t, err)
			ruleMap := make(map[string]any)
			err = json.Unmarshal(origin, &ruleMap)
			require.NoError(t, err)
			result, changed := ReplaceCold(ruleMap)
			require.Equal(t, tt.changed, changed)
			if changed {
				require.Equal(t, string(expected), result)
			}
		})
	}
}
