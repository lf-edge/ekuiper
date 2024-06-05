// Copyright 2024 EMQ Technologies Co., Ltd.
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

package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestWrapUpdatable(t *testing.T) {
	tests := []struct {
		name  string
		value map[string]any
		err   string
	}{
		{
			name: "empty rowkind with wrong key",
			value: map[string]any{
				"nokey": 100,
			},
			err: "key field id not found in data &{ map[nokey:100] 0001-01-01 00:00:00 +0000 UTC map[] map[] {{{0 0} 0 0 {{} 0} {{} 0}} map[] map[]} {0 0} map[]}",
		},
		{
			name: "wrong rowkind type",
			value: map[string]any{
				"rowkind": 100,
			},
			err: "rowkind field rowkind is not a string in data &{ map[rowkind:100] 0001-01-01 00:00:00 +0000 UTC map[] map[] {{{0 0} 0 0 {{} 0} {{} 0}} map[] map[]} {0 0} map[]}",
		},
		{
			name: "wrong rowkind value",
			value: map[string]any{
				"rowkind": "test",
			},
			err: "invalid rowkind test",
		},
	}
	s := &sink{}
	ctx := mockContext.NewMockContext("rule1", "test")
	err := s.Provision(ctx, map[string]any{
		"rowkindField": "rowkind",
		"keyField":     "id",
	})
	assert.NoError(t, err)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.wrapUpdatable(&xsql.Tuple{
				Message: tt.value,
			})
			assert.Error(t, err)
			assert.EqualError(t, err, tt.err)
		})
	}
}
