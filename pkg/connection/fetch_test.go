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

package connection

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestResolveConnectionKey pins the explicit-identity rule: any
// non-empty connectionSelector selects the named connection
// (attach-only), even when it textually equals the anonymous key. An
// empty selector counts as absent.
func TestResolveConnectionKey(t *testing.T) {
	for _, tc := range []struct {
		name         string
		props        map[string]any
		anonymous    string
		wantKey      string
		wantExisting bool
	}{
		{"no props", nil, "anon", "anon", false},
		{"empty props", map[string]any{}, "anon", "anon", false},
		{"named selector", map[string]any{"connectionSelector": "named"}, "anon", "named", true},
		{"selector equals anonymous key", map[string]any{"connectionSelector": "anon"}, "anon", "anon", true},
		{"empty selector is absent", map[string]any{"connectionSelector": ""}, "anon", "anon", false},
		{"non-string selector is absent", map[string]any{"connectionSelector": 42}, "anon", "anon", false},
		{"unrelated props", map[string]any{"server": "x"}, "anon", "anon", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, requireExisting := ResolveConnectionKey(tc.props, tc.anonymous)
			require.Equal(t, tc.wantKey, key)
			require.Equal(t, tc.wantExisting, requireExisting)
		})
	}
}
