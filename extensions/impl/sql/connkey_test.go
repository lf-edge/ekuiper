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

package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLConnectionKey(t *testing.T) {
	const dburl = "mysql://root:@127.0.0.1:3306/test"
	tests := []struct {
		name            string
		props           map[string]any
		wantKey         string
		wantRequire     bool
		wantErrContains string
	}{
		{
			name:        "no selector uses canonical dburl",
			props:       map[string]any{"dburl": dburl},
			wantKey:     dburl,
			wantRequire: false,
		},
		{
			name:        "nil props use canonical dburl",
			props:       nil,
			wantKey:     dburl,
			wantRequire: false,
		},
		{
			name: "non-empty selector references named connection",
			props: map[string]any{
				"dburl":              dburl,
				"connectionSelector": "my-conn",
			},
			wantKey:     "my-conn",
			wantRequire: true,
		},
		{
			name: "empty selector counts as absent",
			props: map[string]any{
				"dburl":              dburl,
				"connectionSelector": "",
			},
			wantKey:     dburl,
			wantRequire: false,
		},
		{
			name: "nil selector counts as absent",
			props: map[string]any{
				"dburl":              dburl,
				"connectionSelector": nil,
			},
			wantKey:     dburl,
			wantRequire: false,
		},
		{
			name: "non-string selector is a static error, not a silent fallback",
			props: map[string]any{
				"dburl":              dburl,
				"connectionSelector": 123,
			},
			wantErrContains: "connectionSelector must be a string",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, requireExisting, err := sqlConnectionKey(tc.props, dburl)
			if tc.wantErrContains != "" {
				require.ErrorContains(t, err, tc.wantErrContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantKey, key)
			require.Equal(t, tc.wantRequire, requireExisting)
		})
	}
}
