// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package urlencoded

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

func TestEncodeDecode(t *testing.T) {
	tt := []struct {
		name string
		m    any
		s    string
		nm   map[string]any
		err  string
	}{
		{
			name: "normal",
			m: map[string]any{
				"a": "b",
				"c": 20,
			},
			s: `a=b&c=20`,
			nm: map[string]any{
				"a": "b",
				"c": "20",
			},
		},
		{
			name: "nested",
			m: map[string]any{
				"a": []any{10, 20, 40},
				"b": []map[string]any{{"a": "b"}},
				"c": map[string]any{"a": "b"},
			},
			s: `a=10&a=20&a=40&b=%5Bmap%5Ba%3Ab%5D%5D&c=map%5Ba%3Ab%5D`,
			nm: map[string]any{
				"a": []string{"10", "20", "40"},
				"b": "[map[a:b]]",
				"c": "map[a:b]",
			},
		},
		{
			name: "unsupport",
			m:    []map[string]any{{"a": 1}},
			err:  "unsupported type [map[a:1]], must be a map",
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			cc, err := NewConverter(nil)
			require.NoError(t, err)
			encode, err := cc.Encode(context.Background(), tc.m)
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
				return
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.s, string(encode))
			}
			rr, err := cc.Decode(context.Background(), []byte(tc.s))
			require.NoError(t, err)
			require.Equal(t, tc.nm, rr)
		})
	}
}
