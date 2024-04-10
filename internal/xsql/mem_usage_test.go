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

package xsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryUsage(t *testing.T) {
	testcases := []struct {
		tuple       *Tuple
		memoryUsage int64
	}{
		{
			tuple: &Tuple{
				Message: map[string]interface{}{
					"key1": int64(1),
				},
			},
			memoryUsage: int64(24),
		},
		{
			tuple: &Tuple{
				Message: map[string]interface{}{
					"key1": int64(1),
					"key2": float64(3.0),
				},
			},
			memoryUsage: int64(48),
		},
		{
			tuple: &Tuple{
				Message: map[string]interface{}{
					"key1": int64(1),
					"key2": float64(3.0),
					"key3": []interface{}{
						int64(1), float64(3.0),
					},
				},
			},
			memoryUsage: int64(80),
		},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.memoryUsage, tc.tuple.MemoryUsage())
	}
}
