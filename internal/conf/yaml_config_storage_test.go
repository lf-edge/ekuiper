// Copyright 2023 EMQ Technologies Co., Ltd.
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

package conf

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/pkg/store"
)

func TestSQLiteStorage(t *testing.T) {
	dataDir, err := GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	s, err := NewSqliteKVStore("test")
	require.NoError(t, err)
	require.NoError(t, s.Set("k1", map[string]interface{}{
		"key1": "value1",
	}))
	require.NoError(t, s.Set("k2", map[string]interface{}{
		"key2": "value2",
	}))
	v, err := s.GetByPrefix("k")
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]interface{}{
		"k1": {
			"key1": "value1",
		},
		"k2": {
			"key2": "value2",
		},
	}, v)
	require.NoError(t, s.Delete("k1"))
	v, err = s.GetByPrefix("k")
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]interface{}{
		"k2": {
			"key2": "value2",
		},
	}, v)
}
