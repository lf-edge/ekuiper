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

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
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

func TestGetYamlConfigAllKeys(t *testing.T) {
	dataDir, err := GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, saveCfgKeyToKV(buildKey("sources", "mqtt", "conf1"), map[string]interface{}{"a": 1}))
	require.NoError(t, saveCfgKeyToKV(buildKey("sources", "sql", "conf1"), map[string]interface{}{"a": 1}))
	m, err := GetYamlConfigAllKeys("sources")
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{
		"mqtt": {}, "sql": {},
	}, m)
	require.NoError(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/conf/storageErr", "return(true)")
	_, err = GetYamlConfigAllKeys("sources")
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/conf/storageErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/conf/getDataErr", "return(true)")
	_, err = GetYamlConfigAllKeys("sources")
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/conf/getDataErr")
}

func TestGetStorage(t *testing.T) {
	IsTesting = true
	defer func() {
		IsTesting = false
	}()
	dataDir, err := GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	kvStore = nil
	s, err := getKVStorage()
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestGetCfgKeyFromStorageByPrefix(t *testing.T) {
	IsTesting = true
	defer func() {
		IsTesting = false
	}()
	kvStore = nil
	s, err := getKVStorage()
	require.NoError(t, err)
	require.NoError(t, s.Set("mock", map[string]interface{}{}))
	require.NoError(t, s.Set("a.b.c", map[string]interface{}{}))
	got, err := getCfgKeyFromStorageByPrefix("")
	require.NoError(t, err)
	_, ok := got["c"]
	require.True(t, ok)
}

func TestGetAllConnConfigs(t *testing.T) {
	IsTesting = true
	defer func() {
		IsTesting = false
	}()
	kvStore = nil
	s, err := getKVStorage()
	require.NoError(t, err)
	require.NoError(t, s.Set("connections.mqtt.abc", map[string]interface{}{
		"a": "b",
	}))
	got, err := GetAllConnConfigs()
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"a": "b",
	}, got["mqtt"]["abc"])
}
