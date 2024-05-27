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

package bump

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
)

func TestBumpVersion(t *testing.T) {
	conf.InitConf()
	data, err := conf.GetDataLoc()
	if err != nil {
		t.Error(err)
	}
	err = store.SetupDefault(data)
	if err != nil {
		t.Error(err)
	}
	require.NoError(t, InitBumpManager())
	GlobalBumpManager.Version = 0
	testBumpVersion1(t)
}

func testBumpVersion1(t *testing.T) {
	dir := os.TempDir()
	prepareBumpVersion1Data(t, dir, "sources")
	prepareBumpVersion1Data(t, dir, "sinks")
	prepareBumpVersion1Data(t, dir, "connections")
	err := bumpFrom0To1(dir)
	require.NoError(t, err)
	require.Equal(t, 1, GlobalBumpManager.Version)
	v, err := loadVersionFromStorage()
	require.NoError(t, err)
	require.Equal(t, 1, v)
	assertBumpVersion1Data(t, "sources")
	assertBumpVersion1Data(t, "sinks")
	assertBumpVersion1Data(t, "connections")
}

func assertBumpVersion1Data(t *testing.T, typ string) {
	d, err := conf.GotCfgFromKVStorage(typ, "mqtt", "conf1")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"p1": 1,
	}, d)
	d, err = conf.GotCfgFromKVStorage(typ, "mqtt", "conf2")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"p2": 2,
	}, d)
}

func prepareBumpVersion1Data(t *testing.T, dir, typ string) {
	sourceDir := filepath.Join(dir, typ)
	require.NoError(t, os.MkdirAll(sourceDir, 0o755))
	file, err := os.Create(filepath.Join(sourceDir, "mqtt.yaml"))
	require.NoError(t, err)
	defer file.Close()
	m := make(map[string]map[string]interface{})
	m["conf1"] = map[string]interface{}{
		"p1": 1,
	}
	m["conf2"] = map[string]interface{}{
		"p2": 2,
	}
	d, err := yaml.Marshal(m)
	require.NoError(t, err)
	_, err = file.Write(d)
	require.NoError(t, err)
}
