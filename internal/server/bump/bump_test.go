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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
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
	testBumpVersion(t, currentVersion)
}

func prepareBumpVersion(t *testing.T, dir string) {
	prepareBumpVersion1Data(t, dir, "sources")
	prepareBumpVersion1Data(t, dir, "sinks")
	prepareBumpVersion1Data(t, dir, "connections")
}

func testBumpVersion(t *testing.T, expectVer int) {
	dir := os.TempDir()
	prepareBumpVersion(t, dir)
	require.NoError(t, BumpToCurrentVersion(dir))
	require.Equal(t, expectVer, GlobalBumpManager.Version)
	v, err := loadVersionFromStorage()
	require.NoError(t, err)
	require.Equal(t, expectVer, v)
	assertBumpVersion1Data(t, "sources")
	assertBumpVersion1Data(t, "sinks")
	assertBumpVersion1Data(t, "connections")
}

func assertBumpVersion1Data(t *testing.T, typ string) {
	d, err := conf.GetCfgFromKVStorage(typ, "mqtt", "conf1")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"p1": 1,
	}, d[fmt.Sprintf("%s.mqtt.conf1", typ)])
	d, err = conf.GetCfgFromKVStorage(typ, "mqtt", "conf2")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"p2": 2,
	}, d[fmt.Sprintf("%s.mqtt.conf2", typ)])
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

func TestBumpManager0To1(t *testing.T) {
	conf.InitConf()
	data, err := conf.GetDataLoc()
	if err != nil {
		t.Error(err)
	}
	err = store.SetupDefault(data)
	if err != nil {
		t.Error(err)
	}

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/initManagerError", "return(true)")
	require.Error(t, InitBumpManager())
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/bump/initManagerError")
	require.NoError(t, InitBumpManager())

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/loadVersionError", "return(true)")
	_, err = loadVersionFromStorage()
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/bump/loadVersionError")

	dir := os.TempDir()
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateReadError", "return(1)")
	require.Error(t, bumpFrom0To1(dir))
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateReadError", "return(2)")
	require.Error(t, bumpFrom0To1(dir))
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateReadError", "return(3)")
	require.Error(t, bumpFrom0To1(dir))
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateReadError")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateUnmarshalErr", "return(true)")
	require.Error(t, bumpFrom0To1(dir))
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateUnmarshalErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateWriteErr", "return(true)")
	require.Error(t, bumpFrom0To1(dir))
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/bump/migrateWriteErr")
}
