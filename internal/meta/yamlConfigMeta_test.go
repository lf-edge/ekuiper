// Copyright 2022 EMQ Technologies Co., Ltd.
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

package meta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

func init() {
	InitYamlConfigManager()
}

func createPaths() {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	dirs := []string{"sources", "sinks", "functions", "services", "services/schemas", "connections"}

	for _, v := range dirs {
		// Create dir if not exist
		realDir := filepath.Join(dataDir, v)
		if _, err := os.Stat(realDir); os.IsNotExist(err) {
			if err := os.MkdirAll(realDir, os.ModePerm); err != nil {
				panic(err)
			}
		}
	}

	files := []string{"connections/connection.yaml"}
	for _, v := range files {
		// Create dir if not exist
		realFile := filepath.Join(dataDir, v)
		if _, err := os.Stat(realFile); os.IsNotExist(err) {
			if _, err := os.Create(realFile); err != nil {
				panic(err)
			}
		}
	}
}

func TestYamlConfigMeta_Ops(t *testing.T) {
	createPaths()

	plgName := "mocksource"
	addData := `{"url":"127.0.0.1","method":"post","headers":{"Accept":"json"}}`
	// init new ConfigOperator, success
	err := AddSourceConfKey(plgName, "new", "en_US", []byte(addData))
	if err != nil {
		t.Error(err)
	}

	// Exist ConfigKey , fail
	err = AddSourceConfKey(plgName, "new", "en_US", []byte(addData))
	if err != nil {
		t.Error("should overwrite exist config key")
	}
}

func TestConfKeyReplace(t *testing.T) {
	a1 := map[string]interface{}{
		"url": "mysql://root:abcd@127.0.0.1:3306/test",
		"key": "value",
	}
	bs, err := json.Marshal(a1)
	require.NoError(t, err)
	err = AddSinkConfKey("sql", "mysql", "", bs)
	require.NoError(t, err)
	a2 := map[string]interface{}{
		"url": "mysql://root:******@127.0.0.1:3306/test",
		"key": "value",
	}
	replaced := replacePasswdForConfig("sink", "sql", "mysql", a2)
	require.Equal(t, a1, replaced)

	err = AddSourceConfKey("sql", "mysql", "", bs)
	require.NoError(t, err)
	replaced = replacePasswdForConfig("source", "sql", "mysql", a2)
	require.Equal(t, a1, replaced)
}

func TestConfKeyErr(t *testing.T) {
	err := DelSourceConfKey("1", "2", "3")
	require.Error(t, err)
	ewc, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	err = DelSinkConfKey("1", "2", "3")
	require.Error(t, err)
	ewc, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	err = DelConnectionConfKey("1", "2", "3")
	require.Error(t, err)
	ewc, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	_, err = GetYamlConf("1", "2")
	require.Error(t, err)
	ewc, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())

	err = AddSourceConfKey("1", "2", "3", nil)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	err = AddSinkConfKey("1", "2", "3", nil)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
}
