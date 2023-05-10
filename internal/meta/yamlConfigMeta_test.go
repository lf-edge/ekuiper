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
	"os"
	"path/filepath"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
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

	//Exist ConfigKey , fail
	err = AddSourceConfKey(plgName, "new", "en_US", []byte(addData))
	if err != nil {
		t.Error("should overwrite exist config key")
	}
}
