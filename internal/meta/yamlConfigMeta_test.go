// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"fmt"
	"sync"
	"testing"
)

type MockSourcesConfigOps struct {
	*ConfigKeys
}

func (m MockSourcesConfigOps) IsSource() bool {
	return true
}

func (m MockSourcesConfigOps) SaveCfgToFile() error {
	return nil
}

func TestYamlConfigMeta_Ops(t *testing.T) {
	plgName := "mocksource"

	yamlKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, plgName)
	addData := `{"url":"127.0.0.1","method":"post","headers":{"Accept":"json"}}`

	ConfigManager.cfgOperators[yamlKey] = MockSourcesConfigOps{
		&ConfigKeys{
			sync.RWMutex{},
			plgName,
			make(map[string]map[string]interface{}),
		},
	}

	// init new ConfigOperator, success
	err := AddSourceConfKey(plgName, "new", "en_US", []byte(addData))
	if err != nil {
		t.Error(err)
	}

	//Exist ConfigKey , fail
	err = AddSourceConfKey(plgName, "new", "en_US", []byte(addData))
	if err == nil {
		t.Error("should return error when overwrite exist config key")
	}

	addData1 := `{"interval":10000, "timeout":200}`

	// no exist key, fail
	noExistKey := "noexist"
	err = AddSourceConfKeyField(noExistKey, "new", "en_US", []byte(addData1))
	if err == nil {
		t.Error("should return error when no exist key")
	}

	// exist key, success
	ExistKey := plgName
	err = AddSourceConfKeyField(ExistKey, "new", "en_US", []byte(addData1))
	if err != nil {
		t.Error(err)
	}

	// exist key, success
	err = DelSourceConfKeyField(ExistKey, "new", "en_US", []byte(addData1))
	if err != nil {
		t.Error(err)
	}

	// exist key, success
	err = DelSourceConfKey(ExistKey, "new", "en_US")
	if err != nil {
		t.Error(err)
	}
}
