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

package conf

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConfigKeys_LoadSourceFile(t *testing.T) {

	mqttCfg, err := NewConfigOperatorFromSourceYaml("mqtt")
	if err != nil {
		t.Error(err)
	}

	expect := mqttCfg.IsSource()
	if expect != true {
		t.Error(expect)
	}
}

func TestConfigKeys_LoadConnectionMqtt(t *testing.T) {
	mqttCfg, err := NewConfigOperatorFromConnectionYaml("mqtt")
	if err != nil {
		t.Error(err)
	}

	actual := mqttCfg.IsSource()
	if actual != false {
		t.Errorf("should be false, but actual is %v", actual)
	}
}

func TestConfigKeys_LoadConnectionEdgex(t *testing.T) {
	edgeXCfg, err := NewConfigOperatorFromConnectionYaml("edgex")
	if err != nil {
		t.Error(err)
	}

	actual := edgeXCfg.IsSource()
	if actual != false {
		t.Errorf("should be false, but actual is %v", actual)
	}
}

func TestConfigKeys_Ops(t *testing.T) {

	httpCfg, err := NewConfigOperatorFromSourceYaml("httppull")
	if err != nil {
		t.Error(err)
	}

	addData := `{"url":"127.0.0.1","method":"post","headers":{"Accept":"json"}}`
	delData := `{"method":"","headers":{"Accept":""}}`

	reqField := make(map[string]interface{})
	_ = json.Unmarshal([]byte(addData), &reqField)

	err = httpCfg.AddConfKey("new", reqField)
	if err != nil {
		t.Error(err)
	}

	if err := isAddData(addData, httpCfg.CopyConfContent()[`new`]); nil != err {
		t.Error(err)
	}

	delField := make(map[string]interface{})
	_ = json.Unmarshal([]byte(delData), &delField)

	err = httpCfg.DeleteConfKeyField("new", delField)
	if err != nil {
		t.Error(err)
	}

	if err := isDelData(delData, httpCfg.CopyConfContent()[`new`]); nil != err {
		t.Error(err)
	}

}

func marshalUn(input, output interface{}) error {
	jsonString, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonString, output)
}

func isDelData(js string, cf map[string]interface{}) error {
	var delNode map[string]interface{}
	if err := json.Unmarshal([]byte(js), &delNode); nil != err {
		return err
	}
	for delk, delv := range delNode {
		if nil == delv {
			if _, ok := cf[delk]; ok {
				return fmt.Errorf("%s still exists", delk)
			}
		}

		switch t := delv.(type) {
		case string:
			if 0 == len(t) {
				if _, ok := cf[delk]; ok {
					return fmt.Errorf("%s still exists", delk)
				}
			}
		case map[string]interface{}:
			if b, err := json.Marshal(t); nil != err {
				return fmt.Errorf("request format error")
			} else {
				var auxCf map[string]interface{}
				if err := marshalUn(cf[delk], &auxCf); nil == err {
					if err := isDelData(string(b), auxCf); nil != err {
						return err
					}
				}
			}
		}

	}
	return nil
}
func isAddData(js string, cf map[string]interface{}) error {
	var addNode map[string]interface{}
	if err := json.Unmarshal([]byte(js), &addNode); nil != err {
		return err
	}
	for addk := range addNode {
		if _, ok := cf[addk]; !ok {
			return fmt.Errorf("not found key:%s", addk)
		}
	}
	return nil
}
