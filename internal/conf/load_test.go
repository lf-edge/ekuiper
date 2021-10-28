// INTECH Process Automation Ltd.
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
	"os"
	"reflect"
	"testing"
)

func TestEnv(t *testing.T) {

	key := "KUIPER__BASIC__CONSOLELOG"
	value := "true"

	err := os.Setenv(key, value)
	if err != nil {
		t.Error(err)
	}

	c := KuiperConf{}
	err = LoadConfig(&c)
	if err != nil {
		t.Error(err)
	}

	if c.Basic.ConsoleLog != true {
		t.Errorf("env variable should set it to true")
	}
}

func TestJsonCamelCase(t *testing.T) {
	key := "HTTPPULL__DEFAULT__BODYTYPE"
	value := "event"

	err := os.Setenv(key, value)
	if err != nil {
		t.Error(err)
	}

	const ConfigName = "sources/httppull.yaml"
	c := make(map[string]interface{})
	err = LoadConfigByName(ConfigName, &c)
	if err != nil {
		t.Error(err)
	}

	if casted, success := c["default"].(map[string]interface{}); success {
		if casted["bodyType"] != "event" {
			t.Errorf("env variable should set it to event")
		}
	} else {
		t.Errorf("returned value does not contains map under 'Basic' key")
	}
}

func TestNestedFields(t *testing.T) {
	key := "EDGEX__DEFAULT__OPTIONAL__PASSWORD"
	value := "password"

	err := os.Setenv(key, value)
	if err != nil {
		t.Error(err)
	}

	const ConfigName = "sources/edgex.yaml"
	c := make(map[string]interface{})
	err = LoadConfigByName(ConfigName, &c)
	if err != nil {
		t.Error(err)
	}

	if casted, success := c["default"].(map[string]interface{}); success {
		if optional, ok := casted["optional"].(map[string]interface{}); ok {
			if optional["Password"] != "password" {
				t.Errorf("Password variable should set it to password")
			}
		} else {
			t.Errorf("returned value does not contains map under 'optional' key")
		}
	} else {
		t.Errorf("returned value does not contains map under 'Basic' key")
	}
}

func TestKeysReplacement(t *testing.T) {
	input := createRandomConfigMap()
	expected := createExpectedRandomConfigMap()
	list := []string{"interval", "Seed", "deduplicate", "pattern"}

	applyKeys(input, list)

	if !reflect.DeepEqual(input, expected) {
		t.Errorf("key names within list should be applied \nexpected - %s\n input   - %s", expected, input)
	}
}

func TestKeyReplacement(t *testing.T) {
	m := createRandomConfigMap()
	expected := createExpectedRandomConfigMap()

	applyKey(m, "Seed")
	applyKey(m, "interval")

	if !reflect.DeepEqual(m, expected) {
		t.Errorf("key names within list should be applied \nexpected - %s\nmap      - %s", expected, m)
	}
}

func createRandomConfigMap() map[string]interface{} {
	pattern := make(map[string]interface{})
	pattern["count"] = 50
	defaultM := make(map[string]interface{})
	defaultM["interval"] = 1000
	defaultM["seed"] = 1
	defaultM["pattern"] = pattern
	defaultM["deduplicate"] = 0
	ext := make(map[string]interface{})
	ext["interval"] = 100
	dedup := make(map[string]interface{})
	dedup["interval"] = 100
	dedup["deduplicated"] = 50
	input := make(map[string]interface{})
	input["default"] = defaultM
	input["ext"] = ext
	input["dedup"] = dedup
	return input
}

func createExpectedRandomConfigMap() map[string]interface{} {
	input := createRandomConfigMap()
	def := input["default"]
	if defMap, ok := def.(map[string]interface{}); ok {
		tmp := defMap["seed"]
		delete(defMap, "seed")
		defMap["Seed"] = tmp
	}
	return input
}
