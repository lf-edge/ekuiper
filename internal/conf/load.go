// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/message"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const Separator = "__"

func LoadConfig(c interface{}) error {
	return LoadConfigByName(ConfFileName, c)
}

func LoadConfigByName(name string, c interface{}) error {
	dir, err := GetConfLoc()
	if err != nil {
		return err
	}
	p := path.Join(dir, name)
	return LoadConfigFromPath(p, c)
}

func LoadConfigFromPath(p string, c interface{}) error {
	prefix := getPrefix(p)
	b, err := ioutil.ReadFile(p)
	if err != nil {
		return err
	}
	configMap := make(map[string]interface{})
	err = yaml.Unmarshal(b, &configMap)
	if err != nil {
		return err
	}
	configs := normalize(configMap)
	err = process(configs, os.Environ(), prefix)
	if err != nil {
		return err
	}
	if _, success := c.(*map[string]interface{}); success {
		names, err := extractKeysFromJsonIfExists(p)
		if err != nil {
			return err
		}
		applyKeys(configs, names)
	}
	return mapstructure.Decode(configs, c)
}

func CorrectsConfigKeysByJson(configs map[string]interface{}, jsonFilePath string) error {
	dir, err := GetConfLoc()
	if err != nil {
		return err
	}
	path := path.Join(dir, jsonFilePath)
	m, err := loadJsonForYaml(path)
	if err != nil {
		return err
	}
	names, err := extractNamesFromProperties(m)
	if err != nil {
		return err
	}

	applyKeys(configs, names)

	return nil
}

func getPrefix(p string) string {
	_, file := path.Split(p)
	return strings.ToUpper(strings.TrimSuffix(file, filepath.Ext(file)))
}

func process(configMap map[string]interface{}, variables []string, prefix string) error {
	for _, e := range variables {
		if !strings.HasPrefix(e, prefix) {
			continue
		}
		pair := strings.SplitN(e, "=", 2)
		if len(pair) != 2 {
			return fmt.Errorf("wrong format of variable")
		}
		keys := nameToKeys(trimPrefix(pair[0], prefix))
		handle(configMap, keys, pair[1])
		printableK := strings.Join(keys, ".")
		printableV := pair[1]
		if strings.Contains(strings.ToLower(printableK), "password") {
			printableV = "*"
		}
		Log.Infof("Set config '%s.%s' to '%s' by environment variable", strings.ToLower(prefix), printableK, printableV)
	}
	return nil
}

func handle(conf map[string]interface{}, keysLeft []string, val string) {
	key := getConfigKey(keysLeft[0])
	if len(keysLeft) == 1 {
		conf[key] = getValueType(val)
	} else if len(keysLeft) >= 2 {
		if v, ok := conf[key]; ok {
			if casted, castSuccess := v.(map[string]interface{}); castSuccess {
				handle(casted, keysLeft[1:], val)
			} else {
				panic("not expected type")
			}
		} else {
			next := make(map[string]interface{})
			conf[key] = next
			handle(next, keysLeft[1:], val)
		}
	}
}

func trimPrefix(key string, prefix string) string {
	p := fmt.Sprintf("%s%s", prefix, Separator)
	return strings.TrimPrefix(key, p)
}

func nameToKeys(key string) []string {
	return strings.Split(strings.ToLower(key), Separator)
}

func getConfigKey(key string) string {
	return strings.ToLower(key)
}

func getValueType(val string) interface{} {
	val = strings.Trim(val, " ")
	if strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]") {
		val = strings.ReplaceAll(val, "[", "")
		val = strings.ReplaceAll(val, "]", "")
		vals := strings.Split(val, ",")
		var ret []interface{}
		for _, v := range vals {
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				ret = append(ret, i)
			} else if b, err := strconv.ParseBool(v); err == nil {
				ret = append(ret, b)
			} else if f, err := strconv.ParseFloat(v, 64); err == nil {
				ret = append(ret, f)
			} else {
				ret = append(ret, v)
			}
		}
		return ret
	} else if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		return i
	} else if b, err := strconv.ParseBool(val); err == nil {
		return b
	} else if f, err := strconv.ParseFloat(val, 64); err == nil {
		return f
	}
	return val
}

func normalize(m map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range m {
		lowered := strings.ToLower(k)
		if casted, success := v.(map[string]interface{}); success {
			node := normalize(casted)
			res[lowered] = node
		} else {
			res[lowered] = v
		}
	}
	return res
}

func applyKeys(m map[string]interface{}, list []string) {
	for _, k := range list {
		applyKey(m, k)
	}
}

func applyKey(m map[string]interface{}, key string) {
	for k, v := range m {
		if casted, ok := v.(map[string]interface{}); ok {
			applyKey(casted, key)
		}
		if key != k && strings.EqualFold(key, k) {
			m[key] = v
			delete(m, k)
		}
	}
}

func extractKeysFromJsonIfExists(yamlPath string) ([]string, error) {
	jsonFilePath := jsonPathForFile(yamlPath)
	_, err := os.Stat(jsonFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make([]string, 0), nil
		} else {
			return nil, err
		}
	}
	m, err := loadJsonForYaml(jsonFilePath)
	if err != nil {
		return nil, err
	}
	return extractNamesFromProperties(m)
}

func loadJsonForYaml(filePath string) (map[string]interface{}, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	err = message.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func jsonPathForFile(yamlPath string) string {
	p := strings.TrimSuffix(yamlPath, filepath.Ext(yamlPath))
	return fmt.Sprintf("%s.json", p)
}

func extractNamesFromProperties(jsonMap map[string]interface{}) ([]string, error) {
	result := make([]string, 0)
	properties, contains := jsonMap["properties"]
	if !contains {
		return nil, fmt.Errorf("json map does not have properties value")
	}
	if propertiesAsMap, success := properties.(map[string]interface{}); success {
		re := extractNamesFromElement(propertiesAsMap)
		result = append(result, re...)
	} else {
		return nil, fmt.Errorf("failed to cast to list of properties")
	}
	return result, nil
}

func extractNamesFromElement(jsonMap map[string]interface{}) []string {
	result := make([]string, 0)
	list := jsonMap["default"]
	if interfaceList, isList := list.([]interface{}); isList {
		for _, element := range interfaceList {
			if m, isMap := element.(map[string]interface{}); isMap {
				re := extractNamesFromElement(m)
				result = append(result, re...)
			}
		}
	} else {
		n := jsonMap["name"]
		if s, isString := n.(string); isString {
			result = append(result, s)
		}
	}

	return result
}

func Printable(m map[string]interface{}) map[string]interface{} {
	printableMap := make(map[string]interface{})
	for k, v := range m {
		if strings.ToLower(k) == "password" {
			printableMap[k] = "***"
		} else {
			if vm, ok := v.(map[string]interface{}); ok {
				printableMap[k] = Printable(vm)
			} else {
				printableMap[k] = v
			}
		}
	}
	return printableMap
}
