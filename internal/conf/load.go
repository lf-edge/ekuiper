// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/replace"
)

var LoadConfigCache map[string]map[string]interface{}

func init() {
	LoadConfigCache = make(map[string]map[string]interface{})
}

func clearLoadConfigCache() {
	LoadConfigCache = make(map[string]map[string]interface{})
}

const Separator = "__"

func LoadConfig(c interface{}) error {
	return LoadConfigByName(ConfFileName, c)
}

func LoadConfigByName(name string, c interface{}) error {
	dir, err := GetConfLoc()
	if err != nil {
		return err
	}
	p := filepath.Join(dir, name)
	return LoadConfigFromPath(p, c)
}

func LoadConfigFromPath(p string, c interface{}) error {
	if cache, ok := LoadConfigCache[p]; ok {
		return cast.MapToStruct(cache, c)
	}
	prefix := getPrefix(p)
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	configMap := make(map[string]interface{})
	err = yaml.Unmarshal(b, &configMap)
	if err != nil {
		return err
	}
	// Make all keys to lowercase to match environment variables then revert it back by checking json defs
	configs := normalize(configMap)
	err = process(configs, GetEnv(), prefix, p)
	if err != nil {
		return err
	}
	// checking json keys
	switch c.(type) {
	case *map[string]interface{}, *map[string]map[string]interface{}:
		names, err := extractKeysFromJsonIfExists(p)
		if err != nil {
			return err
		}
		applyKeys(configs, names)
	}
	LoadConfigCache[p] = configs
	return cast.MapToStruct(configs, c)
}

func CorrectsConfigKeysByJson(configs map[string]interface{}, jsonFilePath string) error {
	dir, err := GetConfLoc()
	if err != nil {
		return err
	}
	path := filepath.Join(dir, jsonFilePath)
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
	_, file := filepath.Split(p)
	return strings.ToUpper(strings.TrimSuffix(file, filepath.Ext(file)))
}

func process(configMap map[string]interface{}, env map[string]string, prefix string, yamlPath string) error {
	fileTypes := extractTypesFromJsonIfExists(yamlPath)
	for key, value := range env {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		keys := nameToKeys(trimPrefix(key, prefix))
		handle(configMap, keys, value, fileTypes)
		printableK := strings.Join(keys, ".")
		printableV := value
		if isSensitiveKey(printableK) || strings.Contains(strings.ToLower(printableK), "kuiper_props") {
			printableV = "***"
		}
		Log.Infof("Set config '%s.%s' to '%s' by environment variable", strings.ToLower(prefix), printableK, printableV)
	}
	return nil
}

func isSensitiveKey(key string) bool {
	return replace.IsSensitiveKey(strings.ToLower(key))
}

func handle(conf map[string]interface{}, keysLeft []string, val string, types map[string]string) {
	key := getConfigKey(keysLeft[0])
	if len(keysLeft) == 1 {
		conf[key] = getValueType(val, types[key])
	} else if len(keysLeft) >= 2 {
		if v, ok := conf[key]; ok {
			if casted, castSuccess := v.(map[string]interface{}); castSuccess {
				handle(casted, keysLeft[1:], val, types)
			} else {
				panic("not expected type")
			}
		} else {
			next := make(map[string]interface{})
			conf[key] = next
			handle(next, keysLeft[1:], val, types)
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

func getValueType(val string, schemaType string) interface{} {
	val = strings.Trim(val, " ")
	switch strings.ToLower(schemaType) {
	case "string", "text":
		return val
	case "int", "int64", "uint", "uint8":
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
		return val
	case "bool", "boolean":
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
		return val
	case "float", "float64", "number":
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
		return val
	case "list_string":
		return parseEnvList(val, true)
	default:
		return inferValueType(val)
	}
}

func inferValueType(val string) interface{} {
	if strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]") {
		return parseEnvList(val, false)
	} else if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		return i
	} else if b, err := strconv.ParseBool(val); err == nil {
		return b
	} else if f, err := strconv.ParseFloat(val, 64); err == nil {
		return f
	}
	return val
}

func parseEnvList(val string, keepString bool) interface{} {
	if !strings.HasPrefix(val, "[") || !strings.HasSuffix(val, "]") {
		return val
	}
	val = strings.ReplaceAll(val, "[", "")
	val = strings.ReplaceAll(val, "]", "")
	vals := strings.Split(val, ",")
	ret := make([]interface{}, 0, len(vals))
	for _, v := range vals {
		if keepString {
			ret = append(ret, v)
			continue
		}
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
}

func extractTypesFromJsonIfExists(yamlPath string) map[string]string {
	return extractTypesFromJsonFile(jsonPathForFile(yamlPath))
}

func extractTypesFromJsonFile(jsonFilePath string) map[string]string {
	types := make(map[string]string)
	if jsonFilePath == "" {
		return types
	}
	if _, err := os.Stat(jsonFilePath); err != nil {
		return types
	}
	m, err := loadJsonForYaml(jsonFilePath)
	if err != nil {
		return types
	}
	extractTypesFromValue(m, types)
	return types
}

func extractTypesFromValue(v interface{}, types map[string]string) {
	switch t := v.(type) {
	case map[string]interface{}:
		name, nameOk := t["name"].(string)
		typ, typeOk := t["type"].(string)
		if nameOk && typeOk && name != "" && typ != "" {
			types[strings.ToLower(name)] = strings.ToLower(typ)
		}
		for _, child := range t {
			extractTypesFromValue(child, types)
		}
	case []interface{}:
		for _, child := range t {
			extractTypesFromValue(child, types)
		}
	}
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
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(data, &m)
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
	switch lt := list.(type) {
	case []interface{}:
		if len(lt) != 0 {
			for _, element := range lt {
				if m, isMap := element.(map[string]interface{}); isMap {
					re := extractNamesFromElement(m)
					result = append(result, re...)
				}
			}
			return result
		}
	case map[string]interface{}:
		if len(lt) != 0 {
			for _, element := range lt {
				if m, isMap := element.(map[string]interface{}); isMap {
					re := extractNamesFromElement(m)
					result = append(result, re...)
				}
			}
			return result
		}
	}
	// If not a list/map, or an empty list/map, then it's a single element
	n := jsonMap["name"]
	if s, isString := n.(string); isString {
		result = append(result, s)
	}
	return result
}

func Printable(m map[string]interface{}) map[string]interface{} {
	return replace.HidePassword(m)
}
