// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

// ComputeConnectionYamlHash reads the effective connection.yaml (file + env overrides)
// without using the global cache, then returns the hex-encoded sha256 of its stable JSON form.
func ComputeConnectionYamlHash() (string, error) {
	confDir, err := GetConfLoc()
	if err != nil {
		return "", fmt.Errorf("get conf loc: %w", err)
	}
	yamlPath := filepath.Join(confDir, "connections/connection.yaml")

	b, err := os.ReadFile(yamlPath)
	if err != nil {
		if os.IsNotExist(err) {
			return stableHash(map[string]interface{}{})
		}
		return "", fmt.Errorf("read connection.yaml: %w", err)
	}

	configMap := make(map[string]interface{})
	if err := yaml.Unmarshal(b, &configMap); err != nil {
		return "", fmt.Errorf("parse connection.yaml: %w", err)
	}
	configs := normalize(configMap)
	prefix := getPrefix(yamlPath)
	_ = process(configs, GetEnv(), prefix)
	names, _ := extractKeysFromJsonIfExists(yamlPath)
	applyKeys(configs, names)

	return stableHash(configs)
}

func stableHash(m map[string]interface{}) (string, error) {
	b, err := stableJSON(m)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h), nil
}

// stableJSON produces a deterministic JSON encoding: sorted keys, no extra whitespace.
func stableJSON(m map[string]interface{}) ([]byte, error) {
	sm := stableMap(m)
	return json.Marshal(sm)
}

func stableMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sm := make(map[string]interface{}, len(m))
	for _, k := range keys {
		sm[k] = stableValue(m[k])
	}
	return sm
}

func stableValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		return stableMap(val)
	default:
		return val
	}
}
