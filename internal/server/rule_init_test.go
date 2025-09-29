// Copyright 2025 EMQ Technologies Co., Ltd.
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

package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func TestInitRuleSet(t *testing.T) {
	testx.InitEnv("ruleinit")
	defer testx.InitEnv("server")
	etcDir, err := conf.GetConfLoc()
	require.NoError(t, err)
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	defer func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.RemoveAll(dataDir)
	}()
	// Put init files
	bytesRead, err := os.ReadFile("test/etc_init.json")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(etcDir, "init.json"), bytesRead, 0o755)
	require.NoError(t, err)
	defer func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.RemoveAll(filepath.Join(etcDir, "init.json"))
	}()
	bytesRead, err = os.ReadFile("test/data_init.json")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dataDir, "init.json"), bytesRead, 0o755)
	require.NoError(t, err)
	// Run init
	initRuleset()
	rules, err := registry.GetAllRulesWithStatus()
	require.NoError(t, err)
	exp := []map[string]interface{}{{"id": "rrr1", "name": "very new version", "status": "error: Rule rrr1 is not found in registry", "tags": []string(nil), "trace": false, "version": "1742965676"}, {"id": "rrr2", "name": "older version", "status": "error: Rule rrr2 is not found in registry", "tags": []string(nil), "trace": false, "version": "1742865676"}, {"id": "ruleData", "name": "only in etc", "status": "error: Rule ruleData is not found in registry", "tags": []string(nil), "trace": false, "version": ""}, {"id": "ruleEtc", "name": "only in etc", "status": "error: Rule ruleEtc is not found in registry", "tags": []string(nil), "trace": false, "version": ""}}
	require.Equal(t, exp, rules)
	// Simulate OTA update
	bytesRead, err = os.ReadFile("test/etc_init2.json")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(etcDir, "init.json"), bytesRead, 0o755)
	require.NoError(t, err)
	initRuleset()
	rules, err = registry.GetAllRulesWithStatus()
	require.NoError(t, err)
	exp = []map[string]any{{"id": "rrr1", "name": "very new version", "status": "error: Rule rrr1 is not found in registry", "tags": []string(nil), "trace": false, "version": "1742965676"}, {"id": "rrr2", "name": "even newer version", "status": "error: Rule rrr2 is not found in registry", "tags": []string(nil), "trace": false, "version": "1742960676"}, {"id": "ruleData", "name": "only in etc", "status": "error: Rule ruleData is not found in registry", "tags": []string(nil), "trace": false, "version": ""}, {"id": "ruleEtc", "name": "only in etc", "status": "error: Rule ruleEtc is not found in registry", "tags": []string(nil), "trace": false, "version": ""}}
	require.Equal(t, exp, rules)
}
