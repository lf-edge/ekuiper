// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestErrors(t *testing.T) {
	// update invalid rule
	err := registry.UpsertRule("test", "selectabc")
	assert.EqualError(t, err, "Invalid rule json: Parse rule selectabc error : invalid character 's' looking for beginning of value.")
	err = registry.UpsertRule("test", `{"id":"test","sql":"SELECT * FROM demo","actions":[{"log":{}}]}`)
	assert.EqualError(t, err, "fail to get stream demo, please check if stream is created")
	// delete rule, no id
	err = registry.DeleteRule("test")
	assert.EqualError(t, err, "rule test not found")
	// restart rule, no id
	err = registry.RestartRule("test")
	assert.EqualError(t, err, "Rule test is not found in registry, please check if it is created")
	_, err = registry.GetRuleStatus("test")
	assert.EqualError(t, err, "Rule test is not found")
	_, err = registry.GetRuleStatusV2("test")
	assert.EqualError(t, err, "Rule test is not found")
	_, err = registry.GetRuleTopo("test")
	assert.EqualError(t, err, "Rule test is not found")
	err = registry.scheduledStart("test")
	assert.EqualError(t, err, "Scheduled rule test is not found in registry, please check if it is deleted")
	err = registry.scheduledStop("test")
	assert.EqualError(t, err, "Scheduled rule test is not found in registry, please check if it is deleted")
	err = registry.stopAtExit("test", "")
	assert.EqualError(t, err, "Rule test is not found in registry, please check if it is deleted")
	// db problems
	registry.register("test", rule.NewState(def.GetDefaultRule("testErrors", "select * from demo"), func(string, bool) {}))
	err = registry.StartRule("test")
	assert.EqualError(t, err, "fail to get stream demo, please check if stream is created")
}

// TestDeleteRuleValidateID ensures that DeleteRule rejects invalid rule ids (path
// traversal, special characters, empty) before any registry, DB or filesystem
// mutation happens.
func TestDeleteRuleValidateID(t *testing.T) {
	invalidIDs := []string{
		"../../target",
		"..",
		"foo/bar",
		"a.b",
		"a b",
	}
	for _, id := range invalidIDs {
		err := registry.DeleteRule(id)
		require.Error(t, err, "expected validation error for id %q", id)
	}
	// Empty id has its own message but must still be rejected.
	require.Error(t, registry.DeleteRule(""))

	// A path traversal id must report invalid characters.
	err := registry.DeleteRule("../../target")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid characters")

	// The rejected id must not have mutated the in-memory registry.
	_, ok := registry.load("../../target")
	require.False(t, ok)
}

// TestDeleteRuleMaliciousIDNoSideEffect reproduces the reporter's attack vector:
// a malicious rule id reaching deleteRuleData, which joins the id onto the data
// location and calls os.RemoveAll. The fix must reject the id before any
// filesystem operation, so the sentinel file survives.
func TestDeleteRuleMaliciousIDNoSideEffect(t *testing.T) {
	dataLoc, err := conf.GetDataLoc()
	require.NoError(t, err)

	// "rule_" + this id traverses one level (the ".." cancels "rule_.."), so
	// deleteRuleData would resolve to dataLoc/<sentinelDir> without validation.
	maliciousName := "../../malicious_delete_target"
	targetPath := filepath.Join(dataLoc, "rule_"+maliciousName)
	require.NoError(t, os.MkdirAll(targetPath, 0o755))
	sentinel := filepath.Join(targetPath, "sentinel.txt")
	require.NoError(t, os.WriteFile(sentinel, []byte("sentinel"), 0o644))
	defer os.RemoveAll(targetPath)

	err = registry.DeleteRule(maliciousName)
	require.Error(t, err)

	// The sentinel must be intact: no filesystem side effect occurred.
	data, statErr := os.ReadFile(sentinel)
	require.NoError(t, statErr)
	require.Equal(t, "sentinel", string(data))
}

// TestDeleteRuleValidID ensures that the new validation does not break the
// normal deletion path for well-formed ids, including the original "not found"
// semantics for absent rules.
func TestDeleteRuleValidID(t *testing.T) {
	streamSQL := `CREATE STREAM valid_delete_stream () WITH (DATASOURCE="valid_delete_stream", TYPE="mqtt")`
	_, err := streamProcessor.ExecStreamSql(streamSQL)
	require.NoError(t, err)
	defer streamProcessor.DropStream("valid_delete_stream", ast.TypeStream)

	ruleID := "valid_delete_rule"
	ruleJson := `{"trigger":false,"id":"valid_delete_rule","sql":"select * from valid_delete_stream","actions":[{"log":{}}]}`
	_, err = registry.CreateRule(ruleID, ruleJson)
	require.NoError(t, err)
	_, ok := registry.load(ruleID)
	require.True(t, ok)

	// A valid id must pass validation and delete the rule from registry and DB.
	require.NoError(t, registry.DeleteRule(ruleID))
	_, ok = registry.load(ruleID)
	require.False(t, ok)
	_, err = ruleProcessor.GetRuleById(ruleID)
	require.Error(t, err) // gone from KV/DB as well

	// Deleting a non-existent rule keeps the original "not found" semantics.
	err = registry.DeleteRule(ruleID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestCoverage(t *testing.T) {
	// Setup
	sql := "CREATE STREAM demo2 () WITH (DATASOURCE=\"demo2\", TYPE=\"mqtt\")"
	_, err := streamProcessor.ExecStreamSql(sql)
	assert.NoError(t, err)
	defer streamProcessor.DropStream("demo2", ast.TypeStream)

	// 1. Temp Rule Coverage (New Temp Rule)
	// Upsert a temp rule
	ruleJson := `{"id": "tempRule1", "sql": "select * from demo2", "actions": [{"log":{}}], "temp": true}`
	err = registry.UpsertRule("tempRule1", ruleJson)
	assert.NoError(t, err)

	// Check in memory
	_, ok := registry.load("tempRule1")
	assert.True(t, ok)

	// Check NOT in DB
	_, err = ruleProcessor.GetRuleById("tempRule1")
	assert.Error(t, err) // Should be not found in DB

	// Cleanup temp rule
	registry.DeleteRule("tempRule1")

	// 2. Save Fail Coverage (DB persistence error)
	// Manually insert a rule into DB
	ruleJson2 := `{"id": "dupRule", "sql": "select * from demo2", "actions": [{"log":{}}]}`
	err = ruleProcessor.ExecCreate("dupRule", ruleJson2)
	assert.NoError(t, err)
	defer ruleProcessor.ExecDrop("dupRule")

	// Try to CreateRule
	// It checks memory first (not found), then creates state, then calls save -> ExecCreate
	// ExecCreate should fail because it's already in DB
	id, err := registry.CreateRule("dupRule", ruleJson2)
	assert.Error(t, err)
	// The error message depends on the KV store implementation, usually "Item ... already exists"
	// But let's just assert error for now
	assert.Equal(t, "dupRule", id)
}
