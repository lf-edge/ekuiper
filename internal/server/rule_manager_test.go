// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"
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

func TestUpsertPersistenceFailureIsNoOp(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM updateTransactionDemo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM updateTransactionDemo`)

	tests := []struct {
		name       string
		id         string
		oldTrigger bool
		newTrigger bool
		oldState   machine.RunState
	}{
		{name: "running rule", id: "persistFailureRunning", oldTrigger: true, newTrigger: false, oldState: machine.Running},
		{name: "stopped rule", id: "persistFailureStopped", oldTrigger: false, newTrigger: true, oldState: machine.Stopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := &RuleRegistry{internal: make(map[string]*rule.State)}
			oldJSON := fmt.Sprintf(`{"id":%q,"triggered":%t,"sql":"select * from updateTransactionDemo","actions":[{"log":{}}]}`, tt.id, tt.oldTrigger)
			newJSON := fmt.Sprintf(`{"id":%q,"triggered":%t,"sql":"select * from updateTransactionDemo where true","actions":[{"log":{}}]}`, tt.id, tt.newTrigger)
			_, err := rr.CreateRule(tt.id, oldJSON)
			require.NoError(t, err)
			defer rr.DeleteRule(tt.id)

			rs, ok := rr.load(tt.id)
			require.True(t, ok)
			oldRule := rs.GetRule()
			oldTopo, oldTopoErr := rs.GetPlainTopology()
			storedBefore, err := ruleProcessor.GetRuleJson(tt.id)
			require.NoError(t, err)

			err = rr.upsertRule(tt.id, newJSON, func(string, string) error {
				return errors.New("mock persistence failure")
			})
			require.EqualError(t, err, "mock persistence failure")
			require.Same(t, oldRule, rs.GetRule())
			require.Equal(t, tt.oldState, rs.GetState())
			currentTopo, currentTopoErr := rs.GetPlainTopology()
			if oldTopoErr == nil {
				require.NoError(t, currentTopoErr)
				require.Same(t, oldTopo, currentTopo)
			} else {
				require.Error(t, currentTopoErr)
			}
			storedAfter, err := ruleProcessor.GetRuleJson(tt.id)
			require.NoError(t, err)
			require.Equal(t, storedBefore, storedAfter)
		})
	}
}

func TestConcurrentUpsertContinuesAfterPersistenceFailure(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM updateConcurrentDemo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM updateConcurrentDemo`)

	const ruleID = "concurrentPersistFailure"
	rr := &RuleRegistry{internal: make(map[string]*rule.State)}
	oldJSON := fmt.Sprintf(`{"id":%q,"version":"1.0.0","triggered":false,"sql":"select * from updateConcurrentDemo","actions":[{"log":{}}]}`, ruleID)
	_, err = rr.CreateRule(ruleID, oldJSON)
	require.NoError(t, err)
	defer rr.DeleteRule(ruleID)

	highJSON := fmt.Sprintf(`{"id":%q,"version":"3.0.0","triggered":false,"sql":"select * from updateConcurrentDemo where true","actions":[{"log":{}}]}`, ruleID)
	lowerJSON := fmt.Sprintf(`{"id":%q,"version":"2.0.0","triggered":false,"sql":"select * from updateConcurrentDemo where true","actions":[{"log":{}}]}`, ruleID)
	highCommitStarted := make(chan struct{})
	allowHighCommitFailure := make(chan struct{})
	highResult := make(chan error, 1)
	go func() {
		highResult <- rr.upsertRule(ruleID, highJSON, func(string, string) error {
			close(highCommitStarted)
			<-allowHighCommitFailure
			return errors.New("mock high-version persistence failure")
		})
	}()
	<-highCommitStarted

	lowerStarted := make(chan struct{})
	lowerResult := make(chan error, 1)
	go func() {
		close(lowerStarted)
		lowerResult <- rr.upsertRule(ruleID, lowerJSON, ruleProcessor.ExecUpsert)
	}()
	<-lowerStarted
	close(allowHighCommitFailure)

	require.EqualError(t, <-highResult, "mock high-version persistence failure")
	require.NoError(t, <-lowerResult)
	rs, ok := rr.load(ruleID)
	require.True(t, ok)
	require.Equal(t, "2.0.0", rs.GetRule().Version)
	storedRule, err := ruleProcessor.GetRuleById(ruleID)
	require.NoError(t, err)
	require.Equal(t, "2.0.0", storedRule.Version)
}

func TestUpsertRejectsTempModeChange(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM updateTempDemo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM updateTempDemo`)

	tests := []struct {
		name    string
		id      string
		oldTemp bool
		newTemp bool
	}{
		{name: "persistent to temporary", id: "persistToTemp", oldTemp: false, newTemp: true},
		{name: "temporary to persistent", id: "tempToPersist", oldTemp: true, newTemp: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := &RuleRegistry{internal: make(map[string]*rule.State)}
			oldJSON := fmt.Sprintf(`{"id":%q,"temp":%t,"sql":"select * from updateTempDemo","actions":[{"log":{}}]}`, tt.id, tt.oldTemp)
			newJSON := fmt.Sprintf(`{"id":%q,"temp":%t,"sql":"select * from updateTempDemo where true","actions":[{"log":{}}]}`, tt.id, tt.newTemp)
			_, err := rr.CreateRule(tt.id, oldJSON)
			require.NoError(t, err)
			defer rr.DeleteRule(tt.id)

			rs, ok := rr.load(tt.id)
			require.True(t, ok)
			oldRule := rs.GetRule()
			err = rr.UpsertRule(tt.id, newJSON)
			require.EqualError(t, err, fmt.Sprintf("rule %s cannot change temp from %t to %t; delete and recreate the rule", tt.id, tt.oldTemp, tt.newTemp))
			require.Same(t, oldRule, rs.GetRule())
		})
	}
}
