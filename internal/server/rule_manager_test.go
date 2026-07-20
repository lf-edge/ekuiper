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
	"context"
	"errors"
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

func TestLoadRule(t *testing.T) {
	rr := &RuleRegistry{internal: make(map[string]*rule.State)}
	triggered := def.GetDefaultRule("loadedRule", "select * from demo")
	triggered.Triggered = true
	stopped := def.GetDefaultRule("stoppedRule", "select * from demo")
	stopped.Triggered = false

	require.Contains(t, rr.LoadRule(triggered), "pending start")
	require.Contains(t, rr.LoadRule(stopped), "was stopped")

	loadedState, ok := rr.load(triggered.Id)
	require.True(t, ok)
	require.Equal(t, machine.Loaded, loadedState.GetState())
	status, err := rr.GetRuleStatusV2(triggered.Id)
	require.NoError(t, err)
	require.Equal(t, "loaded", status["status"])
	stoppedState, ok := rr.load(stopped.Id)
	require.True(t, ok)
	require.Equal(t, machine.Stopped, stoppedState.GetState())
	loadedState.Delete()
	stoppedState.Delete()
}

func TestStartLoadedRules(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM recoveryDemo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM recoveryDemo`)

	rr := &RuleRegistry{internal: make(map[string]*rule.State)}
	invalid := rule.NewLoadedState(def.GetDefaultRule("invalidLoaded", "select * from missingRecoveryStream"), func(string, bool) {})
	valid := rule.NewLoadedState(def.GetDefaultRule("validLoaded", "select * from recoveryDemo"), func(string, bool) {})
	defer invalid.Delete()
	defer valid.Delete()
	rr.register("invalidLoaded", invalid)
	rr.register("validLoaded", valid)

	startLoadedRules(context.Background(), rr, []string{"invalidLoaded", "validLoaded"})
	require.Equal(t, machine.StoppedByErr, invalid.GetState())
	require.Equal(t, machine.Running, valid.GetState())

	canceled := rule.NewLoadedState(def.GetDefaultRule("canceledLoaded", "select * from recoveryDemo"), func(string, bool) {})
	defer canceled.Delete()
	rr.register("canceledLoaded", canceled)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	startLoadedRules(ctx, rr, []string{"canceledLoaded"})
	require.Equal(t, machine.Loaded, canceled.GetState())
}

func TestUpdatePersistenceFailureKeepsStateUsable(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM persistenceDemo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM persistenceDemo`)

	rr := &RuleRegistry{internal: make(map[string]*rule.State)}
	const stoppedRule = `{"id":"persistenceRule","triggered":false,"sql":"select * from persistenceDemo","actions":[{"log":{}}]}`
	const runningRule = `{"id":"persistenceRule","triggered":true,"sql":"select * from persistenceDemo","actions":[{"log":{}}]}`
	_, err = rr.CreateRule("persistenceRule", stoppedRule)
	require.NoError(t, err)
	defer rr.DeleteRule("persistenceRule")

	rs, ok := rr.load("persistenceRule")
	require.True(t, ok)
	err = rr.upsertRule("persistenceRule", runningRule, func(string, string) error {
		return errors.New("mock rule upsert error")
	})
	require.EqualError(t, err, "mock rule upsert error")
	retained, ok := rr.load("persistenceRule")
	require.True(t, ok)
	require.Same(t, rs, retained)
	require.Equal(t, machine.Stopped, retained.GetState())

	require.NoError(t, rr.UpsertRule("persistenceRule", runningRule))
	require.Equal(t, machine.Running, retained.GetState())
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
