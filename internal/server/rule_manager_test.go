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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
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
	err = registry.stopAtExit("test")
	assert.EqualError(t, err, "Rule test is not found in registry, please check if it is deleted")
	// db problems
	registry.register("test", rule.NewState(def.GetDefaultRule("testErrors", "select * from demo"), func(string, bool) {}))
	err = registry.StartRule("test")
	assert.EqualError(t, err, "fail to get stream demo, please check if stream is created")
}
