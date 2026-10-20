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

package rule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"
)

// TestValidateAndRunScheduleRule validates that a schedule rule does not run
// immediately when created or updated, but waits for the rule patrol checker,
// which is the same behavior as the external Start action.
func TestValidateAndRunScheduleRule(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demoSchedule () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="testSchedule")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demoSchedule`)

	r := def.GetDefaultRule("scheduleCreate", "select * from demoSchedule")
	// GetRuleByJsonValidated defaults Triggered to true in the real create path
	r.Triggered = true
	// A schedule range fully in the past
	r.Options.CronDatetimeRange = []schedule.DatetimeRange{{
		Begin: "2020-01-01 00:00:00",
		End:   "2020-01-01 00:01:00",
	}}
	st := NewState(r, func(string, bool) {})
	// ValidateAndRun is called upon rule creation and rule update
	err = st.ValidateAndRun(r)
	require.NoError(t, err)
	// The rule should neither run nor keep the planned topo
	assert.Equal(t, machine.ScheduledStop, st.GetState())
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.ScheduledStop, st.GetState())
	assert.Nil(t, st.topology, "no topo should be kept while waiting for schedule")

	// Simulate the rule patrol checker starting the rule when it falls in range
	err = st.ScheduleStart()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.Running, st.GetState())
	assert.NotNil(t, st.topology)

	// Simulate the rule patrol checker stopping the rule when out of range
	st.ScheduleStop()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.ScheduledStop, st.GetState())
	assert.Nil(t, st.topology)
}

// TestValidateAndRunScheduleRule_TopoGraph ensures that when a schedule rule
// is created or updated, GetTopoGraph() returns the updated topology graph
// immediately, even before the patrol checker plans and runs the new topology.
func TestValidateAndRunScheduleRule_TopoGraph(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demoScheduleTopo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="testScheduleTopo")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demoScheduleTopo`)

	r1 := def.GetDefaultRule("scheduleTopo", "select a from demoScheduleTopo")
	r1.Triggered = true
	r1.Options.CronDatetimeRange = []schedule.DatetimeRange{{
		Begin: "2020-01-01 00:00:00",
		End:   "2020-01-01 00:01:00",
	}}
	st := NewState(r1, func(string, bool) {})
	defer st.Delete()

	err = st.ValidateAndRun(r1)
	require.NoError(t, err)
	assert.Equal(t, machine.ScheduledStop, st.GetState())

	topo1 := st.GetTopoGraph()
	require.NotNil(t, topo1)

	// Simulate starting the rule
	err = st.ScheduleStart()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.Running, st.GetState())

	// Update the running schedule rule with different SQL (adds a filter node to topo)
	r2 := def.GetDefaultRule("scheduleTopo", "select a from demoScheduleTopo where a > 1")
	r2.Triggered = true
	r2.Options.CronDatetimeRange = []schedule.DatetimeRange{{
		Begin: "2020-01-01 00:00:00",
		End:   "2020-01-01 00:01:00",
	}}
	err = st.ValidateAndRun(r2)
	require.NoError(t, err)
	assert.Equal(t, machine.ScheduledStop, st.GetState())

	topo2 := st.GetTopoGraph()
	require.NotNil(t, topo2)
	assert.NotEqual(t, topo1, topo2)
}
