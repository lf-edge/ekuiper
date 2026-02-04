// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
)

// Verify Stop is deferred while Open is in progress
func TestStopDuringOpen(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	assert.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demo`)

	// Create a rule with default valid SQL
	r := def.GetDefaultRule("testStopDuringOpen", "select * from demo")

	// Use a clean state
	st := NewState(r, func(string, bool) {})
	defer st.Delete()

	// 1. Start the rule
	// This will trigger doStart -> runTopo -> tp.Open()
	// Since tp.Open() is fast for default rule (mock sources), we rely on
	// the fact that we call Stop immediately.
	// If the fix works, Stop should wait for Open to complete (or queue action)
	// and NOT crash/race.

	assert.NoError(t, st.Start())

	// Immediate Stop
	st.Stop()

	// Verify final state
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, Stopped, st.GetState())
}

// Verify multiple rapid Start/Stop calls are handled correctly by action queue
func TestRapidStartStop(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	assert.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demo`)

	r := def.GetDefaultRule("testRapidStartStop", "select * from demo")
	st := NewState(r, func(string, bool) {})
	defer st.Delete()

	// Sequence: Start -> Stop -> Start
	assert.NoError(t, st.Start())
	st.Stop()
	assert.NoError(t, st.Start())

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Should be Running
	assert.Equal(t, Running, st.GetState())
}
