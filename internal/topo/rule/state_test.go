// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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
	"errors"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	testx.InitEnv("Rule")
}

func TestAPIs(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	assert.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demo`)
	r := def.GetDefaultRule("testAPI", "select * from demo")
	// Init state
	st := NewState(r, func(string, bool) {})
	assert.Equal(t, r, st.Rule)
	assert.NotNil(t, st.logger)
	assert.Equal(t, machine.Stopped, st.sm.CurrentState())
	assert.Equal(t, "", st.GetLastWill())
	err = st.ResetStreamOffset("test", nil)
	assert.EqualError(t, err, "topo is not initialized, check rule status")
	assert.Nil(t, st.GetStreams())
	k, v := st.GetMetrics()
	assert.Nil(t, k)
	assert.Nil(t, v)
	topo := st.GetTopoGraph()
	assert.Nil(t, topo)
	sm := st.GetStatusMessage()
	assert.Equal(t, "{\n  \"status\": \"stopped\",\n  \"message\": \"\",\n  \"lastStartTimestamp\": 0,\n  \"lastStopTimestamp\": 0,\n  \"nextStartTimestamp\": 0\n}", sm)
	// Start the rule
	e := st.Start()
	assert.NoError(t, e)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.Running, st.GetState())
	err = st.ResetStreamOffset("test", nil)
	assert.EqualError(t, err, "stream test not found in topo")
	assert.Equal(t, []string{"demo"}, st.GetStreams())
	topo = st.GetTopoGraph()
	expTopo := &def.PrintableTopo{
		Sources: []string{"source_demo"},
		Edges: map[string][]any{
			"source_demo": {
				"op_2_project",
			},
			"op_2_project": {
				"op_logToMemory_0_0_transform",
			},
			"op_logToMemory_0_0_transform": {
				"op_logToMemory_0_1_encode",
			},
			"op_logToMemory_0_1_encode": {
				"sink_logToMemory_0",
			},
		},
	}
	assert.Equal(t, expTopo, topo)
	sm = st.GetStatusMessage()
	em := "{\n  \"status\": \"running\",\n  \"message\": \"\",\n  \"lastStartTimestamp\": 0,\n  \"lastStopTimestamp\": 0,\n  \"nextStartTimestamp\": 0,\n  \"source_demo_0_records_in_total\": 0,\n  \"source_demo_0_records_out_total\": 0,\n  \"source_demo_0_messages_processed_total\": 0,\n  \"source_demo_0_process_latency_us\": 0,\n  \"source_demo_0_buffer_length\": 0,\n  \"source_demo_0_last_invocation\": 0,\n  \"source_demo_0_exceptions_total\": 0,\n  \"source_demo_0_last_exception\": \"\",\n  \"source_demo_0_last_exception_time\": 0,\n  \"source_demo_0_connection_status\": 1,\n  \"source_demo_0_connection_last_connected_time\": 1,\n  \"source_demo_0_connection_last_disconnected_time\": 0,\n  \"source_demo_0_connection_last_disconnected_message\": \"\",\n  \"source_demo_0_connection_last_try_time\": 0,\n  \"op_2_project_0_records_in_total\": 0,\n  \"op_2_project_0_records_out_total\": 0,\n  \"op_2_project_0_messages_processed_total\": 0,\n  \"op_2_project_0_process_latency_us\": 0,\n  \"op_2_project_0_buffer_length\": 0,\n  \"op_2_project_0_last_invocation\": 0,\n  \"op_2_project_0_exceptions_total\": 0,\n  \"op_2_project_0_last_exception\": \"\",\n  \"op_2_project_0_last_exception_time\": 0,\n  \"op_logToMemory_0_0_transform_0_records_in_total\": 0,\n  \"op_logToMemory_0_0_transform_0_records_out_total\": 0,\n  \"op_logToMemory_0_0_transform_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_0_transform_0_process_latency_us\": 0,\n  \"op_logToMemory_0_0_transform_0_buffer_length\": 0,\n  \"op_logToMemory_0_0_transform_0_last_invocation\": 0,\n  \"op_logToMemory_0_0_transform_0_exceptions_total\": 0,\n  \"op_logToMemory_0_0_transform_0_last_exception\": \"\",\n  \"op_logToMemory_0_0_transform_0_last_exception_time\": 0,\n  \"op_logToMemory_0_1_encode_0_records_in_total\": 0,\n  \"op_logToMemory_0_1_encode_0_records_out_total\": 0,\n  \"op_logToMemory_0_1_encode_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_1_encode_0_process_latency_us\": 0,\n  \"op_logToMemory_0_1_encode_0_buffer_length\": 0,\n  \"op_logToMemory_0_1_encode_0_last_invocation\": 0,\n  \"op_logToMemory_0_1_encode_0_exceptions_total\": 0,\n  \"op_logToMemory_0_1_encode_0_last_exception\": \"\",\n  \"op_logToMemory_0_1_encode_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_records_in_total\": 0,\n  \"sink_logToMemory_0_0_records_out_total\": 0,\n  \"sink_logToMemory_0_0_messages_processed_total\": 0,\n  \"sink_logToMemory_0_0_process_latency_us\": 0,\n  \"sink_logToMemory_0_0_buffer_length\": 0,\n  \"sink_logToMemory_0_0_last_invocation\": 0,\n  \"sink_logToMemory_0_0_exceptions_total\": 0,\n  \"sink_logToMemory_0_0_last_exception\": \"\",\n  \"sink_logToMemory_0_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_connection_status\": 1,\n  \"sink_logToMemory_0_0_connection_last_connected_time\": 1,\n  \"sink_logToMemory_0_0_connection_last_disconnected_time\": 0,\n  \"sink_logToMemory_0_0_connection_last_disconnected_message\": \"\",\n  \"sink_logToMemory_0_0_connection_last_try_time\": 0\n}"
	re := regexp.MustCompile(`connection_last_connected_time":\s*\d+`)
	rsm := re.ReplaceAllString(sm, `connection_last_connected_time": 1`)
	assert.Equal(t, em, rsm)
	mm := st.GetStatusMap()
	assert.True(t, len(mm) > 0)
	// Check stop metrics
	st.Stop()
	assert.Equal(t, expTopo, st.GetTopoGraph())
	ssm := st.GetStatusMap()
	ssm["sink_logToMemory_0_0_connection_last_connected_time"] = int64(1)
	ssm["source_demo_0_connection_last_connected_time"] = int64(1)
	assert.Equal(t, map[string]any{"lastStartTimestamp": int64(0), "lastStopTimestamp": int64(0), "message": "canceled manually", "nextStartTimestamp": int64(0), "op_2_project_0_buffer_length": int64(0), "op_2_project_0_exceptions_total": int64(0), "op_2_project_0_last_exception": "", "op_2_project_0_last_exception_time": int64(0), "op_2_project_0_last_invocation": int64(0), "op_2_project_0_messages_processed_total": int64(0), "op_2_project_0_process_latency_us": int64(0), "op_2_project_0_records_in_total": int64(0), "op_2_project_0_records_out_total": int64(0), "op_logToMemory_0_0_transform_0_buffer_length": int64(0), "op_logToMemory_0_0_transform_0_exceptions_total": int64(0), "op_logToMemory_0_0_transform_0_last_exception": "", "op_logToMemory_0_0_transform_0_last_exception_time": int64(0), "op_logToMemory_0_0_transform_0_last_invocation": int64(0), "op_logToMemory_0_0_transform_0_messages_processed_total": int64(0), "op_logToMemory_0_0_transform_0_process_latency_us": int64(0), "op_logToMemory_0_0_transform_0_records_in_total": int64(0), "op_logToMemory_0_0_transform_0_records_out_total": int64(0), "op_logToMemory_0_1_encode_0_buffer_length": int64(0), "op_logToMemory_0_1_encode_0_exceptions_total": int64(0), "op_logToMemory_0_1_encode_0_last_exception": "", "op_logToMemory_0_1_encode_0_last_exception_time": int64(0), "op_logToMemory_0_1_encode_0_last_invocation": int64(0), "op_logToMemory_0_1_encode_0_messages_processed_total": int64(0), "op_logToMemory_0_1_encode_0_process_latency_us": int64(0), "op_logToMemory_0_1_encode_0_records_in_total": int64(0), "op_logToMemory_0_1_encode_0_records_out_total": int64(0), "sink_logToMemory_0_0_buffer_length": int64(0), "sink_logToMemory_0_0_exceptions_total": int64(0), "sink_logToMemory_0_0_last_exception": "", "sink_logToMemory_0_0_last_exception_time": int64(0), "sink_logToMemory_0_0_last_invocation": int64(0), "sink_logToMemory_0_0_messages_processed_total": int64(0), "sink_logToMemory_0_0_process_latency_us": int64(0), "sink_logToMemory_0_0_records_in_total": int64(0), "sink_logToMemory_0_0_records_out_total": int64(0), "sink_logToMemory_0_0_connection_last_connected_time": int64(1), "sink_logToMemory_0_0_connection_last_disconnected_message": "", "sink_logToMemory_0_0_connection_last_disconnected_time": int64(0), "sink_logToMemory_0_0_connection_last_try_time": int64(0), "sink_logToMemory_0_0_connection_status": 1, "source_demo_0_buffer_length": int64(0), "source_demo_0_exceptions_total": int64(0), "source_demo_0_last_exception": "", "source_demo_0_last_exception_time": int64(0), "source_demo_0_last_invocation": int64(0), "source_demo_0_messages_processed_total": int64(0), "source_demo_0_process_latency_us": int64(0), "source_demo_0_records_in_total": int64(0), "source_demo_0_records_out_total": int64(0), "source_demo_0_connection_last_connected_time": int64(1), "source_demo_0_connection_last_disconnected_message": "", "source_demo_0_connection_last_disconnected_time": int64(0), "source_demo_0_connection_last_try_time": int64(0), "source_demo_0_connection_status": 1, "status": "stopped"}, ssm)
	em = "{\n  \"status\": \"stopped\",\n  \"message\": \"canceled manually\",\n  \"lastStartTimestamp\": 0,\n  \"lastStopTimestamp\": 0,\n  \"nextStartTimestamp\": 0,\n  \"source_demo_0_records_in_total\": 0,\n  \"source_demo_0_records_out_total\": 0,\n  \"source_demo_0_messages_processed_total\": 0,\n  \"source_demo_0_process_latency_us\": 0,\n  \"source_demo_0_buffer_length\": 0,\n  \"source_demo_0_last_invocation\": 0,\n  \"source_demo_0_exceptions_total\": 0,\n  \"source_demo_0_last_exception\": \"\",\n  \"source_demo_0_last_exception_time\": 0,\n  \"source_demo_0_connection_status\": 1,\n  \"source_demo_0_connection_last_connected_time\": 1,\n  \"source_demo_0_connection_last_disconnected_time\": 0,\n  \"source_demo_0_connection_last_disconnected_message\": \"\",\n  \"source_demo_0_connection_last_try_time\": 0,\n  \"op_2_project_0_records_in_total\": 0,\n  \"op_2_project_0_records_out_total\": 0,\n  \"op_2_project_0_messages_processed_total\": 0,\n  \"op_2_project_0_process_latency_us\": 0,\n  \"op_2_project_0_buffer_length\": 0,\n  \"op_2_project_0_last_invocation\": 0,\n  \"op_2_project_0_exceptions_total\": 0,\n  \"op_2_project_0_last_exception\": \"\",\n  \"op_2_project_0_last_exception_time\": 0,\n  \"op_logToMemory_0_0_transform_0_records_in_total\": 0,\n  \"op_logToMemory_0_0_transform_0_records_out_total\": 0,\n  \"op_logToMemory_0_0_transform_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_0_transform_0_process_latency_us\": 0,\n  \"op_logToMemory_0_0_transform_0_buffer_length\": 0,\n  \"op_logToMemory_0_0_transform_0_last_invocation\": 0,\n  \"op_logToMemory_0_0_transform_0_exceptions_total\": 0,\n  \"op_logToMemory_0_0_transform_0_last_exception\": \"\",\n  \"op_logToMemory_0_0_transform_0_last_exception_time\": 0,\n  \"op_logToMemory_0_1_encode_0_records_in_total\": 0,\n  \"op_logToMemory_0_1_encode_0_records_out_total\": 0,\n  \"op_logToMemory_0_1_encode_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_1_encode_0_process_latency_us\": 0,\n  \"op_logToMemory_0_1_encode_0_buffer_length\": 0,\n  \"op_logToMemory_0_1_encode_0_last_invocation\": 0,\n  \"op_logToMemory_0_1_encode_0_exceptions_total\": 0,\n  \"op_logToMemory_0_1_encode_0_last_exception\": \"\",\n  \"op_logToMemory_0_1_encode_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_records_in_total\": 0,\n  \"sink_logToMemory_0_0_records_out_total\": 0,\n  \"sink_logToMemory_0_0_messages_processed_total\": 0,\n  \"sink_logToMemory_0_0_process_latency_us\": 0,\n  \"sink_logToMemory_0_0_buffer_length\": 0,\n  \"sink_logToMemory_0_0_last_invocation\": 0,\n  \"sink_logToMemory_0_0_exceptions_total\": 0,\n  \"sink_logToMemory_0_0_last_exception\": \"\",\n  \"sink_logToMemory_0_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_connection_status\": 1,\n  \"sink_logToMemory_0_0_connection_last_connected_time\": 1,\n  \"sink_logToMemory_0_0_connection_last_disconnected_time\": 0,\n  \"sink_logToMemory_0_0_connection_last_disconnected_message\": \"\",\n  \"sink_logToMemory_0_0_connection_last_try_time\": 0\n}"
	rsm = re.ReplaceAllString(st.GetStatusMessage(), `connection_last_connected_time": 1`)
	assert.Equal(t, em, rsm)
	assert.Equal(t, machine.Stopped, st.sm.CurrentState())
	// Update rule
	e = st.ValidateAndRun(def.GetDefaultRule("testAPI", "select abc from demo where a > 3"))
	assert.NoError(t, e)
	e = st.Start()
	assert.NoError(t, e)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.Running, st.sm.CurrentState())
	err = st.ResetStreamOffset("test", nil)
	assert.EqualError(t, err, "stream test not found in topo")
	assert.Equal(t, []string{"demo"}, st.GetStreams())
	topo = st.GetTopoGraph()
	expTopo = &def.PrintableTopo{
		Sources: []string{"source_demo"},
		Edges: map[string][]any{
			"source_demo": {
				"op_2_filter",
			},
			"op_2_filter": {
				"op_3_project",
			},
			"op_3_project": {
				"op_logToMemory_0_0_transform",
			},
			"op_logToMemory_0_0_transform": {
				"op_logToMemory_0_1_encode",
			},
			"op_logToMemory_0_1_encode": {
				"sink_logToMemory_0",
			},
		},
	}
	assert.Equal(t, expTopo, topo)
	sm = st.GetStatusMessage()
	rsm = re.ReplaceAllString(sm, `connection_last_connected_time": 1`)
	em = "{\n  \"status\": \"running\",\n  \"message\": \"\",\n  \"lastStartTimestamp\": 0,\n  \"lastStopTimestamp\": 0,\n  \"nextStartTimestamp\": 0,\n  \"source_demo_0_records_in_total\": 0,\n  \"source_demo_0_records_out_total\": 0,\n  \"source_demo_0_messages_processed_total\": 0,\n  \"source_demo_0_process_latency_us\": 0,\n  \"source_demo_0_buffer_length\": 0,\n  \"source_demo_0_last_invocation\": 0,\n  \"source_demo_0_exceptions_total\": 0,\n  \"source_demo_0_last_exception\": \"\",\n  \"source_demo_0_last_exception_time\": 0,\n  \"source_demo_0_connection_status\": 1,\n  \"source_demo_0_connection_last_connected_time\": 1,\n  \"source_demo_0_connection_last_disconnected_time\": 0,\n  \"source_demo_0_connection_last_disconnected_message\": \"\",\n  \"source_demo_0_connection_last_try_time\": 0,\n  \"op_2_filter_0_records_in_total\": 0,\n  \"op_2_filter_0_records_out_total\": 0,\n  \"op_2_filter_0_messages_processed_total\": 0,\n  \"op_2_filter_0_process_latency_us\": 0,\n  \"op_2_filter_0_buffer_length\": 0,\n  \"op_2_filter_0_last_invocation\": 0,\n  \"op_2_filter_0_exceptions_total\": 0,\n  \"op_2_filter_0_last_exception\": \"\",\n  \"op_2_filter_0_last_exception_time\": 0,\n  \"op_3_project_0_records_in_total\": 0,\n  \"op_3_project_0_records_out_total\": 0,\n  \"op_3_project_0_messages_processed_total\": 0,\n  \"op_3_project_0_process_latency_us\": 0,\n  \"op_3_project_0_buffer_length\": 0,\n  \"op_3_project_0_last_invocation\": 0,\n  \"op_3_project_0_exceptions_total\": 0,\n  \"op_3_project_0_last_exception\": \"\",\n  \"op_3_project_0_last_exception_time\": 0,\n  \"op_logToMemory_0_0_transform_0_records_in_total\": 0,\n  \"op_logToMemory_0_0_transform_0_records_out_total\": 0,\n  \"op_logToMemory_0_0_transform_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_0_transform_0_process_latency_us\": 0,\n  \"op_logToMemory_0_0_transform_0_buffer_length\": 0,\n  \"op_logToMemory_0_0_transform_0_last_invocation\": 0,\n  \"op_logToMemory_0_0_transform_0_exceptions_total\": 0,\n  \"op_logToMemory_0_0_transform_0_last_exception\": \"\",\n  \"op_logToMemory_0_0_transform_0_last_exception_time\": 0,\n  \"op_logToMemory_0_1_encode_0_records_in_total\": 0,\n  \"op_logToMemory_0_1_encode_0_records_out_total\": 0,\n  \"op_logToMemory_0_1_encode_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_1_encode_0_process_latency_us\": 0,\n  \"op_logToMemory_0_1_encode_0_buffer_length\": 0,\n  \"op_logToMemory_0_1_encode_0_last_invocation\": 0,\n  \"op_logToMemory_0_1_encode_0_exceptions_total\": 0,\n  \"op_logToMemory_0_1_encode_0_last_exception\": \"\",\n  \"op_logToMemory_0_1_encode_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_records_in_total\": 0,\n  \"sink_logToMemory_0_0_records_out_total\": 0,\n  \"sink_logToMemory_0_0_messages_processed_total\": 0,\n  \"sink_logToMemory_0_0_process_latency_us\": 0,\n  \"sink_logToMemory_0_0_buffer_length\": 0,\n  \"sink_logToMemory_0_0_last_invocation\": 0,\n  \"sink_logToMemory_0_0_exceptions_total\": 0,\n  \"sink_logToMemory_0_0_last_exception\": \"\",\n  \"sink_logToMemory_0_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_connection_status\": 1,\n  \"sink_logToMemory_0_0_connection_last_connected_time\": 1,\n  \"sink_logToMemory_0_0_connection_last_disconnected_time\": 0,\n  \"sink_logToMemory_0_0_connection_last_disconnected_message\": \"\",\n  \"sink_logToMemory_0_0_connection_last_try_time\": 0\n}"
	assert.Equal(t, em, rsm)
	st.Delete()
}

func TestValidateAndRunCommitBoundary(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM updateCommitDemo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	require.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM updateCommitDemo`)

	oldRule := def.GetDefaultRule("updateCommitBoundary", "select * from updateCommitDemo")
	oldRule.Triggered = true
	st := NewState(oldRule, func(string, bool) {})
	require.NoError(t, st.ValidateAndRun(oldRule))
	defer st.Delete()
	require.Equal(t, machine.Running, st.GetState())
	oldTopology := st.topology
	require.NotNil(t, oldTopology)

	t.Run("planning failure skips commit", func(t *testing.T) {
		commitCalled := false
		invalidRule := def.GetDefaultRule(oldRule.Id, "select * from streamThatDoesNotExist")
		invalidRule.Triggered = true
		err := st.ValidateAndRunWithCommit(invalidRule, func() error {
			commitCalled = true
			return nil
		})
		require.Error(t, err)
		require.False(t, commitCalled)
		require.Same(t, oldRule, st.Rule)
		require.Same(t, oldTopology, st.topology)
		require.Equal(t, machine.Running, st.GetState())
	})

	t.Run("commit failure preserves current runtime", func(t *testing.T) {
		candidate := def.GetDefaultRule(oldRule.Id, "select * from updateCommitDemo where true")
		candidate.Triggered = false
		expectedErr := errors.New("commit failed")
		err := st.ValidateAndRunWithCommit(candidate, func() error {
			// The candidate has been planned, but the current runtime remains
			// authoritative until this callback succeeds.
			require.Same(t, oldRule, st.Rule)
			require.Same(t, oldTopology, st.topology)
			require.Equal(t, machine.Running, st.GetState())
			return expectedErr
		})
		require.ErrorIs(t, err, expectedErr)
		require.Same(t, oldRule, st.Rule)
		require.Same(t, oldTopology, st.topology)
		require.Equal(t, machine.Running, st.GetState())
	})

	t.Run("successful commit applies candidate", func(t *testing.T) {
		candidate := def.GetDefaultRule(oldRule.Id, "select * from updateCommitDemo where true")
		candidate.Triggered = false
		err := st.ValidateAndRunWithCommit(candidate, func() error {
			require.Same(t, oldRule, st.Rule)
			require.Same(t, oldTopology, st.topology)
			require.Equal(t, machine.Running, st.GetState())
			return nil
		})
		require.NoError(t, err)
		require.Same(t, candidate, st.Rule)
		require.Nil(t, st.topology)
		require.Equal(t, machine.Stopped, st.GetState())
	})
}

func TestStateTransit(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	assert.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demo`)
	tests := []struct {
		name       string
		r          *def.Rule
		actions    []machine.ActionSignal
		async      bool
		finalState machine.RunState
	}{
		{
			name:       "fast start stop",
			r:          def.GetDefaultRule("testNormal", "select * from demo"),
			actions:    []machine.ActionSignal{machine.ActionSignalStart, machine.ActionSignalStart, machine.ActionSignalStop, machine.ActionSignalStart, machine.ActionSignalStart, machine.ActionSignalStop},
			finalState: machine.Stopped,
		},
		{
			name:       "async fast start stop",
			r:          def.GetDefaultRule("testAsync1", "select * from demo"),
			actions:    []machine.ActionSignal{machine.ActionSignalStart, machine.ActionSignalStop, machine.ActionSignalStop, machine.ActionSignalStart, machine.ActionSignalStop, machine.ActionSignalStart, machine.ActionSignalStart},
			finalState: machine.Running,
			async:      true,
		},
		{
			name:       "invalid",
			r:          def.GetDefaultRule("testAsync2", "select * from demo2"),
			actions:    []machine.ActionSignal{machine.ActionSignalStart, machine.ActionSignalStop, machine.ActionSignalStop, machine.ActionSignalStart, machine.ActionSignalStop, machine.ActionSignalStop, machine.ActionSignalStart},
			finalState: machine.StoppedByErr,
			async:      true,
		},
	}
	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			st := NewState(v.r, func(string, bool) {})
			defer st.Delete()
			var wg sync.WaitGroup
			if v.async {
				wg.Add(len(v.actions) - 1)
			}
			for i, a := range v.actions {
				if i == len(v.actions)-1 {
					break
				}
				if v.async {
					go func() {
						sendAction(st, a)
						wg.Done()
					}()
				} else {
					sendAction(st, a)
				}
			}
			if v.async {
				wg.Wait()
			}
			sendAction(st, v.actions[len(v.actions)-1])
			time.Sleep(500 * time.Millisecond)
			assert.Equal(t, v.finalState, st.GetState())
		})
	}
}

func TestLongScheduleTransit(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	assert.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demo`)
	// set now for schedule rule
	now := time.Date(2024, time.August, 8, 15, 38, 0, 0, time.UTC)
	timex.Set(now.UnixMilli())
	sr := def.GetDefaultRule("testScheduleNotIn", "select * from demo")
	sr.Options.CronDatetimeRange = []schedule.DatetimeRange{
		{
			Begin: "2024-08-08 16:04:01",
			End:   "2024-08-08 16:30:01",
		},
	}
	st := NewState(sr, func(string, bool) {})
	defer st.Delete()
	// Start run, but not in schedule
	e := st.Start()
	assert.NoError(t, e)
	assert.Equal(t, machine.ScheduledStop, st.GetState())
	// Scheduled stop to start, no change
	_ = st.Start()
	assert.Equal(t, machine.ScheduledStop, st.GetState())
	// Scheduled stop to stop, stop
	st.Stop()
	assert.Equal(t, machine.Stopped, st.GetState())
	// Time move to schedule, should start
	timex.Add(30 * time.Minute)
	_ = st.ScheduleStart()
	// Notice: mock the action queue. The action must be the same as the next otherwise it will loop infinitely
	wg1 := sync.WaitGroup{}
	wg1.Add(2)
	go func() {
		_ = st.ScheduleStart()
		wg1.Done()
	}()
	go func() {
		_ = st.ScheduleStart()
		wg1.Done()
	}()
	wg1.Wait()
	st.nextAction()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.Running, st.GetState())
	// Time move out of schedule, scheduled stop
	timex.Add(30 * time.Minute)
	st.ScheduleStop()
	// Notice: mock the action queue. The action must be the same as the next otherwise it will loop infinitely
	go func() {
		st.ScheduleStop()
	}()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, machine.ScheduledStop, st.GetState())
	st.ScheduleStop()
	assert.Equal(t, machine.ScheduledStop, st.GetState())
}

func sendAction(st *State, a machine.ActionSignal) {
	switch a {
	case machine.ActionSignalStart:
		_ = st.Start()
	case machine.ActionSignalStop:
		st.Stop()
	case machine.ActionSignalScheduledStart:
		_ = st.ScheduleStart()
	case machine.ActionSignalScheduledStop:
		st.ScheduleStop()
	}
}

func TestRuleRestart(t *testing.T) {
	// TODO added later
}

func TestStartRechecksStateAfterLock(t *testing.T) {
	tests := []struct {
		name  string
		start func(*State) error
	}{
		{
			name: "start",
			start: func(st *State) error {
				return st.Start()
			},
		},
		{
			name: "scheduled start",
			start: func(st *State) error {
				return st.ScheduleStart()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewState(def.GetDefaultRule("testLockRecheck", "select * from demo"), func(string, bool) {})

			// Hold ruleLock so the start operation stops after changing the state to Starting.
			st.ruleLock.Lock()
			locked := true
			defer func() {
				if locked {
					st.ruleLock.Unlock()
				}
				st.Delete()
			}()
			done := make(chan error, 1)
			go func() {
				done <- tt.start(st)
			}()
			require.Eventually(t, func() bool {
				return st.GetState() == machine.Starting
			}, time.Second, time.Millisecond)

			// Model Bootstrap winning ruleLock and completing the API start first.
			st.transitState(machine.Running, "")
			st.ruleLock.Unlock()
			locked = false

			require.NoError(t, <-done)
			assert.Equal(t, machine.Running, st.GetState())
			assert.Nil(t, st.topology, "the stale start operation must not plan or launch the topology")
		})
	}
}
