// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
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
	st := NewState(r)
	assert.Equal(t, r, st.Rule)
	assert.NotNil(t, st.logger)
	assert.Equal(t, Stopped, st.currentState)
	assert.Equal(t, 0, len(st.actionQ))
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
	assert.Equal(t, Running, st.GetState())
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
	em := "{\n  \"status\": \"running\",\n  \"message\": \"\",\n  \"lastStartTimestamp\": 0,\n  \"lastStopTimestamp\": 0,\n  \"nextStartTimestamp\": 0,\n  \"source_demo_0_records_in_total\": 0,\n  \"source_demo_0_records_out_total\": 0,\n  \"source_demo_0_messages_processed_total\": 0,\n  \"source_demo_0_process_latency_us\": 0,\n  \"source_demo_0_buffer_length\": 0,\n  \"source_demo_0_last_invocation\": 0,\n  \"source_demo_0_exceptions_total\": 0,\n  \"source_demo_0_last_exception\": \"\",\n  \"source_demo_0_last_exception_time\": 0,\n  \"op_2_project_0_records_in_total\": 0,\n  \"op_2_project_0_records_out_total\": 0,\n  \"op_2_project_0_messages_processed_total\": 0,\n  \"op_2_project_0_process_latency_us\": 0,\n  \"op_2_project_0_buffer_length\": 0,\n  \"op_2_project_0_last_invocation\": 0,\n  \"op_2_project_0_exceptions_total\": 0,\n  \"op_2_project_0_last_exception\": \"\",\n  \"op_2_project_0_last_exception_time\": 0,\n  \"op_logToMemory_0_0_transform_0_records_in_total\": 0,\n  \"op_logToMemory_0_0_transform_0_records_out_total\": 0,\n  \"op_logToMemory_0_0_transform_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_0_transform_0_process_latency_us\": 0,\n  \"op_logToMemory_0_0_transform_0_buffer_length\": 0,\n  \"op_logToMemory_0_0_transform_0_last_invocation\": 0,\n  \"op_logToMemory_0_0_transform_0_exceptions_total\": 0,\n  \"op_logToMemory_0_0_transform_0_last_exception\": \"\",\n  \"op_logToMemory_0_0_transform_0_last_exception_time\": 0,\n  \"op_logToMemory_0_1_encode_0_records_in_total\": 0,\n  \"op_logToMemory_0_1_encode_0_records_out_total\": 0,\n  \"op_logToMemory_0_1_encode_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_1_encode_0_process_latency_us\": 0,\n  \"op_logToMemory_0_1_encode_0_buffer_length\": 0,\n  \"op_logToMemory_0_1_encode_0_last_invocation\": 0,\n  \"op_logToMemory_0_1_encode_0_exceptions_total\": 0,\n  \"op_logToMemory_0_1_encode_0_last_exception\": \"\",\n  \"op_logToMemory_0_1_encode_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_records_in_total\": 0,\n  \"sink_logToMemory_0_0_records_out_total\": 0,\n  \"sink_logToMemory_0_0_messages_processed_total\": 0,\n  \"sink_logToMemory_0_0_process_latency_us\": 0,\n  \"sink_logToMemory_0_0_buffer_length\": 0,\n  \"sink_logToMemory_0_0_last_invocation\": 0,\n  \"sink_logToMemory_0_0_exceptions_total\": 0,\n  \"sink_logToMemory_0_0_last_exception\": \"\",\n  \"sink_logToMemory_0_0_last_exception_time\": 0\n}"
	assert.Equal(t, em, sm)
	mm := st.GetStatusMap()
	assert.True(t, len(mm) > 0)
	// Check stop metrics
	st.Stop()
	assert.Equal(t, expTopo, st.GetTopoGraph())
	assert.Equal(t, map[string]any{"lastStartTimestamp": int64(0), "lastStopTimestamp": int64(0), "message": "canceled manually", "nextStartTimestamp": int64(0), "op_2_project_0_buffer_length": int64(0), "op_2_project_0_exceptions_total": int64(0), "op_2_project_0_last_exception": "", "op_2_project_0_last_exception_time": int64(0), "op_2_project_0_last_invocation": int64(0), "op_2_project_0_messages_processed_total": int64(0), "op_2_project_0_process_latency_us": int64(0), "op_2_project_0_records_in_total": int64(0), "op_2_project_0_records_out_total": int64(0), "op_logToMemory_0_0_transform_0_buffer_length": int64(0), "op_logToMemory_0_0_transform_0_exceptions_total": int64(0), "op_logToMemory_0_0_transform_0_last_exception": "", "op_logToMemory_0_0_transform_0_last_exception_time": int64(0), "op_logToMemory_0_0_transform_0_last_invocation": int64(0), "op_logToMemory_0_0_transform_0_messages_processed_total": int64(0), "op_logToMemory_0_0_transform_0_process_latency_us": int64(0), "op_logToMemory_0_0_transform_0_records_in_total": int64(0), "op_logToMemory_0_0_transform_0_records_out_total": int64(0), "op_logToMemory_0_1_encode_0_buffer_length": int64(0), "op_logToMemory_0_1_encode_0_exceptions_total": int64(0), "op_logToMemory_0_1_encode_0_last_exception": "", "op_logToMemory_0_1_encode_0_last_exception_time": int64(0), "op_logToMemory_0_1_encode_0_last_invocation": int64(0), "op_logToMemory_0_1_encode_0_messages_processed_total": int64(0), "op_logToMemory_0_1_encode_0_process_latency_us": int64(0), "op_logToMemory_0_1_encode_0_records_in_total": int64(0), "op_logToMemory_0_1_encode_0_records_out_total": int64(0), "sink_logToMemory_0_0_buffer_length": int64(0), "sink_logToMemory_0_0_exceptions_total": int64(0), "sink_logToMemory_0_0_last_exception": "", "sink_logToMemory_0_0_last_exception_time": int64(0), "sink_logToMemory_0_0_last_invocation": int64(0), "sink_logToMemory_0_0_messages_processed_total": int64(0), "sink_logToMemory_0_0_process_latency_us": int64(0), "sink_logToMemory_0_0_records_in_total": int64(0), "sink_logToMemory_0_0_records_out_total": int64(0), "source_demo_0_buffer_length": int64(0), "source_demo_0_exceptions_total": int64(0), "source_demo_0_last_exception": "", "source_demo_0_last_exception_time": int64(0), "source_demo_0_last_invocation": int64(0), "source_demo_0_messages_processed_total": int64(0), "source_demo_0_process_latency_us": int64(0), "source_demo_0_records_in_total": int64(0), "source_demo_0_records_out_total": int64(0), "status": "stopped"}, st.GetStatusMap())
	em = "{\n  \"status\": \"stopped\",\n  \"message\": \"canceled manually\",\n  \"lastStartTimestamp\": 0,\n  \"lastStopTimestamp\": 0,\n  \"nextStartTimestamp\": 0,\n  \"source_demo_0_records_in_total\": 0,\n  \"source_demo_0_records_out_total\": 0,\n  \"source_demo_0_messages_processed_total\": 0,\n  \"source_demo_0_process_latency_us\": 0,\n  \"source_demo_0_buffer_length\": 0,\n  \"source_demo_0_last_invocation\": 0,\n  \"source_demo_0_exceptions_total\": 0,\n  \"source_demo_0_last_exception\": \"\",\n  \"source_demo_0_last_exception_time\": 0,\n  \"op_2_project_0_records_in_total\": 0,\n  \"op_2_project_0_records_out_total\": 0,\n  \"op_2_project_0_messages_processed_total\": 0,\n  \"op_2_project_0_process_latency_us\": 0,\n  \"op_2_project_0_buffer_length\": 0,\n  \"op_2_project_0_last_invocation\": 0,\n  \"op_2_project_0_exceptions_total\": 0,\n  \"op_2_project_0_last_exception\": \"\",\n  \"op_2_project_0_last_exception_time\": 0,\n  \"op_logToMemory_0_0_transform_0_records_in_total\": 0,\n  \"op_logToMemory_0_0_transform_0_records_out_total\": 0,\n  \"op_logToMemory_0_0_transform_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_0_transform_0_process_latency_us\": 0,\n  \"op_logToMemory_0_0_transform_0_buffer_length\": 0,\n  \"op_logToMemory_0_0_transform_0_last_invocation\": 0,\n  \"op_logToMemory_0_0_transform_0_exceptions_total\": 0,\n  \"op_logToMemory_0_0_transform_0_last_exception\": \"\",\n  \"op_logToMemory_0_0_transform_0_last_exception_time\": 0,\n  \"op_logToMemory_0_1_encode_0_records_in_total\": 0,\n  \"op_logToMemory_0_1_encode_0_records_out_total\": 0,\n  \"op_logToMemory_0_1_encode_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_1_encode_0_process_latency_us\": 0,\n  \"op_logToMemory_0_1_encode_0_buffer_length\": 0,\n  \"op_logToMemory_0_1_encode_0_last_invocation\": 0,\n  \"op_logToMemory_0_1_encode_0_exceptions_total\": 0,\n  \"op_logToMemory_0_1_encode_0_last_exception\": \"\",\n  \"op_logToMemory_0_1_encode_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_records_in_total\": 0,\n  \"sink_logToMemory_0_0_records_out_total\": 0,\n  \"sink_logToMemory_0_0_messages_processed_total\": 0,\n  \"sink_logToMemory_0_0_process_latency_us\": 0,\n  \"sink_logToMemory_0_0_buffer_length\": 0,\n  \"sink_logToMemory_0_0_last_invocation\": 0,\n  \"sink_logToMemory_0_0_exceptions_total\": 0,\n  \"sink_logToMemory_0_0_last_exception\": \"\",\n  \"sink_logToMemory_0_0_last_exception_time\": 0\n}"
	assert.Equal(t, em, st.GetStatusMessage())
	assert.Equal(t, Stopped, st.currentState)
	// Update rule
	st.Rule = def.GetDefaultRule("testAPI", "select abc from demo where a > 3")
	e = st.Validate()
	assert.NoError(t, e)
	e = st.Start()
	assert.NoError(t, e)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, Running, st.currentState)
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
	em = "{\n  \"status\": \"running\",\n  \"message\": \"\",\n  \"lastStartTimestamp\": 0,\n  \"lastStopTimestamp\": 0,\n  \"nextStartTimestamp\": 0,\n  \"source_demo_0_records_in_total\": 0,\n  \"source_demo_0_records_out_total\": 0,\n  \"source_demo_0_messages_processed_total\": 0,\n  \"source_demo_0_process_latency_us\": 0,\n  \"source_demo_0_buffer_length\": 0,\n  \"source_demo_0_last_invocation\": 0,\n  \"source_demo_0_exceptions_total\": 0,\n  \"source_demo_0_last_exception\": \"\",\n  \"source_demo_0_last_exception_time\": 0,\n  \"op_2_filter_0_records_in_total\": 0,\n  \"op_2_filter_0_records_out_total\": 0,\n  \"op_2_filter_0_messages_processed_total\": 0,\n  \"op_2_filter_0_process_latency_us\": 0,\n  \"op_2_filter_0_buffer_length\": 0,\n  \"op_2_filter_0_last_invocation\": 0,\n  \"op_2_filter_0_exceptions_total\": 0,\n  \"op_2_filter_0_last_exception\": \"\",\n  \"op_2_filter_0_last_exception_time\": 0,\n  \"op_3_project_0_records_in_total\": 0,\n  \"op_3_project_0_records_out_total\": 0,\n  \"op_3_project_0_messages_processed_total\": 0,\n  \"op_3_project_0_process_latency_us\": 0,\n  \"op_3_project_0_buffer_length\": 0,\n  \"op_3_project_0_last_invocation\": 0,\n  \"op_3_project_0_exceptions_total\": 0,\n  \"op_3_project_0_last_exception\": \"\",\n  \"op_3_project_0_last_exception_time\": 0,\n  \"op_logToMemory_0_0_transform_0_records_in_total\": 0,\n  \"op_logToMemory_0_0_transform_0_records_out_total\": 0,\n  \"op_logToMemory_0_0_transform_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_0_transform_0_process_latency_us\": 0,\n  \"op_logToMemory_0_0_transform_0_buffer_length\": 0,\n  \"op_logToMemory_0_0_transform_0_last_invocation\": 0,\n  \"op_logToMemory_0_0_transform_0_exceptions_total\": 0,\n  \"op_logToMemory_0_0_transform_0_last_exception\": \"\",\n  \"op_logToMemory_0_0_transform_0_last_exception_time\": 0,\n  \"op_logToMemory_0_1_encode_0_records_in_total\": 0,\n  \"op_logToMemory_0_1_encode_0_records_out_total\": 0,\n  \"op_logToMemory_0_1_encode_0_messages_processed_total\": 0,\n  \"op_logToMemory_0_1_encode_0_process_latency_us\": 0,\n  \"op_logToMemory_0_1_encode_0_buffer_length\": 0,\n  \"op_logToMemory_0_1_encode_0_last_invocation\": 0,\n  \"op_logToMemory_0_1_encode_0_exceptions_total\": 0,\n  \"op_logToMemory_0_1_encode_0_last_exception\": \"\",\n  \"op_logToMemory_0_1_encode_0_last_exception_time\": 0,\n  \"sink_logToMemory_0_0_records_in_total\": 0,\n  \"sink_logToMemory_0_0_records_out_total\": 0,\n  \"sink_logToMemory_0_0_messages_processed_total\": 0,\n  \"sink_logToMemory_0_0_process_latency_us\": 0,\n  \"sink_logToMemory_0_0_buffer_length\": 0,\n  \"sink_logToMemory_0_0_last_invocation\": 0,\n  \"sink_logToMemory_0_0_exceptions_total\": 0,\n  \"sink_logToMemory_0_0_last_exception\": \"\",\n  \"sink_logToMemory_0_0_last_exception_time\": 0\n}"

	assert.Equal(t, em, sm)
	e = st.Delete()
	assert.NoError(t, e)
}

func TestStateTransit(t *testing.T) {
	sp := processor.NewStreamProcessor()
	_, err := sp.ExecStmt(`CREATE STREAM demo () WITH (FORMAT="JSON", TYPE="memory", DATASOURCE="test")`)
	assert.NoError(t, err)
	defer sp.ExecStmt(`DROP STREAM demo`)
	tests := []struct {
		name       string
		r          *def.Rule
		actions    []ActionSignal
		async      bool
		finalState RunState
	}{
		{
			name:       "fast start stop",
			r:          def.GetDefaultRule("testNormal", "select * from demo"),
			actions:    []ActionSignal{ActionSignalStart, ActionSignalStart, ActionSignalStop, ActionSignalStart, ActionSignalStart, ActionSignalStop},
			finalState: Stopped,
		},
		//{
		//	name:       "async fast start stop",
		//	r:          def.GetDefaultRule("testAsync1", "select * from demo"),
		//	actions:    []ActionSignal{ActionSignalStart, ActionSignalStop, ActionSignalStop, ActionSignalStart, ActionSignalStop, ActionSignalStart, ActionSignalStart},
		//	finalState: Running,
		//	async:      true,
		//},
		{
			name:       "invalid",
			r:          def.GetDefaultRule("testAsync2", "select * from demo2"),
			actions:    []ActionSignal{ActionSignalStart, ActionSignalStop, ActionSignalStop, ActionSignalStart, ActionSignalStop, ActionSignalStop, ActionSignalStart},
			finalState: StoppedByErr,
			async:      true,
		},
	}
	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			st := NewState(v.r)
			defer st.Delete()
			st.actionQ = []ActionSignal{ActionSignalStart}
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
	st := NewState(sr)
	defer st.Delete()
	// Start run, but not in schedule
	e := st.Start()
	assert.NoError(t, e)
	assert.Equal(t, ScheduledStop, st.GetState())
	// Scheduled stop to start, no change
	_ = st.Start()
	assert.Equal(t, ScheduledStop, st.GetState())
	// Scheduled stop to stop, stop
	st.Stop()
	assert.Equal(t, Stopped, st.GetState())
	// Time move to schedule, should start
	timex.Add(30 * time.Minute)
	_ = st.ScheduleStart()
	// Notice: mock the action queue. The action must be the same as the next otherwise it will loop infinitely
	st.actionQ = append(st.actionQ, ActionSignalScheduledStart)
	_ = st.ScheduleStart()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, Running, st.GetState())
	// Time move out of schedule, scheduled stop
	timex.Add(30 * time.Minute)
	st.ScheduleStop()
	// Notice: mock the action queue. The action must be the same as the next otherwise it will loop infinitely
	st.actionQ = append(st.actionQ, ActionSignalScheduledStop)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, ScheduledStop, st.GetState())
	st.ScheduleStop()
	assert.Equal(t, ScheduledStop, st.GetState())
}

func sendAction(st *State, a ActionSignal) {
	switch a {
	case ActionSignalStart:
		_ = st.Start()
	case ActionSignalStop:
		st.Stop()
	case ActionSignalScheduledStart:
		_ = st.ScheduleStart()
	case ActionSignalScheduledStop:
		st.ScheduleStop()
	}
}

func TestRuleRestart(t *testing.T) {
	// TODO added later
}
