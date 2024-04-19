// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

var defaultOption = &api.RuleOption{
	IsEventTime:        false,
	LateTol:            1000,
	Concurrency:        1,
	BufferLength:       1024,
	SendMetaToSink:     false,
	SendError:          true,
	Qos:                api.AtMostOnce,
	CheckpointInterval: 300000,
	RestartStrategy: &api.RestartStrategy{
		Attempts:     0,
		Delay:        1000,
		Multiplier:   2,
		MaxDelay:     30000,
		JitterFactor: 0.1,
	},
}

func init() {
	testx.InitEnv("rule")
}

func TestCreate(t *testing.T) {
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	tests := []struct {
		r    *api.Rule
		e    error
		code errorx.ErrorCode
	}{
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT ts FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e: nil,
		},
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e:    errors.New("Parse SQL SELECT FROM demo error: found \"FROM\", expected expression.."),
			code: errorx.ParserError,
		},
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT * FROM demo1",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e:    errors.New("fail to get stream demo1, please check if stream is created"),
			code: errorx.PlanError,
		},
	}
	for i, tt := range tests {
		_, err := NewRuleState(tt.r)
		if err != nil {
			code, ok := errorx.GetErrorCode(err)
			if tt.code != 0 {
				require.True(t, ok)
				require.Equal(t, tt.code, code)
				if !reflect.DeepEqual(err.Error(), tt.e.Error()) {
					t.Errorf("%d.\n\nerror mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.e, err)
				}
				continue
			}
		}
		if !reflect.DeepEqual(err, tt.e) {
			t.Errorf("%d.\n\nerror mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.e, err)
		}
	}
}

func TestUpdate(t *testing.T) {
	ignoreSignal = true
	defer func() {
		ignoreSignal = false
	}()
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	tests := []struct {
		r         *api.Rule
		e         error
		triggered int
		code      errorx.ErrorCode
	}{
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e:         errors.New("Parse SQL SELECT FROM demo error: found \"FROM\", expected expression.."),
			code:      errorx.ParserError,
			triggered: 1,
		},
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT * FROM demo1",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e:         errors.New("fail to get stream demo1, please check if stream is created"),
			code:      errorx.PlanError,
			triggered: 1,
		},
		{
			r: &api.Rule{
				Triggered: true,
				Id:        "test",
				Sql:       "SELECT * FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e:         nil,
			triggered: 1,
		},
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT * FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e:         nil,
			triggered: 0,
		},
	}
	for i, tt := range tests {
		rs, err := NewRuleState(&api.Rule{
			Triggered: true,
			Id:        "test",
			Sql:       "SELECT ts FROM demo",
			Actions: []map[string]interface{}{
				{
					"log": map[string]interface{}{},
				},
			},
			Options: defaultOption,
		})
		require.NoError(t, err)
		err = rs.Start()
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
		require.Equal(t, 1, rs.triggered, fmt.Sprintf("case %v failed", i))
		err = rs.UpdateTopo(tt.r)
		time.Sleep(5 * time.Millisecond)
		if err != nil {
			code, ok := errorx.GetErrorCode(err)
			if tt.code != 0 {
				require.True(t, ok)
				require.Equal(t, tt.code, code)
				require.Equal(t, tt.e.Error(), err.Error(), fmt.Sprintf("case %v failed", i))
				continue
			}
		}
		require.Equal(t, tt.triggered, rs.triggered, fmt.Sprintf("case %v failed", i))
		rs.Close()
	}
}

func TestUpdateScheduleRule(t *testing.T) {
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	scheduleOption1 := *defaultOption
	scheduleOption1.Cron = "mockCron"
	scheduleOption1.Duration = "1s"
	rule1 := &api.Rule{
		Triggered: true,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: &scheduleOption1,
	}
	rs, err := NewRuleState(rule1)
	require.NoError(t, err)
	defer rs.Close()
	err = rs.startScheduleRule()
	require.NoError(t, err)
	require.True(t, rs.cronState.isInSchedule)
	require.Equal(t, "mockCron", rs.cronState.cron)
	require.Equal(t, "1s", rs.cronState.duration)

	scheduleOption2 := *defaultOption
	scheduleOption2.Cron = "mockCron2"
	scheduleOption2.Duration = "2s"
	rule2 := &api.Rule{
		Triggered: true,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: &scheduleOption2,
	}
	err = rs.UpdateTopo(rule2)
	require.NoError(t, err)
	require.Equal(t, "mockCron2", rs.cronState.cron)
	require.Equal(t, "2s", rs.cronState.duration)
}

func TestMultipleAccess(t *testing.T) {
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	rs, err := NewRuleState(&api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer rs.Close()
	err = rs.Start()
	if err != nil {
		t.Error(err)
		return
	}
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		if i%3 == 0 {
			go func(i int) {
				rs.Stop()
				fmt.Printf("%d:%d\n", i, rs.triggered)
				wg.Done()
			}(i)
		} else {
			go func(i int) {
				rs.Start()
				fmt.Printf("%d:%d\n", i, rs.triggered)
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	fmt.Printf("before %d:%d\n", 10, rs.triggered)
	err = rs.Stop()
	assert.NoError(t, err)
	fmt.Printf("%d:%d\n", 10, rs.triggered)
	if rs.triggered != 0 {
		t.Errorf("triggered mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", 0, rs.triggered)
	}
}

// Test rule state message
func TestRuleState_Start(t *testing.T) {
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (TYPE="neuron", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	// Test rule not triggered
	r := &api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	}
	const ruleStopped = "Stopped: canceled manually."
	const ruleStarted = "Running"
	t.Run("test rule loaded but not started", func(t *testing.T) {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		state, err := rs.GetState()
		if err != nil {
			t.Errorf("get rule state error: %v", err)
			return
		}
		if state != ruleStopped {
			t.Errorf("rule state mismatch: exp=%v, got=%v", ruleStopped, state)
			return
		}
	})
	t.Run("test rule started", func(t *testing.T) {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		err = rs.Start()
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(100 * time.Millisecond)
		state, err := rs.GetState()
		if err != nil {
			t.Errorf("get rule state error: %v", err)
			return
		}
		if state != ruleStarted {
			t.Errorf("rule state mismatch: exp=%v, got=%v", ruleStopped, state)
			return
		}
	})
	t.Run("test rule loaded and stopped", func(t *testing.T) {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		err = rs.Start()
		if err != nil {
			t.Error(err)
			return
		}
		err = rs.Close()
		if err != nil {
			t.Error(err)
			return
		}
		state, err := rs.GetState()
		if err != nil {
			t.Errorf("get rule state error: %v", err)
			return
		}
		if state != ruleStopped {
			t.Errorf("rule state mismatch: exp=%v, got=%v", ruleStopped, state)
			return
		}
	})
}

func TestScheduleRule(t *testing.T) {
	conf.IsTesting = true
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (TYPE="neuron", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	// Test rule not triggered
	r := &api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	}
	r.Options.Cron = "mockCron"
	r.Options.Duration = "1s"
	const ruleStarted = "Running"
	const ruleStopped = "Stopped: waiting for next schedule."
	func() {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		if err := rs.startScheduleRule(); err != nil {
			t.Error(err)
			return
		}
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		if err != nil {
			t.Errorf("get rule state error: %v", err)
			return
		}
		if state != ruleStarted {
			t.Errorf("rule state mismatch: exp=%v, got=%v", ruleStarted, state)
			return
		}
		if !rs.cronState.isInSchedule {
			t.Error("cron state should be in schedule")
			return
		}
	}()

	func() {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		if err := rs.startScheduleRule(); err != nil {
			t.Error(err)
			return
		}
		time.Sleep(1500 * time.Millisecond)
		state, err := rs.GetState()
		if err != nil {
			t.Errorf("get rule state error: %v", err)
			return
		}
		if state != ruleStopped {
			t.Errorf("rule state mismatch: exp=%v, got=%v", ruleStopped, state)
			return
		}
		if !rs.cronState.isInSchedule {
			t.Error("cron state should be in schedule")
			return
		}
	}()

	func() {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		if err := rs.startScheduleRule(); err != nil {
			t.Error(err)
			return
		}
		if err := rs.startScheduleRule(); err == nil {
			t.Error("rule can't be register in cron twice")
			return
		} else {
			if err.Error() != "rule test is already in schedule" {
				t.Error("error message wrong")
				return
			}
		}
	}()

	func() {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		if err := rs.startScheduleRule(); err != nil {
			t.Error(err)
			return
		}
		if err := rs.Stop(); err != nil {
			t.Error(err)
			return
		}
		state, err := rs.GetState()
		if err != nil {
			t.Errorf("get rule state error: %v", err)
			return
		}
		if state != "Stopped: canceled manually." {
			t.Errorf("rule state mismatch: exp=%v, got=%v", "Stopped: canceled manually.", state)
			return
		}
		if rs.cronState.isInSchedule {
			t.Error("cron state shouldn't be in schedule")
			return
		}
	}()

	func() {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		if err := rs.Stop(); err != nil {
			t.Error(err)
			return
		}
		if err := rs.Close(); err != nil {
			t.Error(err)
			return
		}
	}()

	func() {
		rs, err := NewRuleState(r)
		if err != nil {
			t.Error(err)
			return
		}
		rs.cronState.isInSchedule = true
		status, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, "Stopped: waiting for next schedule.", status)
	}()
}

func TestScheduleRuleInRange(t *testing.T) {
	now := time.Now()
	m := conf.Clock.(*clock.Mock)
	m.Set(now)
	before := now.AddDate(-10, -10, -10)
	after := now.Add(10 * time.Second)
	conf.IsTesting = true
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (TYPE="neuron", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	// Test rule not triggered
	r := &api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	}
	r.Options.Cron = "mockCron"
	r.Options.Duration = "1s"
	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: before.Format(layout),
			End:   after.Format(layout),
		},
	}
	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		err = rs.startScheduleRule()
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, RuleStarted, state)
		require.True(t, rs.cronState.isInSchedule)
	}()

	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: after.Format(layout),
			End:   after.Format(layout),
		},
	}
	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		err = rs.startScheduleRule()
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, RuleWait, state)
		require.True(t, rs.cronState.isInSchedule)
	}()

	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: before.Format(layout),
			End:   before.Format(layout),
		},
	}
	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		err = rs.startScheduleRule()
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, RuleTerminated, state)
		require.True(t, rs.cronState.isInSchedule)
	}()

	now2, err := time.Parse(layout, "2006-01-02 15:04:01")
	require.NoError(t, err)
	r.Options.Cron = "4 15 * * *"
	r.Options.CronDatetimeRange = nil
	r.Options.Duration = "2s"
	m.Set(now2)

	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		require.NoError(t, rs.startScheduleRule())
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, state, RuleStarted)
		time.Sleep(3 * time.Second)
		state, err = rs.GetState()
		require.NoError(t, err)
		require.Equal(t, state, RuleWait)
	}()
}

const layout = "2006-01-02 15:04:05"

func TestStartLongRunningScheduleRule(t *testing.T) {
	conf.IsTesting = true
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (TYPE="neuron", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	now := time.Now()
	m := conf.Clock.(*clock.Mock)
	m.Set(now)
	before := now.AddDate(-10, -10, -10)
	after := now.Add(10 * time.Second)
	r := &api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	}
	r.Options.Cron = ""
	r.Options.Duration = ""
	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: before.Format(layout),
			End:   after.Format(layout),
		},
	}
	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		require.NoError(t, rs.Start())
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, state, RuleStarted)
	}()
	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: before.Format(layout),
			End:   before.Format(layout),
		},
	}
	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		require.NoError(t, rs.Start())
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, state, RuleTerminated)
	}()

	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: after.Format(layout),
			End:   after.Format(layout),
		},
	}
	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		require.NoError(t, rs.Start())
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, state, RuleWait)
	}()

	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: before.Format(layout),
			End:   after.Format(layout),
		},
	}
	func() {
		rs, err := NewRuleState(r)
		require.NoError(t, err)
		require.NoError(t, rs.Start())
		time.Sleep(500 * time.Millisecond)
		state, err := rs.GetState()
		require.NoError(t, err)
		require.Equal(t, state, RuleStarted)

		require.NoError(t, rs.Stop())
		time.Sleep(500 * time.Millisecond)
		state, err = rs.GetState()
		require.NoError(t, err)
		require.Equal(t, state, RuleStopped)
	}()
}

func TestRuleStateInternalStop(t *testing.T) {
	conf.IsTesting = true
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (TYPE="neuron", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	r := &api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	}
	r.Options.Cron = "123"
	rs, err := NewRuleState(r)
	require.NoError(t, err)
	err = rs.InternalStop()
	require.Error(t, err)

	r.Options.Cron = ""
	r.Options.Duration = ""
	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: layout,
			End:   layout,
		},
	}
	rs, err = NewRuleState(r)
	require.NoError(t, err)
	err = rs.InternalStop()
	require.NoError(t, err)
	require.Equal(t, rs.triggered, 2)
}
