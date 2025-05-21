// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/Rookiecom/cpuprofile"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

func TestHandleScheduleRule(t *testing.T) {
	defer func() {
		cast.SetTimeZone(cast.GetConfiguredTimeZone().String())
	}()
	err := cast.SetTimeZone("UTC")
	require.NoError(t, err)
	now, err := time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
	require.NoError(t, err)
	now = now.In(cast.GetConfiguredTimeZone())
	testcases := []struct {
		Options   *def.RuleOption
		startTime time.Time
		state     rule.RunState
		action    scheduleRuleAction
	}{
		{
			Options: &def.RuleOption{
				Cron:     "",
				Duration: "",
				CronDatetimeRange: []schedule.DatetimeRange{
					{
						Begin: "2006-01-02 15:04:01",
						End:   "2006-01-02 15:04:06",
					},
				},
			},
			action: scheduleRuleActionStart,
		},
		{
			Options: &def.RuleOption{
				Cron:     "",
				Duration: "",
				CronDatetimeRange: []schedule.DatetimeRange{
					{
						Begin: "2006-01-02 15:04:01",
						End:   "2006-01-02 15:04:06",
					},
				},
			},
			action: scheduleRuleActionStart,
		},
		{
			Options: &def.RuleOption{
				Cron:     "",
				Duration: "",
				CronDatetimeRange: []schedule.DatetimeRange{
					{
						Begin: "2006-01-02 15:04:01",
						End:   "2006-01-02 15:04:04",
					},
				},
			},
			action: scheduleRuleActionStop,
		},
		{
			Options: &def.RuleOption{
				Cron:     "",
				Duration: "",
				CronDatetimeRange: []schedule.DatetimeRange{
					{
						Begin: "2006-01-02 15:04:01",
						End:   "2006-01-02 15:04:04",
					},
				},
			},
			action: scheduleRuleActionStop,
		},
		{
			Options: &def.RuleOption{
				Cron:     "4 15 * * *",
				Duration: "10s",
			},
			action: scheduleRuleActionStart,
		},
		{
			Options: &def.RuleOption{
				Cron:     "4 15 * * *",
				Duration: "1s",
			},
			action: scheduleRuleActionStop,
		},
		{
			Options: &def.RuleOption{
				Cron:     "4 15 * * *",
				Duration: "10s",
				CronDatetimeRange: []schedule.DatetimeRange{
					{
						Begin: "2006-01-02 15:04:01",
						End:   "2006-01-02 15:04:06",
					},
				},
			},
			action: scheduleRuleActionStart,
		},
		{
			Options: &def.RuleOption{
				Cron:     "4 15 * * *",
				Duration: "10s",
				CronDatetimeRange: []schedule.DatetimeRange{
					{
						Begin: "2006-01-02 15:04:01",
						End:   "2006-01-02 15:04:02",
					},
				},
			},
			action: scheduleRuleActionStop,
		},
		{
			Options: nil,
			action:  scheduleRuleActionDoNothing,
		},
		{
			Options: &def.RuleOption{
				CronDatetimeRange: []schedule.DatetimeRange{
					{
						Begin: "1332##2",
						End:   "25344@@@",
					},
				},
			},
			action: scheduleRuleActionDoNothing,
		},
		{
			Options: &def.RuleOption{
				Cron:     "###",
				Duration: "###",
			},
			action: scheduleRuleActionDoNothing,
		},
		{
			Options: &def.RuleOption{
				Cron:     "",
				Duration: "10s",
			},
			action: scheduleRuleActionDoNothing,
		},
	}
	for i, tc := range testcases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := &def.Rule{
				Triggered: true,
				Options:   tc.Options,
			}
			rw := ruleWrapper{
				rule:      r,
				state:     tc.state,
				startTime: tc.startTime,
			}
			scheduleRuleSignal := handleScheduleRule(now, rw)
			require.Equal(t, tc.action, scheduleRuleSignal, fmt.Sprintf("case %v", i))
		})
	}
}

func TestRunScheduleRuleChecker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go runScheduleRuleCheckerByInterval(3*time.Second, ctx)
	time.Sleep(1 * time.Second)
	cancel()
}

type testProfile struct{}

func (test *testProfile) StartCPUProfiler(ctx context.Context, t time.Duration) error {
	return nil
}

func (test *testProfile) EnableWindowAggregator(window int) {
	return
}

func (test *testProfile) GetWindowData() cpuprofile.DataSetAggregateMap {
	return cpuprofile.DataSetAggregateMap{}
}

func (test *testProfile) RegisterTag(tag string, ch chan *cpuprofile.DataSetAggregate) {
	return
}

func TestStartCPUProfiling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ekuiperProfiler := &ekuiperProfile{}
	if err := ekuiperProfiler.StartCPUProfiler(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	ekuiperProfiler.EnableWindowAggregator(5)
	if windowData := ekuiperProfiler.GetWindowData(); windowData == nil {
		t.Fatal("cpu profiling windowData is nil")
	}
	go func(ctx context.Context) {
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, pprof.Labels("rule", "test"))
		pprof.SetGoroutineLabels(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Simulate some work
				for i := 0; i < 1000; i++ {
					_ = i * i
				}
			}
		}
	}(ctx)
	recvCh := make(chan *cpuprofile.DataSetAggregate)
	ekuiperProfiler.RegisterTag("rule", recvCh)
	select {
	case recvData := <-recvCh:
		if recvData == nil {
			t.Fatal("cpu profiling recvData is nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for cpu profiling recvData")
	}

	profiler := &testProfile{}
	err := StartCPUProfiling(ctx, profiler)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	cancel()
}
