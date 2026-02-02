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
	"strconv"
	"testing"
	"time"

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
				Duration: "10s",
			},
			action:    doStop,
			startTime: now.Add(-time.Hour),
			state:     rule.Running,
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
