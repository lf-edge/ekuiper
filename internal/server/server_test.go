// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestHandleScheduleRule(t *testing.T) {
	now, err := time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
	require.NoError(t, err)
	testcases := []struct {
		state   string
		begin   string
		end     string
		toStart bool
		toStop  bool
	}{
		{
			state:   "Running",
			begin:   "2006-01-02 15:04:01",
			end:     "2006-01-02 15:04:06",
			toStart: false,
			toStop:  false,
		},
		{
			state:   "Stopped",
			begin:   "2006-01-02 15:04:01",
			end:     "2006-01-02 15:04:06",
			toStart: true,
			toStop:  false,
		},
		{
			state:   "Stopped",
			begin:   "2006-01-02 15:04:01",
			end:     "2006-01-02 15:04:04",
			toStart: false,
			toStop:  false,
		},
		{
			state:   "Running",
			begin:   "2006-01-02 15:04:01",
			end:     "2006-01-02 15:04:04",
			toStart: false,
			toStop:  true,
		},
	}
	for _, tc := range testcases {
		r := &api.Rule{
			Options: &api.RuleOption{
				Cron:     "",
				Duration: "",
				CronDatetimeRange: []api.DatetimeRange{
					{
						Begin: tc.begin,
						End:   tc.end,
					},
				},
			},
		}
		toStart, toStop := handleScheduleRule(now, r, tc.state)
		require.Equal(t, tc.toStart, toStart)
		require.Equal(t, tc.toStop, toStop)
	}
}

func TestRunScheduleRuleChecker(t *testing.T) {
	exit := make(chan struct{})
	go runScheduleRuleCheckerByInterval(3*time.Second, exit)
	time.Sleep(1 * time.Second)
	exit <- struct{}{}
}

func TestHandleScheduleRuleState(t *testing.T) {
	r := &api.Rule{}
	r.Options = &api.RuleOption{}
	now, err := time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
	require.NoError(t, err)
	require.NoError(t, handleScheduleRuleState(now, r, "Running"))
	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: "2006-01-02 15:04:01",
			End:   "2006-01-02 15:04:06",
		},
	}
	require.NoError(t, handleScheduleRuleState(now, r, "Running"))
	r.Options.CronDatetimeRange = []api.DatetimeRange{
		{
			Begin: "2006-01-02 15:04:01",
			End:   "2006-01-02 15:04:02",
		},
	}
	require.NoError(t, handleScheduleRuleState(now, r, "Running"))
}
