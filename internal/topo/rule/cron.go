// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"context"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
)

type cronInterface interface {
	Start()
	AddFunc(spec string, cmd func()) (cron.EntryID, error)
	Remove(id cron.EntryID)
}

var backgroundCron cronInterface

func init() {
	if !conf.IsTesting {
		backgroundCron = cron.New()
	} else {
		backgroundCron = &MockCron{}
	}
	backgroundCron.Start()
}

type cronStateCtx struct {
	cancel  context.CancelFunc
	entryID cron.EntryID
	// isInSchedule indicates the current rule is in scheduled in backgroundCron
	isInSchedule   bool
	startFailedCnt int

	// only used for test
	cron     string
	duration string
}

func (rs *RuleState) isInAllowedTimeRange(now time.Time) (bool, error) {
	return schedule.IsInScheduleRanges(now, rs.Rule.Options.CronDatetimeRange)
}

func (rs *RuleState) GetNextScheduleStartTime() int64 {
	if rs.Rule.IsScheduleRule() && len(rs.Rule.Options.Cron) > 0 {
		isIn, err := schedule.IsInScheduleRanges(time.Now(), rs.Rule.Options.CronDatetimeRange)
		if err == nil && isIn {
			s, err := cron.ParseStandard(rs.Rule.Options.Cron)
			if err == nil {
				return s.Next(time.Now()).UnixMilli()
			}
		}
	}
	return 0
}

func (rs *RuleState) GetScheduleTimestamp() (int64, int64, int64) {
	nextStartTimestamp := rs.GetNextScheduleStartTime()
	rs.Lock()
	defer rs.Unlock()
	return rs.lastStartTimestamp, rs.lastStopTimestamp, nextStartTimestamp
}

func (rs *RuleState) isInRunningSchedule(now time.Time, d time.Duration) (bool, time.Duration, error) {
	allowed, err := rs.isInAllowedTimeRange(now)
	if err != nil {
		return false, 0, err
	}
	if !allowed {
		return false, 0, nil
	}
	cronExpr := rs.Rule.Options.Cron
	if strings.HasPrefix(cronExpr, "mock") {
		return false, 0, nil
	}
	return schedule.IsInRunningSchedule(cronExpr, now, d)
}
