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
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
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
	// isInSchedule indicates the current Rule is in scheduled in backgroundCron
	isInSchedule   bool
	startFailedCnt int

	// only used for test
	cron     string
	duration string
}

func (s *State) isInAllowedTimeRange(now time.Time) (bool, error) {
	return schedule.IsInScheduleRanges(now, s.Rule.Options.CronDatetimeRange)
}

func (s *State) getNextScheduleStartTime() int64 {
	if s.Rule.IsScheduleRule() && len(s.Rule.Options.Cron) > 0 {
		isIn, err := schedule.IsInScheduleRanges(timex.GetNow(), s.Rule.Options.CronDatetimeRange)
		if err == nil && isIn {
			s, err := cron.ParseStandard(s.Rule.Options.Cron)
			if err == nil {
				return s.Next(timex.GetNow()).UnixMilli()
			}
		}
	}
	return 0
}

func (s *State) isInRunningSchedule(now time.Time, d time.Duration) (bool, time.Duration, error) {
	allowed, err := s.isInAllowedTimeRange(now)
	if err != nil {
		return false, 0, err
	}
	if !allowed {
		return false, 0, nil
	}
	cronExpr := s.Rule.Options.Cron
	if strings.HasPrefix(cronExpr, "mock") {
		return false, 0, nil
	}
	return schedule.IsInRunningSchedule(cronExpr, now, d)
}

// registerScheduleRule will register the job in the backgroundCron to run.
// Job will do following 2 things:
// 1. start the Rule in cron if else the job is already stopped
// 2. after the Rule started, start an extract goroutine to stop the Rule after specific duration
func (s *State) registerScheduleRule() error {
	if s.cronState.isInSchedule {
		return fmt.Errorf("Rule %s is already in schedule", s.Rule.Id)
	}
	d, err := time.ParseDuration(s.Rule.Options.Duration)
	if err != nil {
		return err
	}
	var cronCtx context.Context
	cronCtx, s.cronState.cancel = context.WithCancel(context.Background())
	now := timex.GetNow()
	isInRunningSchedule, remainedDuration, err := s.isInRunningSchedule(now, d)
	if err != nil {
		return err
	}
	if isInRunningSchedule {
		if err := s.runScheduleRule(); err != nil {
			return err
		}
		s.stopAfterDuration(remainedDuration, cronCtx)
	}
	entryID, err := backgroundCron.AddFunc(s.Rule.Options.Cron, func() {
		var started bool
		var err error
		if started, err = func() (bool, error) {
			switch backgroundCron.(type) {
			case *MockCron:
				// skip mutex if this is a unit test
			default:
				s.Lock()
				defer s.Unlock()
			}
			now := timex.GetNow()
			allowed, err := s.isInAllowedTimeRange(now)
			if err != nil {
				return false, err
			}
			if !allowed {
				return false, nil
			}

			s.cronState.cron = s.Rule.Options.Cron
			s.cronState.duration = s.Rule.Options.Duration
			return true, s.ScheduleStart()
		}(); err != nil {
			s.Lock()
			s.cronState.startFailedCnt++
			s.Unlock()
			conf.Log.Errorf(err.Error())
			return
		}
		if started {
			s.stopAfterDuration(d, cronCtx)
		}
	})
	if err != nil {
		return err
	}
	s.cronState.isInSchedule = true
	s.cronState.entryID = entryID
	return nil
}

func (s *State) removeScheduleRule() {
	if s.Rule.IsScheduleRule() && s.cronState.isInSchedule {
		s.cronState.isInSchedule = false
		if s.cronState.cancel != nil {
			s.cronState.cancel()
		}
		s.cronState.startFailedCnt = 0
		backgroundCron.Remove(s.cronState.entryID)
	}
}

func (s *State) runScheduleRule() error {
	s.cronState.cron = s.Rule.Options.Cron
	s.cronState.duration = s.Rule.Options.Duration
	return s.ScheduleStart()
}

// stopAfterDuration only for schedule Rule
func (s *State) stopAfterDuration(d time.Duration, cronCtx context.Context) {
	after := time.After(d)
	go func(ctx context.Context) {
		select {
		case <-after:
			s.ScheduleStop()
			return
		case <-cronCtx.Done():
			return
		}
	}(cronCtx)
}
