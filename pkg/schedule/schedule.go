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

package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

const layout = "2006-01-02 15:04:05"

func IsInScheduleRanges(now time.Time, timeRanges []api.DatetimeRange) (bool, error) {
	for _, tRange := range timeRanges {
		isIn, err := IsInScheduleRange(now, tRange.Begin, tRange.End)
		if err != nil {
			return false, err
		}
		if isIn {
			return true, nil
		}
	}
	return false, nil
}

func IsInScheduleRange(now time.Time, start string, end string) (bool, error) {
	return isInTimeRange(now, start, end)
}

func isInTimeRange(now time.Time, start string, end string) (bool, error) {
	s, err := cast.InterfaceToTime(start, layout)
	if err != nil {
		return false, err
	}
	e, err := cast.InterfaceToTime(end, layout)
	if err != nil {
		return false, err
	}
	isBefore := s.Before(now)
	isAfter := e.After(now)
	if isBefore && isAfter {
		return true, nil
	}
	return false, nil
}

func IsAfterTimeRanges(now time.Time, ranges []api.DatetimeRange) bool {
	if len(ranges) < 1 {
		return false
	}
	for _, r := range ranges {
		isAfter, err := IsAfterTimeRange(now, r.End)
		if err != nil || !isAfter {
			return false
		}
	}
	return true
}

func IsAfterTimeRange(now time.Time, end string) (bool, error) {
	e, err := time.Parse(layout, end)
	if err != nil {
		return false, err
	}
	return now.After(e), nil
}

// IsInRunningSchedule checks whether the rule should be running, eg:
// If the duration is 10min, and cron is "0 0 * * *", and the current time is 00:00:02
// And the rule should be started immediately instead of checking it on the next day.
func IsInRunningSchedule(cronExpr string, now time.Time, d time.Duration) (bool, time.Duration, error) {
	s, err := cron.ParseStandard(cronExpr)
	if err != nil {
		return false, 0, err
	}
	previousSchedule := s.Next(now.Add(-d))
	if now.After(previousSchedule) && now.Before(previousSchedule.Add(d)) {
		return true, previousSchedule.Add(d).Sub(now), nil
	}
	return false, 0, nil
}

func ValidateRanges(ranges []api.DatetimeRange) error {
	if len(ranges) < 1 {
		return nil
	}
	for _, r := range ranges {
		if err := validateRange(r); err != nil {
			return err
		}
	}
	return nil
}

func validateRange(r api.DatetimeRange) error {
	s, err := cast.InterfaceToTime(r.Begin, layout)
	if err != nil {
		return err
	}
	e, err := cast.InterfaceToTime(r.End, layout)
	if err != nil {
		return err
	}
	if s.After(e) {
		return fmt.Errorf("begin time shouldn't after end time")
	}
	return nil
}
