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
	"time"

	"github.com/lf-edge/ekuiper/pkg/api"
)

const layout = "2006-01-02 15:04:05"

func IsInScheduleRange(now time.Time, start string, end string) (bool, error) {
	s, err := time.Parse(layout, start)
	if err != nil {
		return false, err
	}
	e, err := time.Parse(layout, end)
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
