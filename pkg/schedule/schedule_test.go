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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsRuleInRunningSchedule(t *testing.T) {
	now, err := time.Parse(layout, "2006-01-02 15:04:01")
	require.NoError(t, err)
	d, err := time.ParseDuration("2s")
	require.NoError(t, err)
	isInSchedule, remainedDuration, err := IsInRunningSchedule("4 15 * * *", now, d)
	require.NoError(t, err)
	require.True(t, isInSchedule)
	require.Equal(t, remainedDuration, time.Second)
}

func TestIsInScheduleRange(t *testing.T) {
	now, err := time.Parse(layout, "2006-01-02 15:04:01")
	require.NoError(t, err)
	_, err = IsInScheduleRange(now, "", "")
	require.Error(t, err)
	_, err = IsInScheduleRange(now, "2006-01-02 15:04:01", "")
	require.Error(t, err)
	isIn, err := IsInScheduleRange(now, "2006-01-02 15:04:00", "2006-01-02 15:04:03")
	require.True(t, isIn)
	isIn, err = IsInScheduleRange(now, "2006-01-02 15:05:00", "2006-01-02 15:05:03")
	require.False(t, isIn)
}

func TestIsAfterTimeRange(t *testing.T) {
	now, err := time.Parse(layout, "2006-01-02 15:04:01")
	require.NoError(t, err)
	_, err = IsAfterTimeRange(now, "")
	require.Error(t, err)
	isAfter, err := IsAfterTimeRange(now, "2006-01-02 15:04:00")
	require.True(t, isAfter)
	isAfter, err = IsAfterTimeRange(now, "2006-01-02 15:04:06")
	require.False(t, isAfter)
}

func TestIsInRunningSchedule(t *testing.T) {
	now, err := time.Parse(layout, "2006-01-02 15:04:02")
	require.NoError(t, err)
	_, _, err = IsInRunningSchedule("", now, time.Second)
	require.Error(t, err)
	isIn, _, err := IsInRunningSchedule("4 15 * * *", now, 3*time.Second)
	require.NoError(t, err)
	require.True(t, isIn)
	isIn, _, err = IsInRunningSchedule("4 15 * * *", now, time.Second)
	require.NoError(t, err)
	require.False(t, isIn)
}
