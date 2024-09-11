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

package schedule

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsInScheduleRanges(t *testing.T) {
	now, err := time.Parse(layout, "2006-01-02 15:04:01")
	require.NoError(t, err)
	testcases := []struct {
		dateRanges []DatetimeRange
		isIn       bool
	}{
		{
			dateRanges: []DatetimeRange{
				{
					Begin: "1999-01-02 15:04:00",
					End:   "3006-01-02 15:04:03",
				},
			},
			isIn: true,
		},
		{
			dateRanges: []DatetimeRange{
				{
					Begin: "1999-01-02 15:04:00",
					End:   "1999-01-02 15:04:03",
				},
			},
			isIn: false,
		},
		{
			dateRanges: []DatetimeRange{
				{
					Begin: "2999-01-02 15:04:00",
					End:   "2999-01-02 15:04:03",
				},
			},
			isIn: false,
		},
		{
			dateRanges: []DatetimeRange{
				{
					BeginTimestamp: 1,
					EndTimestamp:   2797598229000,
				},
			},
			isIn: true,
		},
		{
			dateRanges: []DatetimeRange{
				{
					BeginTimestamp: 1,
					EndTimestamp:   2,
				},
			},
			isIn: false,
		},
		{
			dateRanges: []DatetimeRange{
				{
					BeginTimestamp: 2697598229000,
					EndTimestamp:   2797598229000,
				},
			},
			isIn: false,
		},
	}

	for _, tc := range testcases {
		isIn, err := IsInScheduleRanges(now, tc.dateRanges)
		require.NoError(t, err)
		require.Equal(t, tc.isIn, isIn)
	}
}

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

func TestIsAfterTimeRanges(t *testing.T) {
	now, err := time.Parse(layout, "2006-01-02 15:04:01")
	require.NoError(t, err)
	testcases := []struct {
		dateRanges []DatetimeRange
		isAfter    bool
	}{
		{
			dateRanges: []DatetimeRange{
				{
					Begin: "",
					End:   "1006-01-02 15:04:00",
				},
			},
			isAfter: true,
		},
		{
			dateRanges: []DatetimeRange{
				{
					Begin: "",
					End:   "3006-01-02 15:04:00",
				},
			},
			isAfter: false,
		},
		{
			dateRanges: []DatetimeRange{
				{
					BeginTimestamp: 1,
					EndTimestamp:   2,
				},
			},
			isAfter: true,
		},
		{
			dateRanges: []DatetimeRange{
				{
					BeginTimestamp: 2797598229000,
					EndTimestamp:   2797598329000,
				},
			},
			isAfter: false,
		},
	}
	for _, tc := range testcases {
		isAfter := IsAfterTimeRanges(now, tc.dateRanges)
		require.Equal(t, tc.isAfter, isAfter)
	}
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

func TestValidateSchedule(t *testing.T) {
	tests := []struct {
		begin string
		end   string
		err   error
	}{
		{
			begin: "123",
			end:   "123",
			err:   errors.New("Can't parse string as time: 123"),
		},
		{
			begin: layout,
			end:   "123",
			err:   errors.New("Can't parse string as time: 123"),
		},
		{
			begin: "2006-01-02 15:04:02",
			end:   "2006-01-02 15:04:01",
			err:   errors.New("begin time shouldn't after end time"),
		},
		{
			begin: "2006-01-02 15:04:00",
			end:   "2006-01-02 15:04:01",
			err:   nil,
		},
	}
	for _, tc := range tests {
		rs := []DatetimeRange{
			{
				Begin: tc.begin,
				End:   tc.end,
			},
		}
		err := ValidateRanges(rs)
		if tc.err != nil {
			require.Equal(t, err, tc.err)
		} else {
			require.NoError(t, err)
		}
	}
}
