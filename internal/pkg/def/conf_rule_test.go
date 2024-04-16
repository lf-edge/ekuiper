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

package def

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsLongRunningScheduleRule(t *testing.T) {
	r := &Rule{}
	require.False(t, r.IsLongRunningScheduleRule())
	r.Options = &RuleOption{
		CronDatetimeRange: []DatetimeRange{
			{
				Begin: "1",
				End:   "2",
			},
		},
	}
	require.True(t, r.IsLongRunningScheduleRule())
	r.Options.Cron = "123"
	r.Options.Duration = "123"
	require.False(t, r.IsLongRunningScheduleRule())
}
