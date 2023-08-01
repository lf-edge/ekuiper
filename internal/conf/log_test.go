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

package conf

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogOutdated(t *testing.T) {
	now, err := time.Parse("2006-01-02_15-04-05", "2023-06-29_12-00-00")
	require.NoError(t, err)
	maxDuration := 24 * time.Hour
	testcases := []struct {
		name   string
		remove bool
	}{
		{
			name:   "stream.log",
			remove: false,
		},
		{
			name:   "stream.log.2023-06-20_00-00-00",
			remove: true,
		},
		{
			name:   "stream.log.2023-06-29_00-00-00",
			remove: false,
		},
		{
			name:   "stream.log.2023-error",
			remove: false,
		},
		{
			name:   "rule-demo-1.log.2023-06-20_00-00-00",
			remove: true,
		},
		{
			name:   "rule-demo-2.log.2023-06-29_00-00-00",
			remove: false,
		},
		{
			name:   "rule-demo-3.log.2023-error",
			remove: false,
		},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.remove, isLogOutdated(tc.name, now, maxDuration))
	}
}
