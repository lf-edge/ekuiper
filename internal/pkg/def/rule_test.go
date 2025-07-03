// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

func TestIsScheduleRule(t *testing.T) {
	r := GetDefaultRule("1", "2")
	r.Options.Cron = "* * * * *"
	r.Options.Duration = "2s"
	require.True(t, r.IsScheduleRule())
}

func TestIsMemRule(t *testing.T) {
	r := GetDefaultRule("1", "2")
	r.Tags = []string{memRuleTag}
	require.True(t, r.IsMemRule())
	r.Tags = []string{memRuleTag, "2"}
	require.True(t, r.IsMemRule())
	r.Tags = []string{"2"}
	require.False(t, r.IsMemRule())
}
