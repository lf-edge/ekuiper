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

package server

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
)

func initProcessor() {
	// sleep to avoid database lock
	time.Sleep(time.Second)
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	store.SetupDefault(dataDir)
	c := components["portable"]
	c.register()
}

func TestCheckBeforeDrop(t *testing.T) {
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/mockRules", "return(true)")
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/topo/mockRules", "return(true)")
	initProcessor()
	ref, err := checkRulePluginSource(mockRuleState(), "pyjson")
	require.NoError(t, err)
	require.True(t, ref)

	ref = checkRulePluginSink(mockRuleState(), "print")
	require.True(t, ref)

	ref = checkRulePluginFunction(mockRuleState(), "pyrevert")
	require.True(t, ref)

	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/mockRules")
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/topo/mockRules")
	ref, err = checkRulePluginSource(mockRuleState(), "pyjson")
	require.NoError(t, err)
	require.False(t, ref)

	ref = checkRulePluginSink(mockRuleState(), "print")
	require.False(t, ref)

	ref = checkRulePluginFunction(mockRuleState(), "pyrevert")
	require.False(t, ref)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/mockRules", "return(true)")
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/topo/mockRules", "return(true)")
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/checkPluginErr", "return(1)")
	_, err = checkPluginBeforeDrop("pytest")
	require.Error(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/checkPluginErr", "return(2)")
	_, err = checkPluginBeforeDrop("pytest")
	require.Error(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/checkPluginErr", "return(3)")
	_, err = checkPluginBeforeDrop("pytest")
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/checkPluginErr")
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/mockRules")
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/topo/mockRules")
}
