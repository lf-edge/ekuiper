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

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func init() {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	store.SetupDefault(dataDir)
	c := components["portable"]
	c.register()
	function.Initialize(entries)
	io.Initialize(entries)
}

func getRuleProcessor() *processor.RuleProcessor {
	if ruleProcessor == nil {
		ruleProcessor = processor.NewRuleProcessor()
	}
	return ruleProcessor
}

func getStreamProcessor() *processor.StreamProcessor {
	if streamProcessor == nil {
		streamProcessor = processor.NewStreamProcessor()
	}
	return streamProcessor
}

func TestCheckBeforeDrop(t *testing.T) {
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/MockPortableFunc", "return(true)")
	defer failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/MockPortableFunc")
	dropData()
	prepareData(t)
	ref, err := checkPluginSource("pyjson")
	require.NoError(t, err)
	require.True(t, ref)

	ref, err = checkPluginSink("print")
	require.NoError(t, err)
	require.True(t, ref)

	ref, err = checkPluginFunction("pyrevert")
	require.NoError(t, err)
	require.True(t, ref)

	dropData()
	ref, err = checkPluginSource("pyjson")
	require.NoError(t, err)
	require.False(t, ref)

	ref, err = checkPluginSink("print")
	require.NoError(t, err)
	require.False(t, ref)

	ref, err = checkPluginFunction("pyrevert")
	require.NoError(t, err)
	require.False(t, ref)
}

func prepareData(t *testing.T) {
	pi := &portable.PluginInfo{
		Sources:   []string{"pyjson"},
		Sinks:     []string{"print"},
		Functions: []string{"pyrevert"},
	}
	portableManager.RegisterForTest("pytest", pi)
	s := getStreamProcessor()
	info, err := s.ExecStreamSql(`create stream pyjson () WITH (TYPE="pyjson",FORMAT="JSON")`)
	require.NoError(t, err)
	require.NotNil(t, info)
	_, err = createRule("rule", `{"id":"rule","sql":"SELECT pyrevert(a) from pyjson","triggered":false,"actions":[{"print":{}}]}`)
	require.NoError(t, err)
	_, err = getRuleStatus("rule")
	require.NoError(t, err)
}

func dropData() {
	s := getStreamProcessor()
	deleteRule("rule")
	getRuleProcessor().ExecDrop("rule")
	s.DropStream("pyjson", ast.TypeStream)
}
