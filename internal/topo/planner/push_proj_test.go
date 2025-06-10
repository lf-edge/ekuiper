// Copyright 2025 EMQ Technologies Co., Ltd.
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

package planner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func TestPushProjection(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := `select a as c from sharedStream group by countwindow(2)`
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := createLogicalPlan(stmt, &def.RuleOption{
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	explain, err := ExplainFromLogicalPlan(p, "")
	require.NoError(t, err)
	expect := `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.c,aliasRef:sharedStream.a ]"}
	{"op":"WindowPlan_1","info":"{ length:2, windowType:COUNT_WINDOW, limit: 0 }"}
			{"op":"ProjectPlan_2","info":"Fields:[ sharedStream.a ]"}
					{"op":"DataSourcePlan_3","info":"StreamName: sharedStream, StreamFields:[ a ]"}`
	require.Equal(t, strings.TrimPrefix(expect, "\n"), explain)
}

func TestPushProjectionDisable(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := `select a as c from sharedStream group by countwindow(2)`
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	p, err := createLogicalPlan(stmt, &def.RuleOption{
		Qos: 0,
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			OptimizeControl: &def.OptimizeControl{
				DisableOptimizeRules: []string{"push_projection"},
			},
		},
	}, kv)
	require.NoError(t, err)
	explain, err := ExplainFromLogicalPlan(p, "")
	require.NoError(t, err)
	expect := `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.c,aliasRef:sharedStream.a ]"}
	{"op":"WindowPlan_1","info":"{ length:2, windowType:COUNT_WINDOW, limit: 0 }"}
			{"op":"DataSourcePlan_2","info":"StreamName: sharedStream, StreamFields:[ a ]"}`
	require.Equal(t, strings.TrimPrefix(expect, "\n"), explain)
}
