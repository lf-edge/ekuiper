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

package planner

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func TestExplainPlan(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())

	testcases := []struct {
		sql     string
		explain string
	}{
		{
			sql: "select a, row_number() as index from stream",
			explain: `{"type":"ProjectPlan","info":"Fields:[ $$alias.index,Call:{ name:bypass, args:[wf_row_number_1] }, stream.a ]","id":0,"children":[1]}
	{"type":"WindowFuncPlan","info":"windowFuncField:{name:wf_row_number_1, expr:Call:{ name:row_number }}","id":1,"children":[2]}
			{"type":"DataSourcePlan","info":"StreamName: stream, StreamFields:[ a ]","id":2}`,
		},
	}
	for _, tt := range testcases {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		require.NoError(t, err)
		p, err := createLogicalPlan(stmt, &def.RuleOption{
			Qos: 0,
		}, kv)
		require.NoError(t, err)
		explain, err := ExplainFromLogicalPlan(p, "")
		require.NoError(t, err)
		fmt.Println(explain)
	}
}
