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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
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
			sql: `select a, row_number() as index from stream`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.index,aliasRef:Call:{ name:bypass, args:[wf_row_number_1] }, stream.a ]"}
	{"op":"WindowFuncPlan_1","info":"windowFuncField:{name:wf_row_number_1, expr:Call:{ name:row_number }}"}
			{"op":"DataSourcePlan_2","info":"StreamName: stream, StreamFields:[ a ]"}`,
		},
		{
			sql: `select a as c from stream group by countwindow(2)`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.c,aliasRef:stream.a ]"}
	{"op":"WindowPlan_1","info":"{ length:2, windowType:COUNT_WINDOW, limit: 0 }"}
			{"op":"DataSourcePlan_2","info":"StreamName: stream, StreamFields:[ a ]"}`,
		},
		{
			sql: `select row_number() + 1 as d, b from stream group by countwindow(2)`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.d,aliasRef:binaryExpr:{ Call:{ name:bypass, args:[wf_row_number_1] } + 1 }, stream.b ]"}
	{"op":"WindowFuncPlan_1","info":"windowFuncField:{name:wf_row_number_1, expr:Call:{ name:row_number }}"}
			{"op":"WindowPlan_2","info":"{ length:2, windowType:COUNT_WINDOW, limit: 0 }"}
					{"op":"DataSourcePlan_3","info":"StreamName: stream, StreamFields:[ b ]"}`,
		},
	}
	for _, tc := range testcases {
		stmt, err := xsql.NewParser(strings.NewReader(tc.sql)).Parse()
		require.NoError(t, err)
		p, err := createLogicalPlan(stmt, &def.RuleOption{
			Qos: 0,
		}, kv)
		require.NoError(t, err)
		explain, err := ExplainFromLogicalPlan(p, "")
		require.NoError(t, err)
		require.Equal(t, tc.explain, explain)
	}
}

func prepareStream() error {
	kv, err := store.GetKV("stream")
	if err != nil {
		return err
	}
	streamSqls := map[string]string{
		"sharedStream": `CREATE STREAM sharedStream (
					a BIGINT,
					b BIGINT,
				) WITH (DATASOURCE="src1", SHARED="true");`,
		"stream": `CREATE STREAM stream (
					a BIGINT,
					b BIGINT,
				) WITH (DATASOURCE="src1");`,
	}

	types := map[string]ast.StreamType{
		"sharedStream": ast.TypeStream,
		"stream":       ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			return err
		}
		err = kv.Set(name, string(s))
		if err != nil {
			return err
		}
	}
	return nil
}
