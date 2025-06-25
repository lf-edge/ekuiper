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
		{
			sql: `select count(a) from stream group by countwindow(2)`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:bypass, args:[$$default.inc_agg_col_1] } ]"}
	{"op":"IncAggWindowPlan_1","info":"wType:COUNT_WINDOW, funcs:[Call:{ name:inc_count, args:[stream.a] }->inc_agg_col_1]"}
			{"op":"DataSourcePlan_2","info":"StreamName: stream, StreamFields:[ a ]"}`,
		},
		{
			sql: `select count(a),b from stream group by countwindow(2),b`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:bypass, args:[$$default.inc_agg_col_1] }, stream.b ]"}
	{"op":"IncAggWindowPlan_1","info":"wType:COUNT_WINDOW, Dimension:[stream.b], funcs:[Call:{ name:inc_count, args:[stream.a] }->inc_agg_col_1]"}
			{"op":"DataSourcePlan_2","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
		{
			sql: `select count(a),sum(a),b from stream group by countwindow(2),b`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:bypass, args:[$$default.inc_agg_col_1] }, Call:{ name:bypass, args:[$$default.inc_agg_col_2] }, stream.b ]"}
	{"op":"IncAggWindowPlan_1","info":"wType:COUNT_WINDOW, Dimension:[stream.b], funcs:[Call:{ name:inc_count, args:[stream.a] }->inc_agg_col_1,Call:{ name:inc_sum, args:[stream.a] }->inc_agg_col_2]"}
			{"op":"DataSourcePlan_2","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
		{
			sql: `SELECT *,count(*) from stream group by countWindow(4),b having count(*) > 1 `,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ *, Call:{ name:bypass, args:[$$default.inc_agg_col_1] } ]"}
	{"op":"HavingPlan_1","info":"Condition:{ binaryExpr:{ Call:{ name:bypass, args:[$$default.inc_agg_col_2] } > 1 } }, "}
			{"op":"IncAggWindowPlan_2","info":"wType:COUNT_WINDOW, Dimension:[stream.b], funcs:[Call:{ name:inc_count, args:[*] }->inc_agg_col_1,Call:{ name:inc_count, args:[*] }->inc_agg_col_2]"}
					{"op":"DataSourcePlan_3","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
		{
			sql: `SELECT *  from stream group by countWindow(4),b having count(*) > 1 `,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ * ]"}
	{"op":"HavingPlan_1","info":"Condition:{ binaryExpr:{ Call:{ name:count, args:[*] } > 1 } }, "}
			{"op":"AggregatePlan_2","info":"Dimension:{ stream.b }"}
					{"op":"WindowPlan_3","info":"{ length:4, windowType:COUNT_WINDOW, limit: 0 }"}
							{"op":"DataSourcePlan_4","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
		{
			sql: `SELECT *  from stream left join sharedStream group by countWindow(4) having count(*) > 1 `,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ * ]"}
	{"op":"HavingPlan_1","info":"Condition:{ binaryExpr:{ Call:{ name:count, args:[*] } > 1 } }, "}
			{"op":"JoinPlan_2","info":"Joins:[ { joinType:LEFT_JOIN,  } ]"}
					{"op":"WindowPlan_3","info":"{ length:4, windowType:COUNT_WINDOW, limit: 0 }"}
							{"op":"DataSourcePlan_4","info":"StreamName: stream, StreamFields:[ a, b ]"}
							{"op":"DataSourcePlan_5","info":"StreamName: sharedStream, StreamFields:[ a, b ]"}`,
		},
		{
			sql: `SELECT count(*) from stream group by countWindow(4) filter (where a > 1) `,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ Call:{ name:bypass, args:[$$default.inc_agg_col_1] } ]"}
	{"op":"IncAggWindowPlan_1","info":"wType:COUNT_WINDOW, filter:[binaryExpr:{ stream.a > 1 }], funcs:[Call:{ name:inc_count, args:[*] }->inc_agg_col_1]"}
			{"op":"DataSourcePlan_2","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
	}
	for _, tc := range testcases {
		if tc.sql != `select count(a),sum(a),b from stream group by countwindow(2),b` {
			continue
		}
		stmt, err := xsql.NewParser(strings.NewReader(tc.sql)).Parse()
		require.NoError(t, err)
		p, err := createLogicalPlan(stmt, &def.RuleOption{
			PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
				EnableIncrementalWindow: true,
			},
			Qos: 0,
		}, kv)
		require.NoError(t, err)
		explain, err := ExplainFromLogicalPlan(p, "")
		require.NoError(t, err)
		require.Equal(t, tc.explain, explain, tc.sql)
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
		"memlookup": `CREATE TABLE memlookup() WITH (DATASOURCE="topicB", KEY="key" TYPE="memory", KIND="lookup")`,
	}

	types := map[string]ast.StreamType{
		"sharedStream": ast.TypeStream,
		"stream":       ast.TypeStream,
		"memlookup":    ast.TypeTable,
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

func TestSupportedWindowType(t *testing.T) {
	testcases := []struct {
		w  *ast.Window
		ok bool
	}{
		{
			w: &ast.Window{
				WindowType: ast.COUNT_WINDOW,
				Filter:     &ast.Window{},
			},
			ok: true,
		},
		{
			w: &ast.Window{
				WindowType: ast.SLIDING_WINDOW,
			},
			ok: true,
		},
		{
			w: &ast.Window{
				WindowType: ast.SESSION_WINDOW,
			},
			ok: false,
		},
		{
			w: &ast.Window{
				WindowType: ast.TUMBLING_WINDOW,
			},
			ok: true,
		},
		{
			w: &ast.Window{
				WindowType: ast.COUNT_WINDOW,
			},
			ok: true,
		},
		{
			w: &ast.Window{
				WindowType: ast.COUNT_WINDOW,
				Interval: &ast.IntegerLiteral{
					Val: 1,
				},
			},
			ok: false,
		},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.ok, supportedWindowType(tc.w))
	}
}

func TestExplainPushAlias(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())

	testcases := []struct {
		sql     string
		explain string
	}{
		{
			sql: `select a as a1 from stream`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ stream.a1 ]"}
	{"op":"DataSourcePlan_1","info":"StreamName: stream, StreamFields:[ a ], ColAliasMapping:[ a:a1 ]"}`,
		},
		{
			sql: `select a as a1, * from stream`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.a1,aliasRef:stream.a, * ]"}
	{"op":"DataSourcePlan_1","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
	}
	for _, tc := range testcases {
		stmt, err := xsql.NewParser(strings.NewReader(tc.sql)).Parse()
		require.NoError(t, err)
		p, err := createLogicalPlan(stmt, &def.RuleOption{
			PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
				EnableAliasPushdown: true,
			},
			Qos: 0,
		}, kv)
		require.NoError(t, err)
		explain, err := ExplainFromLogicalPlan(p, "")
		require.NoError(t, err)
		require.Equal(t, tc.explain, explain, tc.sql)
	}
}

func TestExplainPredicatePushDown(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	testcases := []struct {
		sql     string
		explain string
	}{
		{
			sql: `SELECT * FROM stream LEFT JOIN memlookup ON memlookup.id = stream.device_id WHERE stream.device_id +1 =memlookup.id `,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ * ]"}
	{"op":"FilterPlan_1","info":"Condition:{ binaryExpr:{ binaryExpr:{ stream.device_id + 1 } = memlookup.id } }, "}
			{"op":"LookupPlan_2","info":"Join:{ joinType:LEFT_JOIN, expr:binaryExpr:{ memlookup.id = stream.device_id } }"}
					{"op":"DataSourcePlan_3","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
		{
			sql: `SELECT * FROM stream LEFT JOIN memlookup ON memlookup.id = stream.device_id WHERE stream.device_id +1`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ * ]"}
	{"op":"LookupPlan_1","info":"Join:{ joinType:LEFT_JOIN, expr:binaryExpr:{ memlookup.id = stream.device_id } }"}
			{"op":"FilterPlan_2","info":"Condition:{ binaryExpr:{ stream.device_id + 1 } }, "}
					{"op":"DataSourcePlan_3","info":"StreamName: stream, StreamFields:[ a, b ]"}`,
		},
	}
	for _, tc := range testcases {
		stmt, err := xsql.NewParser(strings.NewReader(tc.sql)).Parse()
		require.NoError(t, err)
		p, err := createLogicalPlan(stmt, &def.RuleOption{}, kv)
		require.NoError(t, err)
		explain, err := ExplainFromLogicalPlan(p, "")
		require.NoError(t, err)
		require.Equal(t, tc.explain, explain, tc.sql)
	}
}
