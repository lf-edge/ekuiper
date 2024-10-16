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
	"fmt"
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
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.index,aliasRef:Call:{ name:row_number }, stream.a ]"}
	{"op":"WindowFuncPlan_1","info":"windowFuncField:{name:index, expr:$$alias.index,aliasRef:Call:{ name:row_number }}"}
			{"op":"DataSourcePlan_2","info":"StreamName: stream, StreamFields:[ a ]"}`,
		},
		{
			sql: `select a as c from stream group by countwindow(2)`,
			explain: `{"op":"ProjectPlan_0","info":"Fields:[ $$alias.c,aliasRef:sharedStream.a ]"}
	{"op":"WindowPlan_1","info":"{ length:2, windowType:COUNT_WINDOW, limit: 0 }"}
			{"op":"ProjectPlan_2","info":"Fields:[ sharedStream.a ]"}
					{"op":"DataSourcePlan_3","info":"StreamName: sharedStream, StreamFields:[ a ]"}`,
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
		fmt.Println(explain)
		//require.Equal(t, tc.explain, explain)
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
