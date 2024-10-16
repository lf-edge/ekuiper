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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestPushProjection(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	require.NoError(t, prepareStream())
	sql := `select a as c from sharedStream group by countwindow(2)`
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	assert.NoError(t, err)
	p, err := createLogicalPlan(stmt, &def.RuleOption{
		Qos: 0,
	}, kv)
	require.NoError(t, err)
	explain, err := ExplainFromLogicalPlan(p, "")
	require.NoError(t, err)
	expect := `
{"type":"ProjectPlan","info":"Fields:[ $$alias.c ]","id":0,"children":[1]}
	{"type":"WindowPlan","info":"{ length:2, windowType:COUNT_WINDOW, limit: 0 }","id":1,"children":[2]}
			{"type":"ProjectPlan","info":"Fields:[ sharedStream.a ]","id":2,"children":[3]}
					{"type":"DataSourcePlan","info":"StreamName: sharedStream, StreamFields:[ a ]","id":3}`
	require.Equal(t, strings.TrimPrefix(expect, "\n"), explain)
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
