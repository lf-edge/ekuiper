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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestStmtValidate(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 () WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		err = kv.Set(name, string(s))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(kv, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	tests := []struct {
		sql string
	}{
		{
			sql: "select a[2:1] from src1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := xsql.GetStatementFromSql(tt.sql)
			require.NoError(t, err)
			require.Error(t, validateStmt(stmt))
		})
	}
}
