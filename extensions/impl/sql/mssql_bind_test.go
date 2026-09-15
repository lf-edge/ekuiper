// Copyright 2026 EMQ Technologies Co., Ltd.
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

//go:build (!no_base || sqlserver) && !no_sqlserver

package sql

import (
	"database/sql"
	"testing"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/stretchr/testify/require"

	sqldriver "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/sqldatabase/driver"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

type mssqlTestAlias string

func TestMSSQLBindTransformer(t *testing.T) {
	tf := sqldriver.TransformerFor("sqlserver")
	require.NotNil(t, tf)
	require.NotNil(t, sqldriver.TransformerFor("mssql"))

	// Ordinary strings become VARCHAR, matching the old non-Unicode literal.
	got := tf("O'Brien")
	require.Equal(t, mssql.VarChar("O'Brien"), got)

	// Everything else passes through untouched: only the exact string type
	// converts, never reflection on Kind, valuers or named string aliases.
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	blob := []byte{0x00, 0xff}
	nullStr := sql.NullString{String: "x", Valid: true}
	for _, v := range []any{int64(7), 1.5, true, blob, ts, nullStr, mssqlTestAlias("x")} {
		require.Equal(t, v, tf(v), "type %T must not be converted", v)
	}
}

func TestMSSQLSinkBindsVarChar(t *testing.T) {
	ctx := mockContext.NewMockContext("mssql_sink", "op1")
	s := &SQLSinkConnector{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"dburl": "sqlserver://sa:pw@localhost:1433?database=test",
		"table": "t",
	}))
	b := &sqlSinkBinder{next: s.bindNext, transform: s.bindTransform}
	row, err := s.config.buildInsertRow(ctx, b,
		map[string]any{"a": "O'Brien", "n": 1}, []string{"a", "n"})
	require.NoError(t, err)
	require.Equal(t, "(@p1,@p2)", row)
	require.Len(t, b.args, 2)
	require.Equal(t, mssql.VarChar("O'Brien"), b.args[0])
	require.Equal(t, 1, b.args[1])
}

func TestMSSQLLookupBindsVarChar(t *testing.T) {
	s := &SqlLookupSource{driver: "mssql", table: "t"}
	q, args, err := s.buildGen().buildQuery([]string{"a"}, []string{"a", "b"}, []any{1, "x"})
	require.NoError(t, err)
	require.Equal(t, "SELECT a FROM t WHERE a = @p1 AND b = @p2", q)
	require.Equal(t, 1, args[0])
	require.Equal(t, mssql.VarChar("x"), args[1])
}
