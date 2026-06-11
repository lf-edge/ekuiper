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

//go:build duckdb

package sql

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestDuckDBSinkCollect(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")

	// File-mode URL. Parse it through the SAME path the sink uses so the
	// setup connection and the sink open the identical database file.
	dbPath := filepath.Join(t.TempDir(), "test.db")
	dburl := "duckdb://" + dbPath
	driver, dsn, err := client.ParseDBUrl(dburl)
	require.NoError(t, err)
	require.Equal(t, "duckdb", driver)

	// The sink only issues INSERT; create the target table up front.
	setup, err := sql.Open("duckdb", dsn)
	require.NoError(t, err)
	_, err = setup.Exec("CREATE TABLE t (a INTEGER, b INTEGER)")
	require.NoError(t, err)
	require.NoError(t, setup.Close())

	// Drive the sink with a single insert.
	sink := &SQLSinkConnector{}
	require.NoError(t, sink.Provision(ctx, map[string]any{
		"dburl":  dburl,
		"table":  "t",
		"fields": []string{"a", "b"},
	}))
	sink.Consume(map[string]any{})
	require.NoError(t, sink.Connect(ctx, func(string, string) {}))
	require.NoError(t, sink.collect(ctx, map[string]any{"a": 1, "b": 2}))

	// Reopen the file and confirm the row landed.
	verify, err := sql.Open("duckdb", dsn)
	require.NoError(t, err)
	rows, err := verify.Query("SELECT a, b FROM t WHERE a = 1 AND b = 2")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		var a, b int
		require.NoError(t, rows.Scan(&a, &b))
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
		count++
	}
	require.NoError(t, rows.Close())
	require.NoError(t, verify.Close())
	require.NoError(t, sink.Close(ctx))
	require.Equal(t, 1, count)
}
