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

package sql

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSinkBinderNumbering(t *testing.T) {
	// Multi-row batch inserts share one statement, so postgres-style
	// placeholders must keep numbering across rows.
	b := &sqlSinkBinder{next: dollarBind}
	ctx := mockContext.NewMockContext("1", "2")
	cfg := &sqlSinkConfig{}
	r1, err := cfg.buildInsertRow(ctx, b, map[string]any{"a": 1, "b": "x"}, []string{"a", "b"})
	require.NoError(t, err)
	r2, err := cfg.buildInsertRow(ctx, b, map[string]any{"a": 2}, []string{"a", "b"})
	require.NoError(t, err)
	require.Equal(t, "($1,$2)", r1)
	require.Equal(t, "($3,NULL)", r2)
	require.Equal(t, []any{1, "x", 2}, b.args)
	got := buildInsertSQL("t", []string{"a", "b"}, []string{r1, r2})
	require.Equal(t, "INSERT INTO t (a,b) values ($1,$2),($3,NULL);", got)
}

func TestSinkBinderDialects(t *testing.T) {
	cases := []struct {
		driver string
		next   func(i int) string
		want   string
	}{
		{"mysql", qmarkBind, "?"},
		{"postgres", dollarBind, "$1"},
		{"sqlserver", atPBind, "@p1"},
		{"oracle", colonBind, ":1"},
	}
	for _, tc := range cases {
		b := &sqlSinkBinder{next: tc.next}
		require.Equal(t, tc.want, b.bind("O'Brien"))
		require.Equal(t, []any{"O'Brien"}, b.args)
	}
}

func TestSinkUpdateNilValue(t *testing.T) {
	// Missing values keep the historical NULL literal in SET.
	b := &sqlSinkBinder{next: qmarkBind}
	got, err := buildUpdateSQL("t", []string{"a", "b"}, b,
		map[string]any{"a": 1}, "a", 1)
	require.NoError(t, err)
	require.Equal(t, "UPDATE t SET a=?,b=NULL WHERE a = ?;", got)
	require.Equal(t, []any{1, 1}, b.args)
}

// TestSinkSQLiteRoundTrip exercises insert/update/delete built by the sink
// against a real database without an external server, including a time.Time
// value and a single-quote string that the old %v/”-escaping code mishandled.
func TestSinkSQLiteRoundTrip(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, ts DATETIME)`)
	require.NoError(t, err)

	ctx := mockContext.NewMockContext("1", "2")
	cfg := &sqlSinkConfig{}
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)

	// insert
	b := &sqlSinkBinder{next: qmarkBind}
	row, err := cfg.buildInsertRow(ctx, b,
		map[string]any{"id": 1, "note": "O'Brien", "ts": ts},
		[]string{"id", "note", "ts"})
	require.NoError(t, err)
	q := buildInsertSQL("t", []string{"id", "note", "ts"}, []string{row})
	require.Equal(t, "INSERT INTO t (id,note,ts) values (?,?,?);", q)
	_, err = db.Exec(q, b.args...)
	require.NoError(t, err)

	var note string
	var got time.Time
	require.NoError(t, db.QueryRow(`SELECT note, ts FROM t WHERE id = ?`, 1).Scan(&note, &got))
	require.Equal(t, "O'Brien", note)
	require.True(t, got.Equal(ts), "got %v want %v", got, ts)

	// update
	b = &sqlSinkBinder{next: qmarkBind}
	q, err = buildUpdateSQL("t", []string{"note"}, b, map[string]any{"note": "x'); DROP TABLE t;--"}, "id", 1)
	require.NoError(t, err)
	require.Equal(t, "UPDATE t SET note=? WHERE id = ?;", q)
	_, err = db.Exec(q, b.args...)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(`SELECT note FROM t WHERE id = ?`, 1).Scan(&note))
	require.Equal(t, "x'); DROP TABLE t;--", note)

	// table must still exist: the hostile string traveled as an argument
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	require.Equal(t, 1, count)

	// delete
	b = &sqlSinkBinder{next: qmarkBind}
	q, err = buildDeleteSQL("t", "id", 1, b)
	require.NoError(t, err)
	require.Equal(t, "DELETE FROM t WHERE id = ?;", q)
	_, err = db.Exec(q, b.args...)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	require.Equal(t, 0, count)
}

func TestSinkBinderForDriver(t *testing.T) {
	ctx := mockContext.NewMockContext("sink_binder", "op1")
	for driver, want := range map[string]string{
		"mysql": "?", "sqlite": "?", "sqlite3": "?", "mymysql": "?",
		"postgres": "$1", "postgresql": "$1", "pgx": "$1",
		"sqlserver": "@p1", "mssql": "@p1",
		"oracle": ":1", "godror": ":1", "ora": ":1",
		"clickhouse": "?",
	} {
		b := &sqlSinkBinder{next: sinkBinderForDriver(ctx, driver)}
		require.Equal(t, want, b.bind(1), "driver %s", driver)
	}
}

func TestSinkProvisionErrs(t *testing.T) {
	ctx := mockContext.NewMockContext("sink_prov", "op1")
	s := &SQLSinkConnector{}
	require.Error(t, s.Provision(ctx, map[string]any{}))
	require.Error(t, s.Provision(ctx, map[string]any{"dburl": "sqlite:///x.db"}))
	require.Error(t, s.Provision(ctx, map[string]any{
		"dburl": "sqlite:///x.db", "table": "t", "rowKindField": "action",
	}))
	require.Error(t, s.Provision(ctx, map[string]any{
		"dburl": "123", "table": "t",
	}))
	// Undecodable property types fail struct mapping.
	require.Error(t, s.Provision(ctx, map[string]any{
		"dburl": 123, "table": "t",
	}))
}

// TestSinkSqliteConnectorEndToEnd drives Provision/Connect/Collect/
// CollectList/save/Close against a temp-file sqlite database, covering the
// connector paths that builder-only tests cannot reach.
func TestSinkSqliteConnectorEndToEnd(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("sink_e2e", "op1")
	dburl := fmt.Sprintf("sqlite://%s", filepath.Join(t.TempDir(), "e2e.db"))

	newSink := func(props map[string]any) *SQLSinkConnector {
		s := &SQLSinkConnector{}
		require.NoError(t, s.Provision(ctx, props))
		require.NoError(t, s.Connect(ctx, func(string, string) {}))
		return s
	}

	s := newSink(map[string]any{"dburl": dburl, "table": "t"})
	defer s.Close(ctx)
	_, err := s.conn.GetDB().Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, ts DATETIME)`)
	require.NoError(t, err)

	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	require.NoError(t, s.Collect(ctx, &xsql.Tuple{
		Emitter: "test",
		Message: map[string]any{"id": 1, "note": "O'Brien", "ts": ts},
	}))
	var note string
	var got time.Time
	require.NoError(t, s.conn.GetDB().QueryRow(`SELECT note, ts FROM t WHERE id = 1`).Scan(&note, &got))
	require.Equal(t, "O'Brien", note)
	require.True(t, got.Equal(ts))

	// Batch insert; the second row misses "note" and must store NULL.
	require.NoError(t, s.CollectList(ctx, &xsql.TransformedTupleList{
		Maps: []map[string]any{
			{"id": 2, "note": "a", "ts": ts},
			{"id": 3, "ts": ts},
		},
	}))
	var nullNote sql.NullString
	require.NoError(t, s.conn.GetDB().QueryRow(`SELECT note FROM t WHERE id = 3`).Scan(&nullNote))
	require.False(t, nullNote.Valid)

	// Unsafe dynamic key without configured fields must fail before touching SQL.
	require.Error(t, s.collect(ctx, map[string]any{"bad-key": 1}))
	// Same through the Collect wrapper (covers the exception metric path).
	require.Error(t, s.Collect(ctx, &xsql.Tuple{Message: map[string]any{"bad-key": 1}}))
	// Empty item fails in row building.
	require.Error(t, s.collect(ctx, map[string]any{}))

	// writeToDB error path sets needReconnect; breaking the pooled *sql.DB
	// makes the next Exec fail, and Reconnect redials the sqlite file.
	require.NoError(t, s.conn.GetDB().Close())
	require.Error(t, s.Collect(ctx, &xsql.Tuple{Message: map[string]any{"id": 9}}))
	require.True(t, s.needReconnect)
	require.NoError(t, s.Collect(ctx, &xsql.Tuple{Message: map[string]any{"id": 9}}))
	require.False(t, s.needReconnect)

	// Rowkind paths.
	s2 := newSink(map[string]any{
		"dburl": dburl, "table": "t", "fields": []string{"id", "note"},
		"rowKindField": "action", "keyField": "id",
	})
	defer s2.Close(ctx)
	require.NoError(t, s2.collect(ctx, map[string]any{"id": 10, "note": "n", "action": "insert"}))
	require.NoError(t, s2.collect(ctx, map[string]any{"id": 10, "note": "u", "action": "update"}))
	require.NoError(t, s2.conn.GetDB().QueryRow(`SELECT note FROM t WHERE id = 10`).Scan(&note))
	require.Equal(t, "u", note)
	require.NoError(t, s2.collect(ctx, map[string]any{"id": 10, "note": "u", "action": "delete"}))
	var count int
	require.NoError(t, s2.conn.GetDB().QueryRow(`SELECT COUNT(*) FROM t WHERE id = 10`).Scan(&count))
	require.Equal(t, 0, count)
	require.Error(t, s2.collect(ctx, map[string]any{"id": 1, "action": "mock"}))
	require.Error(t, s2.collect(ctx, map[string]any{"id": 1, "action": 123}))
	require.Error(t, s2.collect(ctx, map[string]any{"note": "x", "action": "update"}))
	require.Error(t, s2.collect(ctx, map[string]any{"note": "x", "action": "delete"}))
	// Nil WHERE keys fail loud instead of matching rows via IS NULL.
	require.Error(t, s2.collect(ctx, map[string]any{"id": nil, "note": "x", "action": "update"}))
	require.Error(t, s2.collect(ctx, map[string]any{"id": nil, "note": "x", "action": "delete"}))

	require.NoError(t, s.Ping(ctx, map[string]any{"dburl": dburl, "table": "t"}))
}

func TestSinkSqliteConnectorMisc(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("sink_misc", "op1")
	dburl := fmt.Sprintf("sqlite://%s", filepath.Join(t.TempDir(), "misc.db"))

	// Consume drops sink-only props.
	s := &SQLSinkConnector{}
	props := map[string]any{"fields": []string{"a"}, "table": "t"}
	s.Consume(props)
	require.Equal(t, map[string]any{"table": "t"}, props)

	// Close/CollectList on a rowkind sink: batch goes through save per row.
	s2 := &SQLSinkConnector{}
	require.NoError(t, s2.Provision(ctx, map[string]any{
		"dburl": dburl, "table": "t", "fields": []string{"id", "note"},
		"rowKindField": "action", "keyField": "id",
	}))
	require.NoError(t, s2.Connect(ctx, func(string, string) {}))
	defer s2.Close(ctx)
	_, err := s2.conn.GetDB().Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)`)
	require.NoError(t, err)
	require.NoError(t, s2.CollectList(ctx, &xsql.TransformedTupleList{
		Maps: []map[string]any{
			{"id": 1, "note": "a", "action": "insert"},
			{"id": 1, "note": "b", "action": "update"},
			// A bad row must not abort the batch; the error is only logged.
			{"id": 2, "note": "c", "action": "mock"},
		},
	}))
	var note string
	require.NoError(t, s2.conn.GetDB().QueryRow(`SELECT note FROM t WHERE id = 1`).Scan(&note))
	require.Equal(t, "b", note)

	// Empty batch is a no-op.
	require.NoError(t, s2.CollectList(ctx, &xsql.TransformedTupleList{}))

	// A batch that fails key extraction surfaces the error.
	s3 := &SQLSinkConnector{}
	require.NoError(t, s3.Provision(ctx, map[string]any{
		"dburl": dburl, "table": "t", "rowKindField": "action", "keyField": "id",
	}))
	require.NoError(t, s3.Connect(ctx, func(string, string) {}))
	defer s3.Close(ctx)
	require.Error(t, s3.CollectList(ctx, &xsql.TransformedTupleList{
		Maps: []map[string]any{{"bad-key": 1, "action": "insert"}},
	}))

	// Close on a never-provisioned connector must not panic.
	require.NoError(t, (&SQLSinkConnector{}).Close(ctx))

	// Close without connect; Ping against bad props.
	bare := &SQLSinkConnector{}
	require.NoError(t, bare.Provision(ctx, map[string]any{"dburl": dburl, "table": "t"}))
	require.NoError(t, bare.Close(ctx))
	require.Error(t, bare.Ping(ctx, map[string]any{"dburl": "", "table": "t"}))
}

func TestChunkRows(t *testing.T) {
	require.Equal(t, 100, chunkRows(0, 10, 100))
	require.Equal(t, 100, chunkRows(2000, 0, 100))
	require.Equal(t, 200, chunkRows(2000, 10, 200))
	require.Equal(t, 40, chunkRows(2000, 50, 100))
	require.Equal(t, 7, chunkRows(2000, 10, 7))
	// A single row wider than the limit still goes out alone and loud.
	require.Equal(t, 1, chunkRows(2000, 3000, 5))
	require.Equal(t, 2000, sinkMaxParams("mssql"))
	require.Equal(t, 2000, sinkMaxParams("sqlserver"))
	require.Equal(t, 65000, sinkMaxParams("postgres"))
	require.Equal(t, 65000, sinkMaxParams("postgresql"))
	require.Equal(t, 65000, sinkMaxParams("pgx"))
	require.Equal(t, 65000, sinkMaxParams("mysql"))
	require.Equal(t, 65000, sinkMaxParams("mymysql"))
	require.Equal(t, 32000, sinkMaxParams("sqlite"))
	require.Equal(t, 32000, sinkMaxParams("sqlite3"))
	require.Equal(t, 0, sinkMaxParams("clickhouse"))
}

func TestSinkChunkedBatchEndToEnd(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("sink_chunk", "op1")
	dburl := fmt.Sprintf("sqlite://%s", filepath.Join(t.TempDir(), "chunk.db"))
	s := &SQLSinkConnector{}
	require.NoError(t, s.Provision(ctx, map[string]any{"dburl": dburl, "table": "t"}))
	require.NoError(t, s.Connect(ctx, func(string, string) {}))
	defer s.Close(ctx)
	_, err := s.conn.GetDB().Exec(`CREATE TABLE t (a BIGINT, b BIGINT)`)
	require.NoError(t, err)

	// Force chunking: 2 columns with a 4-param cap allow 2 rows per Exec.
	s.maxParams = 4
	items := make([]map[string]any, 0, 5)
	for i := 0; i < 5; i++ {
		items = append(items, map[string]any{"a": i, "b": i})
	}
	require.NoError(t, s.collectList(ctx, items))
	var count int
	require.NoError(t, s.conn.GetDB().QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	require.Equal(t, 5, count)
}

func TestSinkChunkedTxRollbackEndToEnd(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("sink_tchunk", "op1")
	dburl := fmt.Sprintf("sqlite://%s", filepath.Join(t.TempDir(), "tchunk.db"))
	s := &SQLSinkConnector{}
	require.NoError(t, s.Provision(ctx, map[string]any{"dburl": dburl, "table": "t"}))
	require.NoError(t, s.Connect(ctx, func(string, string) {}))
	defer s.Close(ctx)
	_, err := s.conn.GetDB().Exec(`CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)`)
	require.NoError(t, err)

	// Force 2 chunks (2 columns, 4-param cap): chunk 1 is fully legal, chunk 2
	// violates the primary key. The whole batch must roll back.
	s.maxParams = 4
	err = s.collectList(ctx, []map[string]any{
		{"a": 1, "b": 1},
		{"a": 2, "b": 2},
		{"a": 1, "b": 99},
		{"a": 3, "b": 3},
	})
	require.Error(t, err)
	var count int
	require.NoError(t, s.conn.GetDB().QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	require.Equal(t, 0, count, "chunk 1 rows must have been rolled back")

	// A clean batch through the same tx path commits.
	require.NoError(t, s.collectList(ctx, []map[string]any{
		{"a": 1, "b": 1},
		{"a": 2, "b": 2},
		{"a": 3, "b": 3},
	}))
	require.NoError(t, s.conn.GetDB().QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	require.Equal(t, 3, count)
}

func TestSinkChunkedBuildErrorEndToEnd(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("sink_tbuilderr", "op1")
	dburl := fmt.Sprintf("sqlite://%s", filepath.Join(t.TempDir(), "tbuilderr.db"))
	s := &SQLSinkConnector{}
	require.NoError(t, s.Provision(ctx, map[string]any{"dburl": dburl, "table": "t"}))
	require.NoError(t, s.Connect(ctx, func(string, string) {}))
	defer s.Close(ctx)
	_, err := s.conn.GetDB().Exec(`CREATE TABLE t (a BIGINT, b BIGINT)`)
	require.NoError(t, err)

	// Force 2 chunks with a deterministically bad row in the second one.
	// The build error must surface raw: no reconnect flag, no IO error
	// (hence no sink retry), and nothing reaches the database.
	s.maxParams = 4
	err = s.collectList(ctx, []map[string]any{
		{"a": 1, "b": 1},
		{"a": 2, "b": 2},
		{},
		{"a": 3, "b": 3},
	})
	require.Error(t, err)
	require.False(t, errorx.IsIOError(err))
	require.False(t, s.needReconnect)
	var count int
	require.NoError(t, s.conn.GetDB().QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	require.Equal(t, 0, count)
}
