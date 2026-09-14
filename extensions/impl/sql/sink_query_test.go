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
	_ "modernc.org/ql/driver"
	_ "modernc.org/sqlite"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
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
	got := buildUpdateSQL("t", []string{"a", "b"}, b,
		map[string]any{"a": 1}, "a", 1)
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
	q = buildUpdateSQL("t", []string{"note"}, b, map[string]any{"note": "x'); DROP TABLE t;--"}, "id", 1)
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
	q = buildDeleteSQL("t", "id", 1, b)
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

func TestSinkDialectCapabilities(t *testing.T) {
	ctx := mockContext.NewMockContext("sink_dialect", "op1")
	// MaxCompute cannot bind: zero-arg inline literals, exactly like before.
	for _, driver := range []string{"maxcompute", "mc"} {
		next, inline := sinkDialect(ctx, driver)
		require.True(t, inline, "driver %s", driver)
		b := &sqlSinkBinder{next: next, inline: inline}
		require.Equal(t, "'O''Brien'", b.bind("O'Brien"))
		require.Equal(t, "7", b.bind(7))
		require.Empty(t, b.args)
	}
	// QL requires numbered binds; everything else binds by default.
	for driver, want := range map[string]string{
		"ql": "$1", "cznic": "$1", "cznicql": "$1",
		"mysql": "?", "postgres": "$1", "sqlserver": "@p1", "oracle": ":1",
	} {
		next, inline := sinkDialect(ctx, driver)
		require.False(t, inline, "driver %s", driver)
		b := &sqlSinkBinder{next: next, inline: inline}
		require.Equal(t, want, b.bind(1), "driver %s", driver)
		require.Equal(t, []any{1}, b.args)
	}
}

func TestSinkMaxComputeInlineSnapshot(t *testing.T) {
	// Byte-identical to the pre-parameterization output: complete SQL text,
	// zero args, so the MaxCompute Exec path passes the query through.
	ctx := mockContext.NewMockContext("sink_mc", "op1")
	next, inline := sinkDialect(ctx, "maxcompute")
	require.True(t, inline)
	newBinder := func() *sqlSinkBinder { return &sqlSinkBinder{next: next, inline: inline} }

	cfg := &sqlSinkConfig{}
	b := newBinder()
	row, err := cfg.buildInsertRow(ctx, b,
		map[string]any{"a": 1, "b": "O'Brien"}, []string{"a", "b"})
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO t (a,b) values (1,'O''Brien');",
		buildInsertSQL("t", []string{"a", "b"}, []string{row}))
	require.Empty(t, b.args)

	b = newBinder()
	got := buildUpdateSQL("t", []string{"b"}, b,
		map[string]any{"b": "x"}, "a", 1)
	require.Equal(t, "UPDATE t SET b='x' WHERE a = 1;", got)
	require.Empty(t, b.args)

	b = newBinder()
	got = buildDeleteSQL("t", "a", "O'Brien", b)
	require.Equal(t, "DELETE FROM t WHERE a = 'O''Brien';", got)
	require.Empty(t, b.args)
}

// TestSinkQLBindEndToEnd proves against a real QL engine that $N binds work
// through database/sql while a bare ? does not, i.e. the old ? default was
// broken for QL and dollarBind fixes it.
func TestSinkQLBindEndToEnd(t *testing.T) {
	db, err := sql.Open("ql-mem", "sink_ql_bind.db")
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("CREATE TABLE t (Qty int, Name string);")
	require.NoError(t, err)
	// Numbered binds as emitted by dollarBind.
	_, err = tx.Exec("INSERT INTO t VALUES ($1, $2), ($3, $4);", 42, "foo", 7, "O'Brien")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	var name string
	require.NoError(t, db.QueryRow("SELECT Name FROM t WHERE Qty == $1;", 7).Scan(&name))
	require.Equal(t, "O'Brien", name)

	// Bare ? is not a valid QL parameter and must fail.
	_, err = db.Exec("INSERT INTO t VALUES (?, ?);", 1, "x")
	require.Error(t, err)
}
