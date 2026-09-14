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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

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
