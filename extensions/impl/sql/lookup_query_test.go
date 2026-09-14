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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestParamSQLGenMySQL(t *testing.T) {
	s := &SqlLookupSource{driver: "mysql", table: "device_alarm"}
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	q, args := s.buildGen().buildQuery(
		[]string{"a_info", "a_time"},
		[]string{"device_id", "a_time"},
		[]any{123, ts},
	)
	require.Equal(t, "SELECT a_info,a_time FROM device_alarm WHERE `device_id` = ? AND `a_time` = ?", q)
	require.Equal(t, []any{123, ts}, args)
}

func TestParamSQLGenPostgres(t *testing.T) {
	s := &SqlLookupSource{driver: "postgres", table: "device_alarm"}
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	q, args := s.buildGen().buildQuery(
		[]string{"a_info", "a_time"},
		[]string{"device_id", "a_time"},
		[]any{123, ts},
	)
	// time.Time must be a bound argument, never inlined as
	// "a_time = 2019-09-19 00:55:15 +0000 UTC"
	require.Equal(t, "SELECT a_info,a_time FROM device_alarm WHERE device_id = $1 AND a_time = $2", q)
	require.Equal(t, []any{123, ts}, args)
}

func TestParamSQLGenSQLServer(t *testing.T) {
	s := &SqlLookupSource{driver: "sqlserver", table: "device_alarm"}
	q, args := s.buildGen().buildQuery(
		[]string{"a"}, []string{"a", "b"}, []any{1, "O'Brien"},
	)
	require.Equal(t, "SELECT a FROM device_alarm WHERE a = @p1 AND b = @p2", q)
	require.Equal(t, []any{1, "O'Brien"}, args)
}

func TestParamSQLGenNull(t *testing.T) {
	s := &SqlLookupSource{driver: "mysql", table: "t"}
	q, args := s.buildGen().buildQuery([]string{"a"}, []string{"a"}, []any{nil})
	require.Equal(t, "SELECT a FROM t WHERE `a` IS NULL", q)
	require.Empty(t, args)
}

func TestParamSQLGenDialects(t *testing.T) {
	cases := []struct {
		driver string
		want   string
	}{
		{"mysql", "SELECT a FROM t WHERE `a` = ? AND `b` = ?"},
		{"sqlite", "SELECT a FROM t WHERE `a` = ? AND `b` = ?"},
		{"sqlite3", "SELECT a FROM t WHERE `a` = ? AND `b` = ?"},
		{"postgres", "SELECT a FROM t WHERE a = $1 AND b = $2"},
		{"postgresql", "SELECT a FROM t WHERE a = $1 AND b = $2"},
		{"pgx", "SELECT a FROM t WHERE a = $1 AND b = $2"},
		{"sqlserver", "SELECT a FROM t WHERE a = @p1 AND b = @p2"},
		{"mssql", "SELECT a FROM t WHERE a = @p1 AND b = @p2"},
		{"oracle", "SELECT a FROM t WHERE a = :1 AND b = :2"},
		{"godror", "SELECT a FROM t WHERE a = :1 AND b = :2"},
	}
	for _, tc := range cases {
		t.Run(tc.driver, func(t *testing.T) {
			s := &SqlLookupSource{driver: tc.driver, table: "t"}
			q, args := s.buildGen().buildQuery([]string{"a"}, []string{"a", "b"}, []any{1, 2})
			require.Equal(t, tc.want, q)
			require.Equal(t, []any{1, 2}, args)
		})
	}
}

func TestParamSQLGenSelectAllAndMixedNull(t *testing.T) {
	s := &SqlLookupSource{driver: "postgres", table: "t"}
	q, args := s.buildGen().buildQuery(nil, []string{"a", "b", "c"}, []any{nil, 1, nil})
	require.Equal(t, "SELECT * FROM t WHERE a IS NULL AND b = $1 AND c IS NULL", q)
	require.Equal(t, []any{1}, args)
}

func TestParamSQLGenValueNeverInlined(t *testing.T) {
	// Regression for #4138: no value may appear in the SQL text, whatever
	// its Go type. Strings with quotes stay as bound args too.
	s := &SqlLookupSource{driver: "postgres", table: "device_alarm"}
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	q, args := s.buildGen().buildQuery(
		[]string{"a_info"}, []string{"device_id", "a_time", "note"}, []any{123, ts, "x'); DROP TABLE device_alarm;--"},
	)
	require.NotContains(t, q, "2019-09-19")
	require.NotContains(t, q, "DROP TABLE")
	require.NotContains(t, q, "O'Brien")
	require.Equal(t, []any{123, ts, "x'); DROP TABLE device_alarm;--"}, args)
	for _, k := range []string{"device_id", "a_time", "note"} {
		require.Contains(t, q, k)
	}
}

// TestParamSQLGenSQLiteRoundTrip reproduces #4138 end to end without an
// external DB: a timestamp + a single-quote string are bound as arguments
// through database/sql and must match the inserted row.
func TestParamSQLGenSQLiteRoundTrip(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE device_alarm (device_id INTEGER, a_time DATETIME, a_info TEXT)`)
	require.NoError(t, err)
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	_, err = db.Exec(`INSERT INTO device_alarm (device_id, a_time, a_info) VALUES (?, ?, ?)`, 123, ts, "O'Brien")
	require.NoError(t, err)

	s := &SqlLookupSource{driver: "sqlite", table: "device_alarm"}
	q, args := s.buildGen().buildQuery(
		[]string{"a_info", "a_time"},
		[]string{"device_id", "a_time"},
		[]any{123, ts},
	)
	require.True(t, strings.HasSuffix(q, "WHERE `device_id` = ? AND `a_time` = ?"))

	rows, err := db.Query(q, args...)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "expected one matching row for timestamp key")
	var info string
	var got time.Time
	require.NoError(t, rows.Scan(&info, &got))
	require.Equal(t, "O'Brien", info)
	require.True(t, got.Equal(ts), "got %v want %v", got, ts)
	require.False(t, rows.Next())
}
