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
	q, args, err := s.buildGen().buildQuery(
		[]string{"a_info", "a_time"},
		[]string{"device_id", "a_time"},
		[]any{123, ts},
	)
	require.NoError(t, err)
	require.Equal(t, "SELECT a_info,a_time FROM device_alarm WHERE `device_id` = ? AND `a_time` = ?", q)
	require.Equal(t, []any{123, ts}, args)
}

func TestParamSQLGenPostgres(t *testing.T) {
	s := &SqlLookupSource{driver: "postgres", table: "device_alarm"}
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	q, args, err := s.buildGen().buildQuery(
		[]string{"a_info", "a_time"},
		[]string{"device_id", "a_time"},
		[]any{123, ts},
	)
	// time.Time must be a bound argument, never inlined as
	// "a_time = 2019-09-19 00:55:15 +0000 UTC"
	require.NoError(t, err)
	require.Equal(t, "SELECT a_info,a_time FROM device_alarm WHERE device_id = $1 AND a_time = $2", q)
	require.Equal(t, []any{123, ts}, args)
}

func TestParamSQLGenSQLServer(t *testing.T) {
	s := &SqlLookupSource{driver: "sqlserver", table: "device_alarm"}
	q, args, err := s.buildGen().buildQuery(
		[]string{"a"}, []string{"a", "b"}, []any{1, "O'Brien"},
	)
	require.NoError(t, err)
	require.Equal(t, "SELECT a FROM device_alarm WHERE a = @p1 AND b = @p2", q)
	require.Equal(t, []any{1, "O'Brien"}, args)
}

func TestParamSQLGenNull(t *testing.T) {
	// A nil lookup key fails loud instead of silently matching rows via
	// IS NULL on a possibly non-unique key.
	for _, driver := range []string{"mysql", "postgres"} {
		s := &SqlLookupSource{driver: driver, table: "t"}
		_, _, err := s.buildGen().buildQuery([]string{"a"}, []string{"a"}, []any{nil})
		require.ErrorContains(t, err, "must not be nil", "driver %s", driver)
	}
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
		{"mymysql", "SELECT a FROM t WHERE `a` = ? AND `b` = ?"},
		// Unknown drivers fall back to bare identifiers with "?" placeholders.
		{"clickhouse", "SELECT a FROM t WHERE a = ? AND b = ?"},
		{"snowflake", "SELECT a FROM t WHERE a = ? AND b = ?"},
	}
	for _, tc := range cases {
		t.Run(tc.driver, func(t *testing.T) {
			s := &SqlLookupSource{driver: tc.driver, table: "t"}
			q, args, err := s.buildGen().buildQuery([]string{"a"}, []string{"a", "b"}, []any{1, 2})
			require.NoError(t, err)
			require.Equal(t, tc.want, q)
			require.Equal(t, []any{1, 2}, args)
		})
	}
}

func TestParamSQLGenSelectAll(t *testing.T) {
	s := &SqlLookupSource{driver: "postgres", table: "t"}
	q, args, err := s.buildGen().buildQuery(nil, []string{"a", "b"}, []any{1, 2})
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM t WHERE a = $1 AND b = $2", q)
	require.Equal(t, []any{1, 2}, args)
}

func TestParamSQLGenValueNeverInlined(t *testing.T) {
	// Regression for #4138: no value may appear in the SQL text, whatever
	// its Go type. Strings with quotes stay as bound args too.
	s := &SqlLookupSource{driver: "postgres", table: "device_alarm"}
	ts := time.Date(2019, 9, 19, 0, 55, 15, 0, time.UTC)
	q, args, err := s.buildGen().buildQuery(
		[]string{"a_info"}, []string{"device_id", "a_time", "note"}, []any{123, ts, "x'); DROP TABLE device_alarm;--"},
	)
	require.NoError(t, err)
	require.NotContains(t, q, "2019-09-19")
	require.NotContains(t, q, "DROP TABLE")
	require.NotContains(t, q, "O'Brien")
	require.Equal(t, []any{123, ts, "x'); DROP TABLE device_alarm;--"}, args)
	for _, k := range []string{"device_id", "a_time", "note"} {
		require.Contains(t, q, k)
	}
}

func TestParamSQLGenRejectsUnsafeIdentifiers(t *testing.T) {
	// Bare-identifier dialects (postgres/sqlserver/oracle/unknown) have no
	// identifier-escaping mechanism, so keys must match the sink-side
	// allowlist [A-Za-z_][A-Za-z0-9_]* and anything else errors out instead
	// of reaching SQL text.
	for _, driver := range []string{"postgres", "sqlserver", "oracle", "clickhouse"} {
		s := &SqlLookupSource{driver: driver, table: "t"}
		for _, key := range []string{
			"a` = 1 OR `1`=`1",
			`a"); DROP TABLE t;--`,
			"device-id",
			"hello world",
			"a b",
			"1a",
		} {
			_, _, err := s.buildGen().buildQuery([]string{"a"}, []string{key}, []any{1})
			require.Error(t, err, "driver %s key %q", driver, key)
			require.Contains(t, err.Error(), "invalid lookup key name")
		}
		// Empty keys are rejected in every dialect with a dedicated error.
		_, _, err := s.buildGen().buildQuery([]string{"a"}, []string{""}, []any{1})
		require.ErrorContains(t, err, "must not be empty")
		_, _, err = s.buildGen().buildQuery([]string{"a`b"}, []string{"a"}, []any{1})
		require.Error(t, err)
	}
}

func TestParamSQLGenBacktickQuotedKey(t *testing.T) {
	// Regression: eKuiper backtick-quoted identifiers (lexical.go
	// ScanBackquoteIdent) arrive with backticks stripped, e.g. a rule joining
	// ON lookup.`device-id` yields key "device-id". The mysql/sqlite path
	// must re-quote (escaping embedded backticks) rather than reject.
	s := &SqlLookupSource{driver: "mysql", table: "device_alarm"}
	q, args, err := s.buildGen().buildQuery(
		[]string{"a_info"}, []string{"device_id", "device-id"}, []any{1, 2},
	)
	require.NoError(t, err)
	require.Equal(t, "SELECT a_info FROM device_alarm WHERE `device_id` = ? AND `device-id` = ?", q)
	require.Equal(t, []any{1, 2}, args)

	q, args, err = s.buildGen().buildQuery([]string{"a"}, []string{"we`ird"}, []any{1})
	require.NoError(t, err)
	require.Equal(t, "SELECT a FROM device_alarm WHERE `we``ird` = ?", q)
	require.Equal(t, []any{1}, args)

	// Empty keys/fields are never valid.
	_, _, err = s.buildGen().buildQuery([]string{"a"}, nil, nil)
	require.Error(t, err)
	_, _, err = s.buildGen().buildQuery([]string{"a"}, []string{""}, []any{1})
	require.ErrorContains(t, err, "must not be empty")
	_, _, err = s.buildGen().buildQuery([]string{""}, []string{"a"}, []any{1})
	require.Error(t, err)
}

// TestParamSQLGenHyphenKeySQLiteRoundTrip proves the backtick-quoted join
// key scenario end to end: column "device-id" can be looked up by key.
func TestParamSQLGenHyphenKeySQLiteRoundTrip(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (`device-id` INTEGER, a_info TEXT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t (`device-id`, a_info) VALUES (?, ?)", 7, "x")
	require.NoError(t, err)

	s := &SqlLookupSource{driver: "sqlite", table: "t"}
	q, args, err := s.buildGen().buildQuery([]string{"a_info"}, []string{"device-id"}, []any{7})
	require.NoError(t, err)
	require.Equal(t, "SELECT a_info FROM t WHERE `device-id` = ?", q)

	rows, err := db.Query(q, args...)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "expected one matching row for hyphenated key")
	var info string
	require.NoError(t, rows.Scan(&info))
	require.Equal(t, "x", info)
	require.False(t, rows.Next())
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
	q, args, err := s.buildGen().buildQuery(
		[]string{"a_info", "a_time"},
		[]string{"device_id", "a_time"},
		[]any{123, ts},
	)
	require.NoError(t, err)
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
