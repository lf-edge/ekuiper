// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package tdengine3

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]interface{}
		expected TaosConfig
		error    string
	}{
		{
			name: "default props test",
			conf: map[string]interface{}{
				"database":    "power",
				"table":       "table",
				"tsFieldName": "ts",
			},
			expected: TaosConfig{
				Host:        "localhost",
				Port:        6041,
				User:        "root",
				Password:    "taosdata",
				Database:    "power",
				Table:       "table",
				TsFieldName: "ts",
			},
		},
		{
			name: "fixed table test",
			conf: map[string]interface{}{
				"host":        "192.168.1.1",
				"port":        6042,
				"user":        "test",
				"password":    "pass",
				"database":    "power",
				"table":       "table",
				"tsFieldName": "ts",
			},
			expected: TaosConfig{
				Host:        "192.168.1.1",
				Port:        6042,
				User:        "test",
				Password:    "pass",
				Database:    "power",
				Table:       "table",
				TsFieldName: "ts",
			},
		},
		{
			name: "no database error",
			conf: map[string]interface{}{
				"table":  "t",
				"fields": []string{"f1", "f2"},
			},
			error: "property database is required",
		},
		{
			name: "no table error",
			conf: map[string]interface{}{
				"database": "power",
				"fields":   []string{"f1", "f2"},
			},
			error: "property table is required",
		},
		{
			name: "no TsFieldName error",
			conf: map[string]interface{}{
				"host":     "localhost",
				"port":     6041,
				"database": "db",
				"table":    "t",
				"fields":   []string{"f1", "f2"},
			},
			error: "property TsFieldName is required",
		},
		{
			name: "no tagFields error",
			conf: map[string]interface{}{
				"host":        "localhost",
				"port":        6041,
				"database":    "db",
				"table":       "t",
				"tsFieldName": "ts",
				"fields":      []string{"f1", "f2"},
				"sTable":      "s",
			},
			error: "property tagFields is required when sTable is set",
		},
	}
	ctx := mockContext.NewMockContext("testconfig", "op")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := tdengineSink3{}
			err := ifsink.Provision(ctx, test.conf)
			if test.error == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Equal(t, test.error, err.Error())
				return
			}
			assert.Equal(t, test.expected, *ifsink.cfg)
		})
	}
}

func TestBuildSql(t *testing.T) {
	tests := []struct {
		conf     *TaosConfig
		data     testx.MockTuple
		expected string
		error    string
	}{
		{
			conf: &TaosConfig{
				ProvideTs:   false,
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
			},
			data:  testx.MockTuple{},
			error: "data is empty",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   false,
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"f1": "v1",
				},
			},
			expected: "INSERT INTO t (ts,f1) values (now,\"v1\")",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "t",
				STable:      "st",
				TsFieldName: "ts",
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"k1":   "v1",
					"tag1": "t1",
				},
			},
			error: "timestamp field not found : ts",
		},
		{
			conf: &TaosConfig{
				Database:    "db",
				Table:       "t",
				STable:      "st",
				TsFieldName: "ts",
				Fields:      []string{"ts", "k1", "tag1"},
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"tag1": "t1",
				},
			},
			error: "field not found : k1",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "t",
				STable:      "st",
				TsFieldName: "ts",
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"ts":   1737628594255,
					"k1":   "v1",
					"tag1": "t1",
				},
			},
			expected: "INSERT INTO t (ts,k1) USING st TAGS(\"t1\") values (1737628594255,\"v1\")",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "t",
				STable:      "st",
				Fields:      []string{"ts", "k1", "k2", "tag1", "tag2"},
				TsFieldName: "ts",
				TagFields:   []string{"tag1", "tag2"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"ts":   1737628594255,
					"k1":   "v1",
					"k2":   2,
					"k3":   "v3",
					"tag1": "t1",
					"tag2": 2,
				},
			},
			expected: "INSERT INTO t (ts,k1,k2) USING st TAGS(\"t1\",2) values (1737628594255,\"v1\",2)",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "{{.name}}",
				STable:      "{{.stName}}",
				TsFieldName: "ts",
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"ts":   1737628594255,
					"k1":   123,
					"tag1": "t1",
				},
				Template: map[string]string{"{{.name}}": "t", "{{.stName}}": "st"},
			},
			expected: "INSERT INTO t (ts,k1) USING st TAGS(\"t1\") values (1737628594255,123)",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, test := range tests {
		sql, err := test.conf.buildSql(test.data)
		if !reflect.DeepEqual(test.error, testx.Errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.error, err)
		} else if test.error == "" && !reflect.DeepEqual(test.expected, sql) {
			t.Errorf("%d\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.expected, sql)
		}
	}
}

type streamNumber int

func (streamNumber) String() string { return "1); DROP TABLE t; --" }

func TestStreamValues(t *testing.T) {
	for _, fields := range [][]string{nil, {"v"}} {
		cfg := TaosConfig{Table: "t", STable: "st", TsFieldName: "ts", Fields: fields, TagFields: []string{"tag"}}
		for _, tc := range []struct {
			value any
			want  string
		}{
			{`a\"), (now, "injected'); --`, `"a\\\"), (now, \"injected'); --"`},
			{"line\n\r\t末尾\\", `"line\n\r\t末尾\\"`},
			{nil, "NULL"},
			{true, "true"},
			{int64(-42), "-42"},
			{uint64(42), "42"},
			{streamNumber(7), "7"},
			{float32(1.25), "1.25"},
			{json.Number("1e2"), "1e2"},
		} {
			query, err := cfg.buildSql(testx.MockTuple{Map: map[string]any{"v": tc.value, "tag": tc.value}})
			require.NoError(t, err)
			assert.Equal(t, "INSERT INTO t (ts,v) USING st TAGS("+tc.want+") values (now,"+tc.want+")", query)
		}
		for _, value := range []any{math.NaN(), math.Inf(1), json.Number("1); --"), []byte("payload"), map[string]any{}, "nul\x00"} {
			for _, field := range []string{"v", "tag"} {
				data := map[string]any{"v": 1, "tag": 1}
				data[field] = value
				_, err := cfg.buildSql(testx.MockTuple{Map: data})
				require.Error(t, err, "%s: %v", field, value)
			}
		}
	}
}

func TestStreamTimestamps(t *testing.T) {
	cfg := TaosConfig{Table: "t", TsFieldName: "ts", ProvideTs: true}
	for _, tc := range []struct {
		value any
		want  string
	}{
		{int64(1737628594255), "1737628594255"},
		{"1737628594255", "1737628594255"},
		{json.Number("1737628594255"), "1737628594255"},
		{"2025-01-23T09:16:34.255Z", `"2025-01-23T09:16:34.255Z"`},
		{"2025-01-23 09:16:34.255", `"2025-01-23 09:16:34.255"`},
		{`'2025-01-23 09:16:34.255'`, `"2025-01-23 09:16:34.255"`},
		{`"2025-01-23T09:16:34.255Z"`, `"2025-01-23T09:16:34.255Z"`},
	} {
		query, err := cfg.buildSql(testx.MockTuple{Map: map[string]any{"ts": tc.value, "v": 1}})
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (ts,v) values ("+tc.want+",1)", query)
	}
	for _, value := range []any{
		nil, true, "now", "0, 1); --", "2025-01-23\"), (now", math.NaN(),
		`'2025-01-23 09:16:34.255"`, `'2025-01-23 09:16:34.255`,
		`'2025-01-23 09:16:34.255', 1); --'`, `"2025-01-23T09:16:34.255Z", 1); --"`,
		`''2025-01-23 09:16:34.255''`, `''`, `'now'`,
	} {
		_, err := cfg.buildSql(testx.MockTuple{Map: map[string]any{"ts": value, "v": 1}})
		require.Error(t, err, "%v", value)
	}
}

func TestStreamIdentifiers(t *testing.T) {
	for _, name := range []string{"t", "Db.t_1", "`My table`", "db.`t;--`"} {
		cfg := TaosConfig{Table: "{{.table}}", STable: "{{.stable}}", TsFieldName: "ts", TagFields: []string{"tag"}}
		query, err := cfg.buildSql(testx.MockTuple{Map: map[string]any{"v": 1}, Template: map[string]string{"{{.table}}": name, "{{.stable}}": name}})
		require.NoError(t, err)
		assert.Contains(t, query, "INSERT INTO "+name+" (ts,v) USING "+name)
	}
	for _, name := range []string{"", "t; DROP TABLE t", "t (ts) values(now)--", "`a`b`", "`a\\`", "a.b.c", "1t", "`a\n`"} {
		for _, template := range []string{"{{.table}}", "{{.stable}}"} {
			cfg := TaosConfig{Table: "{{.table}}", STable: "{{.stable}}", TsFieldName: "ts", TagFields: []string{"tag"}}
			props := map[string]string{"{{.table}}": "t", "{{.stable}}": "st"}
			props[template] = name
			_, err := cfg.buildSql(testx.MockTuple{Map: map[string]any{"v": 1}, Template: props})
			require.Error(t, err, "%s: %q", template, name)
		}
	}
	cfg := TaosConfig{Table: "t", TsFieldName: "ts"}
	for _, name := range []string{"v) values(now, 1)--", "db.v", "`v`);--"} {
		_, err := cfg.buildSql(testx.MockTuple{Map: map[string]any{name: 1}})
		require.Error(t, err)
	}
}

// Set TDENGINE_TEST_DSN to run against a disposable TDengine 3 instance.
func TestStreamValuesRoundTrip(t *testing.T) {
	dsn := os.Getenv("TDENGINE_TEST_DSN")
	if dsn == "" {
		t.Skip("TDENGINE_TEST_DSN is not set")
	}
	db, err := sql.Open("taosWS", dsn)
	require.NoError(t, err)
	defer db.Close()
	name := fmt.Sprintf("ekuiper_values_%d", time.Now().UnixNano())
	_, err = db.Exec("CREATE DATABASE " + name)
	require.NoError(t, err)
	defer func() {
		_, err := db.Exec("DROP DATABASE " + name)
		assert.NoError(t, err)
	}()
	_, err = db.Exec("CREATE STABLE " + name + ".st (ts TIMESTAMP, v NCHAR(256)) TAGS (label NCHAR(256))")
	require.NoError(t, err)
	ctx := mockContext.NewMockContext("roundtrip", "sink")
	for i, value := range []string{`a\"), (now, "injected'); --`, "引号'\"和反斜杠\\", "line\n\r\tend", `\n`, `ends\`} {
		table := fmt.Sprintf("%s.`t%d;--`", name, i)
		timestamps := []any{
			time.Now().UnixMilli(), "2025-01-23T09:16:34.255Z", "2025-01-23 09:16:34.255",
			`'2025-01-23 09:16:34.255'`, `"2025-01-23T09:16:34.255Z"`,
		}
		cfg := &TaosConfig{Table: "{{.table}}", STable: "{{.stable}}", TsFieldName: "ts", TagFields: []string{"tag"}, ProvideTs: true}
		sink := tdengineSink3{cfg: cfg, cli: db}
		tuple := testx.MockTuple{Map: map[string]any{"ts": timestamps[i%len(timestamps)], "v": value, "tag": value}, Template: map[string]string{"{{.table}}": table, "{{.stable}}": name + ".st"}}
		require.NoError(t, sink.Collect(ctx, tuple))
		var gotValue, gotTag string
		require.NoError(t, db.QueryRow("SELECT v, label FROM "+table).Scan(&gotValue, &gotTag))
		assert.Equal(t, value, gotValue)
		assert.Equal(t, value, gotTag)
	}
}
