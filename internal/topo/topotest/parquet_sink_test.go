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

//go:build parquet || full

package topotest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

// readParquetRows decodes a flat parquet file into one map per row.
func readParquetRows(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	st, err := f.Stat()
	require.NoError(t, err)
	pf, err := parquet.OpenFile(f, st.Size())
	require.NoError(t, err)
	fields := pf.Schema().Fields()
	result := make([]map[string]any, 0, pf.NumRows())
	for _, group := range pf.RowGroups() {
		rows := group.Rows()
		buf := make([]parquet.Row, group.NumRows())
		n, _ := rows.ReadRows(buf)
		require.NoError(t, rows.Close())
		for _, row := range buf[:n] {
			m := make(map[string]any, len(fields))
			for _, v := range row {
				name := fields[v.Column()].Name()
				switch {
				case v.IsNull():
					m[name] = nil
				case v.Kind() == parquet.Boolean:
					m[name] = v.Boolean()
				case v.Kind() == parquet.Int64:
					m[name] = v.Int64()
				case v.Kind() == parquet.Double:
					m[name] = v.Double()
				default:
					m[name] = v.String()
				}
			}
			result = append(result, m)
		}
	}
	return result
}

// TestParquetSinkRule runs real rules from a JSON file source, through the SQL
// planner and the sink pipeline, into a parquet file.
func TestParquetSinkRule(t *testing.T) {
	const declared = `(id BIGINT, temp FLOAT, name STRING, ok BOOLEAN)`
	const data = `[
		{"id": 1, "temp": 21.5, "name": "a", "ok": true},
		{"id": 2, "temp": 21, "name": "b", "ok": false},
		{"id": 3, "temp": 22.5, "name": "c", "ok": true},
		{"id": 4, "temp": 23, "name": "d", "ok": false}
	]`
	allColumns := map[string]any{"id": "int64", "temp": "double", "name": "string", "ok": "boolean"}
	allRows := []map[string]any{
		{"id": int64(1), "temp": 21.5, "name": "a", "ok": true},
		{"id": int64(2), "temp": 21.0, "name": "b", "ok": false},
		{"id": int64(3), "temp": 22.5, "name": "c", "ok": true},
		{"id": int64(4), "temp": 23.0, "name": "d", "ok": false},
	}
	tests := []struct {
		name     string
		schema   string
		data     string
		sql      string
		props    map[string]any
		expected []map[string]any
	}{
		{
			name:     "declared schema",
			schema:   declared,
			data:     data,
			sql:      `SELECT id, temp, name, ok FROM %s`,
			props:    map[string]any{"columns": allColumns},
			expected: allRows,
		},
		{
			name:     "select all with every field declared",
			schema:   declared,
			data:     data,
			sql:      `SELECT * FROM %s`,
			props:    map[string]any{"columns": allColumns},
			expected: allRows,
		},
		{
			name:     "single tuples",
			schema:   declared,
			data:     data,
			sql:      `SELECT id, temp, name, ok FROM %s`,
			props:    map[string]any{"columns": allColumns, "sendSingle": true},
			expected: allRows,
		},
		{
			name:     "batches",
			schema:   declared,
			data:     data,
			sql:      `SELECT id, temp, name, ok FROM %s`,
			props:    map[string]any{"columns": allColumns, "batchSize": 2},
			expected: allRows,
		},
		{
			name:   "computed values and aliases",
			schema: declared,
			data:   data,
			sql:    `SELECT id AS key, id * 2 AS doubled, temp * 2 AS t2, upper(name) AS up, temp > 22 AS hot FROM %s WHERE id = 1`,
			props: map[string]any{"columns": map[string]any{
				"key": "int64", "doubled": "int64", "t2": "double", "up": "string", "hot": "boolean",
			}},
			expected: []map[string]any{{"key": int64(1), "doubled": int64(2), "t2": 43.0, "up": "A", "hot": false}},
		},
		{
			name:   "null in a nullable column",
			schema: `(id BIGINT, name STRING)`,
			data:   `[{"id": 1, "name": null}, {"id": 2, "name": "b"}]`,
			sql:    `SELECT id, name FROM %s`,
			props:  map[string]any{"columns": map[string]any{"id": "int64", "name": "string?"}},
			expected: []map[string]any{
				{"id": int64(1), "name": nil},
				{"id": int64(2), "name": "b"},
			},
		},
		{
			name:   "schemaless stream to a double column",
			schema: `()`,
			data:   `[{"v": 21}, {"v": 21.5}]`,
			sql:    `SELECT v FROM %s`,
			props:  map[string]any{"columns": map[string]any{"v": "double"}},
			expected: []map[string]any{
				{"v": 21.0},
				{"v": 21.5},
			},
		},
		{
			// JSON numbers decode to float64 without a schema, and types are not
			// cast. The rows are rejected but the rule must keep running.
			name:   "schemaless stream to an int64 column rejects rows",
			schema: `()`,
			data:   `[{"v": 21}, {"v": 22}]`,
			sql:    `SELECT v FROM %s`,
			props:  map[string]any{"columns": map[string]any{"v": "int64"}},
		},
		{
			name:   "a field that is not declared rejects rows",
			schema: declared,
			data:   data,
			sql:    `SELECT * FROM %s`,
			props:  map[string]any{"columns": map[string]any{"id": "int64"}},
		},
	}
	dataDir, err := conf.GetLoc("data")
	require.NoError(t, err)
	p := processor.NewStreamProcessor()
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := fmt.Sprintf("parquetRule%d", i)
			source := filepath.Join(dataDir, stream+".json")
			require.NoError(t, os.WriteFile(source, []byte(tt.data), 0o600))
			t.Cleanup(func() { _ = os.Remove(source) })

			_, err := p.ExecStmt(fmt.Sprintf(`CREATE STREAM %s %s WITH (DATASOURCE="%s.json", TYPE="file", FORMAT="json");`, stream, tt.schema, stream))
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = p.ExecStmt(fmt.Sprintf(`DROP STREAM %s;`, stream)) })

			out := filepath.Join(t.TempDir(), "out.parquet")
			props := map[string]any{"path": out}
			for k, v := range tt.props {
				props[k] = v
			}
			tp, err := planner.Plan(&def.Rule{
				Id:      stream,
				Sql:     fmt.Sprintf(tt.sql, stream),
				Actions: []map[string]any{{"parquet": props}},
				Options: &def.RuleOption{BufferLength: 100},
			})
			require.NoError(t, err)
			defer tp.Cancel()

			select {
			case err := <-tp.Open():
				// The source is bounded, so a healthy rule ends with EOF. Anything
				// else, such as a panic escaping the sink, fails here.
				require.True(t, errorx.IsEOF(err), "rule stopped with: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("rule did not finish")
			}
			tp.Cancel()

			if tt.expected == nil {
				time.Sleep(50 * time.Millisecond)
				_, err := os.Stat(out)
				assert.True(t, os.IsNotExist(err), "no row was accepted, so no file should exist")
				return
			}
			// The sink closes the file asynchronously after the rule is cancelled.
			require.Eventually(t, func() bool {
				f, err := os.Open(out)
				if err != nil {
					return false
				}
				defer f.Close()
				st, err := f.Stat()
				if err != nil {
					return false
				}
				_, err = parquet.OpenFile(f, st.Size())
				return err == nil
			}, 10*time.Second, 5*time.Millisecond)
			assert.ElementsMatch(t, tt.expected, readParquetRows(t, out))
		})
	}
}
