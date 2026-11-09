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

package file

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

var _ api.TupleCollector = &parquetSink{}

func noopStatus(string, string) {}

func parquetRow(m map[string]any) api.MessageTuple {
	return &xsql.Tuple{Message: m}
}

func parquetRowList(ms ...map[string]any) api.MessageTupleList {
	content := make([]api.MessageTuple, 0, len(ms))
	for _, m := range ms {
		content = append(content, parquetRow(m))
	}
	return &xsql.TransformedTupleList{Content: content}
}

// provisionParquetSink returns a provisioned sink writing out.parquet in a
// fresh directory. Extra props override the defaults.
func provisionParquetSink(t *testing.T, ctx api.StreamContext, extra map[string]any) (*parquetSink, string) {
	t.Helper()
	dir := t.TempDir()
	props := map[string]any{
		"path":    filepath.Join(dir, "out.parquet"),
		"columns": map[string]any{"id": "int64"},
	}
	for k, v := range extra {
		props[k] = v
	}
	p, ok := GetParquetSink().(*parquetSink)
	require.True(t, ok)
	require.NoError(t, p.Provision(ctx, props))
	return p, dir
}

// readParquetRows decodes a flat parquet file into one map per row.
func readParquetRows(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	st, err := f.Stat()
	require.NoError(t, err)
	pf, err := parquet.OpenFile(f, st.Size())
	require.NoError(t, err, "%s is not a valid parquet file", path)

	fields := pf.Schema().Fields()
	result := make([]map[string]any, 0, pf.NumRows())
	for _, group := range pf.RowGroups() {
		rows := group.Rows()
		buf := make([]parquet.Row, group.NumRows())
		n, err := rows.ReadRows(buf)
		if err != nil {
			require.ErrorContains(t, err, "EOF")
		}
		require.Equal(t, int(group.NumRows()), n)
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

// readParquetDir returns the rows of every file in dir, keyed by file name.
func readParquetDir(t *testing.T, dir string) map[string][]map[string]any {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	result := make(map[string][]map[string]any, len(entries))
	for _, e := range entries {
		result[e.Name()] = readParquetRows(t, filepath.Join(dir, e.Name()))
	}
	return result
}

func idRows(ids ...int64) []map[string]any {
	rows := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, map[string]any{"id": id})
	}
	return rows
}

func TestParquetSinkRegistered(t *testing.T) {
	newSink, ok := modules.Sinks["parquet"]
	require.True(t, ok)
	first, second := newSink(), newSink()
	assert.NotSame(t, first, second)
	assert.Implements(t, (*api.TupleCollector)(nil), first)
}

func TestParquetSinkProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "provision")
	path := filepath.Join(t.TempDir(), "out.parquet")
	columns := map[string]any{"id": "int64"}
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name:  "valid",
			props: map[string]any{"path": path, "columns": columns},
		},
		{
			name: "valid with a rolling interval only",
			props: map[string]any{
				"path": path, "columns": columns,
				"rollingNamePattern": "none", "rollingInterval": 1000,
			},
		},
		{
			name:  "default path",
			props: map[string]any{"columns": columns},
		},
		{
			name:  "empty path",
			props: map[string]any{"path": "", "columns": columns},
			err:   "path must be set",
		},
		{
			name:  "path template in the file name",
			props: map[string]any{"path": "{{.x}}.parquet", "columns": columns},
			err:   "path templates cannot be used with parquet",
		},
		{
			name:  "path template in the directory",
			props: map[string]any{"path": filepath.Join("{{.x}}", "out.parquet"), "columns": columns},
			err:   "path templates cannot be used with parquet",
		},
		{
			name:  "wrong file type",
			props: map[string]any{"path": path, "columns": columns, "fileType": "csv"},
			err:   "fileType must be parquet not csv",
		},
		{
			name:  "negative rolling interval",
			props: map[string]any{"path": path, "columns": columns, "rollingInterval": -1},
			err:   "rollingInterval must be positive",
		},
		{
			name:  "negative rolling count",
			props: map[string]any{"path": path, "columns": columns, "rollingCount": -1},
			err:   "rollingCount must be positive",
		},
		{
			name:  "negative check interval",
			props: map[string]any{"path": path, "columns": columns, "checkInterval": -1},
			err:   "checkInterval must be positive",
		},
		{
			name:  "no rolling strategy",
			props: map[string]any{"path": path, "columns": columns, "rollingCount": 0, "rollingInterval": 0},
			err:   "one of rollingInterval, rollingCount must be set",
		},
		{
			name:  "invalid rolling name pattern",
			props: map[string]any{"path": path, "columns": columns, "rollingNamePattern": "test"},
			err:   "rollingNamePattern must be one of prefix, suffix or none",
		},
		{
			name:  "rolling size is rejected",
			props: map[string]any{"path": path, "columns": columns, "rollingSize": 100},
			err:   "rollingSize cannot be used with parquet",
		},
		{
			name:  "compression is rejected",
			props: map[string]any{"path": path, "columns": columns, "compression": "gzip"},
			err:   "compression cannot be used with parquet",
		},
		{
			name:  "encryption is rejected",
			props: map[string]any{"path": path, "columns": columns, "encryption": "aes"},
			err:   "encryption cannot be used with parquet",
		},
		{
			name:  "rolling hook is rejected",
			props: map[string]any{"path": path, "columns": columns, "rollingHook": "hook"},
			err:   "rollingHook cannot be used with parquet",
		},
		{
			name:  "missing columns",
			props: map[string]any{"path": path},
			err:   "columns must be set",
		},
		{
			name:  "empty column name",
			props: map[string]any{"path": path, "columns": map[string]any{"": "int64"}},
			err:   "column name must not be empty",
		},
		{
			name:  "unknown column type",
			props: map[string]any{"path": path, "columns": map[string]any{"id": "int46"}},
			err:   `column "id": unsupported type "int46", must be one of boolean, int64, double, string`,
		},
		{
			name:  "empty column type",
			props: map[string]any{"path": path, "columns": map[string]any{"id": ""}},
			err:   `column "id": unsupported type "", must be one of boolean, int64, double, string`,
		},
		{
			name:  "only the nullable marker",
			props: map[string]any{"path": path, "columns": map[string]any{"id": "?"}},
			err:   `column "id": unsupported type "", must be one of boolean, int64, double, string`,
		},
		{
			name:  "double nullable marker",
			props: map[string]any{"path": path, "columns": map[string]any{"id": "int64??"}},
			err:   `column "id": unsupported type "int64?", must be one of boolean, int64, double, string`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := GetParquetSink().Provision(ctx, tt.props)
			if tt.err == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.err)
		})
	}
}

func TestParquetSinkProvisionNormalizes(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "normalize")
	dir := t.TempDir()
	p, ok := GetParquetSink().(*parquetSink)
	require.True(t, ok)
	require.NoError(t, p.Provision(ctx, map[string]any{
		"path":            filepath.Join(dir, "sub") + string(filepath.Separator) + ".." + string(filepath.Separator) + "out.parquet",
		"columns":         map[string]any{"id": "int64"},
		"rollingInterval": 1000,
	}))
	assert.Equal(t, filepath.Join(dir, "out.parquet"), p.conf.Path)
	assert.Equal(t, time.Second, time.Duration(p.conf.CheckInterval), "checkInterval follows a shorter rollingInterval")
}

func TestParquetSinkSchema(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "schema")
	p, _ := provisionParquetSink(t, ctx, map[string]any{
		"columns": map[string]any{
			"b": "boolean", "bn": "BOOLEAN?",
			"i": "INT64", "in": "int64?",
			"d": "Double", "dn": "double?",
			"s": "string", "sn": "String?",
		},
	})
	expected := map[string]struct {
		kind     parquet.Kind
		optional bool
	}{
		"b": {parquet.Boolean, false}, "bn": {parquet.Boolean, true},
		"i": {parquet.Int64, false}, "in": {parquet.Int64, true},
		"d": {parquet.Double, false}, "dn": {parquet.Double, true},
		"s": {parquet.ByteArray, false}, "sn": {parquet.ByteArray, true},
	}
	fields := p.schema.Fields()
	require.Len(t, fields, len(expected))
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.Name())
		e, ok := expected[f.Name()]
		require.True(t, ok, f.Name())
		assert.Equal(t, e.kind, f.Type().Kind(), f.Name())
		assert.Equal(t, e.optional, f.Optional(), f.Name())
	}
	// Columns are always stored in alphabetical order, whatever the config says.
	assert.Equal(t, []string{"b", "bn", "d", "dn", "i", "in", "s", "sn"}, names)
}

func TestParquetSinkCollect(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "collect")
	p, dir := provisionParquetSink(t, ctx, map[string]any{
		"columns": map[string]any{
			"b": "boolean", "i": "int64", "d": "double", "s": "string",
			"nullable": "string?",
		},
	})
	require.NoError(t, p.Connect(ctx, noopStatus))
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{
		"b": true, "i": int64(1), "d": 1.5, "s": "one", "nullable": "set",
	})))
	require.NoError(t, p.CollectList(ctx, parquetRowList(
		map[string]any{"b": false, "i": int64(2), "d": 2.5, "s": "two", "nullable": nil},
		map[string]any{"b": true, "i": int64(0), "d": 0.0, "s": ""},
	)))
	require.NoError(t, p.Close(ctx))

	assert.Equal(t, []map[string]any{
		{"b": true, "i": int64(1), "d": 1.5, "s": "one", "nullable": "set"},
		{"b": false, "i": int64(2), "d": 2.5, "s": "two", "nullable": nil},
		// Real zero values must not be turned into null, nor nullable ones into zero.
		{"b": true, "i": int64(0), "d": 0.0, "s": "", "nullable": nil},
	}, readParquetRows(t, filepath.Join(dir, "out.parquet")))
}

func TestParquetSinkCreatesParentDirectory(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "mkdir")
	path := filepath.Join(t.TempDir(), "a", "b", "out.parquet")
	p, _ := provisionParquetSink(t, ctx, map[string]any{"path": path})
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, idRows(1), readParquetRows(t, path))
}

// A required column absent from the row is written with its zero value on
// purpose. Declare the column nullable to tell the two cases apart.
func TestParquetSinkMissingColumns(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "missing")
	p, dir := provisionParquetSink(t, ctx, map[string]any{
		"columns": map[string]any{
			"id": "int64",
			"b":  "boolean", "i": "int64", "d": "double", "s": "string",
			"bn": "boolean?", "in": "int64?", "dn": "double?", "sn": "string?",
		},
	})
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, []map[string]any{{
		"id": int64(1),
		"b":  false, "i": int64(0), "d": 0.0, "s": "",
		"bn": nil, "in": nil, "dn": nil, "sn": nil,
	}}, readParquetRows(t, filepath.Join(dir, "out.parquet")))
}

func TestParquetSinkTypeCheck(t *testing.T) {
	columns := map[string]any{"b": "boolean", "i": "int64", "d": "double", "s": "string", "n": "int64?"}
	tests := []struct {
		name string
		row  map[string]any
		err  string
	}{
		{name: "matching types", row: map[string]any{"b": true, "i": int64(1), "d": 1.5, "s": "x", "n": int64(2)}},
		{name: "null in a nullable column", row: map[string]any{"n": nil}},
		{name: "string column", row: map[string]any{"s": 1.0}, err: `column "s": expected a string value, got float64`},
		{name: "int64 column gets a string", row: map[string]any{"i": "abc"}, err: `column "i": expected an int64 value, got string`},
		{name: "int64 column gets a whole float", row: map[string]any{"i": 21.0}, err: `column "i": expected an int64 value, got float64`},
		{name: "int64 column gets an int", row: map[string]any{"i": 21}, err: `column "i": expected an int64 value, got int`},
		{name: "nullable column still checks the type", row: map[string]any{"n": 21.0}, err: `column "n": expected an int64 value, got float64`},
		{name: "double column gets an int64", row: map[string]any{"d": int64(21)}, err: `column "d": expected a double value, got int64`},
		{name: "boolean column", row: map[string]any{"b": "true"}, err: `column "b": expected a boolean value, got string`},
		{name: "null in a required column", row: map[string]any{"i": nil}, err: `column "i": required value is missing`},
		{name: "field not declared", row: map[string]any{"i": int64(1), "other": 1}, err: `column "other": not declared in columns`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := mockContext.NewMockContext("parquet", "typecheck")
			p, dir := provisionParquetSink(t, ctx, map[string]any{"columns": columns})
			err := p.Collect(ctx, parquetRow(tt.row))
			if tt.err == "" {
				require.NoError(t, err)
				require.NoError(t, p.Close(ctx))
				assert.Len(t, readParquetRows(t, filepath.Join(dir, "out.parquet")), 1)
				return
			}
			assert.EqualError(t, err, tt.err)
			require.NoError(t, p.Close(ctx))
			// A rejected row must not leave a file behind.
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

// One bad row rejects the whole batch, so a resent batch cannot duplicate rows.
func TestParquetSinkBatchIsAllOrNothing(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "batch")
	p, dir := provisionParquetSink(t, ctx, nil)
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	err := p.CollectList(ctx, parquetRowList(
		map[string]any{"id": int64(2)},
		map[string]any{"id": "bad"},
		map[string]any{"id": int64(3)},
	))
	assert.EqualError(t, err, `column "id": expected an int64 value, got string`)
	require.NoError(t, p.CollectList(ctx, parquetRowList(
		map[string]any{"id": int64(4)},
		map[string]any{"id": int64(5)},
	)))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, idRows(1, 4, 5), readParquetRows(t, filepath.Join(dir, "out.parquet")))
}

func TestParquetSinkRollingCount(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected map[string][]map[string]any
	}{
		{
			name:    "suffix",
			pattern: "suffix",
			expected: map[string][]map[string]any{
				"out-100.parquet": idRows(0, 1),
				"out-300.parquet": idRows(2, 3),
				"out-500.parquet": idRows(4),
			},
		},
		{
			name:    "prefix",
			pattern: "prefix",
			expected: map[string][]map[string]any{
				"100-out.parquet": idRows(0, 1),
				"300-out.parquet": idRows(2, 3),
				"500-out.parquet": idRows(4),
			},
		},
		{
			// Without a pattern each roll replaces the previous file on purpose.
			name:    "none",
			pattern: "none",
			expected: map[string][]map[string]any{
				"out.parquet": idRows(4),
			},
		},
		{
			name:    "unset behaves like none",
			pattern: "",
			expected: map[string][]map[string]any{
				"out.parquet": idRows(4),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := mockContext.NewMockContext("parquet", "rollingcount")
			mockclock.ResetClock(0)
			c := mockclock.GetMockClock()
			p, dir := provisionParquetSink(t, ctx, map[string]any{
				"rollingCount":       2,
				"rollingNamePattern": tt.pattern,
			})
			require.NoError(t, p.Connect(ctx, noopStatus))
			for i := int64(0); i < 5; i++ {
				c.Add(100 * time.Millisecond)
				require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": i})))
			}
			require.NoError(t, p.Close(ctx))
			assert.Equal(t, tt.expected, readParquetDir(t, dir))
		})
	}
}

// A batch is never split, so a file can exceed rollingCount by one batch.
func TestParquetSinkRollingCountWithBatches(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "rollingbatch")
	mockclock.ResetClock(0)
	c := mockclock.GetMockClock()
	p, dir := provisionParquetSink(t, ctx, map[string]any{"rollingCount": 2, "rollingNamePattern": "suffix"})
	c.Add(100 * time.Millisecond)
	require.NoError(t, p.CollectList(ctx, parquetRowList(
		map[string]any{"id": int64(0)}, map[string]any{"id": int64(1)}, map[string]any{"id": int64(2)},
	)))
	c.Add(100 * time.Millisecond)
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(3)})))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, map[string][]map[string]any{
		"out-100.parquet": idRows(0, 1, 2),
		"out-200.parquet": idRows(3),
	}, readParquetDir(t, dir))
}

func isParquetFileClosed(p *parquetSink) func() bool {
	return func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.file == nil
	}
}

func TestParquetSinkRollingInterval(t *testing.T) {
	ctx, cancel := mockContext.NewMockContext("parquet", "rollinginterval").WithCancel()
	defer cancel()
	mockclock.ResetClock(0)
	c := mockclock.GetMockClock()
	p, dir := provisionParquetSink(t, ctx, map[string]any{
		"rollingCount":       0,
		"rollingInterval":    1000,
		"checkInterval":      1000,
		"rollingNamePattern": "suffix",
	})
	require.NoError(t, p.Connect(ctx, noopStatus))

	// An idle sink creates nothing, however many ticks go by.
	c.Add(time.Second)
	c.Add(time.Second)
	time.Sleep(50 * time.Millisecond)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)

	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(0)})))
	c.Add(500 * time.Millisecond)
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	c.Add(500 * time.Millisecond)
	require.Eventually(t, isParquetFileClosed(p), 5*time.Second, time.Millisecond)

	// Ticks without new rows must not open a new file.
	c.Add(time.Second)
	c.Add(time.Second)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, map[string][]map[string]any{"out-2000.parquet": idRows(0, 1)}, readParquetDir(t, dir))

	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(2)})))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, map[string][]map[string]any{
		"out-2000.parquet": idRows(0, 1),
		"out-5000.parquet": idRows(2),
	}, readParquetDir(t, dir))
}

// A roll from the ticker must also reset the row count of the next file.
func TestParquetSinkRollingIntervalResetsCount(t *testing.T) {
	ctx, cancel := mockContext.NewMockContext("parquet", "rollingreset").WithCancel()
	defer cancel()
	mockclock.ResetClock(0)
	c := mockclock.GetMockClock()
	p, dir := provisionParquetSink(t, ctx, map[string]any{
		"rollingCount":       3,
		"rollingInterval":    1000,
		"checkInterval":      1000,
		"rollingNamePattern": "suffix",
	})
	require.NoError(t, p.Connect(ctx, noopStatus))
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(0)})))
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	c.Add(time.Second)
	require.Eventually(t, isParquetFileClosed(p), 5*time.Second, time.Millisecond)
	for i := int64(2); i < 5; i++ {
		require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": i})))
	}
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, map[string][]map[string]any{
		"out-0.parquet":    idRows(0, 1),
		"out-1000.parquet": idRows(2, 3, 4),
	}, readParquetDir(t, dir))
}

// The checkInterval default must not roll a sink that only sets rollingCount.
func TestParquetSinkNoRollingIntervalKeepsFile(t *testing.T) {
	ctx, cancel := mockContext.NewMockContext("parquet", "nointerval").WithCancel()
	defer cancel()
	mockclock.ResetClock(0)
	c := mockclock.GetMockClock()
	p, dir := provisionParquetSink(t, ctx, map[string]any{"checkInterval": 1000})
	require.NoError(t, p.Connect(ctx, noopStatus))
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(0)})))
	c.Add(5 * time.Second)
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, map[string][]map[string]any{"out.parquet": idRows(0, 1)}, readParquetDir(t, dir))
}

func TestParquetSinkCloseWithoutRows(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "closeempty")
	p, dir := provisionParquetSink(t, ctx, nil)
	require.NoError(t, p.Connect(ctx, noopStatus))
	assert.NoError(t, p.Close(ctx))
	assert.NoError(t, p.Close(ctx), "closing twice is harmless")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestParquetSinkCloseReleasesFile(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "closefile")
	p, dir := provisionParquetSink(t, ctx, nil)
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	file := p.file
	require.NoError(t, p.Close(ctx))
	assert.Nil(t, p.file)
	assert.ErrorIs(t, file.Close(), os.ErrClosed)
	assert.NoError(t, p.Close(ctx), "closing twice is harmless")
	assert.Equal(t, idRows(1), readParquetRows(t, filepath.Join(dir, "out.parquet")))
}

// With a name pattern every file is timestamped, including the first one, so a
// rule restart cannot overwrite the output of the previous run.
func TestParquetSinkRestartKeepsFiles(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "restart")
	mockclock.ResetClock(0)
	c := mockclock.GetMockClock()
	first, dir := provisionParquetSink(t, ctx, map[string]any{"rollingNamePattern": "suffix"})
	path := filepath.Join(dir, "out.parquet")
	c.Add(100 * time.Millisecond)
	require.NoError(t, first.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	require.NoError(t, first.Close(ctx))

	second, _ := provisionParquetSink(t, ctx, map[string]any{"path": path, "rollingNamePattern": "suffix"})
	c.Add(100 * time.Millisecond)
	require.NoError(t, second.Collect(ctx, parquetRow(map[string]any{"id": int64(2)})))
	require.NoError(t, second.Close(ctx))

	assert.Equal(t, map[string][]map[string]any{
		"out-100.parquet": idRows(1),
		"out-200.parquet": idRows(2),
	}, readParquetDir(t, dir))
}

func TestParquetSinkRecoversFromFailedOpen(t *testing.T) {
	if runtime.GOOS == "windows" || os.Getuid() == 0 {
		t.Skip("relies on directory permissions being enforced")
	}
	ctx := mockContext.NewMockContext("parquet", "failedopen")
	mockclock.ResetClock(0)
	c := mockclock.GetMockClock()
	p, dir := provisionParquetSink(t, ctx, map[string]any{"rollingCount": 1, "rollingNamePattern": "suffix"})
	send := func(id int64) error {
		c.Add(100 * time.Millisecond)
		return p.Collect(ctx, parquetRow(map[string]any{"id": id}))
	}
	require.NoError(t, send(0))

	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	err := send(1)
	require.ErrorContains(t, err, "fail to open parquet file")
	assert.ErrorContains(t, err, "out-200.parquet", "the error names the real file")
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.Error(t, send(2))

	require.NoError(t, os.Chmod(dir, 0o700))
	require.NoError(t, send(3))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, map[string][]map[string]any{
		"out-100.parquet": idRows(0),
		"out-400.parquet": idRows(3),
	}, readParquetDir(t, dir))
}

// A file that fails to close must be reported once and then forgotten, or the
// sink would retry the same dead file forever.
func TestParquetSinkRecoversFromFailedClose(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "failedclose")
	mockclock.ResetClock(0)
	c := mockclock.GetMockClock()
	p, dir := provisionParquetSink(t, ctx, map[string]any{"rollingCount": 1, "rollingNamePattern": "suffix"})
	send := func(id int64) error {
		c.Add(100 * time.Millisecond)
		return p.Collect(ctx, parquetRow(map[string]any{"id": id}))
	}
	require.NoError(t, send(0))
	require.NoError(t, p.file.Close())

	err := send(1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrClosed))
	assert.Nil(t, p.file)
	assert.Zero(t, p.dataWrote)

	require.NoError(t, send(2))
	require.NoError(t, p.Close(ctx))
	assert.Equal(t, idRows(2), readParquetRows(t, filepath.Join(dir, "out-300.parquet")))
}

func TestParquetSinkCloseReportsFailure(t *testing.T) {
	ctx := mockContext.NewMockContext("parquet", "closefailure")
	p, _ := provisionParquetSink(t, ctx, nil)
	require.NoError(t, p.Collect(ctx, parquetRow(map[string]any{"id": int64(1)})))
	require.NoError(t, p.file.Close())
	assert.ErrorIs(t, p.Close(ctx), os.ErrClosed)
	assert.NoError(t, p.Close(ctx), "the failure is only reported once")
}

func TestParquetSinkSandbox(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "parquet")
	columns := map[string]any{"id": "int64"}

	// Absolute path outside the data dir must be rejected.
	err := GetParquetSink().Provision(ctx, map[string]any{"path": outside(t), "columns": columns})
	assert.ErrorContains(t, err, "file access denied")

	// Traversal in a relative path must be rejected.
	err = GetParquetSink().Provision(ctx, map[string]any{"path": filepath.Join("..", "secret.parquet"), "columns": columns})
	assert.ErrorContains(t, err, "file access denied")

	// A plain relative path resolves inside the data dir, and that resolved
	// path is the one the sink writes to.
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	p, ok := GetParquetSink().(*parquetSink)
	require.True(t, ok)
	require.NoError(t, p.Provision(ctx, map[string]any{"path": "sandbox_sink.parquet", "columns": columns}))
	canonical, err := filepath.EvalSymlinks(dataDir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(canonical, "sandbox_sink.parquet"), p.conf.Path)
}
