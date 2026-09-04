// Copyright 2026 Timecho Limited
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

package iotdb

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

type fakeWriter struct {
	connectErr   error
	writeErr     error
	closeErr     error
	connectCalls int
	writeCalls   int
	closeCalls   int
	written      []map[string]any
}

func (w *fakeWriter) connect(api.StreamContext, *iotdbConfig) error {
	w.connectCalls++
	return w.connectErr
}

func (w *fakeWriter) write(_ api.StreamContext, data []map[string]any) error {
	w.writeCalls++
	w.written = data
	return w.writeErr
}

func (w *fakeWriter) close() error {
	w.closeCalls++
	return w.closeErr
}

type fakeTableSessionPool struct {
	session  client.ITableSession
	getErr   error
	getCalls int
	closed   bool
}

func (p *fakeTableSessionPool) GetSession() (client.ITableSession, error) {
	p.getCalls++
	return p.session, p.getErr
}

func (p *fakeTableSessionPool) Close() {
	p.closed = true
}

type fakeTableSession struct {
	insertErr  error
	executeErr error
	inserted   *client.Tablet
	statements []string
	closeCalls int
}

func (s *fakeTableSession) Insert(tablet *client.Tablet) error {
	s.inserted = tablet
	return s.insertErr
}

func (s *fakeTableSession) ExecuteNonQueryStatement(statement string) error {
	s.statements = append(s.statements, statement)
	return s.executeErr
}

func (s *fakeTableSession) ExecuteQueryStatement(string, *int64) (*client.SessionDataSet, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeTableSession) Close() error {
	s.closeCalls++
	return nil
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]interface{}
		expected iotdbConfig
		error    string
	}{
		{
			name: "valid tree model with defaults",
			conf: map[string]interface{}{
				"device":       "root.sg.d1",
				"measurements": []interface{}{"temperature", "humidity"},
				"dataTypes":    []interface{}{"FLOAT", "INT32"},
			},
			expected: iotdbConfig{
				Addr:         "127.0.0.1:6667",
				Username:     "root",
				Password:     "root",
				Model:        "tree",
				Device:       "root.sg.d1",
				Measurements: []string{"temperature", "humidity"},
				DataTypes:    []string{"FLOAT", "INT32"},
				BatchSize:    10,
				Timeout:      5000,
				PoolSize:     3,
			},
		},
		{
			name: "valid table model with defaults",
			conf: map[string]interface{}{
				"model":            "table",
				"database":         "db1",
				"table":            "t1",
				"measurements":     []interface{}{"d", "v"},
				"dataTypes":        []interface{}{"TEXT", "DOUBLE"},
				"columnCategories": []interface{}{"TAG", "FIELD"},
			},
			expected: iotdbConfig{
				Addr:             "127.0.0.1:6667",
				Username:         "root",
				Password:         "root",
				Model:            "table",
				Database:         "db1",
				Table:            "t1",
				Measurements:     []string{"d", "v"},
				DataTypes:        []string{"TEXT", "DOUBLE"},
				ColumnCategories: []string{"TAG", "FIELD"},
				BatchSize:        10,
				Timeout:          5000,
				PoolSize:         3,
			},
		},
		{
			name: "valid tree model with custom values",
			conf: map[string]interface{}{
				"addr":         "192.168.1.10:6668",
				"username":     "admin",
				"password":     "secret",
				"model":        "tree",
				"device":       "root.sg.d1",
				"isAligned":    true,
				"measurements": []interface{}{"v"},
				"dataTypes":    []interface{}{"int64"},
				"tsFieldName":  "ts",
				"batchSize":    100,
				"timeout":      3000,
				"poolSize":     5,
			},
			expected: iotdbConfig{
				Addr:         "192.168.1.10:6668",
				Username:     "admin",
				Password:     "secret",
				Model:        "tree",
				Device:       "root.sg.d1",
				IsAligned:    true,
				Measurements: []string{"v"},
				DataTypes:    []string{"INT64"},
				TsFieldName:  "ts",
				BatchSize:    100,
				Timeout:      3000,
				PoolSize:     5,
			},
		},
		{
			name: "invalid model value",
			conf: map[string]interface{}{
				"model":        "foo",
				"device":       "root.sg.d1",
				"measurements": []interface{}{"v"},
				"dataTypes":    []interface{}{"INT32"},
			},
			error: `model must be either "tree" or "table"`,
		},
		{
			name: "tree model missing device",
			conf: map[string]interface{}{
				"model":        "tree",
				"measurements": []interface{}{"v"},
				"dataTypes":    []interface{}{"INT32"},
			},
			error: `device is required when model is "tree"`,
		},
		{
			name: "table model missing database",
			conf: map[string]interface{}{
				"model":            "table",
				"table":            "t1",
				"measurements":     []interface{}{"v"},
				"dataTypes":        []interface{}{"INT32"},
				"columnCategories": []interface{}{"FIELD"},
			},
			error: `database is required when model is "table"`,
		},
		{
			name: "table model missing table",
			conf: map[string]interface{}{
				"model":            "table",
				"database":         "db1",
				"measurements":     []interface{}{"v"},
				"dataTypes":        []interface{}{"INT32"},
				"columnCategories": []interface{}{"FIELD"},
			},
			error: `table is required when model is "table"`,
		},
		{
			name: "measurements missing",
			conf: map[string]interface{}{
				"model":     "tree",
				"device":    "root.sg.d1",
				"dataTypes": []interface{}{"INT32"},
			},
			error: "measurements is required",
		},
		{
			name: "dataTypes missing",
			conf: map[string]interface{}{
				"model":        "tree",
				"device":       "root.sg.d1",
				"measurements": []interface{}{"v"},
			},
			error: "dataTypes is required",
		},
		{
			name: "measurements and dataTypes length mismatch",
			conf: map[string]interface{}{
				"model":        "tree",
				"device":       "root.sg.d1",
				"measurements": []interface{}{"a", "b"},
				"dataTypes":    []interface{}{"INT32"},
			},
			error: "measurements (2) and dataTypes (1) must have the same length",
		},
		{
			name: "invalid data type",
			conf: map[string]interface{}{
				"model":        "tree",
				"device":       "root.sg.d1",
				"measurements": []interface{}{"v"},
				"dataTypes":    []interface{}{"FOOBAR"},
			},
			error: `dataTypes[0]="FOOBAR" is not a supported IoTDB data type`,
		},
		{
			name: "table model columnCategories length mismatch",
			conf: map[string]interface{}{
				"model":            "table",
				"database":         "db1",
				"table":            "t1",
				"measurements":     []interface{}{"a", "b"},
				"dataTypes":        []interface{}{"INT32", "INT64"},
				"columnCategories": []interface{}{"TAG"},
			},
			error: "columnCategories (1) must have the same length as measurements (2)",
		},
		{
			name: "table model invalid column category",
			conf: map[string]interface{}{
				"model":            "table",
				"database":         "db1",
				"table":            "t1",
				"measurements":     []interface{}{"a"},
				"dataTypes":        []interface{}{"INT32"},
				"columnCategories": []interface{}{"BAD"},
			},
			error: `columnCategories[0]="BAD" is not a supported column category (TAG/FIELD/ATTRIBUTE)`,
		},
		{
			name: "table model database root. prefix stripped",
			conf: map[string]interface{}{
				"model":            "table",
				"database":         "root.db1",
				"table":            "t1",
				"measurements":     []interface{}{"d", "v"},
				"dataTypes":        []interface{}{"TEXT", "DOUBLE"},
				"columnCategories": []interface{}{"TAG", "FIELD"},
			},
			expected: iotdbConfig{
				Addr:             "127.0.0.1:6667",
				Username:         "root",
				Password:         "root",
				Model:            "table",
				Database:         "db1",
				Table:            "t1",
				Measurements:     []string{"d", "v"},
				DataTypes:        []string{"TEXT", "DOUBLE"},
				ColumnCategories: []string{"TAG", "FIELD"},
				BatchSize:        10,
				Timeout:          5000,
				PoolSize:         3,
			},
		},
		{
			name: "table model database empty after stripping root. prefix",
			conf: map[string]interface{}{
				"model":            "table",
				"database":         "root.",
				"table":            "t1",
				"measurements":     []interface{}{"v"},
				"dataTypes":        []interface{}{"INT32"},
				"columnCategories": []interface{}{"FIELD"},
			},
			error: "database name cannot be empty after stripping 'root.' prefix",
		},
		{
			name: "table model rejects unsafe database identifier",
			conf: map[string]interface{}{
				"model":            "table",
				"database":         "db1; DROP DATABASE db2",
				"table":            "t1",
				"measurements":     []interface{}{"v"},
				"dataTypes":        []interface{}{"INT32"},
				"columnCategories": []interface{}{"FIELD"},
			},
			error: `database "db1; DROP DATABASE db2" is not a valid identifier; expected [A-Za-z_][A-Za-z0-9_]*`,
		},
		{
			name: "unmarshal error",
			conf: map[string]interface{}{
				"addr": 12,
			},
			error: "error configuring iotdb sink: 1 error(s) decoding:\n\n* 'addr' expected type 'string', got unconvertible type 'int', value: '12'",
		},
	}

	ctx := mockContext.NewMockContext("testconfig", "op")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &iotdbSink{}
			err := s.Provision(ctx, test.conf)
			if test.error == "" {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, s.conf)
				assert.NotNil(t, s.writer)
			} else {
				assert.Error(t, err)
				assert.Equal(t, test.error, err.Error())
			}
		})
	}
}

func TestProvisionWriterSelection(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")

	t.Run("tree model selects treeWriter", func(t *testing.T) {
		s := &iotdbSink{}
		err := s.Provision(ctx, map[string]interface{}{
			"model":        "tree",
			"device":       "root.sg.d1",
			"measurements": []interface{}{"v"},
			"dataTypes":    []interface{}{"INT32"},
		})
		assert.NoError(t, err)
		_, ok := s.writer.(*treeWriter)
		assert.True(t, ok, "expected *treeWriter")
	})

	t.Run("table model selects tableWriter", func(t *testing.T) {
		s := &iotdbSink{}
		err := s.Provision(ctx, map[string]interface{}{
			"model":            "table",
			"database":         "db1",
			"table":            "t1",
			"measurements":     []interface{}{"v"},
			"dataTypes":        []interface{}{"INT64"},
			"columnCategories": []interface{}{"FIELD"},
		})
		assert.NoError(t, err)
		_, ok := s.writer.(*tableWriter)
		assert.True(t, ok, "expected *tableWriter")
	})

	t.Run("invalid configuration returns error", func(t *testing.T) {
		s := &iotdbSink{}
		err := s.Provision(ctx, map[string]interface{}{
			"model": "invalid",
		})
		assert.Error(t, err)
	})
}

func TestApplyDefaults(t *testing.T) {
	c := iotdbConfig{}
	c.applyDefaults()
	assert.Equal(t, "127.0.0.1:6667", c.Addr)
	assert.Equal(t, "root", c.Username)
	assert.Equal(t, "root", c.Password)
	assert.Equal(t, modelTree, c.Model)
	assert.Equal(t, 10, c.BatchSize)
	assert.Equal(t, int64(5000), c.Timeout)
	assert.Equal(t, 3, c.PoolSize)

	// existing values are preserved
	c2 := iotdbConfig{
		Addr:      "1.2.3.4:6667",
		Username:  "u",
		Password:  "p",
		Model:     modelTable,
		BatchSize: 50,
		Timeout:   1000,
		PoolSize:  10,
	}
	c2.applyDefaults()
	assert.Equal(t, "1.2.3.4:6667", c2.Addr)
	assert.Equal(t, "u", c2.Username)
	assert.Equal(t, "p", c2.Password)
	assert.Equal(t, modelTable, c2.Model)
	assert.Equal(t, 50, c2.BatchSize)
	assert.Equal(t, int64(1000), c2.Timeout)
	assert.Equal(t, 10, c2.PoolSize)
}

func TestSplitAddr(t *testing.T) {
	host, port, err := splitAddr("192.168.1.1:6667")
	assert.NoError(t, err)
	assert.Equal(t, "192.168.1.1", host)
	assert.Equal(t, "6667", port)

	host, port, err = splitAddr("[::1]:6667")
	assert.NoError(t, err)
	assert.Equal(t, "::1", host)
	assert.Equal(t, "6667", port)

	_, _, err = splitAddr("invalid")
	assert.Error(t, err)

	_, _, err = splitAddr(":6667")
	assert.Error(t, err)

	_, _, err = splitAddr("host:")
	assert.Error(t, err)
}

func TestNewPoolConfig(t *testing.T) {
	t.Run("single node parses addr", func(t *testing.T) {
		conf := &iotdbConfig{Addr: "[::1]:6667", Username: "u", Password: "p", Database: "db1"}
		got, err := conf.newPoolConfig(conf.Database)
		require.NoError(t, err)
		assert.Equal(t, "::1", got.Host)
		assert.Equal(t, "6667", got.Port)
		assert.Equal(t, "u", got.UserName)
		assert.Equal(t, "p", got.Password)
		assert.Equal(t, "db1", got.Database)

		bootstrap, err := conf.newPoolConfig("")
		require.NoError(t, err)
		assert.Empty(t, bootstrap.Database)
	})

	t.Run("nodeUrls overrides invalid addr", func(t *testing.T) {
		conf := &iotdbConfig{Addr: "not-an-address", NodeUrls: []string{"node1:6667", "node2:6667"}}
		got, err := conf.newPoolConfig("")
		require.NoError(t, err)
		assert.Empty(t, got.Host)
		assert.Empty(t, got.Port)
		assert.Equal(t, conf.NodeUrls, got.NodeUrls)
	})
}

func TestSinkConnect(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")

	t.Run("success", func(t *testing.T) {
		w := &fakeWriter{}
		s := &iotdbSink{writer: w}
		var status, message string
		err := s.Connect(ctx, func(gotStatus, gotMessage string) {
			status, message = gotStatus, gotMessage
		})
		require.NoError(t, err)
		assert.Equal(t, 1, w.connectCalls)
		assert.Equal(t, api.ConnectionConnected, status)
		assert.Empty(t, message)
	})

	t.Run("failure", func(t *testing.T) {
		connectErr := errors.New("connect failed")
		w := &fakeWriter{connectErr: connectErr}
		s := &iotdbSink{writer: w}
		var status, message string
		err := s.Connect(ctx, func(gotStatus, gotMessage string) {
			status, message = gotStatus, gotMessage
		})
		assert.ErrorIs(t, err, connectErr)
		assert.Equal(t, 1, w.connectCalls)
		assert.Equal(t, api.ConnectionDisconnected, status)
		assert.Equal(t, connectErr.Error(), message)
	})
}

func TestSinkCollectErrorClassification(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")

	t.Run("empty batch is a no-op", func(t *testing.T) {
		w := &fakeWriter{}
		s := &iotdbSink{writer: w}
		require.NoError(t, s.collect(ctx, []map[string]any{}))
		assert.Zero(t, w.writeCalls)
	})

	t.Run("success forwards rows", func(t *testing.T) {
		w := &fakeWriter{}
		s := &iotdbSink{writer: w}
		row := map[string]any{"value": 1}
		require.NoError(t, s.collect(ctx, row))
		assert.Equal(t, 1, w.writeCalls)
		assert.Equal(t, []map[string]any{row}, w.written)
	})

	t.Run("conversion error remains non-IO", func(t *testing.T) {
		conversionErr := errors.New("conversion failed")
		w := &fakeWriter{writeErr: conversionErr}
		s := &iotdbSink{writer: w}
		err := s.collect(ctx, map[string]any{"value": "bad"})
		assert.ErrorIs(t, err, conversionErr)
		assert.False(t, errorx.IsIOError(err))
	})

	t.Run("insert error remains IO", func(t *testing.T) {
		insertErr := errorx.NewIOErr("insert failed")
		w := &fakeWriter{writeErr: insertErr}
		s := &iotdbSink{writer: w}
		err := s.collect(ctx, map[string]any{"value": 1})
		assert.Equal(t, insertErr, err)
		assert.True(t, errorx.IsIOError(err))
	})
}

func TestToTSDataType(t *testing.T) {
	tests := []struct {
		in       string
		expected client.TSDataType
		hasErr   bool
	}{
		{"INT32", client.INT32, false},
		{"INT64", client.INT64, false},
		{"FLOAT", client.FLOAT, false},
		{"DOUBLE", client.DOUBLE, false},
		{"BOOLEAN", client.BOOLEAN, false},
		{"TEXT", client.TEXT, false},
		{"STRING", client.STRING, false},
		{"TIMESTAMP", client.TIMESTAMP, false},
		{"BAD", client.UNKNOWN, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := toTSDataType(tt.in)
			if tt.hasErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestToColumnCategory(t *testing.T) {
	tests := []struct {
		in       string
		expected client.ColumnCategory
		hasErr   bool
	}{
		{"TAG", client.TAG, false},
		{"FIELD", client.FIELD, false},
		{"ATTRIBUTE", client.ATTRIBUTE, false},
		{"BAD", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := toColumnCategory(tt.in)
			if tt.hasErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name     string
		in       any
		dt       string
		expected any
		hasErr   bool
	}{
		{"nil returns nil", nil, "INT32", nil, false},
		{"int to INT32", 10, "INT32", int32(10), false},
		{"int64 to INT64", int64(123), "INT64", int64(123), false},
		{"float to FLOAT", 1.5, "FLOAT", float32(1.5), false},
		{"float to DOUBLE", 2.5, "DOUBLE", float64(2.5), false},
		{"bool to BOOLEAN", true, "BOOLEAN", true, false},
		{"string to TEXT", "hello", "TEXT", "hello", false},
		{"string to STRING", "world", "STRING", "world", false},
		{"int to TIMESTAMP", 1700000000000, "TIMESTAMP", int64(1700000000000), false},
		{"unsupported type", 1, "UNKNOWN", nil, true},
		{"invalid bool source", "notabool", "BOOLEAN", nil, true},
		{"int32 overflow errors", int64(1) << 40, "INT32", nil, true},
		{"finite float overflow errors", math.MaxFloat64, "FLOAT", nil, true},
		{"positive infinity errors", math.Inf(1), "FLOAT", nil, true},
		{"negative infinity errors", math.Inf(-1), "FLOAT", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.in, tt.dt)
			if tt.hasErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestBuildMeasurementSchemas(t *testing.T) {
	schemas, err := buildMeasurementSchemas(
		[]string{"a", "b"},
		[]string{"INT32", "DOUBLE"},
	)
	assert.NoError(t, err)
	assert.Len(t, schemas, 2)
	assert.Equal(t, "a", schemas[0].Measurement)
	assert.Equal(t, client.INT32, schemas[0].DataType)
	assert.Equal(t, "b", schemas[1].Measurement)
	assert.Equal(t, client.DOUBLE, schemas[1].DataType)

	_, err = buildMeasurementSchemas(
		[]string{"a"},
		[]string{"BAD"},
	)
	assert.Error(t, err)
}

func TestEnsureDatabase(t *testing.T) {
	t.Run("executes create before using target database", func(t *testing.T) {
		session := &fakeTableSession{}
		pool := &fakeTableSessionPool{session: session}
		require.NoError(t, ensureDatabase(pool, "db1"))
		assert.Equal(t, []string{"CREATE DATABASE IF NOT EXISTS db1"}, session.statements)
		assert.Equal(t, 1, session.closeCalls)
	})

	t.Run("get session failure is returned", func(t *testing.T) {
		pool := &fakeTableSessionPool{getErr: errors.New("dial failed")}
		err := ensureDatabase(pool, "db1")
		assert.EqualError(t, err, "failed to get iotdb table session for database creation: dial failed")
	})

	t.Run("create failure is returned", func(t *testing.T) {
		session := &fakeTableSession{executeErr: errors.New("permission denied")}
		pool := &fakeTableSessionPool{session: session}
		err := ensureDatabase(pool, "db1")
		assert.EqualError(t, err, `create iotdb database "db1": permission denied`)
		assert.Equal(t, 1, session.closeCalls)
	})
}

func TestInsertRelationalTablet(t *testing.T) {
	tablet, err := client.NewRelationalTablet(
		"t1",
		[]*client.MeasurementSchema{{Measurement: "value", DataType: client.INT32}},
		[]client.ColumnCategory{client.FIELD},
		1,
	)
	require.NoError(t, err)

	t.Run("success closes session", func(t *testing.T) {
		session := &fakeTableSession{}
		pool := &fakeTableSessionPool{session: session}
		require.NoError(t, insertRelationalTablet(pool, tablet))
		assert.Same(t, tablet, session.inserted)
		assert.Equal(t, 1, session.closeCalls)
	})

	t.Run("get session failure is IO", func(t *testing.T) {
		pool := &fakeTableSessionPool{getErr: errors.New("pool exhausted")}
		err := insertRelationalTablet(pool, tablet)
		assert.True(t, errorx.IsIOError(err))
		assert.Contains(t, err.Error(), "failed to get iotdb table session")
	})

	t.Run("insert failure is IO and closes session", func(t *testing.T) {
		session := &fakeTableSession{insertErr: errors.New("connection reset")}
		pool := &fakeTableSessionPool{session: session}
		err := insertRelationalTablet(pool, tablet)
		assert.True(t, errorx.IsIOError(err))
		assert.Contains(t, err.Error(), "insert relational tablet")
		assert.Equal(t, 1, session.closeCalls)
	})
}

func TestTableWriterSortsTablet(t *testing.T) {
	session := &fakeTableSession{}
	pool := &fakeTableSessionPool{session: session}
	w := &tableWriter{
		pool: pool,
		conf: &iotdbConfig{
			Database:         "db1",
			Table:            "t1",
			Measurements:     []string{"value"},
			DataTypes:        []string{"INT32"},
			ColumnCategories: []string{"FIELD"},
			TsFieldName:      "ts",
			BatchSize:        10,
		},
	}
	rows := []map[string]any{
		{"ts": 30, "value": 3},
		{"ts": 10, "value": 1},
		{"ts": 20, "value": 2},
	}

	require.NoError(t, w.write(mockContext.NewMockContext("rule", "op"), rows))
	require.NotNil(t, session.inserted)
	assert.Equal(t, []int64{10, 20, 30}, decodeTimestamps(session.inserted.GetTimestampBytes()))
	for row, expected := range []int32{1, 2, 3} {
		value, err := session.inserted.GetValueAt(0, row)
		require.NoError(t, err)
		assert.Equal(t, expected, value)
	}
}

func TestExtractTimestamp(t *testing.T) {
	// when tsFieldName is empty, returns current time (>0)
	ts, err := extractTimestamp(map[string]any{}, "")
	assert.NoError(t, err)
	assert.Greater(t, ts, int64(0))

	// when tsFieldName is set and value exists, returns the value
	ts, err = extractTimestamp(map[string]any{"ts": 12345}, "ts")
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), ts)

	// when tsFieldName is set but missing, returns an error
	ts, err = extractTimestamp(map[string]any{}, "ts")
	assert.Error(t, err)
	assert.Zero(t, ts)

	// when ts value is invalid, returns error
	_, err = extractTimestamp(map[string]any{"ts": "not-a-number"}, "ts")
	assert.Error(t, err)
}

func TestToMapList(t *testing.T) {
	// single map
	got, err := toMapList(map[string]any{"a": 1})
	assert.NoError(t, err)
	assert.Equal(t, []map[string]any{{"a": 1}}, got)

	// list of maps
	in := []map[string]any{{"a": 1}, {"b": 2}}
	got, err = toMapList(in)
	assert.NoError(t, err)
	assert.Equal(t, in, got)

	// unsupported type
	_, err = toMapList([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestSinkClose(t *testing.T) {
	// close on a sink without writer should not panic
	s := &iotdbSink{}
	ctx := mockContext.NewMockContext("rule", "op")
	err := s.Close(ctx)
	assert.NoError(t, err)
}

func TestGetSink(t *testing.T) {
	sink := GetSink()
	assert.NotNil(t, sink)
	_, ok := sink.(*iotdbSink)
	assert.True(t, ok)
}

func TestWriterCloseWithoutConnect(t *testing.T) {
	// close() before a successful connect() must not panic even though the
	// underlying session pool was never initialized.
	assert.NotPanics(t, func() {
		tw := &treeWriter{}
		assert.NoError(t, tw.close())
	})
	assert.NotPanics(t, func() {
		tbw := &tableWriter{}
		assert.NoError(t, tbw.close())
	})
}

func TestNextTimestamp(t *testing.T) {
	// explicit timestamps are used verbatim and do not advance the auto cursor
	var last int64
	ts, err := nextTimestamp(map[string]any{"ts": 100}, "ts", &last)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), ts)
	assert.Equal(t, int64(0), last)

	// auto timestamps strictly increase within a batch even within the same millisecond
	last = 0
	t1, err := nextTimestamp(map[string]any{}, "", &last)
	assert.NoError(t, err)
	t2, err := nextTimestamp(map[string]any{}, "", &last)
	assert.NoError(t, err)
	t3, err := nextTimestamp(map[string]any{}, "", &last)
	assert.NoError(t, err)
	assert.Greater(t, t2, t1)
	assert.Greater(t, t3, t2)
}

func TestFillTablet(t *testing.T) {
	conf := &iotdbConfig{
		Measurements: []string{"temperature", "count", "label"},
		DataTypes:    []string{"FLOAT", "INT32", "TEXT"},
		TsFieldName:  "ts",
	}
	schemas, err := buildMeasurementSchemas(conf.Measurements, conf.DataTypes)
	require.NoError(t, err)

	rows := []map[string]any{
		{"ts": 1002, "temperature": "1.5", "count": "2", "label": "first"},
		{"ts": 1000, "count": 3, "label": "second"},
		{"ts": 1001, "temperature": 4.5},
	}
	tablet, err := client.NewTablet("root.sg.d1", schemas, len(rows))
	require.NoError(t, err)

	lastAuto := int64(0)
	require.NoError(t, fillTablet(tablet, rows, conf, &lastAuto))

	assert.Equal(t, len(rows), tablet.RowSize)
	assert.Equal(t, []int64{1002, 1000, 1001}, decodeTimestamps(tablet.GetTimestampBytes()))
	assert.Equal(t, int64(0), lastAuto)

	expected := [][]any{
		{float32(1.5), int32(2), "first"},
		{nil, int32(3), "second"},
		{float32(4.5), nil, nil},
	}
	for rowIdx := range expected {
		for colIdx := range expected[rowIdx] {
			got, err := tablet.GetValueAt(colIdx, rowIdx)
			require.NoError(t, err)
			assert.Equal(t, expected[rowIdx][colIdx], got)
		}
	}
}

func TestFillTabletRejectsMissingTimestamp(t *testing.T) {
	conf := &iotdbConfig{
		Measurements: []string{"value"},
		DataTypes:    []string{"INT32"},
		TsFieldName:  "ts",
	}
	schemas, err := buildMeasurementSchemas(conf.Measurements, conf.DataTypes)
	require.NoError(t, err)
	tablet, err := client.NewTablet("root.sg.d1", schemas, 1)
	require.NoError(t, err)

	err = fillTablet(tablet, []map[string]any{{"value": 1}}, conf, new(int64))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `timestamp field "ts" is missing`)
	assert.Zero(t, tablet.RowSize)
}

func TestFillTabletAutoTimestampsIncrease(t *testing.T) {
	conf := &iotdbConfig{
		Measurements: []string{"value"},
		DataTypes:    []string{"INT32"},
	}
	schemas, err := buildMeasurementSchemas(conf.Measurements, conf.DataTypes)
	require.NoError(t, err)
	tablet, err := client.NewTablet("root.sg.d1", schemas, 3)
	require.NoError(t, err)

	var lastAuto int64
	require.NoError(t, fillTablet(tablet, []map[string]any{
		{"value": 1}, {"value": 2}, {"value": 3},
	}, conf, &lastAuto))
	timestamps := decodeTimestamps(tablet.GetTimestampBytes())
	assert.Len(t, timestamps, 3)
	assert.Less(t, timestamps[0], timestamps[1])
	assert.Less(t, timestamps[1], timestamps[2])
	assert.Equal(t, timestamps[2], lastAuto)
}

func decodeTimestamps(data []byte) []int64 {
	timestamps := make([]int64, len(data)/8)
	for i := range timestamps {
		timestamps[i] = int64(binary.BigEndian.Uint64(data[i*8 : (i+1)*8]))
	}
	return timestamps
}
