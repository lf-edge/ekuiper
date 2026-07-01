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
	"testing"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/stretchr/testify/assert"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

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
				"model":        "tree",
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
			name:  "model missing",
			conf:  map[string]interface{}{},
			error: `model must be either "tree" or "table"`,
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
	assert.Equal(t, 10, c.BatchSize)
	assert.Equal(t, int64(5000), c.Timeout)
	assert.Equal(t, 3, c.PoolSize)

	// existing values are preserved
	c2 := iotdbConfig{
		Addr:      "1.2.3.4:6667",
		Username:  "u",
		Password:  "p",
		BatchSize: 50,
		Timeout:   1000,
		PoolSize:  10,
	}
	c2.applyDefaults()
	assert.Equal(t, "1.2.3.4:6667", c2.Addr)
	assert.Equal(t, "u", c2.Username)
	assert.Equal(t, "p", c2.Password)
	assert.Equal(t, 50, c2.BatchSize)
	assert.Equal(t, int64(1000), c2.Timeout)
	assert.Equal(t, 10, c2.PoolSize)
}

func TestSplitAddr(t *testing.T) {
	host, port, err := splitAddr("192.168.1.1:6667")
	assert.NoError(t, err)
	assert.Equal(t, "192.168.1.1", host)
	assert.Equal(t, "6667", port)

	_, _, err = splitAddr("invalid")
	assert.Error(t, err)

	_, _, err = splitAddr(":6667")
	assert.Error(t, err)

	_, _, err = splitAddr("host:")
	assert.Error(t, err)
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

func TestExtractTimestamp(t *testing.T) {
	// when tsFieldName is empty, returns current time (>0)
	ts, err := extractTimestamp(map[string]any{}, "")
	assert.NoError(t, err)
	assert.Greater(t, ts, int64(0))

	// when tsFieldName is set and value exists, returns the value
	ts, err = extractTimestamp(map[string]any{"ts": 12345}, "ts")
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), ts)

	// when tsFieldName is set but missing, falls back to current time
	ts, err = extractTimestamp(map[string]any{}, "ts")
	assert.NoError(t, err)
	assert.Greater(t, ts, int64(0))

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
